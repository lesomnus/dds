package participant

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/transport"
)

func scoped(scope Scope) *Participant {
	return &Participant{
		cfg: Config{Scope: scope, Domain: 138, Peers: []netip.Addr{netip.MustParseAddr("10.9.0.7")}},
		local: map[netip.Addr]bool{
			netip.MustParseAddr("127.0.0.1"):    true,
			netip.MustParseAddr("192.168.10.2"): true,
			netip.MustParseAddr("172.17.0.1"):   true, // docker0: every host has it
		},
		nets:      []netip.Prefix{netip.MustParsePrefix("192.168.10.0/24")},
		peerAddrs: map[netip.Addr]bool{netip.MustParseAddr("10.9.0.7"): true},
	}
}

func TestAllowed(t *testing.T) {
	for _, c := range []struct {
		addr          string
		host, network bool
	}{
		{"127.0.0.1", true, true},
		{"192.168.10.2", true, true},   // ours
		{"192.168.10.33", false, true}, // the interface's network
		{"239.255.0.1", false, true},   // the SPDP group
		{"239.255.0.2", false, false},  // another group
		{"10.9.0.7", false, true},      // a peer
		{"10.9.0.8", false, false},
		{"8.8.8.8", false, false},
	} {
		a := netip.MustParseAddr(c.addr)
		if got := scoped(ScopeHost).allowed(a); got != c.host {
			t.Errorf("host scope, %s: %v", a, got)
		}
		if got := scoped(ScopeNetwork).allowed(a); got != c.network {
			t.Errorf("network scope, %s: %v", a, got)
		}
	}
}

func TestReachable(t *testing.T) {
	loc := func(addr string, port uint32) rtps.Locator {
		return rtps.Locator{Kind: rtps.LocatorUDPv4, Addr: netip.MustParseAddr(addr), Port: port}
	}
	ls := []rtps.Locator{loc("172.17.0.1", 7410), loc("192.168.10.33", 7410), loc("192.168.10.2", 7412), loc("0.0.0.0", 7414), loc("192.168.10.40", 0)}
	for _, c := range []struct {
		scope Scope
		from  string
		want  string
	}{
		// On this host: its addresses that are this host's.
		{ScopeHost, "192.168.10.2", "[172.17.0.1:7410 192.168.10.2:7412]"},
		{ScopeNetwork, "192.168.10.2", "[172.17.0.1:7410 192.168.10.2:7412]"},
		// Elsewhere: on the network, not at an address this host has too.
		{ScopeHost, "192.168.10.33", "[]"},
		{ScopeNetwork, "192.168.10.33", "[192.168.10.33:7410]"},
	} {
		if got := fmt.Sprint(scoped(c.scope).reachable(netip.MustParseAddr(c.from), ls)); got != c.want {
			t.Errorf("%s scope, from %s: %s, want %s", c.scope, c.from, got, c.want)
		}
	}
}

func TestSPDPTargets(t *testing.T) {
	if got := scoped(ScopeHost).spdpTargets(); len(got) != 0 {
		t.Errorf("host scope announces to %v", got)
	}
	p := scoped(ScopeNetwork)
	got := p.spdpTargets()
	want := "[239.255.0.1:41900 10.9.0.7:41900 10.9.0.7:41910 10.9.0.7:41912 10.9.0.7:41914 10.9.0.7:41916 10.9.0.7:41918]"
	if fmt.Sprint(got) != want {
		t.Errorf("%v", got)
	}
	p.cfg.NoMulticast = true
	if got := p.spdpTargets(); got[0].Addr() == transport.DefaultMulticastGroup {
		t.Errorf("multicast with NoMulticast: %v", got)
	}
}

func TestWellKnownPorts(t *testing.T) {
	lo := netip.MustParseAddr("127.0.0.1")
	var ps []*Participant
	for i := range 3 {
		p := &Participant{cfg: Config{Scope: ScopeNetwork, Domain: 77, Addr: lo}}
		c, err := p.listen()
		if err != nil {
			t.Skipf("binding the domain's ports: %v", err)
		}
		defer c.Close()
		ps = append(ps, p)
		if want := transport.MetatrafficUnicastPort(77, uint32(p.index)); c.LocalAddr().(*net.UDPAddr).Port != int(want) {
			t.Errorf("index %d on port %d, want %d", p.index, c.LocalAddr().(*net.UDPAddr).Port, want)
		}
		if i > 0 && p.index <= ps[i-1].index {
			t.Errorf("indexes %d then %d", ps[i-1].index, p.index)
		}
	}
	p := &Participant{cfg: Config{Domain: 77, Addr: lo}} // host scope: ephemeral
	c, err := p.listen()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if p.index != -1 {
		t.Errorf("host scope took index %d", p.index)
	}
}

func TestPacer(t *testing.T) {
	now := time.Unix(0, 0)
	var mu sync.Mutex
	var sent []time.Time
	q := newPacer(1000, 500, func(netip.AddrPort, []byte) {
		mu.Lock()
		sent = append(sent, now)
		mu.Unlock()
	})
	q.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	q.sleep = func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	to := netip.MustParseAddrPort("127.0.0.1:1")
	for range 4 {
		q.enqueue(to, make([]byte, 500))
	}
	ctx, cancel := contextWithTimeout(time.Second)
	defer cancel()
	go q.run(ctx)
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		n := len(sent)
		mu.Unlock()
		if n == 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 4 {
		t.Fatalf("%d sent", len(sent))
	}
	// 2000 bytes at 1000 a second with a burst of 500: the first at once,
	// then one every half second.
	if d := sent[3].Sub(sent[0]); d < 1400*time.Millisecond || d > 1600*time.Millisecond {
		t.Errorf("four sent over %s", d)
	}
	for range 5000 {
		q.enqueue(to, []byte{1})
	}
	if q.dropped == 0 {
		t.Error("a full queue dropped nothing")
	}
}

func contextWithTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
