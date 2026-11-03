package participant

import (
	"fmt"
	"net/netip"
	"testing"

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
		// On this host: an address of this host's, on the DDS network first.
		{ScopeHost, "192.168.10.2", "[192.168.10.2:7412]"},
		{ScopeNetwork, "192.168.10.2", "[192.168.10.2:7412]"},
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

func TestReachablePrefersTheFirstNetwork(t *testing.T) {
	p := scoped(ScopeNetwork)
	p.nets = []netip.Prefix{netip.MustParsePrefix("10.1.0.0/16"), netip.MustParsePrefix("192.168.10.0/24")}
	loc := func(addr string) rtps.Locator {
		return rtps.Locator{Kind: rtps.LocatorUDPv4, Addr: netip.MustParseAddr(addr), Port: 7410}
	}
	// On both of our networks: the first of ours wins, whatever its order.
	got := p.reachable(netip.MustParseAddr("192.168.10.33"), []rtps.Locator{loc("192.168.10.33"), loc("10.1.2.3")})
	if fmt.Sprint(got) != "[10.1.2.3:7410]" {
		t.Errorf("%v", got)
	}
}
