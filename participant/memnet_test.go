package participant

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/transport"
	"github.com/lesomnus/dds/transport/memtest"
)

// node is a participant on a network in memory, running until the test
// ends, with the events it reported.
type node struct {
	*Participant
	t      *testing.T
	tr     *memtest.Transport
	cancel context.CancelFunc
	done   chan struct{}

	mu     sync.Mutex
	events []discovery.Event
}

func startNode(t *testing.T, n *memtest.Network, host string, scope Scope, lease time.Duration) *node {
	t.Helper()
	tr := n.Open(host, true)
	nd := &node{t: t, tr: tr, done: make(chan struct{})}
	p, err := New(Config{
		Domain: 1, Scope: scope, Transports: []transport.Transport{tr},
		LeaseDuration: lease, AnnounceInterval: 100 * time.Millisecond,
	}, func(e discovery.Event) {
		nd.mu.Lock()
		nd.events = append(nd.events, e)
		nd.mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	nd.Participant = p
	ctx, cancel := context.WithCancel(context.Background())
	nd.cancel = cancel
	go func() {
		defer close(nd.done)
		_ = p.Run(ctx)
	}()
	t.Cleanup(nd.stop)
	return nd
}

func (nd *node) stop() {
	nd.cancel()
	<-nd.done
}

func (nd *node) lost(prefix rtps.GUIDPrefix) (discovery.ParticipantLost, bool) {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	for _, e := range nd.events {
		if l, ok := e.(discovery.ParticipantLost); ok && l.Participant.GUID.Prefix == prefix {
			return l, true
		}
	}
	return discovery.ParticipantLost{}, false
}

func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// inbox is a subscription's samples.
type inbox struct {
	mu  sync.Mutex
	got []int
}

func (b *inbox) handle(_ rtps.GUID, s Sample) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.got = append(b.got, int(s.Payload[4]))
}

func (b *inbox) samples() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int(nil), b.got...)
}

func payloadOf(i int) []byte { return []byte{0, 1, 0, 0, byte(i)} }

func subscribe(t *testing.T, nd *node, durability rtps.DurabilityKind) *inbox {
	t.Helper()
	b := &inbox{}
	err := nd.Subscribe(Subscription{
		Topic: "t", Type: "T",
		QoS:    ReaderQoS{Reliability: rtps.Reliable, Durability: durability, History: rtps.KeepLast, Depth: 100},
		Handle: b.handle,
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func publish(t *testing.T, nd *node, q WriterQoS) *Writer {
	t.Helper()
	w, err := nd.Publish(Publication{Topic: "t", Type: "T", QoS: q})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func inOrder(got []int, n int) bool {
	if len(got) != n {
		return false
	}
	for i, v := range got {
		if v != i+1 {
			return false
		}
	}
	return true
}

func TestMemPubSub(t *testing.T) {
	n := memtest.NewNetwork("10.0.0.0/24")
	a := startNode(t, n, "10.0.0.1", ScopeNetwork, 10*time.Second)
	b := startNode(t, n, "10.0.0.2", ScopeNetwork, 10*time.Second)
	in := subscribe(t, b, rtps.Volatile)
	w := publish(t, a, WriterQoS{Depth: 100})
	eventually(t, 5*time.Second, "the reader to be ready", func() bool { return len(w.Ready()) == 1 })
	for i := 1; i <= 10; i++ {
		if _, err := w.Write(payloadOf(i)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 5*time.Second, "ten samples in order", func() bool { return inOrder(in.samples(), 10) })
}

func TestMemReliableUnderLossAndDuplication(t *testing.T) {
	n := memtest.NewNetwork("10.0.0.0/24")
	var mu sync.Mutex
	rng := rand.New(rand.NewPCG(5, 6))
	n.SetLoss(func(_, _ netip.AddrPort, _ []byte) bool { mu.Lock(); defer mu.Unlock(); return rng.Float64() < 0.3 })
	n.SetDuplication(func(_, _ netip.AddrPort, _ []byte) bool { mu.Lock(); defer mu.Unlock(); return rng.Float64() < 0.1 })
	a := startNode(t, n, "10.0.0.1", ScopeNetwork, 10*time.Second)
	b := startNode(t, n, "10.0.0.2", ScopeNetwork, 10*time.Second)
	in := subscribe(t, b, rtps.Volatile)
	w := publish(t, a, WriterQoS{Depth: 100})
	eventually(t, 10*time.Second, "the reader to be ready", func() bool { return len(w.Ready()) == 1 })
	for i := 1; i <= 50; i++ {
		if _, err := w.Write(payloadOf(i)); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 20*time.Second, "fifty samples in order", func() bool { return inOrder(in.samples(), 50) })
}

func TestMemTransientLocalLateReader(t *testing.T) {
	n := memtest.NewNetwork("10.0.0.0/24")
	a := startNode(t, n, "10.0.0.1", ScopeNetwork, 10*time.Second)
	w := publish(t, a, WriterQoS{Durability: rtps.TransientLocal, Depth: 3})
	for i := 1; i <= 5; i++ {
		_, _ = w.Write(payloadOf(i))
	}
	b := startNode(t, n, "10.0.0.2", ScopeNetwork, 10*time.Second)
	tl := subscribe(t, b, rtps.TransientLocal)
	vol := subscribe(t, b, rtps.Volatile)
	eventually(t, 5*time.Second, "the history of three", func() bool { return fmt.Sprint(tl.samples()) == "[3 4 5]" })
	eventually(t, 5*time.Second, "both readers ready", func() bool { return len(w.Ready()) == 2 })
	_, _ = w.Write(payloadOf(6))
	eventually(t, 5*time.Second, "the new sample", func() bool { return fmt.Sprint(vol.samples()) == "[6]" && len(tl.samples()) == 4 })
}

func TestMemGoodbyeAndLeaseExpiry(t *testing.T) {
	n := memtest.NewNetwork("10.0.0.0/24")
	obs := startNode(t, n, "10.0.0.1", ScopeNetwork, 10*time.Second)
	polite := startNode(t, n, "10.0.0.2", ScopeNetwork, 10*time.Second)
	crash := startNode(t, n, "10.0.0.3", ScopeNetwork, time.Second)
	eventually(t, 5*time.Second, "both discovered", func() bool { return len(obs.Participants()) == 2 })

	polite.stop()
	eventually(t, 5*time.Second, "the goodbye", func() bool { _, ok := obs.lost(polite.GUIDPrefix()); return ok })
	if l, _ := obs.lost(polite.GUIDPrefix()); l.Reason != discovery.Disposed {
		t.Errorf("polite: %s", l.Reason)
	}

	// The crash: nothing more comes from it, not even a goodbye.
	from := crash.tr.Addr()
	n.SetLoss(func(f, _ netip.AddrPort, _ []byte) bool { return f == from })
	eventually(t, 5*time.Second, "the lease to expire", func() bool { _, ok := obs.lost(crash.GUIDPrefix()); return ok })
	if l, _ := obs.lost(crash.GUIDPrefix()); l.Reason != discovery.LeaseExpired {
		t.Errorf("crash: %s", l.Reason)
	}
}

// TestMemHostScopeStaysHome: a participant in host scope talks with one on
// its host and sends nothing -- not a message, not to the group -- off it.
func TestMemHostScopeStaysHome(t *testing.T) {
	n := memtest.NewNetwork("10.0.0.0/24")
	home := startNode(t, n, "10.0.0.1", ScopeHost, 10*time.Second)
	neighbour := startNode(t, n, "10.0.0.1", ScopeNetwork, 10*time.Second) // same host
	away := startNode(t, n, "10.0.0.2", ScopeNetwork, 10*time.Second)
	in := subscribe(t, home, rtps.Volatile)
	inAway := subscribe(t, away, rtps.Volatile)
	w := publish(t, neighbour, WriterQoS{Depth: 10})
	eventually(t, 5*time.Second, "both readers ready", func() bool { return len(w.Ready()) == 2 })
	_, _ = w.Write(payloadOf(1))
	eventually(t, 5*time.Second, "both readers to read", func() bool { return len(in.samples()) == 1 && len(inAway.samples()) == 1 })
	time.Sleep(300 * time.Millisecond)
	for _, s := range n.Sent() {
		if s.From == home.tr.Addr() && s.To.Addr() != home.tr.Addr().Addr() {
			t.Fatalf("host scope sent to %s", s.To)
		}
	}
	for _, p := range home.Participants() {
		if p.GUID.Prefix == away.GUIDPrefix() {
			t.Error("host scope talks with a participant on another host")
		}
	}
}
