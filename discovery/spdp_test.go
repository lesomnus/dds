package discovery_test

import (
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/dds/discovery"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "rtps", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var (
	local  = netip.MustParseAddrPort("172.22.0.2:43383")
	remote = netip.MustParseAddrPort("172.22.0.9:43383")
)

type recorder struct {
	mu     sync.Mutex
	events []discovery.Event
}

func (r *recorder) add(e discovery.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) take() []discovery.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.events
	r.events = nil
	return out
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }
func newClock() *clock               { return &clock{t: time.Unix(1_800_000_000, 0)} }
func observer(cfg discovery.SPDPConfig, r *recorder) *discovery.SPDPObserver {
	return discovery.NewSPDPObserver(cfg, r.add)
}

func TestAParticipantIsDiscoveredOnceAndRenewed(t *testing.T) {
	c, r := newClock(), &recorder{}
	o := observer(discovery.SPDPConfig{Domain: 77, Now: c.now}, r)

	o.Handle(local, fixture(t, "spdp-talker.rtps"))
	ev := r.take()
	if len(ev) != 1 {
		t.Fatalf("%d events, want Discovered", len(ev))
	}
	d, ok := ev[0].(discovery.ParticipantDiscovered)
	if !ok {
		t.Fatalf("%T, want ParticipantDiscovered", ev[0])
	}
	p := d.Participant
	if p.ProcessName() != "talker" || p.PID() != 7 || p.Hostname() == "" {
		t.Errorf("host %q pid %d process %q", p.Hostname(), p.PID(), p.ProcessName())
	}
	if p.From != local {
		t.Errorf("from %s, want %s", p.From, local)
	}

	// The same announcement again only renews the lease.
	c.add(5 * time.Second)
	o.Handle(local, fixture(t, "spdp-talker.rtps"))
	c.add(8 * time.Second) // 13 s after the first, 8 s after the last
	o.CheckLeases()
	if ev := r.take(); len(ev) != 0 {
		t.Errorf("events %v; a renewal is not a change and the lease is not up", ev)
	}
	if got := o.Participants(); len(got) != 1 || !got[0].FirstSeen.Equal(time.Unix(1_800_000_000, 0)) {
		t.Errorf("participants %v", got)
	}
}

func TestADisposeIsLostAsDisposed(t *testing.T) {
	c, r := newClock(), &recorder{}
	o := observer(discovery.SPDPConfig{Domain: 77, Now: c.now}, r)
	o.Handle(local, fixture(t, "spdp-talker.rtps"))
	r.take()

	o.Handle(local, fixture(t, "spdp-dispose-talker.rtps"))
	ev := r.take()
	if len(ev) != 1 {
		t.Fatalf("%d events, want Lost", len(ev))
	}
	l, ok := ev[0].(discovery.ParticipantLost)
	if !ok || l.Reason != discovery.Disposed {
		t.Fatalf("%#v, want Lost(disposed)", ev[0])
	}
	if l.Participant.ProcessName() != "talker" {
		t.Errorf("lost %q, want talker", l.Participant.ProcessName())
	}
	if len(o.Participants()) != 0 {
		t.Error("still known")
	}
}

func TestSilenceIsLostAsLeaseExpired(t *testing.T) {
	c, r := newClock(), &recorder{}
	o := observer(discovery.SPDPConfig{Domain: 77, Now: c.now}, r)
	o.Handle(local, fixture(t, "spdp-composition.rtps"))
	r.take()

	c.add(10 * time.Second) // exactly the lease: not yet
	o.CheckLeases()
	if ev := r.take(); len(ev) != 0 {
		t.Fatalf("lost at the lease's end: %v", ev)
	}
	c.add(time.Millisecond)
	o.CheckLeases()
	ev := r.take()
	if len(ev) != 1 {
		t.Fatalf("%d events, want Lost", len(ev))
	}
	if l := ev[0].(discovery.ParticipantLost); l.Reason != discovery.LeaseExpired || l.Participant.ProcessName() != "manual_composition" {
		t.Errorf("%v %q", l.Reason, l.Participant.ProcessName())
	}
}

func TestADisposeOfAnUnknownParticipantIsIgnored(t *testing.T) {
	r := &recorder{}
	o := observer(discovery.SPDPConfig{Domain: 77}, r)
	o.Handle(local, fixture(t, "spdp-dispose-talker.rtps"))
	if ev := r.take(); len(ev) != 0 {
		t.Errorf("events %v", ev)
	}
}

func TestTheFilterKeepsOnlyLocalParticipants(t *testing.T) {
	r := &recorder{}
	o := observer(discovery.SPDPConfig{
		Domain: 77,
		Filter: discovery.LocalOnly(map[netip.Addr]bool{local.Addr(): true}),
	}, r)
	o.Handle(remote, fixture(t, "spdp-composition.rtps"))
	o.Handle(local, fixture(t, "spdp-talker.rtps"))
	ev := r.take()
	if len(ev) != 1 || ev[0].(discovery.ParticipantDiscovered).Participant.ProcessName() != "talker" {
		t.Errorf("events %v, want the local talker only", ev)
	}
}

func TestAnotherDomainIsIgnored(t *testing.T) {
	r := &recorder{}
	o := observer(discovery.SPDPConfig{Domain: 78}, r)
	o.Handle(local, fixture(t, "spdp-talker.rtps"))
	if ev := r.take(); len(ev) != 0 {
		t.Errorf("events %v; the talker is in domain 77", ev)
	}
}

func TestGarbageIsIgnored(t *testing.T) {
	r := &recorder{}
	o := observer(discovery.SPDPConfig{Domain: 77}, r)
	for _, b := range [][]byte{nil, []byte("RTPS"), []byte("not rtps at all, not even close")} {
		o.Handle(local, b)
	}
	b := fixture(t, "spdp-talker.rtps")
	o.Handle(local, b[:len(b)/2])
	if ev := r.take(); len(ev) != 0 {
		t.Errorf("events %v", ev)
	}
}
