//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"
)

// Cyclone DDS's default lease is 10 s; a killed participant is lost once it
// has gone unheard that long, within a second's check.
const leaseLimit = 15 * time.Second

func TestParticipantsAreDiscoveredWithTheirProcess(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Spy(t, d, true)

	h.Run(t, d, Talker)
	h.Run(t, d, ManualComposition)
	lines := spy.WaitFor(20*time.Second, "talker and manual_composition", func(ls []SpyLine) bool {
		_, a := Find(ls, "discovered", "talker")
		_, b := Find(ls, "discovered", "manual_composition")
		return a && b
	})
	for _, name := range []string{"talker", "manual_composition"} {
		l, _ := Find(lines, "discovered", name)
		if l.Vendor != "cyclonedds" || l.PID == 0 || l.Hostname == "" || l.Lease != "10s" {
			t.Errorf("%s: %+v", name, l)
		}
		if !strings.Contains(l.Cyclone, "/0.10.") {
			t.Errorf("%s: Cyclone %q, want 0.10.x", name, l.Cyclone)
		}
	}
}

func TestAGracefulStopIsDisposed(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Spy(t, d, true)

	p := h.Run(t, d, Talker)
	spy.WaitFor(20*time.Second, "talker", func(ls []SpyLine) bool { _, ok := Find(ls, "discovered", "talker"); return ok })
	stopped := time.Now()
	p.Stop()
	lines := spy.WaitFor(10*time.Second, "talker lost", func(ls []SpyLine) bool { _, ok := Find(ls, "lost", "talker"); return ok })
	l, _ := Find(lines, "lost", "talker")
	if l.Reason != "disposed" {
		t.Errorf("reason %q, want disposed", l.Reason)
	}
	if took := time.Since(stopped); took > 5*time.Second {
		t.Errorf("lost %s after the stop; a dispose is immediate", took)
	}
}

func TestAKillIsALeaseExpiry(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Spy(t, d, true)

	p := h.Run(t, d, ManualComposition)
	spy.WaitFor(20*time.Second, "manual_composition", func(ls []SpyLine) bool {
		_, ok := Find(ls, "discovered", "manual_composition")
		return ok
	})
	killed := time.Now()
	p.Kill()
	lines := spy.WaitFor(leaseLimit+5*time.Second, "manual_composition lost", func(ls []SpyLine) bool {
		_, ok := Find(ls, "lost", "manual_composition")
		return ok
	})
	l, _ := Find(lines, "lost", "manual_composition")
	if l.Reason != "lease_expired" {
		t.Errorf("reason %q, want lease_expired", l.Reason)
	}
	if took := time.Since(killed); took < 5*time.Second {
		t.Errorf("lost %s after the kill; nothing says so before the lease runs out", took)
	}
}

// TestOnlyLocalParticipantsAreKept: another machine's participants reach the
// host -- multicast crosses the LAN -- but -local keeps them out.
func TestOnlyLocalParticipantsAreKept(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	here, there := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	localSpy := here.Spy(t, d, true)
	allSpy := here.Spy(t, d, false)

	here.Run(t, d, Listener)
	there.Run(t, d, Talker)
	allSpy.WaitFor(20*time.Second, "both, without -local", func(ls []SpyLine) bool {
		_, a := Find(ls, "discovered", "listener")
		_, b := Find(ls, "discovered", "talker")
		return a && b
	})
	lines := localSpy.WaitFor(5*time.Second, "the listener, with -local", func(ls []SpyLine) bool {
		_, ok := Find(ls, "discovered", "listener")
		return ok
	})
	if l, ok := Find(lines, "discovered", "talker"); ok {
		t.Errorf("the other machine's talker was kept: %+v", l)
	}
}
