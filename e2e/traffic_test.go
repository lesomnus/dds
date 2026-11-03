//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"
)

const chatter = "rt/chatter"

// trafficOf is the writer_traffic lines of a topic.
func trafficOf(lines []SpyLine, topic string) []SpyLine {
	var out []SpyLine
	for _, l := range lines {
		if l.Event == "writer_traffic" && l.Topic == topic {
			out = append(out, l)
		}
	}
	return out
}

// TestTalkerRateFromTraffic: the demo talker publishes once a second; the
// traffic says so, and how big its samples are.
func TestTalkerRateFromTraffic(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Traffic(t, d, 5*time.Second)
	h.Run(t, d, Talker)
	h.Run(t, d, Listener) // with no reader the talker sends nothing

	ls := spy.WaitFor(40*time.Second, "three reports of /chatter", func(ls []SpyLine) bool { return len(trafficOf(ls, chatter)) >= 3 })
	rs := trafficOf(ls, chatter)
	for _, r := range rs[1:] { // the first may start mid-way
		if r.Samples < 4 || r.Samples > 6 || r.Rate < 0.8 || r.Rate > 1.2 {
			t.Errorf("%d samples in %s (%.2f/s), want about 5 (1/s)", r.Samples, r.Interval, r.Rate)
		}
		// "Hello World: 12" as std_msgs/String: encapsulation, length, text.
		if per := r.Bytes / max(r.Samples, 1); per < 20 || per > 40 {
			t.Errorf("%d bytes for %d samples", r.Bytes, r.Samples)
		}
	}
	if strings.Contains(spy.Stderr(), "capture:") {
		t.Errorf("capture failed:\n%s", spy.Stderr())
	}
}

// TestTrafficOfOtherHostsNotReported: a talker on another host, read on
// this one, is that host's to report -- not this one's. What goes out over
// the network is seen too, not only the loopback.
func TestTrafficOfOtherHostsNotReported(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	a, b := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	spyA := a.Traffic(t, d, 3*time.Second)
	spyB := b.Traffic(t, d, 3*time.Second)
	b.Run(t, d, Talker)
	a.Run(t, d, Listener)

	ls := spyB.WaitFor(40*time.Second, "B to report its talker", func(ls []SpyLine) bool { return len(trafficOf(ls, chatter)) >= 2 })
	talker := trafficOf(ls, chatter)[0].GUIDPrefix
	time.Sleep(4 * time.Second)
	// A reports its listener's own writers (rt/rosout, ...), not B's.
	reported := 0
	for _, l := range spyA.Lines() {
		if l.Event != "writer_traffic" {
			continue
		}
		reported++
		if l.GUIDPrefix == talker || l.Topic == chatter {
			t.Errorf("A reports B's writer: %+v", l)
		}
	}
	if reported == 0 {
		t.Error("A reports none of its listener's writers")
	}
}

// TestRetransmitRequestsFromTraffic: on a lossy path, the listener asks the
// talker again for samples, and the traffic says how many.
func TestRetransmitRequestsFromTraffic(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Traffic(t, d, 5*time.Second)
	h.Run(t, d, Talker)
	h.Run(t, d, Listener)
	spy.WaitFor(30*time.Second, "a report of /chatter", func(ls []SpyLine) bool { return len(trafficOf(ls, chatter)) >= 1 })
	h.Degrade(t, "loss 40%")

	spy.WaitFor(60*time.Second, "requests to send again", func(ls []SpyLine) bool {
		for _, r := range trafficOf(ls, chatter) {
			if r.RetransmitRequests > 0 {
				return true
			}
		}
		return false
	})
}
