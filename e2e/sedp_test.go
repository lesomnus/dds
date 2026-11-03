//go:build e2e

package e2e

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestEndpointsAgreeWithTheROS2CLI: what we learn over SEDP of /chatter --
// who publishes it, who subscribes, the type and the QoS -- is what
// `ros2 topic info -v` says.
func TestEndpointsAgreeWithTheROS2CLI(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	h.Run(t, d, Talker)
	h.Run(t, d, Listener)
	spy := h.Participant(t, d)

	lines := spy.WaitFor(20*time.Second, "chatter's writer and reader", func(ls []SpyLine) bool {
		return len(FindTopic(ls, "endpoint_discovered", "rt/chatter", "writer")) > 0 &&
			len(FindTopic(ls, "endpoint_discovered", "rt/chatter", "reader")) > 0
	})
	w := FindTopic(lines, "endpoint_discovered", "rt/chatter", "writer")[0]
	r := FindTopic(lines, "endpoint_discovered", "rt/chatter", "reader")[0]
	if w.Type != "std_msgs::msg::dds_::String_" || r.Type != w.Type {
		t.Errorf("types %q, %q", w.Type, r.Type)
	}

	info := h.CLI(t, d, "topic", "info", "-v", "--no-daemon", "/chatter")
	if !strings.Contains(info, "Type: std_msgs/msg/String") || !strings.Contains(info, "Publisher count: 1") ||
		!strings.Contains(info, "Subscription count: 1") {
		t.Fatalf("ros2 topic info -v /chatter:\n%s", info)
	}
	// The publisher's section, then the subscription's.
	sections := strings.SplitN(info, "Subscription count", 2)
	check := func(who string, section string, l SpyLine) {
		t.Helper()
		rel := map[string]string{"reliable": "RELIABLE", "best_effort": "BEST_EFFORT"}[l.Reliability]
		dur := map[string]string{"volatile": "VOLATILE", "transient_local": "TRANSIENT_LOCAL"}[l.Durability]
		hist := regexp.MustCompile(`History \(Depth\): (\w+) \((\d+)\)`).FindStringSubmatch(section)
		if !strings.Contains(section, "Reliability: "+rel) || !strings.Contains(section, "Durability: "+dur) {
			t.Errorf("%s: we saw %s %s; the CLI:\n%s", who, l.Reliability, l.Durability, section)
		}
		if hist == nil || hist[2] != strconv.Itoa(int(l.Depth)) {
			t.Errorf("%s: we saw depth %d; the CLI says %v", who, l.Depth, hist)
		}
	}
	check("publisher", sections[0], w)
	check("subscription", sections[1], r)
}

// TestEndpointsGoWithTheirParticipant: stopping the listener takes its
// chatter reader away.
func TestEndpointsGoWithTheirParticipant(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	h.Run(t, d, Talker)
	listener := h.Run(t, d, Listener)
	spy := h.Participant(t, d)
	spy.WaitFor(20*time.Second, "chatter's reader", func(ls []SpyLine) bool {
		return len(FindTopic(ls, "endpoint_discovered", "rt/chatter", "reader")) > 0
	})

	listener.Stop()
	lines := spy.WaitFor(10*time.Second, "chatter's reader lost", func(ls []SpyLine) bool {
		return len(FindTopic(ls, "endpoint_lost", "rt/chatter", "reader")) > 0
	})
	if l := FindTopic(lines, "endpoint_lost", "rt/chatter", "reader")[0]; l.Reason != "disposed" && l.Reason != "participant_lost" {
		t.Errorf("reason %q", l.Reason)
	}
	if len(FindTopic(lines, "endpoint_lost", "rt/chatter", "writer")) > 0 {
		t.Error("the talker's writer went too")
	}
}

// TestNothingLeavesTheHost: taking part on one machine sends nothing to
// another -- not a datagram -- while that machine's participants are in
// plain view of our multicast socket.
func TestNothingLeavesTheHost(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	here, there := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	here.Run(t, d, Talker)
	there.Run(t, d, Talker)
	there.Run(t, d, Listener)

	hereAddr := here.Addr(t)
	stop := there.Capture(t, "udp and src host "+hereAddr)
	spy := here.Participant(t, d)
	spy.WaitFor(20*time.Second, "the local talker's node", func(ls []SpyLine) bool {
		_, ok := Nodes(ls)["/talker"]
		return ok
	})
	time.Sleep(8 * time.Second) // several announce intervals
	spy.Stop(here)
	seen := stop()

	port := regexp.MustCompile(`participant \w+ at [\d.]+:(\d+)`).FindStringSubmatch(spy.Stderr())
	if port == nil {
		t.Fatalf("no address in ddsspy's log:\n%s", spy.Stderr())
	}
	ours := 0
	for _, l := range seen {
		if strings.Contains(l, hereAddr+"."+port[1]+" ") {
			ours++
			t.Errorf("reached the other machine: %s", l)
		}
	}
	// The capture works: the local talker's SPDP multicast does reach it.
	if len(seen) == 0 {
		t.Error("the other machine saw nothing at all from here; is the capture working?")
	}
	for _, l := range regexp.MustCompile(`sent \d+ datagrams to ([\d.]+):\d+`).FindAllStringSubmatch(spy.Stderr(), -1) {
		if l[1] != hereAddr {
			t.Errorf("sent to %s", l[1])
		}
	}
	for _, l := range spy.Lines() {
		if l.Event == "discovered" && !strings.HasPrefix(l.From, hereAddr+":") {
			t.Errorf("took part with a participant of another machine: %+v", l)
		}
	}
	t.Logf("the other machine saw %d datagrams from here, %d of them ours", len(seen), ours)
}
