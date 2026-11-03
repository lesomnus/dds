//go:build e2e

package e2e

import (
	"testing"
	"time"
)

// TestTheGraphSettlesThroughALossyHost: with the host's loopback losing a
// fifth of its packets, duplicating, reordering and delaying them, every
// node and chatter's endpoints still arrive -- reliably, in the end.
func TestTheGraphSettlesThroughALossyHost(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	h.Degrade(t, "delay 5ms 5ms loss 20% duplicate 5% reorder 25%")
	d := randomDomain()
	h.Run(t, d, Talker)
	h.Run(t, d, Listener)
	h.Run(t, d, ManualComposition)
	spy := h.Participant(t, d)

	want := []string{"/Client", "/Server", "/listener", "/talker"}
	lines := spy.WaitFor(60*time.Second, "every node and chatter's endpoints", func(ls []SpyLine) bool {
		n := Nodes(ls)
		for _, w := range want {
			if _, ok := n[w]; !ok {
				return false
			}
		}
		// talker and listener, twice each: two of each endpoint.
		return len(FindTopic(ls, "endpoint_discovered", "rt/chatter", "writer")) >= 2 &&
			len(FindTopic(ls, "endpoint_discovered", "rt/chatter", "reader")) >= 2
	})
	// Nothing is reported twice for being received twice.
	seen := map[string]bool{}
	for _, l := range lines {
		if l.Event != "endpoint_discovered" {
			continue
		}
		if seen[l.GUID] {
			t.Errorf("%s discovered twice", l.GUID)
		}
		seen[l.GUID] = true
	}
}

// TestARestartedProcessIsANewParticipant: a process killed and started again
// is lost, then discovered anew with its nodes, under another GUID.
func TestARestartedProcessIsANewParticipant(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Participant(t, d)
	first := h.Run(t, d, Talker)
	lines := spy.WaitFor(20*time.Second, "/talker", func(ls []SpyLine) bool { _, ok := Nodes(ls)["/talker"]; return ok })
	before := Nodes(lines)["/talker"].GUIDPrefix

	first.Kill()
	h.Run(t, d, Talker)
	lines = spy.WaitFor(30*time.Second, "the old talker lost and a new one's node", func(ls []SpyLine) bool {
		lost := false
		for _, l := range ls {
			if l.Event == "lost" && l.GUIDPrefix == before {
				lost = true
			}
		}
		n, ok := Nodes(ls)["/talker"]
		return lost && ok && n.GUIDPrefix != before
	})
	_ = lines
}

// TestMemory: the observer's resident memory, with a busy host -- forty-odd
// nodes and their few hundred endpoints -- stays within a few MiB of an idle
// Go program.
func TestMemory(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Participant(t, d)
	time.Sleep(2 * time.Second)
	idle, _ := h.RSS(t, "ddsspy")
	h.Run(t, d, Talker)
	h.Run(t, d, Listener)
	h.Run(t, d, ManualComposition)
	h.Python(t, d, `
import rclpy, time
rclpy.init()
nodes = [rclpy.create_node("many_%02d" % i) for i in range(36)]
time.sleep(600)
`)
	lines := spy.WaitFor(60*time.Second, "forty nodes", func(ls []SpyLine) bool { return len(Nodes(ls)) >= 40 })
	endpoints := 0
	for _, l := range lines {
		if l.Event == "endpoint_discovered" {
			endpoints++
		}
	}
	time.Sleep(3 * time.Second)
	rss, peak := h.RSS(t, "ddsspy")
	t.Logf("idle: RSS %d KiB; %d nodes, %d endpoints: RSS %d KiB, peak %d KiB", idle, len(Nodes(lines)), endpoints, rss, peak)
	if grown := peak - idle; grown > 16*1024 {
		t.Errorf("grew %d KiB for %d endpoints", grown, endpoints)
	}
}
