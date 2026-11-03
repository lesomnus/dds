//go:build e2e

package e2e

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// cliNodes is what `ros2 node list` says, without the CLI's own node.
func cliNodes(t *testing.T, h *Host, d uint32) []string {
	t.Helper()
	var out []string
	for _, l := range strings.Split(h.CLI(t, d, "node", "list", "--no-daemon"), "\n") {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "/") && !strings.HasPrefix(l, "/_ros2cli") {
			out = append(out, l)
		}
	}
	sort.Strings(out)
	return out
}

func names(m map[string]SpyLine) []string {
	var out []string
	for n := range m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// TestNodesAgreeWithROS2NodeList: the nodes we read from ros_discovery_info
// are `ros2 node list`'s -- several to a process included -- and we are not
// among them.
func TestNodesAgreeWithROS2NodeList(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	h.Run(t, d, Talker)
	h.Run(t, d, ManualComposition) // talker, listener, Server, Client
	spy := h.Participant(t, d)

	// Five nodes, four names: /talker twice.
	spy.WaitFor(20*time.Second, "four node names", func(ls []SpyLine) bool { return len(Nodes(ls)) >= 4 })
	time.Sleep(2 * time.Second) // let anything else arrive
	ours := names(Nodes(spy.Lines()))
	theirs := cliNodes(t, h, d)
	// Two talkers: `ros2 node list` prints the name twice.
	uniq := map[string]SpyLine{}
	for _, n := range theirs {
		uniq[n] = SpyLine{}
	}
	if strings.Join(ours, " ") != strings.Join(names(uniq), " ") {
		t.Errorf("we see %v, ros2 node list %v", ours, theirs)
	}
	composition := Nodes(spy.Lines())["/Server"].GUIDPrefix
	if Nodes(spy.Lines())["/Client"].GUIDPrefix != composition {
		t.Error("/Server and /Client are in one process, so one participant")
	}
}

// A process that makes nodes and destroys some, living on.
const nodeChurn = `
import rclpy, time
rclpy.init()
a = rclpy.create_node("stays")
b = rclpy.create_node("goes")
print("up", flush=True)
time.sleep(6)
b.destroy_node()
print("destroyed", flush=True)
time.sleep(600)
`

// TestANodeDestroyedInALiveProcessDisappears: a node goes while its process
// (its participant) stays.
func TestANodeDestroyedInALiveProcessDisappears(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Participant(t, d)
	h.Python(t, d, nodeChurn)

	spy.WaitFor(20*time.Second, "/stays and /goes", func(ls []SpyLine) bool {
		n := Nodes(ls)
		_, a := n["/stays"]
		_, b := n["/goes"]
		return a && b
	})
	lines := spy.WaitFor(20*time.Second, "/goes to go", func(ls []SpyLine) bool {
		for _, l := range ls {
			if l.Event == "node_disappeared" && l.Node == "/goes" {
				return true
			}
		}
		return false
	})
	for _, l := range lines {
		if l.Event == "node_disappeared" && l.Node == "/goes" && l.Reason != "removed" {
			t.Errorf("reason %q, want removed: the process lives on", l.Reason)
		}
		if l.Event == "node_disappeared" && l.Node == "/stays" {
			t.Error("/stays went too")
		}
		if l.Event == "lost" {
			t.Errorf("a participant was lost: %+v", l)
		}
	}
}

// TestAProcessWithManyNodes: thirty nodes in one process make its
// ros_discovery_info sample larger than a fragment (Cyclone's 1344 octets),
// so it arrives in DATA_FRAGs.
func TestAProcessWithManyNodes(t *testing.T) {
	t.Parallel()
	const n = 30
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Participant(t, d)
	h.Python(t, d, fmt.Sprintf(`
import rclpy, time
rclpy.init()
nodes = [rclpy.create_node("many_%%02d" %% i) for i in range(%d)]
time.sleep(600)
`, n))
	lines := spy.WaitFor(40*time.Second, fmt.Sprintf("%d nodes", n), func(ls []SpyLine) bool { return len(Nodes(ls)) >= n })
	for i := range n {
		name := fmt.Sprintf("/many_%02d", i)
		if l, ok := Nodes(lines)[name]; !ok || l.Writers == 0 {
			t.Errorf("%s: %+v", name, l)
		}
	}
}

// TestNodesGoWithTheirProcess: stopping a process takes its nodes away.
func TestNodesGoWithTheirProcess(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	spy := h.Participant(t, d)
	p := h.Run(t, d, ManualComposition)
	spy.WaitFor(20*time.Second, "four nodes", func(ls []SpyLine) bool { return len(Nodes(ls)) >= 4 })
	p.Stop()
	spy.WaitFor(15*time.Second, "no nodes", func(ls []SpyLine) bool { return len(Nodes(ls)) == 0 })
}

// TestAProcessWithManyNodesAlreadyRunning: the same, with the process up
// before we are. Its ros_discovery_info sample is then history the writer
// retransmits, and Cyclone retransmits a fragmented sample's first fragment
// only, leaving the rest to NACK_FRAG.
func TestAProcessWithManyNodesAlreadyRunning(t *testing.T) {
	t.Parallel()
	const n = 30
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	h.Python(t, d, fmt.Sprintf(`
import rclpy, time
rclpy.init()
nodes = [rclpy.create_node("many_%%02d" %% i) for i in range(%d)]
print("up", flush=True)
time.sleep(600)
`, n))
	time.Sleep(5 * time.Second) // up, and its sample sent to no one
	spy := h.Participant(t, d)
	lines := spy.WaitFor(40*time.Second, fmt.Sprintf("%d nodes", n), func(ls []SpyLine) bool { return len(Nodes(ls)) >= n })
	_ = lines
}
