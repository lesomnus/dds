//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

// What std_msgs/msg/String is to DDS. The library knows none of it: these
// are the names and the payload a user of it hands it.
const (
	chatterTopic = "rt/chatter"
	stringType   = "std_msgs::msg::dds_::String_"
)

// CatLine is one line ddscat prints.
type CatLine struct {
	Event   string `json:"event"`
	SN      int64  `json:"sn"`
	Writer  string `json:"writer"`
	Size    int    `json:"size"`
	String  string `json:"string"`
	Readers int    `json:"readers"`
	Error   string `json:"error"`
}

// Cat is ddscat running on a host.
type Cat struct {
	t      testing.TB
	mu     sync.Mutex
	lines  []CatLine
	stderr lockedBuffer
	done   chan struct{}
}

// Cat runs ddscat pub or sub on the host.
func (h *Host) Cat(t testing.TB, domain uint32, mode string, args ...string) *Cat {
	t.Helper()
	argv := append([]string{"exec", h.name, "/ddscat", mode, "-domain", fmt.Sprint(domain), "-iface", h.lan.subnet}, args...)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "docker", argv...)
	c := &Cat{t: t, done: make(chan struct{})}
	cmd.Stderr = &c.stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(c.done)
		sc := bufio.NewScanner(out)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var l CatLine
			if json.Unmarshal(sc.Bytes(), &l) == nil {
				c.mu.Lock()
				c.lines = append(c.lines, l)
				c.mu.Unlock()
			}
		}
		_ = cmd.Wait()
	}()
	t.Cleanup(func() {
		cancel()
		<-c.done
		if t.Failed() {
			t.Logf("ddscat %s on %s:\n%s", mode, h.name, c.stderr.String())
		}
	})
	return c
}

func (c *Cat) Lines() []CatLine {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CatLine(nil), c.lines...)
}

// Events is the lines of one event.
func (c *Cat) Events(event string) []CatLine {
	var out []CatLine
	for _, l := range c.Lines() {
		if l.Event == event {
			out = append(out, l)
		}
	}
	return out
}

// WaitFor polls until there are n lines of an event.
func (c *Cat) WaitFor(d time.Duration, event string, n int) []CatLine {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for {
		if ls := c.Events(event); len(ls) >= n {
			return ls
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out after %s waiting for %d %q; got %d\n%s", d, n, event, len(c.Events(event)), c.stderr.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Done reports whether ddscat has exited.
func (c *Cat) Done() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// heard is what a demo listener's log says it heard, in order.
func heard(log string) []string {
	var out []string
	for _, l := range strings.Split(log, "\n") {
		if _, s, ok := strings.Cut(l, "I heard: ["); ok {
			out = append(out, strings.TrimSuffix(strings.TrimSpace(s), "]"))
		}
	}
	return out
}

// waitHeard polls a listener's log until it has heard n messages.
func waitHeard(t *testing.T, p *Process, d time.Duration, n int) []string {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		hs := heard(p.Logs())
		if len(hs) >= n {
			return hs
		}
		if time.Now().After(deadline) {
			t.Fatalf("the listener heard %d of %d:\n%s", len(hs), n, p.Logs())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestROSListenerHearsUs: what we publish on rt/chatter, a ROS 2 listener
// hears, in order.
func TestROSListenerHearsUs(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	listener := h.Run(t, d, Listener)
	pub := h.Cat(t, d, "pub", "-topic", chatterTopic, "-type", stringType, "-string", "hello from dds {n}", "-rate", "100ms", "-count", "20", "-wait", "1")
	pub.WaitFor(30*time.Second, "matched", 1)
	hs := waitHeard(t, listener, 20*time.Second, 20)
	for i, s := range hs[:20] {
		if want := fmt.Sprintf("hello from dds %d", i+1); s != want {
			t.Fatalf("heard %q as message %d, want %q: %v", s, i+1, want, hs)
		}
	}
}

// TestROSListenerHearsALargeMessage: a sample larger than a datagram goes
// in fragments, and the listener hears it whole.
func TestROSListenerHearsALargeMessage(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	listener := h.Run(t, d, Listener)
	pub := h.Cat(t, d, "pub", "-topic", chatterTopic, "-type", stringType, "-size", "200000", "-rate", "300ms", "-count", "3", "-wait", "1")
	pub.WaitFor(30*time.Second, "matched", 1)
	hs := waitHeard(t, listener, 20*time.Second, 3)
	for _, s := range hs {
		if len(s) != 200000 || strings.Trim(s, "x") != "" {
			t.Errorf("heard %d octets", len(s))
		}
	}
}

// TestROSListenerHearsUsOverALossyPath: on a loopback that loses a third of
// the packets, the reliable listener still hears everything, in order --
// fragments included.
func TestROSListenerHearsUsOverALossyPath(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	listener := h.Run(t, d, Listener) // reliable, keep_last 10
	pub := h.Cat(t, d, "pub", "-topic", chatterTopic, "-type", stringType, "-string", "lossy {n}", "-rate", "200ms", "-count", "30", "-depth", "50", "-wait", "1", "-linger", "20s")
	pub.WaitFor(30*time.Second, "matched", 1)
	h.Degrade(t, "loss 30%")
	hs := waitHeard(t, listener, 60*time.Second, 30)
	for i, s := range hs[:30] {
		if want := fmt.Sprintf("lossy %d", i+1); s != want {
			t.Fatalf("heard %q as message %d, want %q: %v", s, i+1, want, hs)
		}
	}
}

// TestROSListenerHearsLargeMessagesOverALossyPath: fragments lost are
// asked for again (NACK_FRAG) and sent again, alone.
func TestROSListenerHearsLargeMessagesOverALossyPath(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	listener := h.Run(t, d, Listener)
	pub := h.Cat(t, d, "pub", "-topic", chatterTopic, "-type", stringType, "-size", "150000", "-rate", "500ms", "-count", "5", "-wait", "1", "-linger", "30s")
	pub.WaitFor(30*time.Second, "matched", 1)
	h.Degrade(t, "loss 20%")
	hs := waitHeard(t, listener, 60*time.Second, 5)
	for _, s := range hs {
		if len(s) != 150000 {
			t.Errorf("heard %d octets", len(s))
		}
	}
}

// TestWeHearTheROSTalker: what a ROS 2 talker publishes, we read.
func TestWeHearTheROSTalker(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	sub := h.Cat(t, d, "sub", "-topic", chatterTopic, "-type", stringType)
	h.Run(t, d, Talker)
	ss := sub.WaitFor(30*time.Second, "sample", 3)
	for _, s := range ss[:3] {
		if !strings.HasPrefix(s.String, "Hello World: ") {
			t.Errorf("read %+v", s)
		}
	}
	if ss[1].SN != ss[0].SN+1 || ss[2].SN != ss[1].SN+1 {
		t.Errorf("not in order: %+v", ss[:3])
	}
}

// A subscriber with the QoS given, printing what it gets.
const qosSubscriber = `
import rclpy, sys
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy
from std_msgs.msg import String
rclpy.init()
n = rclpy.create_node("qos_sub")
q = QoSProfile(depth=10, history=HistoryPolicy.KEEP_LAST,
    reliability=ReliabilityPolicy.%s, durability=DurabilityPolicy.%s)
n.create_subscription(String, "chatter", lambda m: print("got", m.data, flush=True), q)
print("up", flush=True)
rclpy.spin(n)
`

func got(log string) []string {
	var out []string
	for _, l := range strings.Split(log, "\n") {
		if s, ok := strings.CutPrefix(strings.TrimSpace(l), "got "); ok {
			out = append(out, s)
		}
	}
	return out
}

func waitGot(t *testing.T, p *Process, d time.Duration, n int) []string {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		g := got(p.Logs())
		if len(g) >= n {
			return g
		}
		if time.Now().After(deadline) {
			t.Fatalf("got %d of %d:\n%s", len(g), n, p.Logs())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestTransientLocalAndVolatile: a transient-local writer's history reaches
// a transient-local subscriber that comes late; a volatile subscriber that
// comes as late gets none of it.
func TestTransientLocalAndVolatile(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	// Eight samples, then nothing more: what the subscribers get is history.
	pub := h.Cat(t, d, "pub", "-topic", chatterTopic, "-type", stringType, "-durability", "transient_local", "-depth", "5",
		"-string", "kept {n}", "-rate", "100ms", "-count", "8", "-linger", "120s")
	pub.WaitFor(10*time.Second, "written", 8)
	tl := h.Python(t, d, fmt.Sprintf(qosSubscriber, "RELIABLE", "TRANSIENT_LOCAL"))
	vol := h.Python(t, d, fmt.Sprintf(qosSubscriber, "RELIABLE", "VOLATILE"))
	g := waitGot(t, tl, 30*time.Second, 5)
	if fmt.Sprint(g) != "[kept 4 kept 5 kept 6 kept 7 kept 8]" {
		t.Errorf("transient local got %v, want the history of five", g)
	}
	time.Sleep(3 * time.Second)
	if !strings.Contains(vol.Logs(), "up") {
		t.Fatalf("the volatile subscriber did not start:\n%s", vol.Logs())
	}
	if g := got(vol.Logs()); len(g) != 0 {
		t.Errorf("volatile got %v, written before it came", g)
	}
}

// TestBestEffortSubscriber: a reliable writer serves a best-effort
// subscriber.
func TestBestEffortSubscriber(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	be := h.Python(t, d, fmt.Sprintf(qosSubscriber, "BEST_EFFORT", "VOLATILE"))
	h.Cat(t, d, "pub", "-topic", chatterTopic, "-type", stringType, "-string", "be {n}", "-rate", "200ms")
	waitGot(t, be, 30*time.Second, 5)
}

// TestROS2CLISeesOurPublisher: ros2 topic info counts our writer, with its
// QoS. It is not a node: no ros_discovery_info is published.
func TestROS2CLISeesOurPublisher(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	h := lan.NewHost(t)
	d := randomDomain()
	pub := h.Cat(t, d, "pub", "-topic", "rt/dds_cli_check", "-type", stringType, "-rate", "500ms")
	pub.WaitFor(10*time.Second, "written", 2)
	var out string
	for range 5 {
		out = h.CLI(t, d, "topic", "info", "-v", "--no-daemon", "/dds_cli_check")
		if strings.Contains(out, "Publisher count: 1") {
			break
		}
		time.Sleep(time.Second)
	}
	if !strings.Contains(out, "Publisher count: 1") || !strings.Contains(out, "std_msgs/msg/String") || !strings.Contains(out, "RELIABLE") {
		t.Errorf("ros2 topic info:\n%s", out)
	}
}
