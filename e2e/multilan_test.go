//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestTwoLANsTalkToBoth: a host on two LANs publishes, in network scope on
// both interfaces, to a ROS listener on each -- neither of which is on the
// other's LAN.
func TestTwoLANsTalkToBoth(t *testing.T) {
	t.Parallel()
	lan1, lan2 := NewLAN(t), NewLAN(t)
	m := lan1.NewHost(t)
	lan2.Attach(t, m)
	x, y := lan1.NewHost(t), lan2.NewHost(t)
	d := randomDomain()
	lx, ly := x.Run(t, d, Listener), y.Run(t, d, Listener)
	pub := m.Cat(t, d, "pub", "-scope", "network", "-iface", lan1.subnet+","+lan2.subnet,
		"-topic", chatterTopic, "-type", stringType, "-string", "both {n}", "-rate", "100ms", "-count", "10", "-wait", "2")
	pub.WaitFor(30*time.Second, "matched", 1)
	for _, l := range []*Process{lx, ly} {
		hs := waitHeard(t, l, 20*time.Second, 10)
		for i, s := range hs[:10] {
			if want := fmt.Sprintf("both %d", i+1); s != want {
				t.Fatalf("heard %q as message %d, want %q", s, i+1, want)
			}
		}
	}
}

// TestTwoLANsOnePath: a participant reachable on both LANs is sent to on
// one of them -- the first of ours -- not down both.
func TestTwoLANsOnePath(t *testing.T) {
	t.Parallel()
	lan1, lan2 := NewLAN(t), NewLAN(t)
	m, z := lan1.NewHost(t), lan1.NewHost(t)
	lan2.Attach(t, m)
	lan2.Attach(t, z)
	d := randomDomain()
	both := lan1.subnet + "," + lan2.subnet
	sub := z.Cat(t, d, "sub", "-scope", "network", "-iface", both, "-topic", "rt/path", "-type", stringType)
	time.Sleep(time.Second)
	// z took index 0, so its port; what is sent to it there is unicast.
	stop := z.Capture(t, fmt.Sprintf("udp and dst port %d", 7400+250*d+10))
	m.Cat(t, d, "pub", "-scope", "network", "-iface", both, "-topic", "rt/path", "-type", stringType, "-rate", "100ms", "-count", "20", "-wait", "1")
	ss := sub.WaitFor(30*time.Second, "sample", 20)
	if len(ss) != 20 {
		t.Errorf("%d samples", len(ss))
	}
	seen := stop()
	var eth0, other int
	for _, l := range seen {
		switch {
		case strings.Contains(l, " eth0 "):
			eth0++
		case strings.TrimSpace(l) != "":
			other++
		}
	}
	if eth0 == 0 || other != 0 {
		t.Errorf("%d datagrams on eth0 (LAN 1), %d elsewhere: want all on eth0\n%s", eth0, other, strings.Join(seen, "\n"))
	}
}

// TestTwoLANsHostScopeStaysHome: in the default scope, a host on two LANs
// sends neither of them a datagram.
func TestTwoLANsHostScopeStaysHome(t *testing.T) {
	t.Parallel()
	lan1, lan2 := NewLAN(t), NewLAN(t)
	m := lan1.NewHost(t)
	lan2.Attach(t, m)
	x, y := lan1.NewHost(t), lan2.NewHost(t)
	d := randomDomain()
	x.Run(t, d, Listener)
	y.Run(t, d, Listener)
	stopX := x.Capture(t, "udp and src host "+m.AddrOn(t, lan1))
	stopY := y.Capture(t, "udp and src host "+m.AddrOn(t, lan2))
	time.Sleep(2 * time.Second)
	pub := m.Cat(t, d, "pub", "-iface", lan1.subnet+","+lan2.subnet, "-topic", chatterTopic, "-type", stringType, "-rate", "100ms")
	pub.WaitFor(10*time.Second, "written", 30)
	for name, stop := range map[string]func() []string{"LAN 1": stopX, "LAN 2": stopY} {
		if seen := stop(); len(seen) != 0 {
			t.Errorf("%s got %d datagrams:\n%s", name, len(seen), strings.Join(seen, "\n"))
		}
	}
}
