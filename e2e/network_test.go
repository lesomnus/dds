//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestNetworkWePublishToAnotherHost: with -scope network, a ROS 2 listener
// on another host hears what we publish, in order.
func TestNetworkWePublishToAnotherHost(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	a, b := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	listener := b.Run(t, d, Listener)
	pub := a.Cat(t, d, "pub", "-scope", "network", "-topic", chatterTopic, "-type", stringType, "-string", "over the network {n}", "-rate", "100ms", "-count", "20", "-wait", "1")
	pub.WaitFor(30*time.Second, "matched", 1)
	hs := waitHeard(t, listener, 20*time.Second, 20)
	for i, s := range hs[:20] {
		if want := fmt.Sprintf("over the network %d", i+1); s != want {
			t.Fatalf("heard %q as message %d, want %q: %v", s, i+1, want, hs)
		}
	}
}

// TestNetworkWeSubscribeToAnotherHost: with -scope network, we read a ROS 2
// talker on another host.
func TestNetworkWeSubscribeToAnotherHost(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	a, b := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	sub := a.Cat(t, d, "sub", "-scope", "network", "-topic", chatterTopic, "-type", stringType)
	b.Run(t, d, Talker)
	ss := sub.WaitFor(30*time.Second, "sample", 3)
	for _, s := range ss[:3] {
		if !strings.HasPrefix(s.String, "Hello World: ") {
			t.Errorf("read %+v", s)
		}
	}
}

// TestNetworkLargeMessagesOverALossyLink: on a link that loses packets,
// large samples -- in fragments of Cyclone's size, IP-fragmented datagrams of
// ten -- still arrive whole.
func TestNetworkLargeMessagesOverALossyLink(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	a, b := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	listener := b.Run(t, d, Listener)
	pub := a.Cat(t, d, "pub", "-scope", "network", "-topic", chatterTopic, "-type", stringType, "-size", "100000", "-rate", "500ms", "-count", "5", "-wait", "1", "-linger", "60s")
	pub.WaitFor(30*time.Second, "matched", 1)
	a.DegradeNetwork(t, "loss 10%")
	hs := waitHeard(t, listener, 90*time.Second, 5)
	for _, s := range hs {
		if len(s) != 100000 {
			t.Errorf("heard %d octets", len(s))
		}
	}
}

// TestNetworkPeersWithoutMulticast: with no multicast on either side, a
// participant finds another through the peer it is given -- and not without.
func TestNetworkPeersWithoutMulticast(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	a, b := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	sub := b.Cat(t, d, "sub", "-scope", "network", "-no-multicast", "-topic", "rt/peers", "-type", stringType)
	time.Sleep(time.Second)
	// No peer: neither announces to the other.
	lonely := a.Cat(t, d, "pub", "-scope", "network", "-no-multicast", "-topic", "rt/peers", "-type", stringType, "-string", "lonely {n}", "-rate", "200ms")
	time.Sleep(4 * time.Second)
	if n := len(sub.Events("sample")); n != 0 {
		t.Fatalf("%d samples with no peer and no multicast", n)
	}
	_ = lonely
	a.Cat(t, d, "pub", "-scope", "network", "-no-multicast", "-peer", b.Addr(t), "-topic", "rt/peers", "-type", stringType, "-string", "peer {n}", "-rate", "200ms")
	ss := sub.WaitFor(30*time.Second, "sample", 3)
	for _, s := range ss {
		if !strings.HasPrefix(s.String, "peer ") {
			t.Errorf("read %q: only the one given the peer is heard", s.String)
		}
	}
}

// TestNetworkWellKnownPort: in network scope the participant takes the
// first free participant index, and announces from its port.
func TestNetworkWellKnownPort(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	a, b := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	spy := b.Spy(t, d, false)
	a.Cat(t, d, "pub", "-scope", "network", "-topic", "rt/port", "-type", stringType, "-rate", "500ms")
	ls := spy.WaitFor(20*time.Second, "ddscat's announcement", func(ls []SpyLine) bool {
		_, ok := Find(ls, "discovered", "ddscat")
		return ok
	})
	l, _ := Find(ls, "discovered", "ddscat")
	want := fmt.Sprintf("%s:%d", a.Addr(t), 7400+250*d+10) // index 0
	if l.From != want {
		t.Errorf("announced from %s, want %s", l.From, want)
	}
}

// TestHostScopeStaysOnTheHost: by default nothing leaves the host -- not a
// datagram, whoever is on the network.
func TestHostScopeStaysOnTheHost(t *testing.T) {
	t.Parallel()
	lan := NewLAN(t)
	a, b := lan.NewHost(t), lan.NewHost(t)
	d := randomDomain()
	listener := b.Run(t, d, Listener)
	stop := b.Capture(t, "udp and src host "+a.Addr(t))
	time.Sleep(2 * time.Second) // B's listener announces itself; A hears it
	pub := a.Cat(t, d, "pub", "-topic", chatterTopic, "-type", stringType, "-rate", "100ms")
	pub.WaitFor(10*time.Second, "written", 30)
	if seen := stop(); len(seen) != 0 {
		t.Errorf("the other host got %d datagrams from here:\n%s", len(seen), strings.Join(seen, "\n"))
	}
	if hs := heard(listener.Logs()); len(hs) != 0 {
		t.Errorf("the other host heard %v", hs)
	}
	// The same, in network scope: the capture does see what comes.
	stop = b.Capture(t, "udp and src host "+a.Addr(t))
	a.Cat(t, d, "pub", "-scope", "network", "-topic", chatterTopic, "-type", stringType, "-rate", "100ms", "-wait", "1")
	waitHeard(t, listener, 20*time.Second, 3)
	if seen := stop(); len(seen) == 0 {
		t.Error("in network scope the capture saw nothing either: it sees nothing")
	}
}
