package udp

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/lesomnus/dds/transport"
)

func loopback(t *testing.T) Interface {
	t.Helper()
	ifs, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for i := range ifs {
		if ifs[i].Flags&net.FlagLoopback != 0 {
			return Interface{Interface: &ifs[i], Addr: netip.MustParseAddr("127.0.0.1"), NoMulticast: true}
		}
	}
	t.Skip("no loopback interface")
	return Interface{}
}

func TestWellKnownPorts(t *testing.T) {
	lo := loopback(t)
	var prev = -1
	for range 3 {
		tr, err := Open(Config{Domain: 77, Interfaces: []Interface{lo}, WellKnownPorts: true})
		if err != nil {
			t.Skipf("binding the domain's ports: %v", err)
		}
		defer tr.Close()
		want := transport.MetatrafficUnicastPort(77, uint32(tr.Index()))
		if tr.Unicast()[0].Port != uint32(want) {
			t.Errorf("index %d on port %d, want %d", tr.Index(), tr.Unicast()[0].Port, want)
		}
		if tr.Index() <= prev {
			t.Errorf("indexes %d then %d", prev, tr.Index())
		}
		prev = tr.Index()
	}
	tr, err := Open(Config{Domain: 77, Interfaces: []Interface{lo}})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	if tr.Index() != -1 {
		t.Errorf("ephemeral took index %d", tr.Index())
	}
}

func TestSendReceive(t *testing.T) {
	lo := loopback(t)
	a, err := Open(Config{Domain: 77, Interfaces: []Interface{lo}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(Config{Domain: 77, Interfaces: []Interface{lo}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	got := make(chan transport.Packet, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- b.Run(ctx, func(p transport.Packet) {
			p.Data = append([]byte(nil), p.Data...)
			got <- p
		})
	}()
	if err := a.Send(b.Unicast()[0], []byte("RTPS hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		if string(p.Data) != "RTPS hello" || p.Multicast || p.From.Port() != uint16(a.Unicast()[0].Port) {
			t.Errorf("%+v", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing received")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Run is over; sending still works until Close.
	if err := b.Send(a.Unicast()[0], []byte("still")); err != nil {
		t.Errorf("after Run: %v", err)
	}
	if err := a.Send(transport.UDPv4(netip.AddrPortFrom(transport.DefaultMulticastGroup, 7400)), []byte("x")); err == nil {
		t.Error("multicast sent with SendMulticast off")
	}
}
