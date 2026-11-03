// Package memtest is a network in memory, for testing participants without
// sockets: hosts with addresses on one subnet, UDPv4 locators, the SPDP
// multicast group, and loss or duplication as a test asks.
package memtest

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"

	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/transport"
)

// Network is a subnet of hosts.
type Network struct {
	Prefix netip.Prefix

	mu    sync.Mutex
	ends  map[netip.AddrPort]*Transport
	ports map[netip.Addr]uint16
	// Drop, if set, says whether a message is lost.
	drop func(from, to netip.AddrPort, msg []byte) bool
	// dup, if set, says whether a message arrives twice.
	dup  func(from, to netip.AddrPort, msg []byte) bool
	sent []Sent
}

// Sent is a message the network carried (or lost).
type Sent struct {
	From, To netip.AddrPort
	Size     int
	Lost     bool
}

// NewNetwork is an empty network on a subnet.
func NewNetwork(prefix string) *Network {
	return &Network{Prefix: netip.MustParsePrefix(prefix), ends: map[netip.AddrPort]*Transport{}, ports: map[netip.Addr]uint16{}}
}

// SetLoss makes messages lost when drop says so; nil loses none.
func (n *Network) SetLoss(drop func(from, to netip.AddrPort, msg []byte) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drop = drop
}

// SetDuplication makes messages arrive twice when dup says so.
func (n *Network) SetDuplication(dup func(from, to netip.AddrPort, msg []byte) bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dup = dup
}

// Sent is everything sent so far.
func (n *Network) Sent() []Sent {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Sent(nil), n.sent...)
}

// Transport is one participant's way onto the network, from a host.
type Transport struct {
	net       *Network
	addr      netip.AddrPort
	multicast bool
	inbox     chan transport.Packet
	closed    bool
}

var _ transport.Transport = (*Transport)(nil)

// Open gives a participant on a host (an address of the network) a
// transport, at a port of its own. multicast joins the SPDP group.
func (n *Network) Open(host string, multicast bool) *Transport {
	a := netip.MustParseAddr(host)
	if !n.Prefix.Contains(a) {
		panic(fmt.Sprintf("memtest: %s is not on %s", a, n.Prefix))
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ports[a] == 0 {
		n.ports[a] = 7410
	}
	port := n.ports[a]
	n.ports[a] += 2
	t := &Transport{net: n, addr: netip.AddrPortFrom(a, port), multicast: multicast, inbox: make(chan transport.Packet, 8192)}
	n.ends[t.addr] = t
	return t
}

// Addr is where it receives.
func (t *Transport) Addr() netip.AddrPort { return t.addr }

func (t *Transport) Unicast() []rtps.Locator  { return []rtps.Locator{transport.UDPv4(t.addr)} }
func (t *Transport) LocalAddrs() []netip.Addr { return []netip.Addr{t.addr.Addr()} }
func (t *Transport) Networks() []netip.Prefix { return []netip.Prefix{t.net.Prefix} }

func (t *Transport) Reaches(l rtps.Locator) bool {
	a, ok := transport.AddrPort(l)
	return ok && (a.Addr() == transport.DefaultMulticastGroup || t.net.Prefix.Contains(a.Addr()))
}

// Send delivers a message to the transport at an address, or to every one
// in the multicast group (this one included), unless it is lost.
func (t *Transport) Send(to rtps.Locator, msg []byte) error {
	a, ok := transport.AddrPort(to)
	if !ok {
		return fmt.Errorf("memtest: cannot send to %v", to)
	}
	n := t.net
	n.mu.Lock()
	defer n.mu.Unlock()
	if t.closed {
		return errors.New("memtest: closed")
	}
	var dsts []*Transport
	group := a.Addr() == transport.DefaultMulticastGroup
	if group {
		for _, e := range n.ends {
			if e.multicast {
				dsts = append(dsts, e)
			}
		}
	} else if e, ok := n.ends[a]; ok {
		dsts = append(dsts, e)
	}
	lost := n.drop != nil && n.drop(t.addr, a, msg)
	n.sent = append(n.sent, Sent{From: t.addr, To: a, Size: len(msg), Lost: lost})
	if lost {
		return nil
	}
	times := 1
	if n.dup != nil && n.dup(t.addr, a, msg) {
		times = 2
	}
	for _, d := range dsts {
		if d.closed {
			continue
		}
		for range times {
			select {
			case d.inbox <- transport.Packet{From: t.addr, Multicast: group, Data: append([]byte(nil), msg...)}:
			default: // a full socket buffer
			}
		}
	}
	return nil
}

// Run delivers what arrives until the context ends.
func (t *Transport) Run(ctx context.Context, deliver func(transport.Packet)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case p := <-t.inbox:
			deliver(p)
		}
	}
}

// Close takes the transport off the network.
func (t *Transport) Close() error {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	t.closed = true
	delete(t.net.ends, t.addr)
	return nil
}
