package transport

import (
	"context"
	"net/netip"

	"github.com/lesomnus/dds/rtps"
)

// Transport carries RTPS messages: one kind of locator (UDPv4 here; TCP or
// another one day) on some network interfaces. It knows how to send and
// receive, not whom to: who a participant may talk to is the participant's
// to decide, before it calls Send.
type Transport interface {
	// Unicast is where it receives unicast: the locators a participant
	// announces for itself.
	Unicast() []rtps.Locator
	// LocalAddrs are the addresses of this host, as the transport sees them:
	// a participant that sends from one is on this host.
	LocalAddrs() []netip.Addr
	// Networks are the subnets it reaches directly.
	Networks() []netip.Prefix
	// Reaches reports whether it can send to a locator: the kind is its
	// own, the address one it has a way to.
	Reaches(l rtps.Locator) bool
	// Send sends a message to a locator: a unicast one, or the SPDP
	// multicast group (on every interface that multicasts).
	Send(to rtps.Locator, msg []byte) error
	// Run receives until the context ends, calling deliver with each
	// message from one goroutine or several. A Packet's Data is only valid
	// during the call. Send keeps working after Run returns, until Close.
	Run(ctx context.Context, deliver func(Packet)) error
	// Close releases the transport.
	Close() error
}

// Via is a transport that can multicast out of one of its interfaces alone,
// the one of a locator it receives on: so that what is announced there can
// put that interface's locator first.
type Via interface {
	SendVia(via, to rtps.Locator, msg []byte) error
}

// Packet is a message a transport received.
type Packet struct {
	From netip.AddrPort
	// Multicast: it came to the SPDP multicast group.
	Multicast bool
	Data      []byte
}

// UDPv4 is the locator of an IPv4 UDP address.
func UDPv4(a netip.AddrPort) rtps.Locator {
	return rtps.Locator{Kind: rtps.LocatorUDPv4, Addr: a.Addr(), Port: uint32(a.Port())}
}

// AddrPort is a UDPv4 locator's address; false for another kind or a port
// out of range.
func AddrPort(l rtps.Locator) (netip.AddrPort, bool) {
	if l.Kind != rtps.LocatorUDPv4 || l.Port == 0 || l.Port > 65535 || !l.Addr.IsValid() {
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(l.Addr, uint16(l.Port)), true
}
