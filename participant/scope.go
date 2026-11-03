package participant

import (
	"fmt"
	"net/netip"

	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/transport"
)

// Scope is who a participant talks to.
type Scope int

const (
	// ScopeHost talks only to participants on this host, only at addresses
	// of this host, and multicasts nothing. The default.
	ScopeHost Scope = iota
	// ScopeNetwork talks to participants on the DDS interface's network as
	// well: it announces itself to the SPDP multicast group and to Peers,
	// and sends to addresses of the interface's subnets and to Peers. It
	// sends nowhere else.
	ScopeNetwork
)

func (s Scope) String() string {
	switch s {
	case ScopeHost:
		return "host"
	case ScopeNetwork:
		return "network"
	}
	return fmt.Sprintf("Scope(%d)", int(s))
}

// PortPolicy is how a participant picks its unicast port.
type PortPolicy int

const (
	// PortsDefault is PortsEphemeral in ScopeHost and PortsWellKnown in
	// ScopeNetwork.
	PortsDefault PortPolicy = iota
	// PortsEphemeral lets the system pick a port.
	PortsEphemeral
	// PortsWellKnown takes the first free participant index of the domain
	// and its metatraffic unicast port (7410 + 250·domain + 2·index): the
	// port range a firewall opens for a DDS domain.
	PortsWellKnown
)

// peerIndexes are the participant indexes whose ports a Peer is sent SPDP
// to, besides its SPDP port.
const peerIndexes = 5

// Sizes of what is sent, by scope: datagrams of up to 64 KiB on this host's
// loopback; what Cyclone DDS sends by default on a network.
const (
	hostMaxMessage    = 60064
	hostFragment      = 16000
	networkMaxMessage = 14720
	networkFragment   = 1344
	// messageOverhead is what a message carries besides a sample: header,
	// INFO_DST, INFO_TS, the DATA(_FRAG) and a HEARTBEAT.
	messageOverhead = 128
)

// onNetwork reports whether an address is on the interface's network: in
// one of its subnets, or a peer.
func (p *Participant) onNetwork(a netip.Addr) bool {
	for _, n := range p.nets {
		if n.Contains(a) {
			return true
		}
	}
	return p.peerAddrs[a]
}

// allowed reports whether the scope lets a datagram go to an address.
func (p *Participant) allowed(a netip.Addr) bool {
	if p.local[a] {
		return true
	}
	if p.cfg.Scope != ScopeNetwork {
		return false
	}
	return a == transport.DefaultMulticastGroup || p.onNetwork(a)
}

// reachable is where to reach a participant among its locators: the one
// the scope lets us send to that is best placed, or none. A participant on
// this host (its packets come from an address of this host) is reached at
// an address of this host. One elsewhere is reached on the network -- not at
// any address it shares with this host, like a docker bridge's every host
// has. Of several, the first on the first of our networks wins: one
// destination, not a copy down every path.
func (p *Participant) reachable(from netip.Addr, ls []rtps.Locator) []netip.AddrPort {
	here := p.local[from]
	best, rank := netip.AddrPort{}, -1
	for _, l := range ls {
		a, ok := transport.AddrPort(l)
		if !ok || a.Addr().IsUnspecified() || a.Addr().IsMulticast() {
			continue
		}
		if here {
			ok = p.local[a.Addr()]
		} else {
			ok = p.cfg.Scope == ScopeNetwork && !p.local[a.Addr()] && p.onNetwork(a.Addr())
		}
		if !ok {
			continue
		}
		if r := p.rank(a.Addr()); rank < 0 || r < rank {
			best, rank = a, r
		}
	}
	if rank < 0 {
		return nil
	}
	return []netip.AddrPort{best}
}

// rank orders addresses by our networks: the index of the first that has
// it, the loopback and the rest last.
func (p *Participant) rank(a netip.Addr) int {
	for i, n := range p.nets {
		if n.Contains(a) && !a.IsLoopback() {
			return i
		}
	}
	return len(p.nets)
}

// spdpTargets are where SPDP goes besides the participants already known:
// in ScopeNetwork, the multicast group and the peers' discovery ports.
func (p *Participant) spdpTargets() []netip.AddrPort {
	if p.cfg.Scope != ScopeNetwork {
		return nil
	}
	var out []netip.AddrPort
	spdp := transport.SPDPMulticastPort(p.cfg.Domain)
	if !p.cfg.NoMulticast {
		out = append(out, netip.AddrPortFrom(transport.DefaultMulticastGroup, spdp))
	}
	for _, a := range p.cfg.Peers {
		out = append(out, netip.AddrPortFrom(a, spdp))
		for i := range peerIndexes {
			out = append(out, netip.AddrPortFrom(a, transport.MetatrafficUnicastPort(p.cfg.Domain, uint32(i))))
		}
	}
	return out
}

// ParticipantIndex is the index whose port the participant took, or -1.
func (p *Participant) ParticipantIndex() int {
	for _, t := range p.transports {
		if pt, ok := t.(*transport.Paced); ok {
			t = pt.Transport
		}
		if it, ok := t.(interface{ Index() int }); ok {
			return it.Index()
		}
	}
	return -1
}

// Dropped is how many messages the send queues (MaxSendRate) had no room
// for.
func (p *Participant) Dropped() int64 {
	var n int64
	for _, t := range p.transports {
		if pt, ok := t.(*transport.Paced); ok {
			n += pt.Dropped()
		}
	}
	return n
}
