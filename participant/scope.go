package participant

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"

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

// maxParticipantIndex bounds the indexes of a domain: their ports stay
// below the next domain's.
const maxParticipantIndex = (transport.DomainGain - 10) / transport.ParticipantGain

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

// listen opens the participant's unicast socket, by its port policy.
func (p *Participant) listen() (*net.UDPConn, error) {
	ports := p.cfg.Ports
	if ports == PortsDefault {
		ports = PortsEphemeral
		if p.cfg.Scope == ScopeNetwork {
			ports = PortsWellKnown
		}
	}
	if ports == PortsEphemeral {
		p.index = -1
		return net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(p.cfg.Addr, 0)))
	}
	for i := 0; i < maxParticipantIndex; i++ {
		port := transport.MetatrafficUnicastPort(p.cfg.Domain, uint32(i))
		c, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(p.cfg.Addr, port)))
		if err == nil {
			p.index = i
			return c, nil
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("participant: every participant index of domain %d is taken", p.cfg.Domain)
}

// ParticipantIndex is the index whose port the participant took, or -1.
func (p *Participant) ParticipantIndex() int { return p.index }

// interfaceNets are the subnets of an interface's IPv4 addresses.
func interfaceNets(ifi *net.Interface) []netip.Prefix {
	addrs, err := ifi.Addrs()
	if err != nil {
		return nil
	}
	var out []netip.Prefix
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(n.IP)
		if !ok || !ip.Unmap().Is4() {
			continue
		}
		bits, _ := n.Mask.Size()
		out = append(out, netip.PrefixFrom(ip.Unmap(), bits).Masked())
	}
	return out
}

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

// reachable keeps the UDPv4 locators of a participant that the scope lets
// us send to. A participant on this host (its packets come from an address
// of this host) is reached at addresses of this host. One elsewhere is
// reached on the network -- not at any address it shares with this host,
// like a docker bridge's every host has.
func (p *Participant) reachable(from netip.Addr, ls []rtps.Locator) []netip.AddrPort {
	here := p.local[from]
	var out []netip.AddrPort
	for _, l := range ls {
		if l.Kind != rtps.LocatorUDPv4 || l.Port == 0 || l.Port >= 65536 || !l.Addr.IsValid() || l.Addr.IsUnspecified() {
			continue
		}
		ok := p.local[l.Addr]
		if !here {
			ok = p.cfg.Scope == ScopeNetwork && !p.local[l.Addr] && p.onNetwork(l.Addr)
		}
		if ok {
			out = append(out, netip.AddrPortFrom(l.Addr, uint16(l.Port)))
		}
	}
	return out
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

// setMulticast makes the unicast socket send multicast out of the DDS
// interface, to this link only, and to this host too.
func (p *Participant) setMulticast() error {
	pc := ipv4.NewPacketConn(p.conn)
	if err := pc.SetMulticastInterface(p.cfg.Interface); err != nil {
		return err
	}
	if err := pc.SetMulticastTTL(1); err != nil {
		return err
	}
	return pc.SetMulticastLoopback(true)
}

// pacer sends at most rate bytes a second, in bursts of up to burst, from a
// queue: what does not fit in the queue is dropped (and counted), for the
// reliable protocol to send again.
type pacer struct {
	rate, burst float64
	queue       chan paced
	now         func() time.Time
	sleep       func(time.Duration)
	write       func(netip.AddrPort, []byte)

	mu      sync.Mutex
	dropped int64
}

type paced struct {
	to  netip.AddrPort
	msg []byte
}

func newPacer(rate, burst int, write func(netip.AddrPort, []byte)) *pacer {
	return &pacer{
		rate: float64(rate), burst: float64(max(burst, 1)),
		queue: make(chan paced, 4096),
		now:   time.Now, sleep: time.Sleep, write: write,
	}
}

func (q *pacer) enqueue(to netip.AddrPort, msg []byte) {
	select {
	case q.queue <- paced{to, msg}:
	default:
		q.mu.Lock()
		q.dropped++
		q.mu.Unlock()
	}
}

// run sends the queue until the context ends.
func (q *pacer) run(ctx context.Context) {
	tokens, last := q.burst, q.now()
	for {
		var m paced
		select {
		case <-ctx.Done():
			return
		case m = <-q.queue:
		}
		need := float64(len(m.msg))
		for {
			now := q.now()
			tokens = min(q.burst, tokens+now.Sub(last).Seconds()*q.rate)
			last = now
			if tokens >= need || tokens >= q.burst {
				break
			}
			q.sleep(time.Duration((min(need, q.burst) - tokens) / q.rate * float64(time.Second)))
		}
		tokens -= need
		q.write(m.to, m.msg)
	}
}

// Dropped is how many datagrams the send queue had no room for.
func (p *Participant) Dropped() int64 {
	if p.pacer == nil {
		return 0
	}
	p.pacer.mu.Lock()
	defer p.pacer.mu.Unlock()
	return p.pacer.dropped
}
