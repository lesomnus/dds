// Package transport carries RTPS messages over UDP: the well-known ports
// (RTPS 2.5, 9.6.1.1) and the sockets discovery listens and speaks on.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
)

// Port mapping parameters (RTPS 2.5, 9.6.1.1), with their default values.
const (
	PortBase        = 7400 // PB
	DomainGain      = 250  // DG
	ParticipantGain = 2    // PG
	offsetMetaMulti = 0    // d0
	offsetMetaUni   = 10   // d1
	offsetUserMulti = 1    // d2
	offsetUserUni   = 11   // d3
)

// MaxDomainID is the largest domain ID the default port mapping fits in a
// 16-bit port.
const MaxDomainID = 232

// DefaultMulticastGroup is where SPDP is announced by default (RTPS 2.5,
// 9.6.1.4.1).
var DefaultMulticastGroup = netip.MustParseAddr("239.255.0.1")

// SPDPMulticastPort is the port SPDP is announced to in a domain.
func SPDPMulticastPort(domain uint32) uint16 {
	return uint16(PortBase + DomainGain*domain + offsetMetaMulti)
}

// MetatrafficUnicastPort is the discovery unicast port of the participant
// with the index in a domain. Implementations need not use it (Cyclone DDS
// picks a free port unless told an index); it is what one can expect when
// an index is set.
func MetatrafficUnicastPort(domain, participantIndex uint32) uint16 {
	return uint16(PortBase + DomainGain*domain + offsetMetaUni + ParticipantGain*participantIndex)
}

// CheckDomain reports whether the domain ID fits the default port mapping.
func CheckDomain(domain uint32) error {
	if domain > MaxDomainID {
		return fmt.Errorf("transport: domain %d does not fit the default port mapping (max %d)", domain, MaxDomainID)
	}
	return nil
}

// Packet is one UDP datagram received.
type Packet struct {
	From netip.AddrPort
	Data []byte
}

// MulticastListener receives what is sent to a multicast group and port on
// one interface. It only listens: nothing is sent from it.
type MulticastListener struct {
	conn *net.UDPConn
	ifi  *net.Interface
}

// ListenMulticast joins group:port on the interface. The socket is bound with
// SO_REUSEADDR, so DDS processes on the same host keep theirs.
func ListenMulticast(ifi *net.Interface, group netip.Addr, port uint16) (*MulticastListener, error) {
	if !group.IsMulticast() {
		return nil, fmt.Errorf("transport: %s is not a multicast address", group)
	}
	conn, err := net.ListenMulticastUDP("udp4", ifi, net.UDPAddrFromAddrPort(netip.AddrPortFrom(group, port)))
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(1 << 20)
	return &MulticastListener{conn: conn, ifi: ifi}, nil
}

// maxDatagram is the largest UDP payload.
const maxDatagram = 65507

// Receive delivers datagrams to fn until the context ends or the socket
// fails. fn's Data is only valid during the call.
func (l *MulticastListener) Receive(ctx context.Context, fn func(Packet)) error {
	stop := context.AfterFunc(ctx, func() { _ = l.conn.Close() })
	defer stop()
	buf := make([]byte, maxDatagram)
	for {
		n, from, err := l.conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		fn(Packet{From: netip.AddrPortFrom(from.Addr().Unmap(), from.Port()), Data: buf[:n]})
	}
}

// Close stops listening.
func (l *MulticastListener) Close() error { return l.conn.Close() }

// InterfaceByAddr finds the interface with an address in prefix -- how Cyclone
// DDS configurations name one (<NetworkInterface address="192.168.10.0"/>
// means the interface on 192.168.10.0/24) -- or, for a bare address, the
// interface that has it.
func InterfaceByAddr(prefix netip.Prefix) (*net.Interface, netip.Addr, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, netip.Addr{}, err
	}
	for i := range ifs {
		addrs, err := ifs[i].Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			n, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(n.IP)
			if !ok {
				continue
			}
			if prefix.Contains(ip.Unmap()) {
				return &ifs[i], ip.Unmap(), nil
			}
		}
	}
	return nil, netip.Addr{}, fmt.Errorf("transport: no interface has an address in %s", prefix)
}

// LocalAddrs returns every address of this host's interfaces, for telling a
// local peer from a remote one.
func LocalAddrs() (map[netip.Addr]bool, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	out := make(map[netip.Addr]bool, len(addrs))
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok {
			if ip, ok := netip.AddrFromSlice(n.IP); ok {
				out[ip.Unmap()] = true
			}
		}
	}
	return out, nil
}
