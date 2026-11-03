// Package pcap reads the UDP datagrams out of a libpcap capture, for tests
// that replay traffic captured from DDS implementations. It knows the link
// types tcpdump writes for "-i any" (Linux cooked v1 and v2), Ethernet, raw
// IP and loopback, and IPv4 only.
package pcap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"time"
)

// Datagram is one UDP datagram.
type Datagram struct {
	Time time.Time
	Src  netip.AddrPort
	Dst  netip.AddrPort
	Data []byte
}

// Link types (https://www.tcpdump.org/linktypes.html).
const (
	linkNull      = 0
	linkEthernet  = 1
	linkRaw       = 101
	linkLinuxSLL  = 113
	linkIPv4      = 228
	linkLinuxSLL2 = 276
)

// ReadFile reads every UDP/IPv4 datagram in a capture file.
func ReadFile(name string) ([]Datagram, error) {
	b, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return Read(b)
}

// Read reads every UDP/IPv4 datagram in a capture.
func Read(b []byte) ([]Datagram, error) {
	if len(b) < 24 {
		return nil, io.ErrUnexpectedEOF
	}
	var o binary.ByteOrder
	nano := false
	switch binary.LittleEndian.Uint32(b[0:4]) {
	case 0xa1b2c3d4:
		o = binary.LittleEndian
	case 0xa1b23c4d:
		o, nano = binary.LittleEndian, true
	case 0xd4c3b2a1:
		o = binary.BigEndian
	case 0x4d3cb2a1:
		o, nano = binary.BigEndian, true
	default:
		return nil, errors.New("pcap: not a pcap file (pcapng is not supported)")
	}
	link := o.Uint32(b[20:24]) & 0x0fffffff
	var out []Datagram
	at := 24
	for len(b)-at >= 16 {
		sec, sub := o.Uint32(b[at:]), o.Uint32(b[at+4:])
		incl := int(o.Uint32(b[at+8:]))
		at += 16
		if incl > len(b)-at {
			return out, io.ErrUnexpectedEOF
		}
		frame := b[at : at+incl]
		at += incl
		ns := int64(sub) * 1000
		if nano {
			ns = int64(sub)
		}
		ip, err := ipPayload(link, frame)
		if err != nil {
			return out, err
		}
		if d, ok := udp(ip); ok {
			d.Time = time.Unix(int64(sec), ns)
			out = append(out, d)
		}
	}
	return out, nil
}

// ipPayload strips the link header, returning the IPv4 packet or nil for
// anything else.
func ipPayload(link uint32, f []byte) ([]byte, error) {
	var proto uint16
	switch link {
	case linkLinuxSLL:
		if len(f) < 16 {
			return nil, nil
		}
		proto, f = binary.BigEndian.Uint16(f[14:16]), f[16:]
	case linkLinuxSLL2:
		if len(f) < 20 {
			return nil, nil
		}
		proto, f = binary.BigEndian.Uint16(f[0:2]), f[20:]
	case linkEthernet:
		if len(f) < 14 {
			return nil, nil
		}
		proto, f = binary.BigEndian.Uint16(f[12:14]), f[14:]
	case linkNull:
		if len(f) < 4 {
			return nil, nil
		}
		proto, f = 0x0800, f[4:]
	case linkRaw, linkIPv4:
		proto = 0x0800
	default:
		return nil, fmt.Errorf("pcap: link type %d not supported", link)
	}
	if proto != 0x0800 {
		return nil, nil
	}
	return f, nil
}

func udp(ip []byte) (Datagram, bool) {
	if len(ip) < 20 || ip[0]>>4 != 4 || ip[9] != 17 {
		return Datagram{}, false
	}
	ihl := int(ip[0]&0x0f) * 4
	total := int(binary.BigEndian.Uint16(ip[2:4]))
	if frag := binary.BigEndian.Uint16(ip[6:8]); frag&0x3fff != 0 {
		return Datagram{}, false // IP fragments are not reassembled
	}
	if ihl < 20 || total > len(ip) || total < ihl+8 {
		return Datagram{}, false
	}
	u := ip[ihl:total]
	src := netip.AddrPortFrom(netip.AddrFrom4([4]byte(ip[12:16])), binary.BigEndian.Uint16(u[0:2]))
	dst := netip.AddrPortFrom(netip.AddrFrom4([4]byte(ip[16:20])), binary.BigEndian.Uint16(u[2:4]))
	n := int(binary.BigEndian.Uint16(u[4:6]))
	if n < 8 || n > len(u) {
		return Datagram{}, false
	}
	return Datagram{Src: src, Dst: dst, Data: append([]byte(nil), u[8:n]...)}, true
}
