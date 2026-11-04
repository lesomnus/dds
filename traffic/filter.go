package traffic

import (
	"net/netip"

	"golang.org/x/net/bpf"
)

// rtpsMagic is "RTPS", where every RTPS message starts.
const rtpsMagic = 0x52545053

// Filter is a BPF program over IPv4 packets (network header first, as a
// packet socket of SOCK_DGRAM sees them) that keeps the first snaplen bytes
// of UDP datagrams carrying an RTPS message, and drops everything else.
//
// It looks at the payload rather than the ports: Cyclone DDS, unless given a
// participant index, receives unicast on ports of its own choosing. Of a
// datagram the IP layer fragmented only the first fragment is kept: the
// others have neither the UDP header nor the RTPS one.
func Filter(snaplen uint32) []bpf.Instruction {
	const drop = 0 // placeholder: the jumps below count to the last instruction
	prog := []bpf.Instruction{
		// IPv4, UDP.
		bpf.LoadAbsolute{Off: 0, Size: 1},
		bpf.ALUOpConstant{Op: bpf.ALUOpShiftRight, Val: 4},
		bpf.JumpIf{Cond: bpf.JumpNotEqual, Val: 4, SkipTrue: 8},
		bpf.LoadAbsolute{Off: 9, Size: 1},
		bpf.JumpIf{Cond: bpf.JumpNotEqual, Val: 17, SkipTrue: 6},
		// The first fragment, or a datagram whole.
		bpf.LoadAbsolute{Off: 6, Size: 2},
		bpf.JumpIf{Cond: bpf.JumpBitsSet, Val: 0x1fff, SkipTrue: 4},
		// X = the IP header's length; the RTPS header is 8 after it.
		bpf.LoadMemShift{Off: 0},
		bpf.LoadIndirect{Off: 8, Size: 4},
		bpf.JumpIf{Cond: bpf.JumpNotEqual, Val: rtpsMagic, SkipTrue: 1},
		bpf.RetConstant{Val: snaplen},
		bpf.RetConstant{Val: drop},
	}
	return prog
}

// rtpsPayload returns the RTPS message of an IPv4 packet the filter kept
// (cut short as the packet is), with the UDP endpoints.
func rtpsPayload(p []byte) (b []byte, src, dst netip.AddrPort, ok bool) {
	if len(p) < 20 || p[0]>>4 != 4 {
		return nil, src, dst, false
	}
	ihl := int(p[0]&0x0f) * 4
	if ihl < 20 || len(p) < ihl+8 {
		return nil, src, dst, false
	}
	u := p[ihl:]
	src = netip.AddrPortFrom(netip.AddrFrom4([4]byte(p[12:16])), uint16(u[0])<<8|uint16(u[1]))
	dst = netip.AddrPortFrom(netip.AddrFrom4([4]byte(p[16:20])), uint16(u[2])<<8|uint16(u[3]))
	return u[8:], src, dst, true
}

// SPDP group and ports (RTPS 2.5, 9.6.1.1): 239.255.0.1, 7400 + 250·domain.
const (
	spdpGroup   = 0xefff0001
	portBase    = 7400
	domainGain  = 250
	maxDomainID = 232
	maxSPDPPort = portBase + domainGain*maxDomainID
	spdpSnaplen = 4096
)

// SPDPFilter is a BPF program like Filter that keeps only SPDP
// announcements: RTPS datagrams to the SPDP multicast group, at the SPDP
// port of a domain. Every other packet is dropped in the kernel, so a
// capture with it can stay open at little cost. It keeps snaplen bytes:
// enough for a whole announcement (SPDPSnaplen).
func SPDPFilter(snaplen uint32) []bpf.Instruction {
	return []bpf.Instruction{
		// 0: IPv4
		bpf.LoadAbsolute{Off: 0, Size: 1},
		bpf.ALUOpConstant{Op: bpf.ALUOpShiftRight, Val: 4},
		bpf.JumpIf{Cond: bpf.JumpNotEqual, Val: 4, SkipTrue: 16},
		// 3: UDP
		bpf.LoadAbsolute{Off: 9, Size: 1},
		bpf.JumpIf{Cond: bpf.JumpNotEqual, Val: 17, SkipTrue: 14},
		// 5: the first fragment, or a datagram whole
		bpf.LoadAbsolute{Off: 6, Size: 2},
		bpf.JumpIf{Cond: bpf.JumpBitsSet, Val: 0x1fff, SkipTrue: 12},
		// 7: to the SPDP group
		bpf.LoadAbsolute{Off: 16, Size: 4},
		bpf.JumpIf{Cond: bpf.JumpNotEqual, Val: spdpGroup, SkipTrue: 10},
		// 9: X = the IP header's length; the destination port, in range
		bpf.LoadMemShift{Off: 0},
		bpf.LoadIndirect{Off: 2, Size: 2},
		bpf.JumpIf{Cond: bpf.JumpLessThan, Val: portBase, SkipTrue: 7},
		bpf.JumpIf{Cond: bpf.JumpGreaterThan, Val: maxSPDPPort, SkipTrue: 6},
		// 13: and a domain's SPDP port: (port - 7400) mod 250 == 0
		bpf.ALUOpConstant{Op: bpf.ALUOpSub, Val: portBase},
		bpf.ALUOpConstant{Op: bpf.ALUOpMod, Val: domainGain},
		bpf.JumpIf{Cond: bpf.JumpNotEqual, Val: 0, SkipTrue: 3},
		// 16: RTPS
		bpf.LoadIndirect{Off: 8, Size: 4},
		bpf.JumpIf{Cond: bpf.JumpNotEqual, Val: rtpsMagic, SkipTrue: 1},
		bpf.RetConstant{Val: snaplen},
		// 19: drop
		bpf.RetConstant{Val: 0},
	}
}

// SPDPSnaplen is what SPDPFilter should keep: a whole announcement.
const SPDPSnaplen = spdpSnaplen

// SPDPDomain is the domain whose SPDP port a port is, by the default port
// mapping.
func SPDPDomain(port uint16) (uint32, bool) {
	p := int(port) - portBase
	if p < 0 || p%domainGain != 0 || p/domainGain > maxDomainID {
		return 0, false
	}
	return uint32(p / domainGain), true
}
