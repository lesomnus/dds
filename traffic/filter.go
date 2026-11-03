package traffic

import "golang.org/x/net/bpf"

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
// (cut short as the packet is), with the UDP ports.
func rtpsPayload(p []byte) (b []byte, src, dst uint16, ok bool) {
	if len(p) < 20 || p[0]>>4 != 4 {
		return nil, 0, 0, false
	}
	ihl := int(p[0]&0x0f) * 4
	if ihl < 20 || len(p) < ihl+8 {
		return nil, 0, 0, false
	}
	u := p[ihl:]
	return u[8:], uint16(u[0])<<8 | uint16(u[1]), uint16(u[2])<<8 | uint16(u[3]), true
}
