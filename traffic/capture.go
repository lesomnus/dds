package traffic

import (
	"net/netip"
	"time"

	"golang.org/x/net/bpf"
)

// CaptureConfig configures a capture.
type CaptureConfig struct {
	// Interfaces to capture on, by name.
	Interfaces []string
	// Snaplen is how many bytes of each IP packet are kept: enough for the
	// RTPS submessage headers that come first. 256 when zero. With a Filter
	// of its own, at least what that keeps.
	Snaplen int
	// Buffer is the socket's receive buffer, in bytes. 4 MiB when zero.
	Buffer int
	// Filter is the BPF program that picks what is captured, over IPv4
	// packets; nil is Filter(Snaplen), every RTPS datagram. It must keep only
	// IPv4 UDP datagrams carrying RTPS: what it keeps is read as such.
	Filter []bpf.Instruction
}

// Packet is an RTPS message as captured.
type Packet struct {
	// Message is the RTPS message, cut short as the capture keeps it.
	Message []byte
	// Src and Dst are the UDP endpoints; SrcPort and DstPort their ports.
	Src, Dst         netip.AddrPort
	SrcPort, DstPort uint16
	// Interface is where it was captured; Outgoing whether this host sent it.
	Interface string
	Outgoing  bool
	At        time.Time
}
