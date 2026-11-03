package traffic

import "time"

// CaptureConfig configures a capture.
type CaptureConfig struct {
	// Interfaces to capture on, by name.
	Interfaces []string
	// Snaplen is how many bytes of each IP packet are kept: enough for the
	// RTPS submessage headers that come first. 256 when zero.
	Snaplen int
	// Buffer is the socket's receive buffer, in bytes. 4 MiB when zero.
	Buffer int
}

// Packet is an RTPS message as captured.
type Packet struct {
	// Message is the RTPS message, cut short as the capture keeps it.
	Message []byte
	// SrcPort and DstPort are the UDP ports.
	SrcPort, DstPort uint16
	// Interface is where it was captured; Outgoing whether this host sent it.
	Interface string
	Outgoing  bool
	At        time.Time
}
