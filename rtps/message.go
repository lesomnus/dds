package rtps

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// HeaderSize is the size of an RTPS message header.
const HeaderSize = 20

var magic = [4]byte{'R', 'T', 'P', 'S'}

// Header is the RTPS message header.
type Header struct {
	Version ProtocolVersion
	Vendor  VendorID
	Prefix  GUIDPrefix
}

// SubmessageID identifies a submessage kind (RTPS 2.5, 9.4.5.1.1).
type SubmessageID byte

const (
	SubPad          SubmessageID = 0x01
	SubAckNack      SubmessageID = 0x06
	SubHeartbeat    SubmessageID = 0x07
	SubGap          SubmessageID = 0x08
	SubInfoTS       SubmessageID = 0x09
	SubInfoSrc      SubmessageID = 0x0c
	SubInfoReplyIP4 SubmessageID = 0x0d
	SubInfoDst      SubmessageID = 0x0e
	SubInfoReply    SubmessageID = 0x0f
	SubNackFrag     SubmessageID = 0x12
	SubHeartbeatFrg SubmessageID = 0x13
	SubData         SubmessageID = 0x15
	SubDataFrag     SubmessageID = 0x16
)

// Submessage is one submessage of a message. Body aliases the message.
type Submessage struct {
	ID    SubmessageID
	Flags byte
	Body  []byte
}

// FlagEndianness is bit 0 of every submessage's flags: set for little endian.
const FlagEndianness = 0x01

// Order is the byte order the submessage's body is in.
func (s Submessage) Order() binary.ByteOrder {
	if s.Flags&FlagEndianness != 0 {
		return binary.LittleEndian
	}
	return binary.BigEndian
}

var (
	ErrNotRTPS   = errors.New("rtps: not an RTPS message")
	ErrTruncated = errors.New("rtps: truncated")
)

// ParseHeader reads the header of a message.
func ParseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrNotRTPS
	}
	if [4]byte(b[0:4]) != magic {
		return Header{}, ErrNotRTPS
	}
	var h Header
	h.Version = ProtocolVersion{b[4], b[5]}
	h.Vendor = VendorID{b[6], b[7]}
	copy(h.Prefix[:], b[8:20])
	if h.Version.Major != 2 {
		return h, fmt.Errorf("rtps: protocol version %s not supported", h.Version)
	}
	return h, nil
}

// Submessages splits the body of a message (after its header) into
// submessages. A submessage whose length is zero extends to the end of the
// message, as the specification allows for all but PAD and INFO_TS.
func Submessages(b []byte) ([]Submessage, error) {
	var out []Submessage
	for len(b) > 0 {
		if len(b) < 4 {
			return out, ErrTruncated
		}
		s := Submessage{ID: SubmessageID(b[0]), Flags: b[1]}
		n := int(s.Order().Uint16(b[2:4]))
		b = b[4:]
		if n == 0 && s.ID != SubPad && s.ID != SubInfoTS {
			n = len(b)
		}
		if n > len(b) {
			return out, ErrTruncated
		}
		s.Body = b[:n:n]
		out = append(out, s)
		b = b[n:]
	}
	return out, nil
}

// Message is a parsed message: its header and submessages.
type Message struct {
	Header      Header
	Submessages []Submessage
}

// Parse reads a whole message. The submessages alias b.
func Parse(b []byte) (Message, error) {
	h, err := ParseHeader(b)
	if err != nil {
		return Message{Header: h}, err
	}
	subs, err := Submessages(b[HeaderSize:])
	return Message{Header: h, Submessages: subs}, err
}
