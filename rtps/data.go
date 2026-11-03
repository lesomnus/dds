package rtps

import (
	"encoding/binary"
	"time"
)

// DATA flags (RTPS 2.5, 9.4.5.3.1), beyond FlagEndianness.
const (
	DataFlagInlineQoS = 0x02 // Q: InlineQos is present
	DataFlagData      = 0x04 // D: the payload is the data
	DataFlagKey       = 0x08 // K: the payload is the key only
)

// SequenceNumber orders a writer's changes. It is 64-bit on the wire as a
// signed high and an unsigned low word.
type SequenceNumber int64

func readSN(o binary.ByteOrder, b []byte) SequenceNumber {
	hi := int32(o.Uint32(b[0:4]))
	lo := o.Uint32(b[4:8])
	return SequenceNumber(int64(hi)<<32 | int64(lo))
}

// Data is a DATA submessage.
type Data struct {
	Reader EntityID
	Writer EntityID
	SN     SequenceNumber

	// InlineQoS is the inline parameter list, in the submessage's byte order;
	// nil without the Q flag.
	InlineQoS ParameterList
	// Payload is the serialized payload (encapsulation header included); nil
	// without the D or K flag. KeyOnly is set for K.
	Payload []byte
	KeyOnly bool
}

// ParseData reads a DATA submessage. The slices it returns alias s.Body.
func ParseData(s Submessage) (Data, error) {
	b := s.Body
	if len(b) < 20 {
		return Data{}, ErrTruncated
	}
	o := s.Order()
	toInline := int(o.Uint16(b[2:4]))
	var d Data
	copy(d.Reader[:], b[4:8])
	copy(d.Writer[:], b[8:12])
	d.SN = readSN(o, b[12:20])

	// octetsToInlineQos counts from the end of that field (offset 4), and
	// lets a later version put more fields before the inline QoS.
	at := 4 + toInline
	if at < 20 || at > len(b) {
		return Data{}, ErrTruncated
	}
	rest := b[at:]
	if s.Flags&DataFlagInlineQoS != 0 {
		pl, n, err := ReadParameterList(o, rest)
		if err != nil {
			return Data{}, err
		}
		d.InlineQoS = pl
		rest = rest[n:]
	}
	if s.Flags&(DataFlagData|DataFlagKey) != 0 {
		d.Payload = rest
		d.KeyOnly = s.Flags&DataFlagKey != 0 && s.Flags&DataFlagData == 0
	}
	return d, nil
}

// InfoTS is an INFO_TS submessage: the source timestamp for the submessages
// after it, or none if Invalid.
type InfoTS struct {
	Time    time.Time
	Invalid bool
}

const infoTSFlagInvalidate = 0x02

// ParseInfoTS reads an INFO_TS submessage.
func ParseInfoTS(s Submessage) (InfoTS, error) {
	if s.Flags&infoTSFlagInvalidate != 0 {
		return InfoTS{Invalid: true}, nil
	}
	if len(s.Body) < 8 {
		return InfoTS{}, ErrTruncated
	}
	return InfoTS{Time: readTime(s.Order(), s.Body[0:8])}, nil
}

// readTime reads an RTPS Time_t: seconds and a binary fraction of a second.
func readTime(o binary.ByteOrder, b []byte) time.Time {
	sec := int64(int32(o.Uint32(b[0:4])))
	frac := uint64(o.Uint32(b[4:8]))
	return time.Unix(sec, int64(frac*1e9>>32))
}

// ParseInfoDst reads an INFO_DST submessage: the participant the submessages
// after it are for. A zero prefix means any.
func ParseInfoDst(s Submessage) (GUIDPrefix, error) {
	if len(s.Body) < 12 {
		return GUIDPrefix{}, ErrTruncated
	}
	var p GUIDPrefix
	copy(p[:], s.Body[:12])
	return p, nil
}
