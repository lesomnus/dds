package rtps

import (
	"encoding/binary"
	"fmt"
)

// SequenceNumberSet is a base and a bitmap of up to 256 sequence numbers
// after it (RTPS 2.5, 9.4.2.6).
type SequenceNumberSet struct {
	Base SequenceNumber
	// NumBits is how many bits of Bitmap are meaningful.
	NumBits uint32
	Bitmap  [8]uint32
}

// Has reports whether sn is in the set.
func (s SequenceNumberSet) Has(sn SequenceNumber) bool {
	d := sn - s.Base
	if d < 0 || d >= SequenceNumber(s.NumBits) {
		return false
	}
	return s.Bitmap[d/32]&(1<<(31-uint(d%32))) != 0
}

// Add puts sn in the set, if it is within Base..Base+255; it grows NumBits to
// cover it.
func (s *SequenceNumberSet) Add(sn SequenceNumber) bool {
	d := sn - s.Base
	if d < 0 || d >= 256 {
		return false
	}
	s.Bitmap[d/32] |= 1 << (31 - uint(d%32))
	if uint32(d)+1 > s.NumBits {
		s.NumBits = uint32(d) + 1
	}
	return true
}

// Members lists the numbers in the set, in order.
func (s SequenceNumberSet) Members() []SequenceNumber {
	var out []SequenceNumber
	for i := uint32(0); i < s.NumBits; i++ {
		if s.Bitmap[i/32]&(1<<(31-i%32)) != 0 {
			out = append(out, s.Base+SequenceNumber(i))
		}
	}
	return out
}

func readSNSet(o binary.ByteOrder, b []byte) (SequenceNumberSet, int, error) {
	if len(b) < 12 {
		return SequenceNumberSet{}, 0, ErrTruncated
	}
	s := SequenceNumberSet{Base: readSN(o, b[0:8]), NumBits: o.Uint32(b[8:12])}
	if s.NumBits > 256 {
		return s, 0, fmt.Errorf("rtps: sequence number set of %d bits", s.NumBits)
	}
	words := int(s.NumBits+31) / 32
	if len(b) < 12+4*words {
		return s, 0, ErrTruncated
	}
	for i := range words {
		s.Bitmap[i] = o.Uint32(b[12+4*i:])
	}
	return s, 12 + 4*words, nil
}

func appendSN(b []byte, o binary.AppendByteOrder, sn SequenceNumber) []byte {
	b = o.AppendUint32(b, uint32(int32(sn>>32)))
	return o.AppendUint32(b, uint32(sn))
}

func appendSNSet(b []byte, o binary.AppendByteOrder, s SequenceNumberSet) []byte {
	b = appendSN(b, o, s.Base)
	b = o.AppendUint32(b, s.NumBits)
	for i := range int(s.NumBits+31) / 32 {
		b = o.AppendUint32(b, s.Bitmap[i])
	}
	return b
}

// HEARTBEAT flags.
const (
	HeartbeatFlagFinal      = 0x02 // F: the writer needs no answer
	HeartbeatFlagLiveliness = 0x04 // L
)

// Heartbeat is a HEARTBEAT submessage: the writer has First..Last.
type Heartbeat struct {
	Reader, Writer EntityID
	First, Last    SequenceNumber
	Count          int32
	Final          bool
}

// ParseHeartbeat reads a HEARTBEAT submessage.
func ParseHeartbeat(s Submessage) (Heartbeat, error) {
	b := s.Body
	if len(b) < 28 {
		return Heartbeat{}, ErrTruncated
	}
	o := s.Order()
	h := Heartbeat{
		Reader: EntityID(b[0:4]), Writer: EntityID(b[4:8]),
		First: readSN(o, b[8:16]), Last: readSN(o, b[16:24]),
		Count: int32(o.Uint32(b[24:28])),
		Final: s.Flags&HeartbeatFlagFinal != 0,
	}
	return h, nil
}

// ACKNACK flags.
const AckNackFlagFinal = 0x02 // F: the reader needs no reply

// AckNack is an ACKNACK submessage: the reader has everything before
// State.Base, and asks again for the numbers in State.
type AckNack struct {
	Reader, Writer EntityID
	State          SequenceNumberSet
	Count          int32
	Final          bool
}

// ParseAckNack reads an ACKNACK submessage.
func ParseAckNack(s Submessage) (AckNack, error) {
	b := s.Body
	if len(b) < 8 {
		return AckNack{}, ErrTruncated
	}
	o := s.Order()
	a := AckNack{Reader: EntityID(b[0:4]), Writer: EntityID(b[4:8]), Final: s.Flags&AckNackFlagFinal != 0}
	set, n, err := readSNSet(o, b[8:])
	if err != nil {
		return a, err
	}
	a.State = set
	if len(b) < 8+n+4 {
		return a, ErrTruncated
	}
	a.Count = int32(o.Uint32(b[8+n:]))
	return a, nil
}

// Gap is a GAP submessage: Start..List.Base-1, and the numbers in List, will
// never be sent.
type Gap struct {
	Reader, Writer EntityID
	Start          SequenceNumber
	List           SequenceNumberSet
}

// Irrelevant reports whether the gap covers sn.
func (g Gap) Irrelevant(sn SequenceNumber) bool {
	return (sn >= g.Start && sn < g.List.Base) || g.List.Has(sn)
}

// ParseGap reads a GAP submessage.
func ParseGap(s Submessage) (Gap, error) {
	b := s.Body
	if len(b) < 16 {
		return Gap{}, ErrTruncated
	}
	o := s.Order()
	g := Gap{Reader: EntityID(b[0:4]), Writer: EntityID(b[4:8]), Start: readSN(o, b[8:16])}
	set, _, err := readSNSet(o, b[16:])
	if err != nil {
		return g, err
	}
	g.List = set
	return g, nil
}

// DataFrag is a DATA_FRAG submessage: fragments of one serialized payload.
type DataFrag struct {
	Reader, Writer EntityID
	SN             SequenceNumber
	// FragmentStart is the 1-based number of the first fragment here.
	FragmentStart  uint32
	FragmentsInSub uint16
	FragmentSize   uint16
	SampleSize     uint32
	InlineQoS      ParameterList
	Fragments      []byte
}

// ParseDataFrag reads a DATA_FRAG submessage. Fragments aliases s.Body.
func ParseDataFrag(s Submessage) (DataFrag, error) {
	b := s.Body
	if len(b) < 32 {
		return DataFrag{}, ErrTruncated
	}
	o := s.Order()
	toInline := int(o.Uint16(b[2:4]))
	f := DataFrag{
		Reader: EntityID(b[4:8]), Writer: EntityID(b[8:12]),
		SN:             readSN(o, b[12:20]),
		FragmentStart:  o.Uint32(b[20:24]),
		FragmentsInSub: o.Uint16(b[24:26]),
		FragmentSize:   o.Uint16(b[26:28]),
		SampleSize:     o.Uint32(b[28:32]),
	}
	at := 4 + toInline
	if at < 32 || at > len(b) {
		return DataFrag{}, ErrTruncated
	}
	rest := b[at:]
	if s.Flags&DataFlagInlineQoS != 0 {
		pl, n, err := ReadParameterList(o, rest)
		if err != nil {
			return DataFrag{}, err
		}
		f.InlineQoS = pl
		rest = rest[n:]
	}
	if f.FragmentStart == 0 || f.FragmentSize == 0 {
		return DataFrag{}, fmt.Errorf("rtps: DATA_FRAG with fragment %d of size %d", f.FragmentStart, f.FragmentSize)
	}
	f.Fragments = rest
	return f, nil
}
