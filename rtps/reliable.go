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

// FragmentNumberSet is a base and a bitmap of up to 256 fragment numbers
// after it (RTPS 2.5, 9.4.2.8). Fragment numbers start at 1.
type FragmentNumberSet struct {
	Base    uint32
	NumBits uint32
	Bitmap  [8]uint32
}

// Add puts n in the set, if it is within Base..Base+255.
func (s *FragmentNumberSet) Add(n uint32) bool {
	if n < s.Base || n-s.Base >= 256 {
		return false
	}
	d := n - s.Base
	s.Bitmap[d/32] |= 1 << (31 - d%32)
	if d+1 > s.NumBits {
		s.NumBits = d + 1
	}
	return true
}

// Members lists the numbers in the set, in order.
func (s FragmentNumberSet) Members() []uint32 {
	var out []uint32
	for i := uint32(0); i < s.NumBits; i++ {
		if s.Bitmap[i/32]&(1<<(31-i%32)) != 0 {
			out = append(out, s.Base+i)
		}
	}
	return out
}

func readFragSet(o binary.ByteOrder, b []byte) (FragmentNumberSet, int, error) {
	if len(b) < 8 {
		return FragmentNumberSet{}, 0, ErrTruncated
	}
	s := FragmentNumberSet{Base: o.Uint32(b[0:4]), NumBits: o.Uint32(b[4:8])}
	if s.NumBits > 256 {
		return s, 0, fmt.Errorf("rtps: fragment number set of %d bits", s.NumBits)
	}
	words := int(s.NumBits+31) / 32
	if len(b) < 8+4*words {
		return s, 0, ErrTruncated
	}
	for i := range words {
		s.Bitmap[i] = o.Uint32(b[8+4*i:])
	}
	return s, 8 + 4*words, nil
}

// HeartbeatFrag is a HEARTBEAT_FRAG submessage: the writer has fragments
// 1..LastFragment of sample SN.
type HeartbeatFrag struct {
	Reader, Writer EntityID
	SN             SequenceNumber
	LastFragment   uint32
	Count          int32
}

// ParseHeartbeatFrag reads a HEARTBEAT_FRAG submessage.
func ParseHeartbeatFrag(s Submessage) (HeartbeatFrag, error) {
	b := s.Body
	if len(b) < 24 {
		return HeartbeatFrag{}, ErrTruncated
	}
	o := s.Order()
	return HeartbeatFrag{
		Reader: EntityID(b[0:4]), Writer: EntityID(b[4:8]),
		SN: readSN(o, b[8:16]), LastFragment: o.Uint32(b[16:20]), Count: int32(o.Uint32(b[20:24])),
	}, nil
}

// NackFrag is a NACK_FRAG submessage: the reader asks again for the
// fragments in Missing of sample SN.
type NackFrag struct {
	Reader, Writer EntityID
	SN             SequenceNumber
	Missing        FragmentNumberSet
	Count          int32
}

// ParseNackFrag reads a NACK_FRAG submessage.
func ParseNackFrag(s Submessage) (NackFrag, error) {
	b := s.Body
	if len(b) < 16 {
		return NackFrag{}, ErrTruncated
	}
	o := s.Order()
	n := NackFrag{Reader: EntityID(b[0:4]), Writer: EntityID(b[4:8]), SN: readSN(o, b[8:16])}
	set, k, err := readFragSet(o, b[16:])
	if err != nil {
		return n, err
	}
	n.Missing = set
	if len(b) < 16+k+4 {
		return n, ErrTruncated
	}
	n.Count = int32(o.Uint32(b[16+k:]))
	return n, nil
}
