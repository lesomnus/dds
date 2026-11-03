package rtps

import (
	"encoding/binary"
	"time"
)

// le is the byte order everything this package writes is in.
var le = binary.LittleEndian

// Builder writes one RTPS message, little endian.
type Builder struct {
	b []byte
}

// NewBuilder starts a message from the participant.
func NewBuilder(prefix GUIDPrefix, vendor VendorID) *Builder {
	b := make([]byte, 0, 512)
	b = append(b, magic[:]...)
	b = append(b, 2, 1) // protocol 2.1, what Cyclone DDS 0.10 speaks
	b = append(b, vendor[:]...)
	b = append(b, prefix[:]...)
	return &Builder{b: b}
}

// Bytes returns the message.
func (w *Builder) Bytes() []byte { return w.b }

// sub appends a submessage header and body. Every body here is a multiple of
// four octets long, which keeps the next header aligned.
func (w *Builder) sub(id SubmessageID, flags byte, body []byte) {
	w.b = append(w.b, byte(id), flags|FlagEndianness)
	w.b = le.AppendUint16(w.b, uint16(len(body)))
	w.b = append(w.b, body...)
}

// InfoTS sets the source timestamp of what follows.
func (w *Builder) InfoTS(t time.Time) *Builder {
	sec := t.Unix()
	frac := uint64(t.Nanosecond()) << 32 / 1e9
	body := le.AppendUint32(nil, uint32(int32(sec)))
	body = le.AppendUint32(body, uint32(frac))
	w.sub(SubInfoTS, 0, body)
	return w
}

// InfoDst addresses what follows to one participant.
func (w *Builder) InfoDst(p GUIDPrefix) *Builder {
	w.sub(SubInfoDst, 0, p[:])
	return w
}

// Data appends a DATA submessage. inlineQoS, if not nil, is an encoded
// parameter list (sentinel included); payload, if not nil, a serialized
// payload with its encapsulation header. keyOnly marks payload as the key.
func (w *Builder) Data(reader, writer EntityID, sn SequenceNumber, inlineQoS, payload []byte, keyOnly bool) *Builder {
	body := make([]byte, 0, 20+len(inlineQoS)+len(payload)+3)
	body = append(body, 0, 0)        // extraFlags
	body = le.AppendUint16(body, 16) // octetsToInlineQos: to after writerSN
	body = append(body, reader[:]...)
	body = append(body, writer[:]...)
	body = appendSN(body, le, sn)
	var flags byte
	if inlineQoS != nil {
		flags |= DataFlagInlineQoS
		body = append(body, inlineQoS...)
	}
	if payload != nil {
		if keyOnly {
			flags |= DataFlagKey
		} else {
			flags |= DataFlagData
		}
		body = append(body, payload...)
		for len(body)%4 != 0 {
			body = append(body, 0)
		}
	}
	w.sub(SubData, flags, body)
	return w
}

// Heartbeat appends a HEARTBEAT: the writer has first..last.
func (w *Builder) Heartbeat(reader, writer EntityID, first, last SequenceNumber, count int32, final bool) *Builder {
	body := make([]byte, 0, 28)
	body = append(body, reader[:]...)
	body = append(body, writer[:]...)
	body = appendSN(body, le, first)
	body = appendSN(body, le, last)
	body = le.AppendUint32(body, uint32(count))
	var flags byte
	if final {
		flags |= HeartbeatFlagFinal
	}
	w.sub(SubHeartbeat, flags, body)
	return w
}

// AckNack appends an ACKNACK: everything before state.Base was received,
// the numbers in state are asked for again.
func (w *Builder) AckNack(reader, writer EntityID, state SequenceNumberSet, count int32, final bool) *Builder {
	body := make([]byte, 0, 48)
	body = append(body, reader[:]...)
	body = append(body, writer[:]...)
	body = appendSNSet(body, le, state)
	body = le.AppendUint32(body, uint32(count))
	var flags byte
	if final {
		flags |= AckNackFlagFinal
	}
	w.sub(SubAckNack, flags, body)
	return w
}

// NackFrag appends a NACK_FRAG: fragments of sample sn asked for again.
func (w *Builder) NackFrag(reader, writer EntityID, sn SequenceNumber, missing FragmentNumberSet, count int32) *Builder {
	body := make([]byte, 0, 52)
	body = append(body, reader[:]...)
	body = append(body, writer[:]...)
	body = appendSN(body, le, sn)
	body = le.AppendUint32(body, missing.Base)
	body = le.AppendUint32(body, missing.NumBits)
	for i := range int(missing.NumBits+31) / 32 {
		body = le.AppendUint32(body, missing.Bitmap[i])
	}
	body = le.AppendUint32(body, uint32(count))
	w.sub(SubNackFrag, 0, body)
	return w
}

// ParameterListWriter encodes a parameter list (PL_CDR_LE).
type ParameterListWriter struct {
	b []byte
}

// Payload returns the list as a serialized payload: the PL_CDR_LE
// encapsulation header, the parameters, the sentinel.
func (p *ParameterListWriter) Payload() []byte {
	out := []byte{0x00, byte(EncPLCDRLE), 0, 0}
	out = append(out, p.b...)
	return append(out, byte(PIDSentinel), 0, 0, 0)
}

// List returns the parameters and the sentinel, for inline QoS.
func (p *ParameterListWriter) List() []byte {
	return append(append([]byte(nil), p.b...), byte(PIDSentinel), 0, 0, 0)
}

// Raw appends a parameter with the value as given, padded to four octets.
func (p *ParameterListWriter) Raw(id ParameterID, v []byte) *ParameterListWriter {
	n := (len(v) + 3) &^ 3
	p.b = le.AppendUint16(p.b, uint16(id))
	p.b = le.AppendUint16(p.b, uint16(n))
	p.b = append(p.b, v...)
	for range n - len(v) {
		p.b = append(p.b, 0)
	}
	return p
}

func (p *ParameterListWriter) Uint32(id ParameterID, v uint32) *ParameterListWriter {
	return p.Raw(id, le.AppendUint32(nil, v))
}

func (p *ParameterListWriter) GUID(id ParameterID, g GUID) *ParameterListWriter {
	return p.Raw(id, append(append([]byte(nil), g.Prefix[:]...), g.Entity[:]...))
}

func (p *ParameterListWriter) Locator(id ParameterID, l Locator) *ParameterListWriter {
	v := le.AppendUint32(nil, uint32(l.Kind))
	v = le.AppendUint32(v, l.Port)
	addr := l.Addr.As16()
	if l.Addr.Is4() {
		a4 := l.Addr.As4()
		addr = [16]byte{}
		copy(addr[12:], a4[:])
	}
	return p.Raw(id, append(v, addr[:]...))
}

func (p *ParameterListWriter) Duration(id ParameterID, d time.Duration) *ParameterListWriter {
	var sec, frac uint32
	if d == DurationInfinite {
		sec, frac = 0x7fffffff, 0xffffffff
	} else {
		sec = uint32(d / time.Second)
		frac = uint32(uint64(d%time.Second) << 32 / uint64(time.Second))
	}
	return p.Raw(id, le.AppendUint32(le.AppendUint32(nil, sec), frac))
}

func appendCDRString(b []byte, s string) []byte {
	b = le.AppendUint32(b, uint32(len(s)+1))
	b = append(b, s...)
	b = append(b, 0)
	for len(b)%4 != 0 {
		b = append(b, 0)
	}
	return b
}

func (p *ParameterListWriter) String(id ParameterID, s string) *ParameterListWriter {
	return p.Raw(id, appendCDRString(nil, s))
}

func (p *ParameterListWriter) Octets(id ParameterID, v []byte) *ParameterListWriter {
	return p.Raw(id, append(le.AppendUint32(nil, uint32(len(v))), v...))
}

// Properties appends a PID_PROPERTY_LIST of the name/value pairs, in order,
// and no binary properties.
func (p *ParameterListWriter) Properties(kv ...[2]string) *ParameterListWriter {
	v := le.AppendUint32(nil, uint32(len(kv)))
	for _, e := range kv {
		v = appendCDRString(v, e[0])
		v = appendCDRString(v, e[1])
	}
	v = le.AppendUint32(v, 0) // binary properties
	return p.Raw(PIDPropertyList, v)
}
