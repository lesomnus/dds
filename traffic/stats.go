// Package traffic measures DDS writers from the RTPS traffic they send,
// without taking part: how many samples each has written (from its
// sequence numbers), how big they are, and how often its readers ask for
// samples again.
//
// A writer numbers its samples 1, 2, 3, ... Seeing any DATA, DATA_FRAG or
// HEARTBEAT of it now and then is enough to know how many it has written
// since: missing a packet, or seeing one twice, changes nothing. Only what
// actually goes over the network is seen: a writer no reader is matched with
// sends nothing, and shared-memory delivery is invisible.
package traffic

import (
	"encoding/binary"
	"math"
	"sync"
	"time"

	"github.com/lesomnus/dds/rtps"
)

// Writer is what has been seen of one writer.
type Writer struct {
	GUID rtps.GUID
	// FirstSN is the highest sequence number when it was first seen;
	// MaxSN the highest since. MaxSN - FirstSN samples were written in
	// between.
	FirstSN, MaxSN rtps.SequenceNumber
	FirstSeen      time.Time
	LastSeen       time.Time
	// Sized samples are those whose size was seen, totalling SizedBytes
	// (serialized payloads, encapsulation included).
	Sized      int64
	SizedBytes int64
	// RetransmitRequests counts the sequence numbers readers asked for again
	// (ACKNACK), and FragmentRequests the fragments (NACK_FRAG).
	RetransmitRequests int64
	FragmentRequests   int64
}

// Samples is how many samples it has written since it was first seen.
func (w Writer) Samples() int64 { return int64(w.MaxSN - w.FirstSN) }

// Bytes estimates the bytes of those samples: those whose size was seen, and
// the others at their average.
func (w Writer) Bytes() int64 {
	n := w.Samples()
	if w.Sized == 0 || n <= w.Sized {
		return w.SizedBytes
	}
	est := float64(w.SizedBytes) + float64(n-w.Sized)*float64(w.SizedBytes)/float64(w.Sized)
	if est >= math.MaxInt64 {
		return math.MaxInt64 // sequence numbers made up
	}
	return int64(est)
}

type writer struct {
	Writer
	lastSized rtps.SequenceNumber // sizes are counted in order, once each
}

// readerKey is a reader-writer pair, whose ACKNACKs and NACK_FRAGs are
// numbered.
type readerKey struct {
	reader rtps.GUID
	writer rtps.GUID
}

// Stats accumulates what the traffic says of writers. It is safe for
// concurrent use.
type Stats struct {
	mu      sync.Mutex
	writers map[rtps.GUID]*writer
	acks    map[readerKey]int32
	nacks   map[readerKey]int32
	// MaxWriters bounds how many writers are tracked, against traffic that
	// makes them up.
	MaxWriters int
}

// NewStats returns empty statistics.
func NewStats() *Stats {
	return &Stats{
		writers:    map[rtps.GUID]*writer{},
		acks:       map[readerKey]int32{},
		nacks:      map[readerKey]int32{},
		MaxWriters: 1 << 16,
	}
}

func (s *Stats) writer(g rtps.GUID, at time.Time) *writer {
	w, ok := s.writers[g]
	if !ok {
		if len(s.writers) >= s.MaxWriters {
			return nil
		}
		w = &writer{Writer: Writer{GUID: g, FirstSN: -1, FirstSeen: at}}
		s.writers[g] = w
	}
	w.LastSeen = at
	return w
}

// sn records that the writer has written up to sn.
func (w *writer) sn(sn rtps.SequenceNumber) {
	if sn < 0 {
		return
	}
	if w.FirstSN < 0 {
		// The first sight: what came before is not ours to count.
		w.FirstSN, w.MaxSN, w.lastSized = sn, sn, sn
		return
	}
	if sn > w.MaxSN {
		w.MaxSN = sn
	}
}

// sized records the size of sample sn, unless it is one already counted (or
// older): a retransmission, another fragment of it, or one before the first
// sight.
func (w *writer) sized(sn rtps.SequenceNumber, n int64) {
	if sn <= w.lastSized || n < 0 {
		return
	}
	w.lastSized = sn
	w.Sized++
	w.SizedBytes += n
}

// submessage is a submessage as captured: Body may be cut short of Len.
type submessage struct {
	rtps.Submessage
	Len int
}

// submessages walks a message that may be cut short: the last submessage is
// what of it is there.
func submessages(b []byte) []submessage {
	var out []submessage
	for len(b) >= 4 {
		s := rtps.Submessage{ID: rtps.SubmessageID(b[0]), Flags: b[1]}
		n := int(s.Order().Uint16(b[2:4]))
		b = b[4:]
		if n == 0 && s.ID != rtps.SubPad && s.ID != rtps.SubInfoTS {
			n = len(b) // up to the end of the message, which is not known cut
		}
		have := min(n, len(b))
		s.Body = b[:have:have]
		out = append(out, submessage{s, n})
		b = b[have:]
	}
	return out
}

// Observe takes one RTPS message as it was seen at a moment. It may be cut
// short (a capture keeps the first bytes of each packet): what is there is
// read.
func (s *Stats) Observe(b []byte, at time.Time) {
	h, err := rtps.ParseHeader(b)
	if err != nil {
		return
	}
	subs := submessages(b[rtps.HeaderSize:])
	src := h.Prefix
	var dst rtps.GUIDPrefix // the participant INFO_DST names
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sub := range subs {
		switch sub.ID {
		case rtps.SubInfoDst:
			if p, err := rtps.ParseInfoDst(sub.Submessage); err == nil {
				dst = p
			}
		case rtps.SubInfoSrc:
			return // relayed: someone else's traffic
		case rtps.SubData:
			writer, sn, size, ok := dataSize(sub)
			if !ok || builtin(writer) {
				continue
			}
			if w := s.writer(rtps.GUID{Prefix: src, Entity: writer}, at); w != nil {
				w.sn(sn)
				if size >= 0 {
					w.sized(sn, size)
				}
			}
		case rtps.SubDataFrag:
			if len(sub.Body) < 32 {
				continue
			}
			o := sub.Order()
			writer := rtps.EntityID(sub.Body[8:12])
			if builtin(writer) {
				continue
			}
			sn := readSN(o, sub.Body[12:20])
			if w := s.writer(rtps.GUID{Prefix: src, Entity: writer}, at); w != nil {
				w.sn(sn)
				w.sized(sn, int64(o.Uint32(sub.Body[28:32])))
			}
		case rtps.SubHeartbeat:
			hb, err := rtps.ParseHeartbeat(sub.Submessage)
			if err != nil || builtin(hb.Writer) || hb.Last < hb.First-1 {
				continue
			}
			if w := s.writer(rtps.GUID{Prefix: src, Entity: hb.Writer}, at); w != nil {
				w.sn(hb.Last)
			}
		case rtps.SubAckNack:
			a, err := rtps.ParseAckNack(sub.Submessage)
			if err != nil || builtin(a.Writer) || dst.IsZero() {
				continue
			}
			k := readerKey{rtps.GUID{Prefix: src, Entity: a.Reader}, rtps.GUID{Prefix: dst, Entity: a.Writer}}
			if last, ok := s.acks[k]; ok && a.Count <= last {
				continue // this one, again
			}
			s.acks[k] = a.Count
			if n := len(a.State.Members()); n > 0 {
				if w := s.writer(k.writer, at); w != nil {
					w.RetransmitRequests += int64(n)
				}
			}
		case rtps.SubNackFrag:
			nf, err := rtps.ParseNackFrag(sub.Submessage)
			if err != nil || builtin(nf.Writer) || dst.IsZero() {
				continue
			}
			k := readerKey{rtps.GUID{Prefix: src, Entity: nf.Reader}, rtps.GUID{Prefix: dst, Entity: nf.Writer}}
			if last, ok := s.nacks[k]; ok && nf.Count <= last {
				continue
			}
			s.nacks[k] = nf.Count
			if w := s.writer(k.writer, at); w != nil {
				w.FragmentRequests += int64(len(nf.Missing.Members()))
			}
		}
	}
}

// dataSize reads what a DATA submessage says of its sample: its writer, its
// sequence number and its serialized size by the submessage's length (-1
// when it carries no data, or the inline QoS is cut short).
func dataSize(s submessage) (rtps.EntityID, rtps.SequenceNumber, int64, bool) {
	b := s.Body
	if len(b) < 20 {
		return rtps.EntityID{}, 0, 0, false
	}
	o := s.Order()
	writer := rtps.EntityID(b[8:12])
	sn := readSN(o, b[12:20])
	if s.Flags&rtps.DataFlagData == 0 {
		return writer, sn, -1, true // a key or a status: no sample
	}
	at := 4 + int(o.Uint16(b[2:4]))
	if at < 20 {
		return writer, sn, -1, true
	}
	if s.Flags&rtps.DataFlagInlineQoS != 0 {
		if at > len(b) {
			return writer, sn, -1, true
		}
		_, n, err := rtps.ReadParameterList(o, b[at:])
		if err != nil {
			return writer, sn, -1, true
		}
		at += n
	}
	if at > s.Len {
		return writer, sn, -1, true
	}
	return writer, sn, int64(s.Len - at), true
}

func readSN(o binary.ByteOrder, b []byte) rtps.SequenceNumber {
	return rtps.SequenceNumber(int64(int32(o.Uint32(b[0:4])))<<32 | int64(o.Uint32(b[4:8])))
}

// builtin reports whether an entity is a built-in one (discovery): its
// traffic is not a topic's.
func builtin(e rtps.EntityID) bool { return e.Kind()&0xc0 == 0xc0 }

// Writers returns what has been seen of every writer.
func (s *Stats) Writers() []Writer {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Writer, 0, len(s.writers))
	for _, w := range s.writers {
		if w.FirstSN >= 0 {
			out = append(out, w.Writer)
		}
	}
	return out
}

// Writer returns what has been seen of one writer.
func (s *Stats) Writer(g rtps.GUID) (Writer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.writers[g]
	if !ok || w.FirstSN < 0 {
		return Writer{}, false
	}
	return w.Writer, true
}

// ForgetParticipant drops the writers of a participant (gone), and the
// reader pairs that name it.
func (s *Stats) ForgetParticipant(p rtps.GUIDPrefix) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for g := range s.writers {
		if g.Prefix == p {
			delete(s.writers, g)
		}
	}
	for _, m := range []map[readerKey]int32{s.acks, s.nacks} {
		for k := range m {
			if k.writer.Prefix == p || k.reader.Prefix == p {
				delete(m, k)
			}
		}
	}
}

// Forget drops a writer (gone), and the reader pairs that name it.
func (s *Stats) Forget(g rtps.GUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.writers, g)
	for _, m := range []map[readerKey]int32{s.acks, s.nacks} {
		for k := range m {
			if k.writer == g {
				delete(m, k)
			}
		}
	}
}

// Prune drops the writers drop says to, and the reader pairs that name
// them: writers of participants gone without a word, say.
func (s *Stats) Prune(drop func(Writer) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gone := map[rtps.GUID]bool{}
	for g, w := range s.writers {
		if drop(w.Writer) {
			delete(s.writers, g)
			gone[g] = true
		}
	}
	if len(gone) == 0 {
		return
	}
	for _, m := range []map[readerKey]int32{s.acks, s.nacks} {
		for k := range m {
			if gone[k.writer] {
				delete(m, k)
			}
		}
	}
}
