package participant

import (
	"errors"
	"net/netip"
	"sort"
	"time"

	"github.com/lesomnus/dds/rtps"
)

// ErrHistoryFull is what a KEEP_ALL writer returns when its history holds
// as many samples as it may and a reliable reader has yet to acknowledge the
// oldest.
var ErrHistoryFull = errors.New("participant: the writer's history is full of unacknowledged samples")

// Sizes of what a writer sends. A sample larger than maxDataPayload goes in
// DATA_FRAGs of fragSize octets, fragsPerMessage to a datagram: a datagram
// stays under 64 KiB.
const (
	maxDataPayload  = 60000
	fragSize        = 16000
	fragsPerMessage = 3
	heartbeatPeriod = 200 * time.Millisecond
)

// outbound is a message to send, to every address.
type outbound struct {
	to  []netip.AddrPort
	msg []byte
}

// change is a sample in a writer's history.
type change struct {
	sn        rtps.SequenceNumber
	at        time.Time
	payload   []byte
	inlineQoS []byte
	keyOnly   bool
	key       rtps.GUID // of a keyed (built-in) writer: the instance
}

// readerProxy is a writer's view of one matched remote reader (RTPS 2.5,
// 8.4.7.5, a stateful writer's ReaderProxy).
type readerProxy struct {
	guid     rtps.GUID
	addrs    []netip.AddrPort
	reliable bool
	// start is the first sequence number that concerns it: a volatile
	// writer's samples from before the match do not.
	start rtps.SequenceNumber
	// acked: it has every sample before.
	acked    rtps.SequenceNumber
	ackCount int32
	haveAck  bool
	nfCount  int32
	lastHB   time.Time
}

// writerConfig is what a writer is.
type writerConfig struct {
	id             rtps.EntityID
	reliable       bool
	transientLocal bool
	// keepAll keeps every sample (up to maxSamples) until each reliable
	// reader has it; otherwise the last depth are kept.
	keepAll    bool
	depth      int
	maxSamples int
	// keyed keeps one change per key: the built-in SEDP writers, whose
	// instances are endpoints.
	keyed bool
}

// stateWriter is the writer side of delivery to the readers matched with it
// (RTPS 2.5, 8.4.9, a stateful writer). It does no I/O and keeps no clock:
// the participant feeds it what happens and sends what it returns.
type stateWriter struct {
	cfg     writerConfig
	prefix  rtps.GUIDPrefix
	history []*change // by sequence number
	last    rtps.SequenceNumber
	hbCount int32
	readers map[rtps.GUID]*readerProxy
}

func newStateWriter(prefix rtps.GUIDPrefix, cfg writerConfig) *stateWriter {
	if cfg.depth <= 0 {
		cfg.depth = 1
	}
	if cfg.maxSamples <= 0 {
		cfg.maxSamples = 1000
	}
	return &stateWriter{cfg: cfg, prefix: prefix, readers: map[rtps.GUID]*readerProxy{}}
}

func (w *stateWriter) builder(dst rtps.GUIDPrefix) *rtps.Builder {
	return rtps.NewBuilder(w.prefix, VendorID).InfoDst(dst)
}

// write adds a sample and returns what to send its readers.
func (w *stateWriter) write(c change) (rtps.SequenceNumber, []outbound, error) {
	if w.cfg.keepAll && !w.cfg.keyed {
		w.trimAcked()
		if len(w.history) >= w.cfg.maxSamples {
			return 0, nil, ErrHistoryFull
		}
	}
	w.last++
	c.sn = w.last
	if w.cfg.keyed {
		// The instance's previous change is replaced: it is no longer
		// relevant to anyone (a GAP, if asked for).
		for i, h := range w.history {
			if h.key == c.key {
				w.history = append(w.history[:i], w.history[i+1:]...)
				break
			}
		}
	}
	w.history = append(w.history, &c)
	if !w.cfg.keepAll && !w.cfg.keyed && len(w.history) > w.cfg.depth {
		w.history = append(w.history[:0], w.history[len(w.history)-w.cfg.depth:]...)
	}
	var out []outbound
	for _, r := range w.sortedReaders() {
		msgs := w.dataMessages(r, &c)
		if r.reliable {
			// The last datagram asks for an acknowledgement.
			last := len(msgs) - 1
			b := rtps.NewBuilder(w.prefix, VendorID)
			msgs[last] = append(msgs[last], w.heartbeatSub(r, b)...)
		}
		for _, m := range msgs {
			out = append(out, outbound{to: r.addrs, msg: m})
		}
	}
	return c.sn, out, nil
}

// heartbeatSub is a HEARTBEAT to one reader, as submessages to append to a
// message addressed to its participant.
func (w *stateWriter) heartbeatSub(r *readerProxy, b *rtps.Builder) []byte {
	first, last := w.available(r)
	w.hbCount++
	b.Heartbeat(r.guid.Entity, w.cfg.id, first, last, w.hbCount, false)
	return b.Bytes()[rtps.HeaderSize:]
}

// available is the range of sequence numbers a reader may still get:
// first > last when there is none.
func (w *stateWriter) available(r *readerProxy) (first, last rtps.SequenceNumber) {
	first = w.last + 1
	for _, c := range w.history {
		if c.sn >= r.start {
			first = c.sn
			break
		}
	}
	return first, w.last
}

// dataMessages are the datagrams carrying a change to one reader: one DATA,
// or DATA_FRAGs.
func (w *stateWriter) dataMessages(r *readerProxy, c *change) [][]byte {
	if len(c.payload) <= maxDataPayload {
		b := w.builder(r.guid.Prefix).InfoTS(c.at)
		b.Data(r.guid.Entity, w.cfg.id, c.sn, c.inlineQoS, c.payload, c.keyOnly)
		return [][]byte{b.Bytes()}
	}
	n := w.fragments(c)
	var out [][]byte
	for start := uint32(1); start <= n; start += fragsPerMessage {
		out = append(out, w.fragMessage(r, c, start, min(fragsPerMessage, n-start+1)))
	}
	return out
}

// fragments is how many fragments a change is cut in.
func (w *stateWriter) fragments(c *change) uint32 {
	return uint32((len(c.payload) + fragSize - 1) / fragSize)
}

// fragMessage is a datagram with fragments start .. start+count-1 of a
// change.
func (w *stateWriter) fragMessage(r *readerProxy, c *change, start, count uint32) []byte {
	from := int(start-1) * fragSize
	to := min(len(c.payload), from+int(count)*fragSize)
	b := w.builder(r.guid.Prefix).InfoTS(c.at)
	var qos []byte
	if start == 1 {
		qos = c.inlineQoS
	}
	b.DataFrag(r.guid.Entity, w.cfg.id, c.sn, start, uint16(count), fragSize, uint32(len(c.payload)), qos, c.payload[from:to])
	return b.Bytes()
}

func (w *stateWriter) sortedReaders() []*readerProxy {
	out := make([]*readerProxy, 0, len(w.readers))
	for _, r := range w.readers {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].guid.String() < out[j].guid.String() })
	return out
}

// addReader matches a reader. A transient-local writer sends a
// transient-local reader its history; any other reader gets only what is
// written from now on.
func (w *stateWriter) addReader(guid rtps.GUID, addrs []netip.AddrPort, reliable, transientLocal bool, at time.Time) []outbound {
	if _, ok := w.readers[guid]; ok {
		return nil
	}
	history := w.cfg.transientLocal && transientLocal
	r := &readerProxy{guid: guid, addrs: addrs, reliable: reliable, start: w.last + 1}
	if history {
		r.start = 1
	}
	r.acked = r.start
	w.readers[guid] = r
	var out []outbound
	if history {
		for _, c := range w.history {
			for _, m := range w.dataMessages(r, c) {
				out = append(out, outbound{to: addrs, msg: m})
			}
		}
	}
	if reliable {
		b := w.builder(guid.Prefix)
		w.heartbeatSub(r, b)
		r.lastHB = at
		out = append(out, outbound{to: addrs, msg: b.Bytes()})
	}
	return out
}

// removeReader unmatches a reader.
func (w *stateWriter) removeReader(guid rtps.GUID) { delete(w.readers, guid) }

// removeParticipant unmatches every reader of a participant.
func (w *stateWriter) removeParticipant(p rtps.GUIDPrefix) {
	for g := range w.readers {
		if g.Prefix == p {
			delete(w.readers, g)
		}
	}
}

func (w *stateWriter) find(sn rtps.SequenceNumber) *change {
	i := sort.Search(len(w.history), func(i int) bool { return w.history[i].sn >= sn })
	if i < len(w.history) && w.history[i].sn == sn {
		return w.history[i]
	}
	return nil
}

// onAckNack takes a reader's ACKNACK: what it has, and what it asks for
// again. Those still in the history are sent again; the others are declared
// gone with a GAP.
func (w *stateWriter) onAckNack(reader rtps.GUID, a rtps.AckNack, at time.Time) []outbound {
	r, ok := w.readers[reader]
	if !ok || !r.reliable {
		return nil
	}
	if r.haveAck && a.Count <= r.ackCount {
		return nil // seen before
	}
	r.haveAck, r.ackCount = true, a.Count
	if a.State.Base > r.acked {
		r.acked = a.State.Base
	}
	want := a.State.Members()
	var out []outbound
	var gaps []rtps.SequenceNumber
	for _, sn := range want {
		c := w.find(sn)
		if c == nil || sn < r.start {
			gaps = append(gaps, sn)
			continue
		}
		for _, m := range w.dataMessages(r, c) {
			out = append(out, outbound{to: r.addrs, msg: m})
		}
	}
	// A reader behind what is still available is told what it cannot have.
	first, last := w.available(r)
	if r.acked < first && first <= last+1 {
		gaps = append(gaps, r.acked)
	}
	// It acknowledged less than there is and asked for nothing (a
	// preemptive ACKNACK, or it lost the heartbeat): tell it what there is,
	// though not more often than heartbeats go -- what it lacks may be on
	// its way.
	idle := len(want) == 0 && r.acked <= last && at.Sub(r.lastHB) >= heartbeatPeriod
	if len(gaps) > 0 || len(out) > 0 || idle {
		b := w.builder(reader.Prefix)
		for _, run := range runs(gaps, first) {
			b.Gap(reader.Entity, w.cfg.id, run[0], rtps.SequenceNumberSet{Base: run[1] + 1})
		}
		if r.acked <= last {
			w.heartbeatSub(r, b)
			r.lastHB = at
		}
		out = append(out, outbound{to: r.addrs, msg: b.Bytes()})
	}
	if w.cfg.keepAll && !w.cfg.transientLocal {
		w.trimAcked()
	}
	return out
}

// runs groups sequence numbers into contiguous [from, to] runs. One below
// first stretches to first-1: everything before what is available is gone.
func runs(sns []rtps.SequenceNumber, first rtps.SequenceNumber) [][2]rtps.SequenceNumber {
	sort.Slice(sns, func(i, j int) bool { return sns[i] < sns[j] })
	var out [][2]rtps.SequenceNumber
	for _, sn := range sns {
		to := sn
		if sn < first {
			to = first - 1
		}
		if n := len(out); n > 0 && sn <= out[n-1][1]+1 {
			out[n-1][1] = max(out[n-1][1], to)
			continue
		}
		out = append(out, [2]rtps.SequenceNumber{sn, to})
	}
	return out
}

// onNackFrag sends a reader the fragments it asks for again.
func (w *stateWriter) onNackFrag(reader rtps.GUID, nf rtps.NackFrag) []outbound {
	r, ok := w.readers[reader]
	if !ok || !r.reliable {
		return nil
	}
	if nf.Count <= r.nfCount && r.nfCount != 0 {
		return nil
	}
	r.nfCount = nf.Count
	c := w.find(nf.SN)
	if c == nil || len(c.payload) <= maxDataPayload {
		return nil
	}
	n := w.fragments(c)
	var out []outbound
	for _, f := range nf.Missing.Members() {
		if f >= 1 && f <= n {
			out = append(out, outbound{to: r.addrs, msg: w.fragMessage(r, c, f, 1)})
		}
	}
	return out
}

// tick heartbeats, once a heartbeat period, the reliable readers that have
// yet to acknowledge everything -- and those that have yet to answer at all:
// a reader may learn of the writer after the writer learns of it, and until
// it does it ignores what the writer sends.
func (w *stateWriter) tick(at time.Time) []outbound {
	var out []outbound
	for _, r := range w.sortedReaders() {
		if !r.reliable || at.Sub(r.lastHB) < heartbeatPeriod {
			continue
		}
		if r.haveAck && r.acked > w.last {
			continue
		}
		if first, last := w.available(r); r.haveAck && first > last && r.acked >= first {
			continue // nothing it could still want
		}
		b := w.builder(r.guid.Prefix)
		w.heartbeatSub(r, b)
		r.lastHB = at
		out = append(out, outbound{to: r.addrs, msg: b.Bytes()})
	}
	return out
}

// trimAcked drops the samples every reliable reader has.
func (w *stateWriter) trimAcked() {
	low := w.last + 1
	for _, r := range w.readers {
		if r.reliable && r.acked < low {
			low = r.acked
		}
	}
	i := 0
	for i < len(w.history) && w.history[i].sn < low {
		i++
	}
	w.history = append(w.history[:0], w.history[i:]...)
}

// ready is the readers known to have matched the writer too: a reliable
// one once it has answered, a best-effort one (which never answers) as soon
// as it is matched.
func (w *stateWriter) ready() []rtps.GUID {
	var out []rtps.GUID
	for _, r := range w.sortedReaders() {
		if !r.reliable || r.haveAck {
			out = append(out, r.guid)
		}
	}
	return out
}

// matched is the readers matched.
func (w *stateWriter) matched() []rtps.GUID {
	out := make([]rtps.GUID, 0, len(w.readers))
	for _, r := range w.sortedReaders() {
		out = append(out, r.guid)
	}
	return out
}
