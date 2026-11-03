package participant

import (
	"github.com/lesomnus/dds/rtps"
)

// Sample is a change delivered by a reliable reader, in its writer's order.
type Sample struct {
	SN        rtps.SequenceNumber
	InlineQoS rtps.ParameterList
	Payload   []byte
	KeyOnly   bool
}

// maxPending bounds what a writer proxy holds out of order, and the
// fragments it reassembles, against a writer (or a forger) that sends far
// ahead.
const (
	maxPending      = 1024
	maxSampleSize   = 1 << 20
	maxReassemblies = 16
)

// writerProxy is the reader side of reliable delivery from one remote
// writer (RTPS 2.5, 8.4.12, a stateful reader's WriterProxy): it orders what
// arrives, fills the gaps the writer declares, and says what is missing when
// the writer asks.
//
// It does no I/O and keeps no clock: the participant feeds it submessages
// and sends the ACKNACKs it returns.
type writerProxy struct {
	next      rtps.SequenceNumber // the first not yet delivered
	pending   map[rtps.SequenceNumber]*Sample
	gaps      map[rtps.SequenceNumber]bool
	frags     map[rtps.SequenceNumber]*reassembly
	lastHB    int32
	haveHB    bool
	ackCount  int32
	delivered int64
}

func newWriterProxy() *writerProxy {
	return &writerProxy{
		next:    1,
		pending: map[rtps.SequenceNumber]*Sample{},
		gaps:    map[rtps.SequenceNumber]bool{},
		frags:   map[rtps.SequenceNumber]*reassembly{},
	}
}

// data takes a DATA and returns what can now be delivered in order. The
// sample's slices are copied: d aliases a receive buffer.
func (w *writerProxy) data(d rtps.Data) []Sample {
	if d.SN < w.next || w.has(d.SN) || d.SN-w.next >= maxPending {
		return nil
	}
	s := &Sample{SN: d.SN, InlineQoS: copyParams(d.InlineQoS), KeyOnly: d.KeyOnly}
	if d.Payload != nil {
		s.Payload = append([]byte(nil), d.Payload...)
	}
	w.pending[d.SN] = s
	delete(w.frags, d.SN)
	return w.flush()
}

// fragment takes a DATA_FRAG; once a sample's fragments are all in, it is a
// DATA.
func (w *writerProxy) fragment(f rtps.DataFrag) []Sample {
	if f.SN < w.next || w.has(f.SN) || f.SN-w.next >= maxPending {
		return nil
	}
	r, ok := w.frags[f.SN]
	if !ok {
		if len(w.frags) >= maxReassemblies || f.SampleSize == 0 || f.SampleSize > maxSampleSize {
			return nil
		}
		r = newReassembly(f.SampleSize, f.FragmentSize)
		w.frags[f.SN] = r
	}
	if f.FragmentStart == 1 {
		r.inlineQoS = copyParams(f.InlineQoS)
	}
	if !r.add(f) {
		return nil
	}
	delete(w.frags, f.SN)
	return w.data(rtps.Data{SN: f.SN, InlineQoS: r.inlineQoS, Payload: r.buf})
}

// gap takes a GAP: the numbers it covers will never come.
func (w *writerProxy) gap(g rtps.Gap) []Sample {
	for sn := max(g.Start, w.next); sn < g.List.Base; sn++ {
		if sn-w.next >= maxPending {
			break
		}
		w.markGap(sn)
	}
	for _, sn := range g.List.Members() {
		if sn >= w.next && sn-w.next < maxPending {
			w.markGap(sn)
		}
	}
	return w.flush()
}

func (w *writerProxy) markGap(sn rtps.SequenceNumber) {
	if _, ok := w.pending[sn]; !ok {
		w.gaps[sn] = true
	}
	delete(w.frags, sn)
}

// heartbeat takes a HEARTBEAT and returns what can now be delivered and,
// if the writer is to be answered, the ACKNACK state to answer with.
//
// What the writer no longer has (before First) is given up. The answer
// acknowledges everything before the first missing number and asks for
// what is missing up to Last, at most 256 numbers at a time. A final
// heartbeat is answered only if something is missing.
func (w *writerProxy) heartbeat(h rtps.Heartbeat) ([]Sample, *rtps.SequenceNumberSet) {
	if w.haveHB && h.Count <= w.lastHB {
		return nil, nil
	}
	w.haveHB, w.lastHB = true, h.Count

	var out []Sample
	if h.First > w.next {
		for sn := range w.pending {
			if sn < h.First {
				delete(w.pending, sn)
			}
		}
		for sn := range w.gaps {
			if sn < h.First {
				delete(w.gaps, sn)
			}
		}
		for sn := range w.frags {
			if sn < h.First {
				delete(w.frags, sn)
			}
		}
		w.next = h.First
		out = w.flush()
	}

	set := rtps.SequenceNumberSet{Base: w.next}
	for sn := w.next; sn <= h.Last && sn-w.next < 256; sn++ {
		if !w.has(sn) {
			set.Add(sn)
		}
	}
	if set.NumBits == 0 && h.Final {
		return out, nil
	}
	return out, &set
}

// preemptive is the ACKNACK a reader sends a writer it has just matched,
// before any heartbeat: nothing received, nothing asked, answer me.
func (w *writerProxy) preemptive() rtps.SequenceNumberSet {
	return rtps.SequenceNumberSet{Base: w.next}
}

// nextAckCount numbers the ACKNACKs to this writer.
func (w *writerProxy) nextAckCount() int32 {
	w.ackCount++
	return w.ackCount
}

func (w *writerProxy) has(sn rtps.SequenceNumber) bool {
	_, p := w.pending[sn]
	return p || w.gaps[sn]
}

// flush delivers the run of samples from next on, skipping gaps.
func (w *writerProxy) flush() []Sample {
	var out []Sample
	for {
		if s, ok := w.pending[w.next]; ok {
			out = append(out, *s)
			delete(w.pending, w.next)
			w.delivered++
		} else if w.gaps[w.next] {
			delete(w.gaps, w.next)
		} else {
			return out
		}
		w.next++
	}
}

func copyParams(pl rtps.ParameterList) rtps.ParameterList {
	if pl.Params == nil {
		return rtps.ParameterList{}
	}
	out := rtps.ParameterList{Order: pl.Order, Params: make([]rtps.Parameter, len(pl.Params))}
	for i, p := range pl.Params {
		out.Params[i] = rtps.Parameter{ID: p.ID, Value: append([]byte(nil), p.Value...)}
	}
	return out
}

// reassembly collects the fragments of one sample.
type reassembly struct {
	buf       []byte
	fragSize  int
	have      []bool
	missing   int
	inlineQoS rtps.ParameterList
}

func newReassembly(size uint32, fragSize uint16) *reassembly {
	n := (int(size) + int(fragSize) - 1) / int(fragSize)
	return &reassembly{buf: make([]byte, size), fragSize: int(fragSize), have: make([]bool, n), missing: n}
}

// add copies the fragments in; it reports whether the sample is complete.
func (r *reassembly) add(f rtps.DataFrag) bool {
	if int(f.FragmentSize) != r.fragSize || int(f.SampleSize) != len(r.buf) {
		return false
	}
	data := f.Fragments
	for i := range int(f.FragmentsInSub) {
		idx := int(f.FragmentStart) - 1 + i
		if idx < 0 || idx >= len(r.have) {
			return false
		}
		start := idx * r.fragSize
		end := min(start+r.fragSize, len(r.buf))
		n := end - start
		if len(data) < n {
			return false
		}
		if !r.have[idx] {
			copy(r.buf[start:end], data[:n])
			r.have[idx] = true
			r.missing--
		}
		data = data[n:]
	}
	return r.missing == 0
}
