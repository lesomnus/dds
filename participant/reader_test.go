package participant

import (
	"testing"

	"github.com/lesomnus/dds/rtps"
)

func dataSN(sn rtps.SequenceNumber) rtps.Data {
	return rtps.Data{SN: sn, Payload: []byte{byte(sn)}}
}

func sns(ss []Sample) []rtps.SequenceNumber {
	out := make([]rtps.SequenceNumber, len(ss))
	for i, s := range ss {
		out[i] = s.SN
	}
	return out
}

func eq(a []rtps.SequenceNumber, b ...rtps.SequenceNumber) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestInOrderIsDeliveredAsItComes(t *testing.T) {
	w := newWriterProxy()
	for sn := rtps.SequenceNumber(1); sn <= 3; sn++ {
		if got := sns(w.data(dataSN(sn))); !eq(got, sn) {
			t.Fatalf("sn %d delivered %v", sn, got)
		}
	}
}

func TestOutOfOrderWaitsForTheHole(t *testing.T) {
	w := newWriterProxy()
	if got := w.data(dataSN(2)); len(got) != 0 {
		t.Fatalf("delivered %v before 1", sns(got))
	}
	w.data(dataSN(3))
	if got := sns(w.data(dataSN(1))); !eq(got, 1, 2, 3) {
		t.Fatalf("delivered %v, want 1 2 3", got)
	}
	if got := w.data(dataSN(2)); len(got) != 0 {
		t.Errorf("a duplicate was delivered again: %v", sns(got))
	}
}

func TestAHeartbeatIsAnsweredWithWhatIsMissing(t *testing.T) {
	w := newWriterProxy()
	w.data(dataSN(1))
	w.data(dataSN(3))
	_, ack := w.heartbeat(rtps.Heartbeat{First: 1, Last: 5, Count: 1})
	if ack == nil {
		t.Fatal("no answer")
	}
	if ack.Base != 2 || !eq(ack.Members(), 2, 4, 5) {
		t.Errorf("base %d missing %v, want base 2 missing 2 4 5", ack.Base, ack.Members())
	}
	// The same heartbeat again (same count) is not answered twice.
	if _, again := w.heartbeat(rtps.Heartbeat{First: 1, Last: 5, Count: 1}); again != nil {
		t.Error("an old heartbeat was answered")
	}
}

func TestAFinalHeartbeatWithNothingMissingNeedsNoAnswer(t *testing.T) {
	w := newWriterProxy()
	w.data(dataSN(1))
	w.data(dataSN(2))
	if _, ack := w.heartbeat(rtps.Heartbeat{First: 1, Last: 2, Count: 1, Final: true}); ack != nil {
		t.Errorf("answered %+v", ack)
	}
	_, ack := w.heartbeat(rtps.Heartbeat{First: 1, Last: 2, Count: 2})
	if ack == nil || ack.Base != 3 || ack.NumBits != 0 {
		t.Errorf("a non-final heartbeat is acknowledged: %+v", ack)
	}
}

func TestWhatTheWriterNoLongerHasIsGivenUp(t *testing.T) {
	w := newWriterProxy()
	w.data(dataSN(5))
	// The writer has 5..7 only: 1..4 are gone, and 5 can go out.
	got, ack := w.heartbeat(rtps.Heartbeat{First: 5, Last: 7, Count: 1})
	if !eq(sns(got), 5) {
		t.Fatalf("delivered %v, want 5 once 1..4 are gone", sns(got))
	}
	if ack == nil || ack.Base != 6 || !eq(ack.Members(), 6, 7) {
		t.Errorf("ack %+v", ack)
	}
	// Still holding back behind a hole the writer does have.
	w2 := newWriterProxy()
	w2.data(dataSN(5))
	if got, _ := w2.heartbeat(rtps.Heartbeat{First: 4, Last: 6, Count: 1}); len(got) != 0 {
		t.Errorf("delivered %v with 4 still to come", sns(got))
	}
}

func TestAGapIsSkipped(t *testing.T) {
	w := newWriterProxy()
	w.data(dataSN(4))
	list := rtps.SequenceNumberSet{Base: 3}
	got := w.gap(rtps.Gap{Start: 1, List: list}) // 1 and 2 will never come
	if len(got) != 0 {
		t.Fatalf("delivered %v with 3 missing", sns(got))
	}
	list = rtps.SequenceNumberSet{Base: 3}
	list.Add(3)
	if got := sns(w.gap(rtps.Gap{Start: 3, List: list})); !eq(got, 4) {
		t.Fatalf("delivered %v, want 4", got)
	}
	if _, ack := w.heartbeat(rtps.Heartbeat{First: 1, Last: 4, Count: 1}); ack == nil || ack.Base != 5 || ack.NumBits != 0 {
		t.Errorf("ack %+v, want everything acknowledged", ack)
	}
}

func TestFragmentsAreReassembled(t *testing.T) {
	w := newWriterProxy()
	payload := make([]byte, 2500)
	for i := range payload {
		payload[i] = byte(i)
	}
	frag := func(start, n int) rtps.DataFrag {
		from := (start - 1) * 1000
		to := min(from+n*1000, len(payload))
		return rtps.DataFrag{SN: 1, FragmentStart: uint32(start), FragmentsInSub: uint16(n), FragmentSize: 1000,
			SampleSize: uint32(len(payload)), Fragments: payload[from:to]}
	}
	if got := w.fragment(frag(3, 1)); len(got) != 0 {
		t.Fatal("delivered a part")
	}
	if got := w.fragment(frag(1, 1)); len(got) != 0 {
		t.Fatal("delivered a part")
	}
	got := w.fragment(frag(2, 1))
	if len(got) != 1 || string(got[0].Payload) != string(payload) {
		t.Fatalf("reassembled %d samples, payload ok %v", len(got), len(got) == 1 && string(got[0].Payload) == string(payload))
	}
	// Missing fragments mean a missing sample: the heartbeat asks for it whole.
	w.fragment(rtps.DataFrag{SN: 2, FragmentStart: 1, FragmentsInSub: 1, FragmentSize: 1000, SampleSize: 2500, Fragments: payload[:1000]})
	if _, ack := w.heartbeat(rtps.Heartbeat{First: 1, Last: 2, Count: 1}); ack == nil || !eq(ack.Members(), 2) {
		t.Errorf("ack %+v, want 2 asked again", ack)
	}
}

func TestFarAheadIsRefused(t *testing.T) {
	w := newWriterProxy()
	if got := w.data(dataSN(maxPending + 5)); len(got) != 0 || len(w.pending) != 0 {
		t.Errorf("held a sample %d ahead", maxPending+5)
	}
	if got := w.fragment(rtps.DataFrag{SN: 1, FragmentStart: 1, FragmentsInSub: 1, FragmentSize: 1, SampleSize: maxSampleSize + 1}); len(got) != 0 || len(w.frags) != 0 {
		t.Error("reassembling a sample past the size limit")
	}
}

func TestPayloadsAreCopied(t *testing.T) {
	w := newWriterProxy()
	buf := []byte{1, 2, 3}
	got := w.data(rtps.Data{SN: 1, Payload: buf})
	buf[0] = 9
	if got[0].Payload[0] != 1 {
		t.Error("the sample aliases the receive buffer")
	}
}

// FuzzWriterProxy drives a writer proxy with arbitrary submessages: it must
// not panic, deliver a number twice, deliver out of order, or hold more than
// its bounds.
func FuzzWriterProxy(f *testing.F) {
	f.Add([]byte{0, 1, 0, 3, 0, 2, 2, 1, 5, 1, 1, 3})
	f.Fuzz(func(t *testing.T, ops []byte) {
		w := newWriterProxy()
		last := rtps.SequenceNumber(0)
		check := func(ss []Sample) {
			for _, s := range ss {
				if s.SN <= last {
					t.Fatalf("delivered %d after %d", s.SN, last)
				}
				last = s.SN
			}
		}
		for i := 0; i+2 < len(ops); i += 3 {
			a := rtps.SequenceNumber(ops[i+1])
			b := rtps.SequenceNumber(ops[i+2])
			switch ops[i] % 4 {
			case 0:
				check(w.data(dataSN(a)))
			case 1:
				ss, _ := w.heartbeat(rtps.Heartbeat{First: a, Last: b, Count: int32(i)})
				check(ss)
			case 2:
				list := rtps.SequenceNumberSet{Base: b}
				list.Add(b)
				check(w.gap(rtps.Gap{Start: a, List: list}))
			case 3:
				check(w.fragment(rtps.DataFrag{SN: a, FragmentStart: uint32(b%4) + 1, FragmentsInSub: 1, FragmentSize: 4, SampleSize: 13, Fragments: []byte{1, 2, 3, 4}}))
			}
			if len(w.pending) > maxPending || len(w.frags) > maxReassemblies {
				t.Fatalf("holding %d pending, %d reassemblies", len(w.pending), len(w.frags))
			}
		}
	})
}
