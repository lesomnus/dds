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
	_, ack, _ := w.heartbeat(rtps.Heartbeat{First: 1, Last: 5, Count: 1})
	if ack == nil {
		t.Fatal("no answer")
	}
	if ack.Base != 2 || !eq(ack.Members(), 2, 4, 5) {
		t.Errorf("base %d missing %v, want base 2 missing 2 4 5", ack.Base, ack.Members())
	}
	// The same heartbeat again (same count) is not answered twice.
	if _, again, _ := w.heartbeat(rtps.Heartbeat{First: 1, Last: 5, Count: 1}); again != nil {
		t.Error("an old heartbeat was answered")
	}
}

func TestAFinalHeartbeatWithNothingMissingNeedsNoAnswer(t *testing.T) {
	w := newWriterProxy()
	w.data(dataSN(1))
	w.data(dataSN(2))
	if _, ack, _ := w.heartbeat(rtps.Heartbeat{First: 1, Last: 2, Count: 1, Final: true}); ack != nil {
		t.Errorf("answered %+v", ack)
	}
	_, ack, _ := w.heartbeat(rtps.Heartbeat{First: 1, Last: 2, Count: 2})
	if ack == nil || ack.Base != 3 || ack.NumBits != 0 {
		t.Errorf("a non-final heartbeat is acknowledged: %+v", ack)
	}
}

func TestWhatTheWriterNoLongerHasIsGivenUp(t *testing.T) {
	w := newWriterProxy()
	w.data(dataSN(5))
	// The writer has 5..7 only: 1..4 are gone, and 5 can go out.
	got, ack, _ := w.heartbeat(rtps.Heartbeat{First: 5, Last: 7, Count: 1})
	if !eq(sns(got), 5) {
		t.Fatalf("delivered %v, want 5 once 1..4 are gone", sns(got))
	}
	if ack == nil || ack.Base != 6 || !eq(ack.Members(), 6, 7) {
		t.Errorf("ack %+v", ack)
	}
	// Still holding back behind a hole the writer does have.
	w2 := newWriterProxy()
	w2.data(dataSN(5))
	if got, _, _ := w2.heartbeat(rtps.Heartbeat{First: 4, Last: 6, Count: 1}); len(got) != 0 {
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
	if _, ack, _ := w.heartbeat(rtps.Heartbeat{First: 1, Last: 4, Count: 1}); ack == nil || ack.Base != 5 || ack.NumBits != 0 {
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
	// A sample partly in is not asked for whole: its missing fragments are.
	w.fragment(rtps.DataFrag{SN: 2, FragmentStart: 1, FragmentsInSub: 1, FragmentSize: 1000, SampleSize: 2500, Fragments: payload[:1000]})
	_, ack, nacks := w.heartbeat(rtps.Heartbeat{First: 1, Last: 3, Count: 1})
	if ack == nil || ack.Base != 2 || !eq(ack.Members(), 3) {
		t.Errorf("ack %+v, want 3 asked for and 2 not", ack)
	}
	if len(nacks) != 1 || nacks[0].SN != 2 || fmtU(nacks[0].Missing.Members()) != "2 3" {
		t.Errorf("nack_frags %+v, want fragments 2 3 of 2", nacks)
	}
}

func fmtU(ns []uint32) string {
	out := ""
	for i, n := range ns {
		if i > 0 {
			out += " "
		}
		out += string(rune('0' + n))
	}
	return out
}

// TestARetransmittedFirstFragmentIsFollowedByNackFrag is what Cyclone does
// with history: it sends the first fragment of a fragmented sample again,
// then a heartbeat, and sends the rest only when NACK_FRAG asks.
func TestARetransmittedFirstFragmentIsFollowedByNackFrag(t *testing.T) {
	w := newWriterProxy()
	payload := make([]byte, 3000)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	frag := func(n int) rtps.DataFrag {
		from := (n - 1) * 1000
		return rtps.DataFrag{SN: 1, FragmentStart: uint32(n), FragmentsInSub: 1, FragmentSize: 1000, SampleSize: 3000, Fragments: payload[from : from+1000]}
	}
	w.fragment(frag(1))
	_, ack, nacks := w.heartbeat(rtps.Heartbeat{First: 1, Last: 1, Count: 1, Final: true})
	if ack == nil || ack.NumBits != 0 || len(nacks) != 1 || fmtU(nacks[0].Missing.Members()) != "2 3" {
		t.Fatalf("a final heartbeat is still answered while fragments are missing: ack %+v nacks %+v", ack, nacks)
	}
	if got := w.fragment(frag(3)); len(got) != 0 {
		t.Fatal("complete too early")
	}
	if n := w.heartbeatFrag(rtps.HeartbeatFrag{SN: 1, LastFragment: 3, Count: 1}); n == nil || fmtU(n.Missing.Members()) != "2" {
		t.Fatalf("heartbeat_frag answer %+v, want fragment 2", n)
	}
	got := w.fragment(frag(2))
	if len(got) != 1 || string(got[0].Payload) != string(payload) {
		t.Fatal("not reassembled")
	}
	if n := w.heartbeatFrag(rtps.HeartbeatFrag{SN: 1, LastFragment: 3, Count: 2}); n != nil {
		t.Errorf("answered a heartbeat_frag for a sample delivered: %+v", n)
	}
}

func TestAHeartbeatFragForAnUnseenSampleAsksForAll(t *testing.T) {
	w := newWriterProxy()
	n := w.heartbeatFrag(rtps.HeartbeatFrag{SN: 1, LastFragment: 3, Count: 1})
	if n == nil || fmtU(n.Missing.Members()) != "1 2 3" {
		t.Errorf("%+v", n)
	}
	if again := w.heartbeatFrag(rtps.HeartbeatFrag{SN: 1, LastFragment: 3, Count: 1}); again != nil {
		t.Error("the same heartbeat_frag answered twice")
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
			switch ops[i] % 5 {
			case 0:
				check(w.data(dataSN(a)))
			case 1:
				ss, _, _ := w.heartbeat(rtps.Heartbeat{First: a, Last: b, Count: int32(i)})
				check(ss)
			case 2:
				list := rtps.SequenceNumberSet{Base: b}
				list.Add(b)
				check(w.gap(rtps.Gap{Start: a, List: list}))
			case 4:
				_ = w.heartbeatFrag(rtps.HeartbeatFrag{SN: a, LastFragment: uint32(b % 8), Count: int32(i)})
			case 3:
				check(w.fragment(rtps.DataFrag{SN: a, FragmentStart: uint32(b%4) + 1, FragmentsInSub: 1, FragmentSize: 4, SampleSize: 13, Fragments: []byte{1, 2, 3, 4}}))
			}
			if len(w.pending) > maxPending || len(w.frags) > maxReassemblies {
				t.Fatalf("holding %d pending, %d reassemblies", len(w.pending), len(w.frags))
			}
		}
	})
}
