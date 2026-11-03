package traffic

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/lesomnus/dds/rtps"
)

var (
	pubPrefix = rtps.GUIDPrefix{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	subPrefix = rtps.GUIDPrefix{12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	vendor    = rtps.VendorID{0x01, 0x10}
	userW     = rtps.EntityID{0, 0, 0x12, 0x03} // a user writer, keyless
	userR     = rtps.EntityID{0, 0, 0x13, 0x04}
	t0        = time.Unix(1_800_000_000, 0)
)

func payload(n int) []byte {
	b := make([]byte, n)
	copy(b, []byte{0, 1, 0, 0}) // CDR_LE
	return b
}

func data(sn rtps.SequenceNumber, size int) []byte {
	return rtps.NewBuilder(pubPrefix, vendor).InfoTS(t0).Data(userR, userW, sn, nil, payload(size), false).Bytes()
}

func TestSamplesFromSequenceNumbers(t *testing.T) {
	s := NewStats()
	// Seen from 10 on, with 13 and 15 missed and 12 seen twice.
	for i, sn := range []rtps.SequenceNumber{10, 11, 12, 12, 14, 16} {
		s.Observe(data(sn, 100), t0.Add(time.Duration(i)*time.Second))
	}
	w, ok := s.Writer(rtps.GUID{Prefix: pubPrefix, Entity: userW})
	if !ok {
		t.Fatal("writer not seen")
	}
	if w.Samples() != 6 {
		t.Errorf("samples %d, want 6 (11..16)", w.Samples())
	}
	// 11, 12, 14 and 16 sized (12 once); 13 and 15 estimated.
	if w.Sized != 4 || w.SizedBytes != 400 || w.Bytes() != 600 {
		t.Errorf("sized %d (%d bytes), bytes %d", w.Sized, w.SizedBytes, w.Bytes())
	}
	if !w.FirstSeen.Equal(t0) || !w.LastSeen.Equal(t0.Add(5*time.Second)) {
		t.Errorf("seen %s..%s", w.FirstSeen, w.LastSeen)
	}
}

func TestHeartbeatAdvances(t *testing.T) {
	s := NewStats()
	s.Observe(rtps.NewBuilder(pubPrefix, vendor).Heartbeat(rtps.EntityID{}, userW, 1, 5, 1, false).Bytes(), t0)
	s.Observe(rtps.NewBuilder(pubPrefix, vendor).Heartbeat(rtps.EntityID{}, userW, 1, 25, 2, false).Bytes(), t0.Add(time.Second))
	w, _ := s.Writer(rtps.GUID{Prefix: pubPrefix, Entity: userW})
	if w.Samples() != 20 || w.Bytes() != 0 {
		t.Errorf("samples %d bytes %d", w.Samples(), w.Bytes())
	}
}

func TestCutShort(t *testing.T) {
	s := NewStats()
	s.Observe(data(1, 64), t0)
	// A big sample, of which the capture kept 60 bytes: its size is what
	// the submessage says.
	full := data(2, 10_000)
	s.Observe(full[:60], t0)
	w, _ := s.Writer(rtps.GUID{Prefix: pubPrefix, Entity: userW})
	if w.Samples() != 1 || w.SizedBytes != 10_000 {
		t.Errorf("samples %d, sized bytes %d", w.Samples(), w.SizedBytes)
	}
}

func TestBuiltinIgnored(t *testing.T) {
	s := NewStats()
	b := rtps.NewBuilder(pubPrefix, vendor).Data(rtps.EntityID{}, rtps.EntitySPDPWriter, 3, nil, payload(40), false).Bytes()
	s.Observe(b, t0)
	if ws := s.Writers(); len(ws) != 0 {
		t.Errorf("%+v", ws)
	}
}

func TestRetransmitRequests(t *testing.T) {
	s := NewStats()
	s.Observe(data(1, 10), t0)
	var set rtps.SequenceNumberSet
	set.Base = 3
	set.Add(3)
	set.Add(5)
	ack := rtps.NewBuilder(subPrefix, vendor).InfoDst(pubPrefix).AckNack(userR, userW, set, 7, false).Bytes()
	s.Observe(ack, t0)
	s.Observe(ack, t0) // the same, seen again: not counted twice
	var none rtps.SequenceNumberSet
	none.Base = 6
	s.Observe(rtps.NewBuilder(subPrefix, vendor).InfoDst(pubPrefix).AckNack(userR, userW, none, 8, false).Bytes(), t0)

	var frags rtps.FragmentNumberSet
	frags.Base = 2
	frags.Add(2)
	frags.Add(3)
	frags.Add(4)
	s.Observe(rtps.NewBuilder(subPrefix, vendor).InfoDst(pubPrefix).NackFrag(userR, userW, 5, frags, 1).Bytes(), t0)

	w, _ := s.Writer(rtps.GUID{Prefix: pubPrefix, Entity: userW})
	if w.RetransmitRequests != 2 || w.FragmentRequests != 3 {
		t.Errorf("retransmit %d fragment %d", w.RetransmitRequests, w.FragmentRequests)
	}
	// Without INFO_DST the writer's participant is unknown: not counted.
	s.Observe(rtps.NewBuilder(subPrefix, vendor).AckNack(userR, userW, set, 9, false).Bytes(), t0)
	if w, _ = s.Writer(rtps.GUID{Prefix: pubPrefix, Entity: userW}); w.RetransmitRequests != 2 {
		t.Errorf("retransmit %d", w.RetransmitRequests)
	}
}

// dataFrag builds a DATA_FRAG by hand: the builder has none.
func dataFrag(sn rtps.SequenceNumber, start uint32, sampleSize uint32) []byte {
	m := rtps.NewBuilder(pubPrefix, vendor).Bytes()
	body := make([]byte, 32, 32+64)
	le := binary.LittleEndian
	le.PutUint16(body[2:4], 28)
	copy(body[4:8], userR[:])
	copy(body[8:12], userW[:])
	le.PutUint32(body[12:16], uint32(sn>>32))
	le.PutUint32(body[16:20], uint32(sn))
	le.PutUint32(body[20:24], start)
	le.PutUint16(body[24:26], 1)
	le.PutUint16(body[26:28], 64)
	le.PutUint32(body[28:32], sampleSize)
	body = append(body, make([]byte, 64)...)
	m = append(m, byte(rtps.SubDataFrag), 0x01, 0, 0)
	le.PutUint16(m[len(m)-2:], uint16(len(body)))
	return append(m, body...)
}

func TestFragmentsSizedOnce(t *testing.T) {
	s := NewStats()
	s.Observe(data(1, 10), t0)
	for f := uint32(1); f <= 4; f++ {
		s.Observe(dataFrag(2, f, 256), t0)
	}
	s.Observe(dataFrag(2, 3, 256), t0) // retransmitted
	w, _ := s.Writer(rtps.GUID{Prefix: pubPrefix, Entity: userW})
	if w.Samples() != 1 || w.Sized != 1 || w.SizedBytes != 256 {
		t.Errorf("samples %d sized %d (%d bytes)", w.Samples(), w.Sized, w.SizedBytes)
	}
}

func TestForgetParticipant(t *testing.T) {
	s := NewStats()
	s.Observe(data(1, 10), t0)
	s.ForgetParticipant(pubPrefix)
	if len(s.Writers()) != 0 {
		t.Error("not forgotten")
	}
}

func TestMaxWriters(t *testing.T) {
	s := NewStats()
	s.MaxWriters = 1
	s.Observe(data(1, 10), t0)
	other := rtps.NewBuilder(subPrefix, vendor).Data(userR, userW, 1, nil, payload(10), false).Bytes()
	s.Observe(other, t0)
	if n := len(s.Writers()); n != 1 {
		t.Errorf("%d writers", n)
	}
}

func FuzzObserve(f *testing.F) {
	f.Add(data(1, 100))
	f.Add(dataFrag(2, 1, 1000))
	f.Fuzz(func(t *testing.T, b []byte) {
		s := NewStats()
		s.Observe(b, t0)
		for _, w := range s.Writers() {
			if w.Samples() < 0 || w.Bytes() < 0 {
				t.Fatalf("%+v", w)
			}
		}
	})
}

// BenchmarkObserve is the cost of one captured DATA message.
func BenchmarkObserve(b *testing.B) {
	s := NewStats()
	msgs := make([][]byte, 1024)
	for i := range msgs {
		msgs[i] = data(rtps.SequenceNumber(i+1), 2000)[:256]
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		s.Observe(msgs[i%len(msgs)], t0)
	}
}
