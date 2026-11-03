package participant

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"testing"
	"time"

	"github.com/lesomnus/dds/rtps"
)

var (
	wPrefix = rtps.GUIDPrefix{0xaa, 1}
	rGUID   = rtps.GUID{Prefix: rtps.GUIDPrefix{0xbb, 2}, Entity: rtps.EntityID{0, 0, 1, 0x04}}
	wID     = rtps.EntityID{0, 0, 1, 0x03}
	addr    = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:7411")}
)

// link carries a writer's messages to one reader (a writerProxy), losing
// what drop says, and the reader's answers back.
type link struct {
	t     *testing.T
	w     *stateWriter
	r     *writerProxy
	got   []Sample
	drop  func(msg []byte) bool
	now   time.Time
	acks  int
	nacks int
}

func newLink(t *testing.T, cfg writerConfig) *link {
	cfg.id = wID
	return &link{t: t, w: newStateWriter(wPrefix, cfg), r: newWriterProxy(), now: time.Unix(1_800_000_000, 0)}
}

// deliver feeds messages to the reader, and its answers to the writer, until
// nothing more is said.
func (l *link) deliver(out []outbound) {
	for rounds := 0; len(out) > 0; rounds++ {
		if rounds > 10_000 {
			l.t.Fatal("no end to the conversation")
		}
		var next []outbound
		for _, o := range out {
			if l.drop != nil && l.drop(o.msg) {
				continue
			}
			next = append(next, l.receive(o.msg)...)
		}
		out = next
	}
}

func (l *link) receive(b []byte) []outbound {
	m, err := rtps.Parse(b)
	if err != nil {
		l.t.Fatalf("writer sent garbage: %v", err)
	}
	if m.Header.Prefix != wPrefix {
		l.t.Fatalf("from %s", m.Header.Prefix)
	}
	var out []outbound
	for _, s := range m.Submessages {
		switch s.ID {
		case rtps.SubInfoDst:
			if d, _ := rtps.ParseInfoDst(s); d != rGUID.Prefix {
				l.t.Fatalf("addressed to %s", d)
			}
		case rtps.SubData:
			d, err := rtps.ParseData(s)
			if err != nil || d.Reader != rGUID.Entity || d.Writer != wID {
				l.t.Fatalf("DATA %+v %v", d, err)
			}
			l.got = append(l.got, l.r.data(d)...)
		case rtps.SubDataFrag:
			f, err := rtps.ParseDataFrag(s)
			if err != nil {
				l.t.Fatal(err)
			}
			l.got = append(l.got, l.r.fragment(f)...)
		case rtps.SubGap:
			g, err := rtps.ParseGap(s)
			if err != nil {
				l.t.Fatal(err)
			}
			l.got = append(l.got, l.r.gap(g)...)
		case rtps.SubHeartbeat:
			h, err := rtps.ParseHeartbeat(s)
			if err != nil {
				l.t.Fatal(err)
			}
			ss, ack, nacks := l.r.heartbeat(h)
			l.got = append(l.got, ss...)
			for _, n := range nacks {
				l.nacks++
				out = append(out, l.w.onNackFrag(rGUID, rtps.NackFrag{Reader: rGUID.Entity, Writer: wID, SN: n.SN, Missing: n.Missing, Count: l.r.nextNackCount()})...)
			}
			if ack != nil {
				l.acks++
				out = append(out, l.w.onAckNack(rGUID, rtps.AckNack{Reader: rGUID.Entity, Writer: wID, State: *ack, Count: l.r.nextAckCount()}, l.now)...)
			}
		}
	}
	return out
}

// tick moves the clock a heartbeat period on.
func (l *link) tick() {
	l.now = l.now.Add(heartbeatPeriod)
	l.deliver(l.w.tick(l.now))
}

func (l *link) write(payload []byte) {
	l.t.Helper()
	_, out, err := l.w.write(change{at: l.now, payload: payload})
	if err != nil {
		l.t.Fatal(err)
	}
	l.deliver(out)
}

func sample(i, size int) []byte {
	b := bytes.Repeat([]byte{byte(i)}, size)
	copy(b, []byte{0, 1, 0, 0})
	return b
}

func (l *link) payloads() []int {
	var out []int
	for _, s := range l.got {
		if s.Payload != nil {
			out = append(out, int(s.Payload[len(s.Payload)-1]))
		}
	}
	return out
}

func TestReliableInOrder(t *testing.T) {
	l := newLink(t, writerConfig{reliable: true, depth: 10})
	l.deliver(l.w.addReader(rGUID, addr, true, l.now))
	for i := 1; i <= 5; i++ {
		l.write(sample(i, 32))
	}
	if got := fmt.Sprint(l.payloads()); got != "[1 2 3 4 5]" {
		t.Errorf("got %s", got)
	}
	if r := l.w.readers[rGUID]; r.acked != 6 {
		t.Errorf("acked %d, want 6", r.acked)
	}
	if out := l.w.tick(l.now.Add(time.Hour)); len(out) != 0 {
		t.Error("heartbeats to a reader that has everything")
	}
}

func TestReliableUnderLoss(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	l := newLink(t, writerConfig{reliable: true, depth: 100})
	l.deliver(l.w.addReader(rGUID, addr, true, l.now))
	l.drop = func([]byte) bool { return rng.Float64() < 0.4 }
	for i := 1; i <= 50; i++ {
		l.write(sample(i, 32))
	}
	for range 200 {
		l.tick()
	}
	got := l.payloads()
	if len(got) != 50 {
		t.Fatalf("%d samples of 50: %v", len(got), got)
	}
	for i, v := range got {
		if v != i+1 {
			t.Fatalf("out of order: %v", got)
		}
	}
}

func TestKeepLastGapsWhatIsGone(t *testing.T) {
	l := newLink(t, writerConfig{reliable: true, depth: 3})
	l.deliver(l.w.addReader(rGUID, addr, true, l.now))
	lost := true
	l.drop = func([]byte) bool { return lost }
	for i := 1; i <= 8; i++ {
		l.write(sample(i, 32))
	}
	lost = false
	l.tick()
	// 1..5 are gone from a history of three: the reader is told so, and gets
	// 6..8.
	if got := fmt.Sprint(l.payloads()); got != "[6 7 8]" {
		t.Errorf("got %s", got)
	}
	if r := l.w.readers[rGUID]; r.acked != 9 {
		t.Errorf("acked %d", r.acked)
	}
}

func TestDurability(t *testing.T) {
	for _, tl := range []bool{false, true} {
		l := newLink(t, writerConfig{reliable: true, transientLocal: tl, depth: 2})
		for i := 1; i <= 4; i++ {
			l.write(sample(i, 32)) // nobody matched yet
		}
		l.deliver(l.w.addReader(rGUID, addr, true, l.now))
		l.write(sample(5, 32))
		l.tick()
		want := "[5]"
		if tl {
			want = "[3 4 5]" // the history of two, then the new one
		}
		if got := fmt.Sprint(l.payloads()); got != want {
			t.Errorf("transient local %v: got %s, want %s", tl, got, want)
		}
	}
}

func TestBestEffortReaderGetsNoHeartbeats(t *testing.T) {
	l := newLink(t, writerConfig{reliable: true, depth: 2})
	if out := l.w.addReader(rGUID, addr, false, l.now); len(out) != 0 {
		t.Errorf("%d messages on a best-effort match", len(out))
	}
	_, out, _ := l.w.write(change{at: l.now, payload: sample(1, 8)})
	if len(out) != 1 {
		t.Fatalf("%d messages", len(out))
	}
	m, _ := rtps.Parse(out[0].msg)
	for _, s := range m.Submessages {
		if s.ID == rtps.SubHeartbeat {
			t.Error("a heartbeat to a best-effort reader")
		}
	}
	if out := l.w.tick(l.now.Add(time.Hour)); len(out) != 0 {
		t.Error("heartbeats to a best-effort reader")
	}
}

func TestFragmentsUnderLoss(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	l := newLink(t, writerConfig{reliable: true, depth: 4})
	l.deliver(l.w.addReader(rGUID, addr, true, l.now))
	big := sample(7, 200_000) // 13 fragments, 5 datagrams
	l.drop = func([]byte) bool { return rng.Float64() < 0.3 }
	l.write(big)
	l.write(sample(8, 100))
	for range 100 {
		l.tick()
	}
	if len(l.got) != 2 || !bytes.Equal(l.got[0].Payload, big) || l.got[1].Payload[99] != 8 {
		t.Fatalf("%d samples", len(l.got))
	}
	if l.nacks == 0 {
		t.Log("no NACK_FRAG was needed: the seed lost no fragment of a sample half-received")
	}
	for _, o := range func() []outbound { _, o, _ := l.w.write(change{at: l.now, payload: big}); return o }() {
		if len(o.msg) > 65507 {
			t.Errorf("a datagram of %d octets", len(o.msg))
		}
	}
}

func TestKeepAllFull(t *testing.T) {
	l := newLink(t, writerConfig{reliable: true, keepAll: true, maxSamples: 3})
	l.deliver(l.w.addReader(rGUID, addr, true, l.now))
	l.drop = func([]byte) bool { return true } // nothing is acknowledged
	for i := 1; i <= 3; i++ {
		l.write(sample(i, 8))
	}
	if _, _, err := l.w.write(change{at: l.now, payload: sample(4, 8)}); err != ErrHistoryFull {
		t.Fatalf("err %v", err)
	}
	l.drop = nil
	l.tick()
	if _, _, err := l.w.write(change{at: l.now, payload: sample(4, 8)}); err != nil {
		t.Fatalf("after the acknowledgements: %v", err)
	}
}

func TestKeyedKeepsOnePerInstance(t *testing.T) {
	w := newStateWriter(wPrefix, writerConfig{id: rtps.EntitySEDPSubscriptionsWriter, reliable: true, transientLocal: true, keyed: true})
	a, b := rtps.GUID{Entity: rtps.EntityID{1}}, rtps.GUID{Entity: rtps.EntityID{2}}
	w.write(change{key: a, payload: []byte{0, 3, 0, 0, 1}})
	w.write(change{key: b, payload: []byte{0, 3, 0, 0, 2}})
	w.write(change{key: a, payload: []byte{0, 3, 0, 0, 3}}) // a again: its dispose, say
	if len(w.history) != 2 || w.history[0].sn != 2 || w.history[1].sn != 3 {
		t.Errorf("history %+v", w.history)
	}
}

func TestRuns(t *testing.T) {
	got := runs([]rtps.SequenceNumber{9, 2, 3, 5, 1}, 4)
	if fmt.Sprint(got) != "[[1 3] [5 5] [9 9]]" {
		t.Errorf("%v", got)
	}
}

func TestHeartbeatsUntilTheReaderAnswers(t *testing.T) {
	l := newLink(t, writerConfig{reliable: true, depth: 2})
	// The reader does not know the writer yet: it ignores everything.
	deaf := true
	l.drop = func([]byte) bool { return deaf }
	l.deliver(l.w.addReader(rGUID, addr, true, l.now))
	if len(l.w.ready()) != 0 {
		t.Error("ready before it answered")
	}
	l.now = l.now.Add(heartbeatPeriod)
	if out := l.w.tick(l.now); len(out) != 1 {
		t.Fatalf("%d heartbeats to a reader that has not answered", len(out))
	}
	deaf = false
	l.tick()
	if len(l.w.ready()) != 1 {
		t.Error("not ready after it answered")
	}
	if out := l.w.tick(l.now.Add(time.Hour)); len(out) != 0 {
		t.Error("heartbeats once it answered and has everything")
	}
}

// FuzzStateWriter feeds a writer whatever a reader might send it: ACKNACKs
// and NACK_FRAGs of any content. It must not panic, and what it sends must
// stay within a datagram.
func FuzzStateWriter(f *testing.F) {
	var set rtps.SequenceNumberSet
	set.Base = 1
	set.Add(2)
	var frags rtps.FragmentNumberSet
	frags.Base = 1
	frags.Add(5)
	f.Add(rtps.NewBuilder(rGUID.Prefix, VendorID).AckNack(rGUID.Entity, wID, set, 1, false).NackFrag(rGUID.Entity, wID, 3, frags, 1).Bytes())
	f.Fuzz(func(t *testing.T, b []byte) {
		w := newStateWriter(wPrefix, writerConfig{id: wID, reliable: true, depth: 3})
		now := time.Unix(1_800_000_000, 0)
		w.addReader(rGUID, addr, true, now)
		for i := 1; i <= 4; i++ {
			w.write(change{at: now, payload: sample(i, 70_000)})
		}
		m, _ := rtps.Parse(b)
		var out []outbound
		for _, s := range m.Submessages {
			switch s.ID {
			case rtps.SubAckNack:
				if a, err := rtps.ParseAckNack(s); err == nil {
					out = append(out, w.onAckNack(rGUID, a, now)...)
				}
			case rtps.SubNackFrag:
				if n, err := rtps.ParseNackFrag(s); err == nil {
					out = append(out, w.onNackFrag(rGUID, n)...)
				}
			}
		}
		out = append(out, w.tick(now.Add(time.Hour))...)
		for _, o := range out {
			if len(o.msg) > 65507 {
				t.Fatalf("a datagram of %d octets", len(o.msg))
			}
			if _, err := rtps.Parse(o.msg); err != nil {
				t.Fatalf("sent garbage: %v", err)
			}
		}
	})
}
