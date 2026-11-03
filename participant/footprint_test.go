package participant

import (
	"log/slog"
	"net/netip"
	"runtime"
	"testing"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/rtps"
)

// testParticipant is a Participant without sockets, for feeding it what a
// peer would send.
func testParticipant() *Participant {
	return &Participant{
		cfg:       Config{Now: time.Now, Logger: slog.New(slog.DiscardHandler)},
		log:       slog.New(slog.DiscardHandler),
		peers:     map[rtps.GUIDPrefix]*peer{},
		endpoints: map[rtps.GUID]*discovery.Endpoint{},
		readers:   map[rtps.EntityID]*userReader{},
		sent:      map[netip.AddrPort]int64{},
		nextKey:   1,
	}
}

// endpointSample is an SEDP announcement of a writer with ROS-like names and
// QoS.
func endpointSample(part rtps.GUIDPrefix, i int) Sample {
	g := rtps.GUID{Prefix: part, Entity: rtps.EntityID{byte(i >> 16), byte(i >> 8), byte(i), 0x03}}
	payload := (&rtps.ParameterListWriter{}).
		GUID(rtps.PIDEndpointGUID, g).
		GUID(rtps.PIDParticipantGUID, rtps.GUID{Prefix: part, Entity: rtps.EntityParticipant}).
		String(rtps.PIDTopicName, "rr/some_node_name/describe_parametersReply").
		String(rtps.PIDTypeName, "rcl_interfaces::srv::dds_::DescribeParameters_Response_").
		Raw(rtps.PIDReliability, []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}).
		Raw(rtps.PIDHistory, []byte{0, 0, 0, 0, 0xe8, 0x03, 0, 0}).
		Octets(rtps.PIDUserData, []byte("typehash=RIHS01_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef;")).
		Payload()
	return Sample{SN: rtps.SequenceNumber(i + 1), Payload: payload}
}

// TestEndpointFootprint measures what the participant keeps per remote
// endpoint, the bulk of its memory on a busy host.
func TestEndpointFootprint(t *testing.T) {
	const n = 2000
	p := testParticipant()
	part := rtps.GUIDPrefix{1, 2, 3}
	pe := &peer{data: discovery.Participant{ParticipantData: rtps.ParticipantData{GUID: rtps.GUID{Prefix: part}}}, proxies: map[rtps.GUID]*writerProxy{}}
	p.peers[part] = pe
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	// The samples are made and dropped batch by batch, so that only what the
	// participant keeps is left when the heap is measured again.
	before := heap()
	for i := 0; i < n; i += 100 {
		batch := make([]Sample, 0, 100)
		for j := i; j < i+100; j++ {
			batch = append(batch, endpointSample(part, j))
		}
		p.onSEDP(pe, rtps.Writer, batch)
	}
	after := heap()
	if len(p.endpoints) != n {
		t.Fatalf("%d endpoints, want %d", len(p.endpoints), n)
	}
	per := (int64(after) - int64(before)) / n
	t.Logf("%d endpoints: %d KiB, %d B each", n, (int64(after)-int64(before))/1024, per)
	if per > 2048 {
		t.Errorf("%d B per endpoint", per)
	}
	runtime.KeepAlive(p)
}

// BenchmarkSEDPIngest is the cost of taking in one endpoint announcement.
func BenchmarkSEDPIngest(b *testing.B) {
	p := testParticipant()
	part := rtps.GUIDPrefix{1, 2, 3}
	pe := &peer{data: discovery.Participant{ParticipantData: rtps.ParticipantData{GUID: rtps.GUID{Prefix: part}}}, proxies: map[rtps.GUID]*writerProxy{}}
	p.peers[part] = pe
	samples := make([]Sample, 4096)
	for i := range samples {
		samples[i] = endpointSample(part, i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		p.onSEDP(pe, rtps.Writer, samples[i%len(samples):i%len(samples)+1])
	}
}
