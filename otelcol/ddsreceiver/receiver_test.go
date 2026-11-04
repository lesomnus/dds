package ddsreceiver

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/dds/cdr"
	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/participant"
	"github.com/lesomnus/dds/ros2"
	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/traffic"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/lesomnus/dds/otelcol/ddsreceiver/internal/procmap"
)

var (
	prefix = rtps.GUIDPrefix{0x01, 0x10, 0xaa}
	talker = discovery.Participant{
		ParticipantData: rtps.ParticipantData{
			GUID:               rtps.GUID{Prefix: prefix, Entity: rtps.EntityParticipant},
			Vendor:             rtps.VendorCyclone,
			LeaseDuration:      10 * time.Second,
			UserData:           []byte("enclave=/;"),
			Properties:         map[string]string{"__Hostname": "work", "__Pid": "7", "__ProcessName": "talker"},
			MetatrafficUnicast: []rtps.Locator{{Kind: rtps.LocatorUDPv4, Port: 7410, Addr: netip.MustParseAddr("192.168.10.33")}},
		},
		From:      netip.MustParseAddrPort("192.168.10.33:51000"),
		FirstSeen: time.Unix(1_800_000_000, 0),
		LastSeen:  time.Unix(1_800_000_000, 0),
	}
	chatter = discovery.Endpoint{EndpointData: rtps.EndpointData{
		Kind: rtps.Writer, GUID: rtps.GUID{Prefix: prefix, Entity: rtps.EntityID{0, 0, 0x17, 0x03}}, Participant: prefix,
		Topic: "rt/chatter", Type: "std_msgs::msg::dds_::String_",
		QoS: rtps.QoS{Reliability: rtps.BestEffort, Durability: rtps.Volatile, History: rtps.KeepLast, Depth: 7, Deadline: rtps.DurationInfinite, LeaseDuration: rtps.DurationInfinite},
	}}
	proc = procmap.Process{PID: 4242, Comm: "talker", Start: time.Unix(1_799_999_000, 0), Cgroup: "0::/system.slice/docker-abc.scope\n"}
)

func attrs(t *testing.T, rs plog.LogRecordSlice, i int) map[string]any {
	t.Helper()
	return rs.At(i).Attributes().AsRaw()
}

func TestEventsBecomeRecords(t *testing.T) {
	_, rs := newLogs()
	p := participantState{Participant: talker, Process: proc}
	now := time.Unix(1_800_000_100, 0)
	for _, e := range []any{
		discovery.ParticipantDiscovered{Participant: talker},
		discovery.EndpointDiscovered{Endpoint: chatter},
		ros2.NodeAppeared{Node: ros2.Node{Namespace: "/", Name: "talker", Participant: prefix, Writers: []rtps.GUID{chatter.GUID}}, At: now},
		discovery.ParticipantLost{Participant: talker, Reason: discovery.LeaseExpired, At: now},
	} {
		if !appendEvent(rs, e, p, now, true) {
			t.Fatalf("%T not recorded", e)
		}
	}
	if rs.Len() != 4 {
		t.Fatalf("%d records", rs.Len())
	}
	a := attrs(t, rs, 0)
	for k, want := range map[string]any{
		attrEvent: evParticipantDiscovered, attrDDSProcess: "talker", attrDDSPID: int64(7), attrDDSHost: "work",
		attrProcessPID: int64(4242), attrProcessStart: int64(1_799_999_000), attrEnclave: "/", attrVendor: "cyclonedds",
		attrLease: 10.0,
	} {
		if a[k] != want {
			t.Errorf("participant %s = %v, want %v", k, a[k], want)
		}
	}
	a = attrs(t, rs, 1)
	for k, want := range map[string]any{
		attrEvent: evEndpointDiscovered, attrTopic: "rt/chatter", attrROSName: "/chatter", attrROSKind: "topic",
		attrROSType: "std_msgs/msg/String", attrReliability: "best_effort", attrDepth: int64(7), attrDeadline: -1.0,
		attrProcessPID: int64(4242),
	} {
		if a[k] != want {
			t.Errorf("endpoint %s = %v, want %v", k, a[k], want)
		}
	}
	a = attrs(t, rs, 2)
	if a[attrNode] != "/talker" || a[attrNodeWriters] != int64(1) {
		t.Errorf("node %v", a)
	}
	lost := rs.At(3)
	if lost.SeverityNumber() != plog.SeverityNumberWarn || attrs(t, rs, 3)[attrReason] != "lease_expired" {
		t.Errorf("a lease expiry is a warning with its reason: %v %v", lost.SeverityNumber(), attrs(t, rs, 3))
	}
	if got := lost.Timestamp().AsTime(); !got.Equal(now) {
		t.Errorf("timestamp %s", got)
	}
}

func TestSnapshotHealth(t *testing.T) {
	reader := chatter
	reader.Kind = rtps.Reader
	reader.GUID.Entity = rtps.EntityID{0, 0, 0x18, 0x04}
	reader.QoS.Reliability = rtps.Reliable // asks more than the writer offers
	lonely := chatter
	lonely.Kind, lonely.Topic = rtps.Reader, "rt/nobody_publishes"
	lonely.GUID.Entity = rtps.EntityID{0, 0, 0x19, 0x04}
	service := chatter
	service.Topic, service.GUID.Entity = "rr/talker/get_parametersReply", rtps.EntityID{0, 0, 0x1a, 0x03}

	s := snapshot{Endpoints: []discovery.Endpoint{chatter, reader, lonely, service}}
	h := s.health(true)
	if h.IncompatiblePairs != 1 || h.ReadersWithoutWriters != 1 || h.WritersWithoutReaders != 0 {
		t.Errorf("%+v: want 1 incompatible pair, 1 reader without a writer; services not counted", h)
	}
	md := snapshotMetrics(s, true)
	if md.MetricCount() != 5 {
		t.Errorf("%d metrics", md.MetricCount())
	}
	// Without ROS a service is a topic like another: its writer is unmatched.
	if h := s.health(false); h.WritersWithoutReaders != 1 {
		t.Errorf("%+v: want the service's writer unmatched", h)
	}
	if md := snapshotMetrics(s, false); md.MetricCount() != 4 {
		t.Errorf("without ROS, %d metrics: want no ros.nodes", md.MetricCount())
	}
}

func TestStateFollowsEvents(t *testing.T) {
	s := newState()
	resolve := func(discovery.Participant) procmap.Process { return proc }
	s.apply(discovery.ParticipantDiscovered{Participant: talker}, resolve)
	s.apply(discovery.EndpointDiscovered{Endpoint: chatter}, resolve)
	s.apply(ros2.NodeAppeared{Node: ros2.Node{Name: "talker", Namespace: "/", Participant: prefix}}, resolve)
	snap := s.snapshot(time.Now())
	if len(snap.Participants) != 1 || len(snap.Endpoints) != 1 || len(snap.Nodes) != 1 || snap.Participants[0].Process.PID != 4242 {
		t.Fatalf("%+v", snap)
	}
	_, rs := newLogs()
	appendSnapshot(rs, snap)
	if rs.Len() != 1 || attrs(t, rs, 0)[attrEvent] != evNodeState || attrs(t, rs, 0)[attrProcessPID] != int64(4242) {
		t.Errorf("state record %v", attrs(t, rs, 0))
	}
	p, ok := s.apply(discovery.ParticipantLost{Participant: talker, Reason: discovery.Disposed}, resolve)
	if !ok || p.Process.PID != 4242 {
		t.Errorf("a lost participant is recorded as it was: %+v", p)
	}
	if snap := s.snapshot(time.Now()); len(snap.Participants) != 0 || len(snap.Nodes) != 0 {
		t.Errorf("after the loss: %+v", snap)
	}
}

func TestLocalPortsAreTheParticipantsOwn(t *testing.T) {
	p := talker
	p.DefaultUnicast = []rtps.Locator{
		{Kind: rtps.LocatorUDPv4, Port: 7411, Addr: netip.MustParseAddr("192.168.10.33")},
		{Kind: rtps.LocatorUDPv4, Port: 7412, Addr: netip.MustParseAddr("10.0.0.1")},      // another address: not its sockets here
		{Kind: rtps.LocatorUDPv4, Port: 7410, Addr: netip.MustParseAddr("192.168.10.33")}, // again
	}
	got := localPorts(p)
	if len(got) != 2 || got[0] != 7410 || got[1] != 7411 {
		t.Errorf("ports %v", got)
	}
}

// fakeCapture captures what it is given, then waits.
type fakeCapture struct{ packets [][]byte }

func (f *fakeCapture) Run(ctx context.Context, fn func(traffic.Packet)) error {
	for _, p := range f.packets {
		fn(traffic.Packet{Message: p, At: time.Now()})
	}
	<-ctx.Done()
	return ctx.Err()
}
func (f *fakeCapture) Drops() uint64 { return 3 }
func (f *fakeCapture) Close() error  { return nil }

// chatterData is the talker's chatter sample sn as it goes over the wire, or
// another participant's.
func chatterData(from rtps.GUIDPrefix, sn rtps.SequenceNumber) []byte {
	return rtps.NewBuilder(from, rtps.VendorCyclone).Data(rtps.EntityID{}, chatter.GUID.Entity, sn, nil, make([]byte, 24), false).Bytes()
}

func TestTrafficMetrics(t *testing.T) {
	s := newState()
	resolve := func(discovery.Participant) procmap.Process { return proc }
	s.apply(discovery.ParticipantDiscovered{Participant: talker}, resolve)
	s.apply(discovery.EndpointDiscovered{Endpoint: chatter}, resolve)
	s.apply(ros2.NodeAppeared{Node: ros2.Node{Name: "talker", Namespace: "/", Participant: prefix, Writers: []rtps.GUID{chatter.GUID}}}, resolve)
	stats := traffic.NewStats()
	at := time.Unix(1_800_000_000, 0)
	for sn := rtps.SequenceNumber(1); sn <= 11; sn++ {
		stats.Observe(chatterData(prefix, sn), at)
		stats.Observe(chatterData(rtps.GUIDPrefix{9}, sn), at) // another host's
	}
	ws := s.localWriters(stats.Writers())
	if len(ws) != 1 {
		t.Fatalf("%d writers, want the local one", len(ws))
	}
	md := trafficMetrics(at, at, ws, 3, true)
	got := map[string]pmetric.NumberDataPoint{}
	rm := md.ResourceMetrics().At(0)
	ms := rm.ScopeMetrics().At(0).Metrics()
	for i := range ms.Len() {
		m := ms.At(i)
		if !m.Sum().IsMonotonic() || m.Sum().AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
			t.Errorf("%s is not a cumulative counter", m.Name())
		}
		for j := range m.Sum().DataPoints().Len() {
			dp := m.Sum().DataPoints().At(j)
			key := m.Name()
			if k, ok := dp.Attributes().Get(attrRequestKind); ok {
				key += "/" + k.Str()
			}
			got[key] = dp
		}
	}
	for name, want := range map[string]int64{mWriterSamples: 10, mWriterBytes: 240, mWriterRetransmit + "/sample": 0, mWriterRetransmit + "/fragment": 0, mCaptureDrops: 3} {
		if dp, ok := got[name]; !ok || dp.IntValue() != want {
			t.Errorf("%s = %v, want %d", name, dp.IntValue(), want)
		}
	}
	a := got[mWriterSamples].Attributes().AsRaw()
	for k, want := range map[string]any{
		attrTopic: "rt/chatter", attrROSName: "/chatter", attrNode: "/talker", attrProcessPID: int64(4242),
		attrParticipant: prefix.String(), attrEndpoint: chatter.GUID.String(),
	} {
		if a[k] != want {
			t.Errorf("%s = %v, want %v", k, a[k], want)
		}
	}
	if v, _ := rm.Resource().Attributes().Get("service.name"); v.Str() != "dds" {
		t.Errorf("service.name %q", v.Str())
	}
	// Without ROS, no ROS names.
	a = trafficMetrics(at, at, ws, 0, false).ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).Attributes().AsRaw()
	if _, ok := a[attrROSName]; ok {
		t.Errorf("ROS names without ROS: %v", a)
	}
	if _, ok := a[attrNode]; ok {
		t.Errorf("ROS node without ROS: %v", a)
	}
}

// fakeParticipant replays events when run.
type fakeParticipant struct {
	events func(discovery.Event)
	subs   []participant.Subscription
	replay []discovery.Event
}

func (f *fakeParticipant) Subscribe(s participant.Subscription) error {
	f.subs = append(f.subs, s)
	return nil
}

func (f *fakeParticipant) Run(ctx context.Context) error {
	for _, e := range f.replay {
		f.events(e)
	}
	// And the nodes, as ros_discovery_info would carry them.
	if len(f.subs) > 0 {
		var gid ros2.Gid
		copy(gid.Data[:12], prefix[:])
		copy(gid.Data[12:], rtps.EntityParticipant[:])
		b, _ := cdr.Marshal(&ros2.ParticipantEntitiesInfo{Gid: gid, NodeEntitiesInfoSeq: []ros2.NodeEntitiesInfo{{NodeNamespace: "/", NodeName: "talker"}}})
		f.subs[0].Handle(rtps.GUID{Prefix: prefix, Entity: rtps.EntityID{0, 0, 4, 3}}, participant.Sample{Payload: b})
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestTheReceiverIsSharedAndReports(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Interface = "127.0.0.0/8"
	cfg.SnapshotInterval = 200 * time.Millisecond
	cfg.ProcRoot = t.TempDir() // no /proc: no processes, and that is fine
	cfg.Traffic.CollectionInterval = 200 * time.Millisecond
	set := receivertest.NewNopSettings(f.Type())
	set.ID = component.NewIDWithName(f.Type(), t.Name())

	logs, metrics := new(consumertest.LogsSink), new(consumertest.MetricsSink)
	lr, err := f.CreateLogs(context.Background(), set, cfg, logs)
	if err != nil {
		t.Fatal(err)
	}
	mr, err := f.CreateMetrics(context.Background(), set, cfg, metrics)
	if err != nil {
		t.Fatal(err)
	}
	r := lr.(handle).r
	if mr.(handle).r != r {
		t.Fatal("logs and metrics do not share the participant")
	}
	fake := &fakeParticipant{replay: []discovery.Event{
		discovery.ParticipantDiscovered{Participant: talker},
		discovery.EndpointDiscovered{Endpoint: chatter},
	}}
	var openedMu sync.Mutex
	var opened traffic.CaptureConfig
	r.openCapture = func(c traffic.CaptureConfig) (capturer, error) {
		openedMu.Lock()
		opened = c
		openedMu.Unlock()
		return &fakeCapture{packets: [][]byte{chatterData(prefix, 1), chatterData(prefix, 4)}}, nil
	}
	runs := 0
	r.newParticipant = func(c participant.Config, ev func(discovery.Event)) (runner, error) {
		runs++
		fake.events = ev
		return fake, nil
	}

	host := componenttest.NewNopHost()
	if err := lr.Start(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	if err := mr.Start(context.Background(), host); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Fatalf("%d participants, want one", runs)
	}
	if len(fake.subs) != 1 || fake.subs[0].Topic != ros2.DiscoveryTopic {
		t.Fatalf("subscriptions %+v", fake.subs)
	}
	openedMu.Lock()
	ifs := opened.Interfaces
	openedMu.Unlock()
	if len(ifs) == 0 || ifs[0] != "lo" {
		t.Errorf("captures on %v, want the DDS interface (lo here) first", ifs)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		events := map[string]bool{}
		for _, ld := range logs.AllLogs() {
			rs := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
			for i := range rs.Len() {
				v, _ := rs.At(i).Attributes().Get(attrEvent)
				events[v.Str()] = true
			}
		}
		samples := int64(-1)
		for _, md := range metrics.AllMetrics() {
			ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			for i := range ms.Len() {
				if ms.At(i).Name() == mWriterSamples && ms.At(i).Sum().DataPoints().Len() > 0 {
					samples = ms.At(i).Sum().DataPoints().At(0).IntValue()
				}
			}
		}
		if events[evParticipantDiscovered] && events[evEndpointDiscovered] && events[evNodeAppeared] && events[evNodeState] && samples == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("events %v, %d metric points, %d samples", events, metrics.DataPointCount(), samples)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err := lr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	sharedMu.Lock()
	_, still := shared[set.ID]
	sharedMu.Unlock()
	if still {
		t.Error("the receiver outlived its pipelines")
	}
	_ = pcommon.NewMap
}

// TestTrafficInWindows: with a window shorter than the interval, the
// capture is opened for each window and closed after; writers are counted
// across windows by their sequence numbers.
func TestTrafficInWindows(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Interface = "127.0.0.0/8"
	cfg.ProcRoot = t.TempDir()
	cfg.Traffic.CollectionInterval = 100 * time.Millisecond
	cfg.Traffic.Window = 20 * time.Millisecond
	set := receivertest.NewNopSettings(f.Type())
	set.ID = component.NewIDWithName(f.Type(), t.Name())
	metrics := new(consumertest.MetricsSink)
	mr, err := f.CreateMetrics(context.Background(), set, cfg, metrics)
	if err != nil {
		t.Fatal(err)
	}
	r := mr.(handle).r
	var mu sync.Mutex
	opens, open := 0, 0
	var sn rtps.SequenceNumber
	r.openCapture = func(traffic.CaptureConfig) (capturer, error) {
		mu.Lock()
		defer mu.Unlock()
		opens++
		open++
		// Each window sees the writer once, ten samples on.
		sn += 10
		return &windowCapture{packets: [][]byte{chatterData(prefix, sn)}, closed: func() { mu.Lock(); open--; mu.Unlock() }}, nil
	}
	r.newParticipant = func(c participant.Config, ev func(discovery.Event)) (runner, error) {
		return &fakeParticipant{events: ev, replay: []discovery.Event{
			discovery.ParticipantDiscovered{Participant: talker},
			discovery.EndpointDiscovered{Endpoint: chatter},
		}}, nil
	}
	if err := mr.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	samples := int64(0)
	for samples < 30 {
		if time.Now().After(deadline) {
			t.Fatalf("%d samples counted", samples)
		}
		time.Sleep(20 * time.Millisecond)
		for _, md := range metrics.AllMetrics() {
			ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			for i := range ms.Len() {
				if ms.At(i).Name() == mWriterSamples && ms.At(i).Sum().DataPoints().Len() > 0 {
					samples = max(samples, ms.At(i).Sum().DataPoints().At(0).IntValue())
				}
			}
		}
	}
	if err := mr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if opens < 4 {
		t.Errorf("opened %d times", opens)
	}
	if open != 0 {
		t.Errorf("%d captures left open", open)
	}
}

// windowCapture is a capture that sees its packets, until its window ends.
type windowCapture struct {
	packets [][]byte
	closed  func()
}

func (w *windowCapture) Run(ctx context.Context, fn func(traffic.Packet)) error {
	for _, p := range w.packets {
		fn(traffic.Packet{Message: p, At: time.Now()})
	}
	<-ctx.Done()
	return ctx.Err()
}
func (w *windowCapture) Drops() uint64 { return 1 }
func (w *windowCapture) Close() error  { w.closed(); return nil }

func TestNoCaptureNoTraffic(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Interface = "127.0.0.0/8"
	cfg.ProcRoot = t.TempDir()
	set := receivertest.NewNopSettings(f.Type())
	set.ID = component.NewIDWithName(f.Type(), t.Name())
	mr, err := f.CreateMetrics(context.Background(), set, cfg, new(consumertest.MetricsSink))
	if err != nil {
		t.Fatal(err)
	}
	r := mr.(handle).r
	r.openCapture = func(traffic.CaptureConfig) (capturer, error) { return nil, errors.New("operation not permitted") }
	r.newParticipant = func(c participant.Config, ev func(discovery.Event)) (runner, error) {
		return &fakeParticipant{events: ev}, nil
	}
	if err := mr.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("a capture that cannot open stops the receiver: %v", err)
	}
	if r.stats != nil {
		t.Error("traffic without a capture")
	}
	if err := mr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConfigValidate(t *testing.T) {
	c := createDefaultConfig()
	if c.Validate() == nil {
		t.Error("no interface: no error")
	}
	c.Interface = "192.168.10.0/24"
	if err := c.Validate(); err != nil {
		t.Error(err)
	}
	c.Domain = 233
	if c.Validate() == nil {
		t.Error("domain 233: no error")
	}
	c.Domain, c.Interface = 138, "192.168.10.0"
	if c.Validate() == nil {
		t.Error("a bare address: no error")
	}
}
