package rtps_test

import (
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/dds/internal/pcap"
	"github.com/lesomnus/dds/rtps"
)

// cyclone-talker-composition.pcap: demo_nodes_cpp talker and
// composition manual_composition on Cyclone 0.10.5 in one network namespace,
// domain 77, captured with tcpdump -i any: their SPDP, SEDP, user data and
// the reliability traffic between them.
func capture(t testing.TB) []pcap.Datagram {
	t.Helper()
	ds, err := pcap.ReadFile(filepath.Join("testdata", "cyclone-talker-composition.pcap"))
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

func sedpEndpoints(t *testing.T) []rtps.EndpointData {
	t.Helper()
	var out []rtps.EndpointData
	for _, d := range capture(t) {
		m, err := rtps.Parse(d.Data)
		if err != nil && len(m.Submessages) == 0 {
			continue
		}
		for _, s := range m.Submessages {
			if s.ID != rtps.SubData {
				continue
			}
			dd, err := rtps.ParseData(s)
			if err != nil || dd.Payload == nil || dd.KeyOnly {
				continue
			}
			var kind rtps.EndpointKind
			switch dd.Writer {
			case rtps.EntitySEDPPublicationsWriter:
				kind = rtps.Writer
			case rtps.EntitySEDPSubscriptionsWriter:
				kind = rtps.Reader
			default:
				continue
			}
			pl, err := rtps.ReadParameterListPayload(dd.Payload)
			if err != nil {
				t.Fatal(err)
			}
			e, err := rtps.ParseEndpointData(kind, pl)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, e)
		}
	}
	return out
}

func TestParseCycloneSEDP(t *testing.T) {
	eps := sedpEndpoints(t)
	if len(eps) == 0 {
		t.Fatal("no SEDP data in the capture")
	}
	var chatterWriter, chatterReader, discovery *rtps.EndpointData
	for i := range eps {
		e := &eps[i]
		switch {
		case e.Topic == "rt/chatter" && e.Kind == rtps.Writer:
			chatterWriter = e
		case e.Topic == "rt/chatter" && e.Kind == rtps.Reader:
			chatterReader = e
		case e.Topic == "ros_discovery_info" && e.Kind == rtps.Writer:
			discovery = e
		}
		if e.Participant != e.GUID.Prefix {
			t.Errorf("%s %s: participant %s", e.Topic, e.GUID, e.Participant)
		}
	}
	if chatterWriter == nil || chatterReader == nil || discovery == nil {
		t.Fatalf("missing endpoints: chatter writer %v, reader %v, ros_discovery_info %v", chatterWriter != nil, chatterReader != nil, discovery != nil)
	}
	if chatterWriter.Type != "std_msgs::msg::dds_::String_" {
		t.Errorf("chatter type %q", chatterWriter.Type)
	}
	q := chatterWriter.QoS
	if q.Reliability != rtps.Reliable || q.Durability != rtps.Volatile || q.History != rtps.KeepLast || q.Depth != 10 {
		t.Errorf("talker's chatter QoS %s %s %s %d, want reliable volatile keep_last 10 (demo_nodes_cpp talker)",
			q.Reliability, q.Durability, q.History, q.Depth)
	}
	// ROS 2 Jazzy puts the type hash in the endpoint's USER_DATA.
	if got := string(q.UserData); len(got) < 9 || got[:9] != "typehash=" {
		t.Errorf("user data %q, want typehash=...", got)
	}

	d := discovery.QoS
	if discovery.Type != "rmw_dds_common::msg::dds_::ParticipantEntitiesInfo_" {
		t.Errorf("ros_discovery_info type %q", discovery.Type)
	}
	if d.Reliability != rtps.Reliable || d.Durability != rtps.TransientLocal || d.History != rtps.KeepLast || d.Depth != 1 {
		t.Errorf("ros_discovery_info QoS %s %s %s %d, want reliable transient_local keep_last 1", d.Reliability, d.Durability, d.History, d.Depth)
	}
}

func TestParseCycloneReliabilityTraffic(t *testing.T) {
	var hb, an int
	for _, d := range capture(t) {
		m, _ := rtps.Parse(d.Data)
		for _, s := range m.Submessages {
			switch s.ID {
			case rtps.SubHeartbeat:
				h, err := rtps.ParseHeartbeat(s)
				if err != nil {
					t.Fatal(err)
				}
				if h.First < 1 || h.Last < h.First-1 {
					t.Errorf("heartbeat %d..%d", h.First, h.Last)
				}
				hb++
			case rtps.SubAckNack:
				a, err := rtps.ParseAckNack(s)
				if err != nil {
					t.Fatal(err)
				}
				if a.State.Base < 1 {
					t.Errorf("acknack base %d", a.State.Base)
				}
				an++
			case rtps.SubGap:
				if _, err := rtps.ParseGap(s); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if hb == 0 || an == 0 {
		t.Errorf("%d heartbeats, %d acknacks", hb, an)
	}
}

func TestSequenceNumberSet(t *testing.T) {
	s := rtps.SequenceNumberSet{Base: 10}
	for _, sn := range []rtps.SequenceNumber{10, 12, 41, 265} {
		if !s.Add(sn) {
			t.Fatalf("Add(%d) refused", sn)
		}
	}
	if s.Add(9) || s.Add(266) {
		t.Error("added outside the window")
	}
	if got := s.Members(); len(got) != 4 || got[0] != 10 || got[3] != 265 {
		t.Errorf("members %v", got)
	}
	if s.NumBits != 256 || !s.Has(41) || s.Has(11) {
		t.Errorf("bits %d, has 41 %v, has 11 %v", s.NumBits, s.Has(41), s.Has(11))
	}
}

// TestBuiltMessagesParse writes each submessage this package can write and
// reads it back.
func TestBuiltMessagesParse(t *testing.T) {
	prefix := rtps.GUIDPrefix{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	reader := rtps.EntitySEDPPublicationsReader
	writer := rtps.EntitySEDPPublicationsWriter
	set := rtps.SequenceNumberSet{Base: 3}
	set.Add(3)
	set.Add(7)
	pl := (&rtps.ParameterListWriter{}).
		GUID(rtps.PIDParticipantGUID, rtps.GUID{Prefix: prefix, Entity: rtps.EntityParticipant}).
		Duration(rtps.PIDParticipantLease, 10*time.Second).
		Locator(rtps.PIDMetatrafficUnicastLoc, rtps.Locator{Kind: rtps.LocatorUDPv4, Port: 7410, Addr: netip.MustParseAddr("10.1.2.3")}).
		String(rtps.PIDEntityName, "observer").
		Properties([2]string{"__ProcessName", "ddsspy"}, [2]string{"__Pid", "42"}).
		Payload()

	b := rtps.NewBuilder(prefix, rtps.VendorUnknown).
		InfoTS(time.Unix(1_800_000_000, 500_000_000)).
		InfoDst(rtps.GUIDPrefix{9}).
		Data(rtps.EntitySPDPReader, rtps.EntitySPDPWriter, 5, nil, pl, false).
		Heartbeat(reader, writer, 1, 9, 4, true).
		AckNack(reader, writer, set, 2, false).
		Bytes()

	m, err := rtps.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	if m.Header.Prefix != prefix || len(m.Submessages) != 5 {
		t.Fatalf("prefix %s, %d submessages", m.Header.Prefix, len(m.Submessages))
	}
	ts, _ := rtps.ParseInfoTS(m.Submessages[0])
	if !ts.Time.Equal(time.Unix(1_800_000_000, 500_000_000)) {
		t.Errorf("timestamp %s", ts.Time)
	}
	d, err := rtps.ParseData(m.Submessages[2])
	if err != nil || d.SN != 5 {
		t.Fatalf("data %v sn %d", err, d.SN)
	}
	got, err := rtps.ReadParameterListPayload(d.Payload)
	if err != nil {
		t.Fatal(err)
	}
	p, err := rtps.ParseParticipantData(got, rtps.VendorUnknown)
	if err != nil {
		t.Fatal(err)
	}
	if p.LeaseDuration != 10*time.Second || p.EntityName != "observer" || p.Properties["__Pid"] != "42" ||
		len(p.MetatrafficUnicast) != 1 || p.MetatrafficUnicast[0].String() != "10.1.2.3:7410" {
		t.Errorf("round trip: %+v", p)
	}
	h, _ := rtps.ParseHeartbeat(m.Submessages[3])
	if h.First != 1 || h.Last != 9 || h.Count != 4 || !h.Final {
		t.Errorf("heartbeat %+v", h)
	}
	a, _ := rtps.ParseAckNack(m.Submessages[4])
	if a.State.Base != 3 || !a.State.Has(7) || a.State.Has(4) || a.Count != 2 || a.Final {
		t.Errorf("acknack %+v", a)
	}
}

func TestNackFragRoundTrip(t *testing.T) {
	var set rtps.FragmentNumberSet
	set.Base = 2
	set.Add(2)
	set.Add(5)
	b := rtps.NewBuilder(rtps.GUIDPrefix{1}, rtps.VendorUnknown).
		NackFrag(rtps.EntityID{0, 0, 1, 4}, rtps.EntityID{0, 0, 4, 3}, 9, set, 3).Bytes()
	m, err := rtps.Parse(b)
	if err != nil || len(m.Submessages) != 1 {
		t.Fatal(err)
	}
	n, err := rtps.ParseNackFrag(m.Submessages[0])
	if err != nil {
		t.Fatal(err)
	}
	if n.SN != 9 || n.Count != 3 || len(n.Missing.Members()) != 2 || n.Missing.Members()[1] != 5 {
		t.Errorf("%+v", n)
	}
}
