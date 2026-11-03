package ros2_test

import (
	"testing"
	"time"

	"github.com/lesomnus/dds/cdr"
	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/ros2"
	"github.com/lesomnus/dds/rtps"
)

func info(t *testing.T, part rtps.GUIDPrefix, nodes ...string) []byte {
	t.Helper()
	var gid ros2.Gid
	copy(gid.Data[:12], part[:])
	copy(gid.Data[12:], rtps.EntityParticipant[:])
	v := ros2.ParticipantEntitiesInfo{Gid: gid}
	for i, n := range nodes {
		w := gid
		w.Data[15] = byte(i + 1)
		v.NodeEntitiesInfoSeq = append(v.NodeEntitiesInfoSeq, ros2.NodeEntitiesInfo{NodeNamespace: "/", NodeName: n, WriterGidSeq: []ros2.Gid{w}})
	}
	b, err := cdr.Marshal(&v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestGraphFollowsNodes(t *testing.T) {
	var evs []any
	g := ros2.NewGraph(func(e any) { evs = append(evs, e) })
	part := rtps.GUIDPrefix{1, 2, 3}
	writer := rtps.GUID{Prefix: part, Entity: rtps.EntityID{0, 0, 4, 3}}

	g.HandleSample(writer, info(t, part, "a", "b"))
	if len(evs) != 2 || evs[0].(ros2.NodeAppeared).Node.FullName() != "/a" || evs[1].(ros2.NodeAppeared).Node.FullName() != "/b" {
		t.Fatalf("events %v", evs)
	}
	evs = nil
	g.HandleSample(writer, info(t, part, "a", "b")) // nothing changed
	if len(evs) != 0 {
		t.Fatalf("events %v for the same nodes", evs)
	}
	g.HandleSample(writer, info(t, part, "a"))
	if len(evs) != 1 || evs[0].(ros2.NodeDisappeared).Reason != ros2.NodeRemoved {
		t.Fatalf("events %v, want /b removed", evs)
	}
	evs = nil

	// Another participant cannot speak for this one.
	g.HandleSample(rtps.GUID{Prefix: rtps.GUIDPrefix{9}}, info(t, part, "forged"))
	if len(evs) != 0 {
		t.Fatalf("events %v from a forger", evs)
	}

	g.HandleDiscovery(discovery.ParticipantLost{
		Participant: discovery.Participant{ParticipantData: rtps.ParticipantData{GUID: rtps.GUID{Prefix: part}}},
		Reason:      discovery.LeaseExpired, At: time.Now(),
	})
	if len(evs) != 1 || evs[0].(ros2.NodeDisappeared).Reason != ros2.NodeParticipantLost || len(g.Nodes()) != 0 {
		t.Fatalf("events %v, nodes %v", evs, g.Nodes())
	}
}

func TestGarbageSamplesAreIgnored(t *testing.T) {
	var evs []any
	g := ros2.NewGraph(func(e any) { evs = append(evs, e) })
	for _, b := range [][]byte{nil, {0, 1}, {0, 1, 0, 0, 0xff, 0xff, 0xff, 0x7f}} {
		g.HandleSample(rtps.GUID{}, b)
	}
	if len(evs) != 0 {
		t.Errorf("events %v", evs)
	}
}

func FuzzParseParticipantEntitiesInfo(f *testing.F) {
	for _, b := range discoverySamplesF(f) {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if v, err := ros2.ParseParticipantEntitiesInfo(b); err == nil {
			_ = v.Nodes()
		}
	})
}
