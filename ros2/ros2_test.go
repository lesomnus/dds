package ros2_test

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/lesomnus/dds/internal/pcap"
	"github.com/lesomnus/dds/ros2"
	"github.com/lesomnus/dds/rtps"
)

// discoverySamples returns the ros_discovery_info samples in the capture of a
// talker and a manual_composition: DATA from each participant's first user
// writer (entity 00000403), which rmw creates for the topic.
func discoverySamples(t *testing.T) [][]byte { return discoverySamplesF(t) }

func discoverySamplesF(t testing.TB) [][]byte {
	t.Helper()
	ds, err := pcap.ReadFile(filepath.Join("..", "rtps", "testdata", "cyclone-talker-composition.pcap"))
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	for _, d := range ds {
		m, _ := rtps.Parse(d.Data)
		for _, s := range m.Submessages {
			if s.ID != rtps.SubData {
				continue
			}
			dd, err := rtps.ParseData(s)
			if err != nil || dd.Writer != (rtps.EntityID{0, 0, 4, 3}) || dd.Payload == nil {
				continue
			}
			out = append(out, dd.Payload)
		}
	}
	return out
}

func TestParseParticipantEntitiesInfo(t *testing.T) {
	samples := discoverySamples(t)
	if len(samples) == 0 {
		t.Fatal("no ros_discovery_info samples")
	}
	nodes := map[string]ros2.Node{}
	for _, b := range samples {
		info, err := ros2.ParseParticipantEntitiesInfo(b)
		if err != nil {
			t.Fatal(err)
		}
		if info.Gid.GUID().Entity != rtps.EntityParticipant {
			t.Errorf("gid %s is not a participant's", info.Gid.GUID())
		}
		// Each node's fullest announcement: as a process shuts down, rmw
		// announces its nodes again as their endpoints go.
		for _, n := range info.Nodes() {
			if prev, ok := nodes[n.FullName()]; !ok || len(n.Readers)+len(n.Writers) > len(prev.Readers)+len(prev.Writers) {
				nodes[n.FullName()] = n
			}
		}
	}
	var names []string
	for n := range nodes {
		names = append(names, n)
	}
	sort.Strings(names)
	// demo_nodes_cpp talker, and composition's manual_composition: talker,
	// listener, server and client in one process.
	for _, want := range []string{"/talker", "/listener", "/Server", "/Client"} {
		if _, ok := nodes[want]; !ok {
			t.Errorf("no node %s; have %v", want, names)
		}
	}
	talker := nodes["/talker"]
	if len(talker.Writers) == 0 || len(talker.Readers) == 0 {
		t.Errorf("talker has %d writers, %d readers", len(talker.Writers), len(talker.Readers))
	}
	for _, w := range talker.Writers {
		if w.Prefix != talker.Participant {
			t.Errorf("talker's writer %s is not in its participant %s", w, talker.Participant)
		}
	}
}

func TestParseTopic(t *testing.T) {
	for _, c := range []struct {
		dds, ros string
		kind     ros2.TopicKind
	}{
		{"rt/chatter", "/chatter", ros2.ROSTopic},
		{"rt/talker/rosout", "/talker/rosout", ros2.ROSTopic},
		{"rq/talker/get_parametersRequest", "/talker/get_parameters", ros2.ServiceRequest},
		{"rr/talker/get_parametersReply", "/talker/get_parameters", ros2.ServiceReply},
		{"ros_discovery_info", "ros_discovery_info", ros2.NotROS},
	} {
		ros, kind := ros2.ParseTopic(c.dds)
		if ros != c.ros || kind != c.kind {
			t.Errorf("ParseTopic(%q) = %q, %d; want %q, %d", c.dds, ros, kind, c.ros, c.kind)
		}
	}
}

func TestParseType(t *testing.T) {
	for dds, ros := range map[string]string{
		"std_msgs::msg::dds_::String_":                        "std_msgs/msg/String",
		"rcl_interfaces::srv::dds_::GetParameters_Request_":   "rcl_interfaces/srv/GetParameters",
		"rcl_interfaces::srv::dds_::GetParameters_Response_":  "rcl_interfaces/srv/GetParameters",
		"rmw_dds_common::msg::dds_::ParticipantEntitiesInfo_": "rmw_dds_common/msg/ParticipantEntitiesInfo",
		"NotARosType": "NotARosType",
	} {
		if got := ros2.ParseType(dds); got != ros {
			t.Errorf("ParseType(%q) = %q, want %q", dds, got, ros)
		}
	}
}
