// Package ros2 reads what a ROS 2 system says of itself over DDS: the nodes
// each participant hosts (ros_discovery_info) and how ROS names map to DDS
// topics and types.
package ros2

import (
	"fmt"
	"strings"

	"github.com/lesomnus/dds/cdr"
	"github.com/lesomnus/dds/rtps"
)

// The topic every ROS 2 participant announces its nodes on, and its type
// (rmw_dds_common).
const (
	DiscoveryTopic = "ros_discovery_info"
	DiscoveryType  = "rmw_dds_common::msg::dds_::ParticipantEntitiesInfo_"
)

// Gid is rmw_dds_common/msg/Gid: in ROS 2 Iron and later (and so Jazzy), 16
// octets -- a DDS GUID.
type Gid struct {
	Data [16]uint8
}

// GUID reads the Gid as a DDS GUID.
func (g Gid) GUID() rtps.GUID {
	var out rtps.GUID
	copy(out.Prefix[:], g.Data[:12])
	copy(out.Entity[:], g.Data[12:])
	return out
}

// NodeEntitiesInfo is rmw_dds_common/msg/NodeEntitiesInfo: a node and its
// readers and writers.
type NodeEntitiesInfo struct {
	NodeNamespace string
	NodeName      string
	ReaderGidSeq  []Gid
	WriterGidSeq  []Gid
}

// ParticipantEntitiesInfo is rmw_dds_common/msg/ParticipantEntitiesInfo: the
// nodes a participant hosts.
type ParticipantEntitiesInfo struct {
	Gid                 Gid
	NodeEntitiesInfoSeq []NodeEntitiesInfo
}

// ParseParticipantEntitiesInfo reads a ros_discovery_info sample: its
// serialized payload, encapsulation header included.
func ParseParticipantEntitiesInfo(payload []byte) (ParticipantEntitiesInfo, error) {
	var v ParticipantEntitiesInfo
	if err := cdr.Unmarshal(payload, &v); err != nil {
		return v, fmt.Errorf("ros2: ParticipantEntitiesInfo: %w", err)
	}
	return v, nil
}

// Node is a ROS node: its fully qualified name and its endpoints.
type Node struct {
	Namespace string
	Name      string
	// Participant hosts it.
	Participant rtps.GUIDPrefix
	Readers     []rtps.GUID
	Writers     []rtps.GUID
}

// FullName is the node's fully qualified name ("/ns/name", "/name").
func (n Node) FullName() string {
	ns := n.Namespace
	if ns == "" || ns == "/" {
		return "/" + n.Name
	}
	return strings.TrimSuffix(ns, "/") + "/" + n.Name
}

// Nodes lists the nodes a ParticipantEntitiesInfo describes.
func (p ParticipantEntitiesInfo) Nodes() []Node {
	part := p.Gid.GUID().Prefix
	out := make([]Node, 0, len(p.NodeEntitiesInfoSeq))
	for _, n := range p.NodeEntitiesInfoSeq {
		node := Node{Namespace: n.NodeNamespace, Name: n.NodeName, Participant: part}
		for _, g := range n.ReaderGidSeq {
			node.Readers = append(node.Readers, g.GUID())
		}
		for _, g := range n.WriterGidSeq {
			node.Writers = append(node.Writers, g.GUID())
		}
		out = append(out, node)
	}
	return out
}

// Topic kinds by DDS topic prefix (rmw: "rt/" topics, "rq/" service
// requests, "rr/" service replies).
type TopicKind int

const (
	NotROS TopicKind = iota
	ROSTopic
	ServiceRequest
	ServiceReply
)

// ParseTopic maps a DDS topic name to a ROS name: "rt/chatter" is the topic
// "/chatter"; "rq/talker/get_parametersRequest" is the service
// "/talker/get_parameters"'s request. A DDS topic of no ROS kind is returned
// as it is.
func ParseTopic(dds string) (string, TopicKind) {
	switch {
	case strings.HasPrefix(dds, "rt/"):
		return "/" + dds[3:], ROSTopic
	case strings.HasPrefix(dds, "rq/") && strings.HasSuffix(dds, "Request"):
		return "/" + strings.TrimSuffix(dds[3:], "Request"), ServiceRequest
	case strings.HasPrefix(dds, "rr/") && strings.HasSuffix(dds, "Reply"):
		return "/" + strings.TrimSuffix(dds[3:], "Reply"), ServiceReply
	}
	return dds, NotROS
}

// ParseType maps a DDS type name to a ROS one:
// "std_msgs::msg::dds_::String_" is "std_msgs/msg/String", and a service's
// "..._Request_"/"..._Response_" is its service type. Anything else is
// returned as it is.
func ParseType(dds string) string {
	parts := strings.Split(dds, "::")
	if len(parts) != 4 || parts[2] != "dds_" || !strings.HasSuffix(parts[3], "_") {
		return dds
	}
	name := strings.TrimSuffix(parts[3], "_")
	if parts[1] == "srv" || parts[1] == "action" {
		name = strings.TrimSuffix(strings.TrimSuffix(name, "_Request"), "_Response")
	}
	return parts[0] + "/" + parts[1] + "/" + name
}
