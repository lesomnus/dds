package ddsreceiver

import (
	"fmt"
	"strings"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/ros2"
	"github.com/lesomnus/dds/rtps"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// ServiceName is the service.name of what the receiver emits.
const ServiceName = "dds"

const scopeName = "github.com/lesomnus/dds/otelcol/ddsreceiver"

// Attribute keys.
const (
	attrEvent = "event.name"

	attrParticipant     = "dds.participant.guid_prefix"
	attrVendor          = "dds.vendor"
	attrDDSHost         = "dds.participant.hostname"
	attrDDSPID          = "dds.participant.pid" // in the participant's own PID namespace
	attrDDSProcess      = "dds.participant.process_name"
	attrDDSVersion      = "dds.participant.version"
	attrLease           = "dds.participant.lease_seconds"
	attrEnclave         = "ros.enclave"
	attrReason          = "dds.lost.reason"
	attrProcessPID      = "process.pid" // the host's
	attrProcessStart    = "process.start_time"
	attrProcessComm     = "process.executable.name"
	attrProcessCgroup   = "process.cgroup"
	attrEndpoint        = "dds.endpoint.guid"
	attrEndpointKind    = "dds.endpoint.kind"
	attrTopic           = "dds.topic"
	attrType            = "dds.type"
	attrROSName         = "ros.name"
	attrROSKind         = "ros.kind"
	attrROSType         = "ros.type"
	attrReliability     = "dds.qos.reliability"
	attrDurability      = "dds.qos.durability"
	attrHistory         = "dds.qos.history"
	attrDepth           = "dds.qos.depth"
	attrDeadline        = "dds.qos.deadline_seconds"
	attrLiveliness      = "dds.qos.liveliness"
	attrLivelinessLease = "dds.qos.liveliness_lease_seconds"
	attrNode            = "ros.node.name"
	attrNodeNamespace   = "ros.node.namespace"
	attrNodeReaders     = "ros.node.readers"
	attrNodeWriters     = "ros.node.writers"
)

// Event names.
const (
	evParticipantDiscovered = "dds.participant.discovered"
	evParticipantUpdated    = "dds.participant.updated"
	evParticipantLost       = "dds.participant.lost"
	evEndpointDiscovered    = "dds.endpoint.discovered"
	evEndpointUpdated       = "dds.endpoint.updated"
	evEndpointLost          = "dds.endpoint.lost"
	evNodeAppeared          = "ros.node.appeared"
	evNodeUpdated           = "ros.node.updated"
	evNodeDisappeared       = "ros.node.disappeared"
	// The state of the graph, every snapshot_interval: one record per node.
	evNodeState = "ros.node.state"
)

// Resource attributes.
const (
	attrDomain = "dds.domain.id"
	attrMode   = "dds.collection.mode" // active, passive
)

// Collection modes.
const (
	modeActive  = "active"
	modePassive = "passive"
)

// resource is what a batch of records is about: a domain, collected one
// way; or, zero, the receiver itself (the capture).
type resource struct {
	domain uint32
	mode   string
}

func (r resource) put(a pcommon.Map) {
	a.PutStr("service.name", ServiceName)
	if r.mode != "" {
		a.PutInt(attrDomain, int64(r.domain))
		a.PutStr(attrMode, r.mode)
	}
}

func newLogs(res resource) (plog.Logs, plog.LogRecordSlice) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	res.put(rl.Resource().Attributes())
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(scopeName)
	return ld, sl.LogRecords()
}

func seconds(d time.Duration) float64 {
	if d == rtps.DurationInfinite {
		return -1 // forever
	}
	return d.Seconds()
}

func putParticipant(a pcommon.Map, p participantState, ros bool) {
	a.PutStr(attrParticipant, p.GUID.Prefix.String())
	a.PutStr(attrVendor, p.Vendor.String())
	if v := p.Hostname(); v != "" {
		a.PutStr(attrDDSHost, v)
	}
	if v := p.PID(); v != 0 {
		a.PutInt(attrDDSPID, int64(v))
	}
	if v := p.ProcessName(); v != "" {
		a.PutStr(attrDDSProcess, v)
	}
	if v := p.CycloneInternals; v != "" {
		a.PutStr(attrDDSVersion, v)
	}
	if v, ok := strings.CutPrefix(string(p.UserData), "enclave="); ok && ros {
		a.PutStr(attrEnclave, strings.TrimSuffix(v, ";"))
	}
	a.PutDouble(attrLease, seconds(p.LeaseDuration))
	putProcess(a, p)
}

func putProcess(a pcommon.Map, p participantState) {
	if p.Process.PID == 0 {
		return
	}
	a.PutInt(attrProcessPID, int64(p.Process.PID))
	a.PutStr(attrProcessComm, p.Process.Comm)
	if !p.Process.Start.IsZero() {
		a.PutInt(attrProcessStart, p.Process.Start.Unix())
	}
	if p.Process.Cgroup != "" {
		a.PutStr(attrProcessCgroup, p.Process.Cgroup)
	}
}

func putEndpoint(a pcommon.Map, e discovery.Endpoint, ros bool) {
	putTopic(a, e, ros)
	q := e.QoS
	a.PutStr(attrReliability, q.Reliability.String())
	a.PutStr(attrDurability, q.Durability.String())
	a.PutStr(attrHistory, q.History.String())
	a.PutInt(attrDepth, int64(q.Depth))
	a.PutDouble(attrDeadline, seconds(q.Deadline))
	a.PutStr(attrLiveliness, q.Liveliness.String())
	a.PutDouble(attrLivelinessLease, seconds(q.LeaseDuration))
}

// putTopic names an endpoint: its GUID, kind, topic and type, and with ROS
// the ROS names.
func putTopic(a pcommon.Map, e discovery.Endpoint, ros bool) {
	a.PutStr(attrEndpoint, e.GUID.String())
	a.PutStr(attrEndpointKind, e.Kind.String())
	a.PutStr(attrTopic, e.Topic)
	a.PutStr(attrType, e.Type)
	if !ros {
		return
	}
	name, kind := ros2.ParseTopic(e.Topic)
	if kind != ros2.NotROS {
		a.PutStr(attrROSName, name)
		a.PutStr(attrROSKind, map[ros2.TopicKind]string{ros2.ROSTopic: "topic", ros2.ServiceRequest: "service_request", ros2.ServiceReply: "service_reply"}[kind])
		a.PutStr(attrROSType, ros2.ParseType(e.Type))
	}
}

func putNode(a pcommon.Map, n ros2.Node) {
	a.PutStr(attrNode, n.FullName())
	a.PutStr(attrNodeNamespace, n.Namespace)
	a.PutInt(attrNodeReaders, int64(len(n.Readers)))
	a.PutInt(attrNodeWriters, int64(len(n.Writers)))
}

// appendEvent appends a record for an event, with its participant as it
// was. It reports whether the event is one it records.
func appendEvent(rs plog.LogRecordSlice, e any, p participantState, observed time.Time, ros bool) bool {
	var name, body string
	var at time.Time
	r := plog.NewLogRecord()
	a := r.Attributes()
	who := func() string {
		if p.Process.PID != 0 {
			return fmt.Sprintf("%s[%d]", p.Process.Comm, p.Process.PID)
		}
		if p.ProcessName() != "" {
			return p.ProcessName()
		}
		return p.GUID.Prefix.String()
	}
	switch e := e.(type) {
	case discovery.ParticipantDiscovered:
		name, at, body = evParticipantDiscovered, e.Participant.FirstSeen, "participant of "+who()+" discovered"
	case discovery.ParticipantUpdated:
		name, at, body = evParticipantUpdated, e.Participant.LastSeen, "participant of "+who()+" updated"
	case discovery.ParticipantLost:
		name, at, body = evParticipantLost, e.At, fmt.Sprintf("participant of %s lost: %s", who(), e.Reason)
		a.PutStr(attrReason, e.Reason.String())
	case discovery.EndpointDiscovered:
		name, at = evEndpointDiscovered, e.Endpoint.FirstSeen
		body = fmt.Sprintf("%s %s of %s discovered", e.Endpoint.Kind, e.Endpoint.Topic, who())
		putEndpoint(a, e.Endpoint, ros)
	case discovery.EndpointUpdated:
		name, at = evEndpointUpdated, e.Endpoint.LastSeen
		body = fmt.Sprintf("%s %s of %s updated", e.Endpoint.Kind, e.Endpoint.Topic, who())
		putEndpoint(a, e.Endpoint, ros)
	case discovery.EndpointLost:
		name, at = evEndpointLost, e.At
		body = fmt.Sprintf("%s %s of %s lost: %s", e.Endpoint.Kind, e.Endpoint.Topic, who(), e.Reason)
		putEndpoint(a, e.Endpoint, ros)
		a.PutStr(attrReason, e.Reason.String())
	case ros2.NodeAppeared:
		name, at, body = evNodeAppeared, e.At, "node "+e.Node.FullName()+" appeared in "+who()
		putNode(a, e.Node)
	case ros2.NodeUpdated:
		name, at, body = evNodeUpdated, e.At, "node "+e.Node.FullName()+" updated in "+who()
		putNode(a, e.Node)
	case ros2.NodeDisappeared:
		name, at = evNodeDisappeared, e.At
		body = fmt.Sprintf("node %s disappeared from %s: %s", e.Node.FullName(), who(), e.Reason)
		putNode(a, e.Node)
		a.PutStr(attrReason, e.Reason.String())
	default:
		return false
	}
	if p.GUID.Prefix.IsZero() {
		// The participant is not known (an endpoint of one lost meanwhile):
		// the record still names it.
		switch e := e.(type) {
		case discovery.EndpointDiscovered:
			p.GUID.Prefix = e.Endpoint.Participant
		case discovery.EndpointUpdated:
			p.GUID.Prefix = e.Endpoint.Participant
		case discovery.EndpointLost:
			p.GUID.Prefix = e.Endpoint.Participant
		case ros2.NodeAppeared:
			p.GUID.Prefix = e.Node.Participant
		case ros2.NodeUpdated:
			p.GUID.Prefix = e.Node.Participant
		case ros2.NodeDisappeared:
			p.GUID.Prefix = e.Node.Participant
		}
	}
	putParticipant(a, p, ros)
	a.PutStr(attrEvent, name)
	r.Body().SetStr(body)
	r.SetSeverityNumber(plog.SeverityNumberInfo)
	r.SetSeverityText("INFO")
	if e, ok := e.(discovery.ParticipantLost); ok && e.Reason == discovery.LeaseExpired {
		// A participant that went silent: it crashed, or the network did.
		r.SetSeverityNumber(plog.SeverityNumberWarn)
		r.SetSeverityText("WARN")
	}
	if at.IsZero() {
		at = observed
	}
	r.SetTimestamp(pcommon.NewTimestampFromTime(at))
	r.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))
	r.MoveTo(rs.AppendEmpty())
	return true
}

// appendSnapshot appends the state records: one per node, with the host
// process hosting it -- what process metrics are joined with.
func appendSnapshot(rs plog.LogRecordSlice, s snapshot) {
	parts := make(map[rtps.GUIDPrefix]participantState, len(s.Participants))
	for _, p := range s.Participants {
		parts[p.GUID.Prefix] = p
	}
	ts := pcommon.NewTimestampFromTime(s.At)
	for _, n := range s.Nodes {
		r := rs.AppendEmpty()
		r.SetTimestamp(ts)
		r.SetObservedTimestamp(ts)
		r.SetSeverityNumber(plog.SeverityNumberInfo)
		r.SetSeverityText("INFO")
		a := r.Attributes()
		a.PutStr(attrEvent, evNodeState)
		p, ok := parts[n.Participant]
		if !ok {
			p = participantState{}
			p.GUID.Prefix = n.Participant
		}
		putParticipant(a, p, true)
		putNode(a, n)
		r.Body().SetStr("node " + n.FullName())
	}
}

// Metric names.
const (
	mParticipants = "dds.participants"
	mNodes        = "ros.nodes"
	mEndpoints    = "dds.endpoints"
	mUnmatched    = "dds.endpoints.unmatched"
	mIncompatible = "dds.qos.incompatible_pairs"
)

func newMetrics(res resource) (pmetric.Metrics, pmetric.MetricSlice) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	res.put(rm.Resource().Attributes())
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName(scopeName)
	return md, sm.Metrics()
}

// snapshotMetrics is the graph's size and health.
func snapshotMetrics(res resource, s snapshot, ros bool) pmetric.Metrics {
	md, ms := newMetrics(res)
	ts := pcommon.NewTimestampFromTime(s.At)

	gauge := func(name, desc, unit string) pmetric.NumberDataPointSlice {
		m := ms.AppendEmpty()
		m.SetName(name)
		m.SetDescription(desc)
		m.SetUnit(unit)
		return m.SetEmptyGauge().DataPoints()
	}
	point := func(dps pmetric.NumberDataPointSlice, v int64, kv ...string) {
		dp := dps.AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetIntValue(v)
		for i := 0; i+1 < len(kv); i += 2 {
			dp.Attributes().PutStr(kv[i], kv[i+1])
		}
	}

	point(gauge(mParticipants, "DDS participants on this host.", "{participant}"), int64(len(s.Participants)))
	if ros {
		point(gauge(mNodes, "ROS nodes on this host.", "{node}"), int64(len(s.Nodes)))
	}
	var writers, readers int64
	for _, e := range s.Endpoints {
		if e.Kind == rtps.Writer {
			writers++
		} else {
			readers++
		}
	}
	eps := gauge(mEndpoints, "DDS endpoints on this host.", "{endpoint}")
	point(eps, writers, attrEndpointKind, "writer")
	point(eps, readers, attrEndpointKind, "reader")
	h := s.health(ros)
	un := gauge(mUnmatched, "Endpoints with no counterpart on this host, by topic and type: readers no writer feeds, writers no reader reads. With ROS, services are left out.", "{endpoint}")
	point(un, int64(h.ReadersWithoutWriters), attrEndpointKind, "reader")
	point(un, int64(h.WritersWithoutReaders), attrEndpointKind, "writer")
	point(gauge(mIncompatible, "Reader/writer pairs on a topic where the reader asks more reliability or durability than the writer offers.", "{pair}"), int64(h.IncompatiblePairs))
	return md
}
