package ddsreceiver

import (
	"sort"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/traffic"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Metric names of the traffic.
const (
	mWriterSamples    = "dds.writer.samples"
	mWriterBytes      = "dds.writer.bytes"
	mWriterRetransmit = "dds.writer.retransmit_requests"
	mCaptureDrops     = "dds.capture.drops"

	attrRequestKind = "dds.request.kind" // sample (ACKNACK), fragment (NACK_FRAG)
)

// writerTraffic is one local writer's traffic, with what is known of it.
type writerTraffic struct {
	traffic.Writer
	Endpoint    *discovery.Endpoint // nil if SEDP has not told of it
	Participant participantState
	Node        string // the ROS node it is of; "" if none is known
}

// localWriters is the traffic of the writers of this host's participants,
// sorted by GUID.
func (s *state) localWriters(ws []traffic.Writer) []writerTraffic {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodeOf := map[rtps.GUID]string{}
	for _, ns := range s.nodes {
		for _, n := range ns {
			for _, w := range n.Writers {
				nodeOf[w] = n.FullName()
			}
		}
	}
	var out []writerTraffic
	for _, w := range ws {
		p, ok := s.participants[w.GUID.Prefix]
		if !ok {
			continue // another host's
		}
		wt := writerTraffic{Writer: w, Participant: *p, Node: nodeOf[w.GUID]}
		if e, ok := s.endpoints[w.GUID]; ok {
			wt.Endpoint = &e
		}
		out = append(out, wt)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GUID.String() < out[j].GUID.String() })
	return out
}

// isLocal reports whether a participant is one of this host's.
func (s *state) isLocal(p rtps.GUIDPrefix) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.participants[p]
	return ok
}

func counter(ms pmetric.MetricSlice, name, desc, unit string) pmetric.NumberDataPointSlice {
	m := ms.AppendEmpty()
	m.SetName(name)
	m.SetDescription(desc)
	m.SetUnit(unit)
	s := m.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	return s.DataPoints()
}

// trafficMetrics is what each local writer of a domain has written, as
// cumulative sums since it was first seen.
func trafficMetrics(res resource, at time.Time, ws []writerTraffic, ros bool) pmetric.Metrics {
	md, ms := newMetrics(res)
	ts := pcommon.NewTimestampFromTime(at)
	sum := func(name, desc, unit string) pmetric.NumberDataPointSlice { return counter(ms, name, desc, unit) }
	samples := sum(mWriterSamples, "Samples the writer has written since it was first seen, by its sequence numbers.", "{sample}")
	bytes := sum(mWriterBytes, "Serialized bytes of those samples; samples whose size was not seen are counted at the average.", "By")
	requests := sum(mWriterRetransmit, "What readers asked the writer to send again: samples (ACKNACK) and fragments (NACK_FRAG).", "{request}")
	for _, w := range ws {
		start := pcommon.NewTimestampFromTime(w.FirstSeen)
		point := func(dps pmetric.NumberDataPointSlice, v int64) pcommon.Map {
			dp := dps.AppendEmpty()
			dp.SetStartTimestamp(start)
			dp.SetTimestamp(ts)
			dp.SetIntValue(v)
			a := dp.Attributes()
			if w.Endpoint != nil {
				putTopic(a, *w.Endpoint, ros)
			} else {
				a.PutStr(attrEndpoint, w.GUID.String())
				a.PutStr(attrEndpointKind, rtps.Writer.String())
			}
			a.PutStr(attrParticipant, w.GUID.Prefix.String())
			if v := w.Participant.ProcessName(); v != "" {
				a.PutStr(attrDDSProcess, v)
			}
			putProcess(a, w.Participant)
			if ros && w.Node != "" {
				a.PutStr(attrNode, w.Node)
			}
			return a
		}
		point(samples, w.Samples())
		point(bytes, w.Bytes())
		point(requests, w.RetransmitRequests).PutStr(attrRequestKind, "sample")
		point(requests, w.FragmentRequests).PutStr(attrRequestKind, "fragment")
	}
	return md
}

// dropsMetrics is the packets the capture dropped since it began: of the
// receiver, not of a domain.
func dropsMetrics(at, began time.Time, drops uint64) pmetric.Metrics {
	md, ms := newMetrics(resource{})
	dp := counter(ms, mCaptureDrops, "Packets the kernel dropped before the capture read them. Sample counts survive drops; bytes and requests may be undercounted.", "{packet}").AppendEmpty()
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(began))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(at))
	dp.SetIntValue(int64(drops))
	return md
}
