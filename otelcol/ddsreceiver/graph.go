package ddsreceiver

import (
	"sort"
	"sync"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/ros2"
	"github.com/lesomnus/dds/rtps"

	"github.com/lesomnus/dds/otelcol/ddsreceiver/internal/procmap"
)

// state is the graph as the receiver knows it: participants with the host
// processes they are, their endpoints, and the nodes they host.
type state struct {
	mu           sync.Mutex
	participants map[rtps.GUIDPrefix]*participantState
	endpoints    map[rtps.GUID]discovery.Endpoint
	nodes        map[rtps.GUIDPrefix]map[string]ros2.Node
}

type participantState struct {
	discovery.Participant
	// Process is the host process; zero if it could not be resolved.
	Process procmap.Process
}

func newState() *state {
	return &state{
		participants: map[rtps.GUIDPrefix]*participantState{},
		endpoints:    map[rtps.GUID]discovery.Endpoint{},
		nodes:        map[rtps.GUIDPrefix]map[string]ros2.Node{},
	}
}

// localPorts are the ports a participant receives on, of the addresses that
// are local: the sockets its process holds.
func localPorts(p discovery.Participant) []uint16 {
	var out []uint16
	seen := map[uint32]bool{}
	for _, ls := range [][]rtps.Locator{p.MetatrafficUnicast, p.DefaultUnicast} {
		for _, l := range ls {
			if l.Kind == rtps.LocatorUDPv4 && l.Port > 0 && l.Port < 65536 && !seen[l.Port] && l.Addr == p.From.Addr() {
				seen[l.Port] = true
				out = append(out, uint16(l.Port))
			}
		}
	}
	return out
}

func (s *state) participant(prefix rtps.GUIDPrefix) (participantState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.participants[prefix]
	if !ok {
		return participantState{}, false
	}
	return *p, true
}

// apply updates the state with an event. For a participant lost or a node
// gone it returns the participant as it was, for the record.
func (s *state) apply(e any, resolve func(discovery.Participant) procmap.Process) (participantState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch e := e.(type) {
	case discovery.ParticipantDiscovered:
		s.mu.Unlock()
		proc := resolve(e.Participant) // reads /proc: not under the lock
		s.mu.Lock()
		p := &participantState{Participant: e.Participant, Process: proc}
		s.participants[e.Participant.GUID.Prefix] = p
		return *p, true
	case discovery.ParticipantUpdated:
		if p, ok := s.participants[e.Participant.GUID.Prefix]; ok {
			p.Participant = e.Participant
			return *p, true
		}
	case discovery.ParticipantLost:
		p, ok := s.participants[e.Participant.GUID.Prefix]
		delete(s.participants, e.Participant.GUID.Prefix)
		delete(s.nodes, e.Participant.GUID.Prefix)
		if ok {
			return *p, true
		}
		return participantState{Participant: e.Participant}, true
	case discovery.EndpointDiscovered:
		s.endpoints[e.Endpoint.GUID] = e.Endpoint
		return s.of(e.Endpoint.Participant)
	case discovery.EndpointUpdated:
		s.endpoints[e.Endpoint.GUID] = e.Endpoint
		return s.of(e.Endpoint.Participant)
	case discovery.EndpointLost:
		delete(s.endpoints, e.Endpoint.GUID)
		return s.of(e.Endpoint.Participant)
	case ros2.NodeAppeared:
		s.node(e.Node, true)
		return s.of(e.Node.Participant)
	case ros2.NodeUpdated:
		s.node(e.Node, true)
		return s.of(e.Node.Participant)
	case ros2.NodeDisappeared:
		s.node(e.Node, false)
		return s.of(e.Node.Participant)
	}
	return participantState{}, false
}

// of is the participant, with mu held.
func (s *state) of(prefix rtps.GUIDPrefix) (participantState, bool) {
	p, ok := s.participants[prefix]
	if !ok {
		return participantState{}, false
	}
	return *p, true
}

func (s *state) node(n ros2.Node, present bool) {
	ns := s.nodes[n.Participant]
	if present {
		if ns == nil {
			ns = map[string]ros2.Node{}
			s.nodes[n.Participant] = ns
		}
		ns[n.FullName()] = n
		return
	}
	delete(ns, n.FullName())
	if len(ns) == 0 {
		delete(s.nodes, n.Participant)
	}
}

// snapshot is the graph at a moment.
type snapshot struct {
	At           time.Time
	Participants []participantState
	Nodes        []ros2.Node
	Endpoints    []discovery.Endpoint
}

func (s *state) snapshot(now time.Time) snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := snapshot{At: now}
	for _, p := range s.participants {
		out.Participants = append(out.Participants, *p)
	}
	for _, ns := range s.nodes {
		for _, n := range ns {
			out.Nodes = append(out.Nodes, n)
		}
	}
	for _, e := range s.endpoints {
		out.Endpoints = append(out.Endpoints, e)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].FullName() < out.Nodes[j].FullName() })
	return out
}

// health counts what in the graph is likely wrong, by topic: readers no
// writer feeds, writers no reader reads, and reader/writer pairs on a topic
// whose QoS do not match (a reader asking more than the writer offers). With
// ROS, services are left out: a server with no client is no fault.
type health struct {
	ReadersWithoutWriters int
	WritersWithoutReaders int
	IncompatiblePairs     int
}

func (s snapshot) health(ros bool) health {
	type side struct{ readers, writers []rtps.QoS }
	topics := map[string]*side{}
	for _, e := range s.Endpoints {
		if _, kind := ros2.ParseTopic(e.Topic); ros && (kind == ros2.ServiceRequest || kind == ros2.ServiceReply) {
			continue
		}
		t := topics[e.Topic+"\x00"+e.Type]
		if t == nil {
			t = &side{}
			topics[e.Topic+"\x00"+e.Type] = t
		}
		if e.Kind == rtps.Writer {
			t.writers = append(t.writers, e.QoS)
		} else {
			t.readers = append(t.readers, e.QoS)
		}
	}
	var h health
	for _, t := range topics {
		if len(t.writers) == 0 {
			h.ReadersWithoutWriters += len(t.readers)
		}
		if len(t.readers) == 0 {
			h.WritersWithoutReaders += len(t.writers)
		}
		for _, r := range t.readers {
			for _, w := range t.writers {
				if w.Reliability < r.Reliability || w.Durability < r.Durability {
					h.IncompatiblePairs++
				}
			}
		}
	}
	return h
}
