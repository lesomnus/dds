package ros2

import (
	"sort"
	"sync"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/participant"
	"github.com/lesomnus/dds/rtps"
)

// NodeGoneReason says how a node went away.
type NodeGoneReason int

const (
	// NodeRemoved: its participant announced its nodes without it -- the
	// node was destroyed, the process lives on.
	NodeRemoved NodeGoneReason = iota + 1
	// NodeParticipantLost: its participant went away.
	NodeParticipantLost
)

func (r NodeGoneReason) String() string {
	switch r {
	case NodeRemoved:
		return "removed"
	case NodeParticipantLost:
		return "participant_lost"
	}
	return "unknown"
}

// Node events.
type (
	NodeAppeared struct {
		Node Node
		At   time.Time
	}
	// NodeUpdated: its endpoints changed.
	NodeUpdated struct {
		Node Node
		At   time.Time
	}
	NodeDisappeared struct {
		Node   Node
		Reason NodeGoneReason
		At     time.Time
	}
)

// Graph follows the ROS nodes of the participants a Participant observes.
// Feed it the participant's discovery events (HandleDiscovery) and
// ros_discovery_info samples (HandleSample); Subscribe wires the latter.
type Graph struct {
	events func(any)
	now    func() time.Time

	mu    sync.Mutex
	nodes map[rtps.GUIDPrefix]map[string]Node // participant -> full name -> node
}

// NewGraph returns a graph that reports node events to events.
func NewGraph(events func(any)) *Graph {
	return &Graph{events: events, now: time.Now, nodes: map[rtps.GUIDPrefix]map[string]Node{}}
}

// Subscribe subscribes p to ros_discovery_info, feeding the graph. It asks
// for what rmw offers: reliable and transient-local, so the last
// announcement of every participant arrives as soon as it is matched.
func (g *Graph) Subscribe(p *participant.Participant) error {
	return p.Subscribe(participant.Subscription{
		Topic: DiscoveryTopic,
		Type:  DiscoveryType,
		QoS: participant.ReaderQoS{
			Reliability: rtps.Reliable,
			Durability:  rtps.TransientLocal,
			History:     rtps.KeepLast,
			Depth:       1,
		},
		Handle: func(writer rtps.GUID, s participant.Sample) { g.HandleSample(writer, s.Payload) },
	})
}

// Nodes returns the nodes known now, by full name and participant.
func (g *Graph) Nodes() []Node {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []Node
	for _, ns := range g.nodes {
		for _, n := range ns {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FullName() != out[j].FullName() {
			return out[i].FullName() < out[j].FullName()
		}
		return string(out[i].Participant[:]) < string(out[j].Participant[:])
	})
	return out
}

// HandleSample takes a ros_discovery_info sample: a participant's nodes as
// they are now.
func (g *Graph) HandleSample(writer rtps.GUID, payload []byte) {
	info, err := ParseParticipantEntitiesInfo(payload)
	if err != nil {
		return
	}
	part := info.Gid.GUID().Prefix
	if part != writer.Prefix {
		return // a participant speaks for itself only
	}
	now := g.now()
	next := map[string]Node{}
	for _, n := range info.Nodes() {
		next[n.FullName()] = n
	}
	var evs []any
	g.mu.Lock()
	prev := g.nodes[part]
	for name, n := range next {
		old, ok := prev[name]
		switch {
		case !ok:
			evs = append(evs, NodeAppeared{Node: n, At: now})
		case !sameEndpoints(old, n):
			evs = append(evs, NodeUpdated{Node: n, At: now})
		}
	}
	for name, n := range prev {
		if _, ok := next[name]; !ok {
			evs = append(evs, NodeDisappeared{Node: n, Reason: NodeRemoved, At: now})
		}
	}
	if len(next) == 0 {
		delete(g.nodes, part)
	} else {
		g.nodes[part] = next
	}
	g.mu.Unlock()
	g.emit(evs)
}

// HandleDiscovery takes the participant's discovery events: a participant
// lost takes its nodes with it.
func (g *Graph) HandleDiscovery(e discovery.Event) {
	l, ok := e.(discovery.ParticipantLost)
	if !ok {
		return
	}
	part := l.Participant.GUID.Prefix
	g.mu.Lock()
	gone := g.nodes[part]
	delete(g.nodes, part)
	g.mu.Unlock()
	var evs []any
	for _, n := range gone {
		evs = append(evs, NodeDisappeared{Node: n, Reason: NodeParticipantLost, At: l.At})
	}
	g.emit(evs)
}

func (g *Graph) emit(evs []any) {
	sort.SliceStable(evs, func(i, j int) bool { return nodeOf(evs[i]).FullName() < nodeOf(evs[j]).FullName() })
	for _, e := range evs {
		if g.events != nil {
			g.events(e)
		}
	}
}

func nodeOf(e any) Node {
	switch e := e.(type) {
	case NodeAppeared:
		return e.Node
	case NodeUpdated:
		return e.Node
	case NodeDisappeared:
		return e.Node
	}
	return Node{}
}

func sameEndpoints(a, b Node) bool {
	return sameGUIDs(a.Readers, b.Readers) && sameGUIDs(a.Writers, b.Writers)
}

func sameGUIDs(a, b []rtps.GUID) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[rtps.GUID]int, len(a))
	for _, g := range a {
		seen[g]++
	}
	for _, g := range b {
		if seen[g] == 0 {
			return false
		}
		seen[g]--
	}
	return true
}
