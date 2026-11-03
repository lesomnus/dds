package participant

import (
	"errors"
	"net/netip"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/rtps"
)

// ReaderQoS is the QoS a subscription asks for. A writer matches if it offers
// at least as much: reliable for reliable, a durability at least as strong.
type ReaderQoS struct {
	Reliability rtps.ReliabilityKind
	Durability  rtps.DurabilityKind
	History     rtps.HistoryKind
	Depth       int32
}

// Subscription is a topic to read.
type Subscription struct {
	Topic string
	Type  string
	QoS   ReaderQoS
	// Keyed: the type has a key (the reader's entity kind says so).
	Keyed bool
	// Handle gets each sample with data, in its writer's order. It is called
	// with no lock held, from the receiving goroutine: it must not block.
	Handle func(writer rtps.GUID, s Sample)
}

// userReader is a subscription and the writers matched with it.
type userReader struct {
	id      rtps.EntityID
	sub     Subscription
	matched map[rtps.GUID]bool
}

func (r *userReader) deliver(writer rtps.GUID, ss []Sample) {
	for _, s := range ss {
		if s.Payload != nil && !s.KeyOnly {
			r.sub.Handle(writer, s)
		}
	}
}

// Subscribe adds a reader. It is announced to every peer (SEDP), and matched
// with the writers of the topic and type that offer enough. Call it before
// Run, or while running.
func (p *Participant) Subscribe(s Subscription) error {
	if s.Topic == "" || s.Type == "" || s.Handle == nil {
		return errors.New("participant: a subscription needs a topic, a type and a handler")
	}
	if s.QoS.Reliability == 0 {
		s.QoS.Reliability = rtps.BestEffort
	}
	if s.QoS.Depth <= 0 {
		s.QoS.Depth = 1
	}
	kind := byte(0x04) // user-defined reader, no key
	if s.Keyed {
		kind = 0x07
	}
	p.mu.Lock()
	k := p.nextKey
	p.nextKey++
	id := rtps.EntityID{byte(k >> 16), byte(k >> 8), byte(k), kind}
	r := &userReader{id: id, sub: s, matched: map[rtps.GUID]bool{}}
	p.readers[id] = r
	p.subs = append(p.subs, p.readerData(r))
	var nudge []matchNudge
	for _, ep := range p.endpoints {
		if n, ok := p.matchOne(r, ep); ok {
			nudge = append(nudge, n)
		}
	}
	peers := p.peerList()
	p.mu.Unlock()
	for _, pe := range peers {
		p.sendSubscriptions(pe, nil)
	}
	p.nudge(nudge)
	return nil
}

// readerData is the SEDP announcement of one of our readers.
func (p *Participant) readerData(r *userReader) []byte {
	q := r.sub.QoS
	return (&rtps.ParameterListWriter{}).
		GUID(rtps.PIDEndpointGUID, rtps.GUID{Prefix: p.prefix, Entity: r.id}).
		GUID(rtps.PIDParticipantGUID, rtps.GUID{Prefix: p.prefix, Entity: rtps.EntityParticipant}).
		String(rtps.PIDTopicName, r.sub.Topic).
		String(rtps.PIDTypeName, r.sub.Type).
		Raw(rtps.PIDReliability, append(le32(uint32(q.Reliability)), 0, 0, 0, 0, 0, 0, 0, 0)).
		Uint32(rtps.PIDDurability, uint32(q.Durability)).
		Raw(rtps.PIDHistory, append(le32(uint32(q.History)), le32(uint32(q.Depth))...)).
		Payload()
}

func le32(v uint32) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }

// matchNudge is a preemptive ACKNACK owed to a newly matched writer.
type matchNudge struct {
	to     []netip.AddrPort
	dst    rtps.GUIDPrefix
	reader rtps.EntityID
	writer rtps.EntityID
	set    rtps.SequenceNumberSet
	count  int32
}

// nudge sends the preemptive ACKNACKs of new matches.
func (p *Participant) nudge(ns []matchNudge) {
	for _, n := range ns {
		msg := rtps.NewBuilder(p.prefix, VendorID).InfoDst(n.dst).AckNack(n.reader, n.writer, n.set, n.count, false).Bytes()
		p.send(n.to, msg)
	}
}

// compatible reports whether a writer offers what a reader asks for
// (DDS 1.4, 2.2.3: RELIABILITY and DURABILITY are requested/offered).
func compatible(w rtps.QoS, r ReaderQoS) bool {
	return w.Reliability >= r.Reliability && w.Durability >= r.Durability
}

// matchOne matches a discovered writer with one of our readers. Called with
// mu held.
func (p *Participant) matchOne(r *userReader, ep *discovery.Endpoint) (matchNudge, bool) {
	if ep.Kind != rtps.Writer || ep.Topic != r.sub.Topic || ep.Type != r.sub.Type || !compatible(ep.QoS, r.sub.QoS) {
		return matchNudge{}, false
	}
	if r.matched[ep.GUID] {
		return matchNudge{}, false
	}
	pe, ok := p.peers[ep.Participant]
	if !ok {
		return matchNudge{}, false
	}
	r.matched[ep.GUID] = true
	w := p.proxy(pe, ep.GUID)
	return matchNudge{
		to: p.userAddrs(pe, ep), dst: ep.Participant, reader: r.id, writer: ep.GUID.Entity,
		set: w.preemptive(), count: w.nextAckCount(),
	}, true
}

// match matches a newly discovered endpoint with every reader. Called with
// mu held; the nudges are for after it is released.
func (p *Participant) match(ep *discovery.Endpoint) []matchNudge {
	var out []matchNudge
	for _, r := range p.readers {
		if n, ok := p.matchOne(r, ep); ok {
			out = append(out, n)
		}
	}
	return out
}

// unmatch forgets a writer gone. Called with mu held.
func (p *Participant) unmatch(ep *discovery.Endpoint) {
	for _, r := range p.readers {
		delete(r.matched, ep.GUID)
	}
	if pe, ok := p.peers[ep.Participant]; ok {
		delete(pe.proxies, ep.GUID)
	}
}

// userAddrs is where traffic for a user writer goes: its unicast locators
// on this host, else its participant's default ones, else its metatraffic
// ones. Called with mu held.
func (p *Participant) userAddrs(pe *peer, ep *discovery.Endpoint) []netip.AddrPort {
	if a := p.localLocators(ep.Unicast); len(a) > 0 {
		return a
	}
	if a := p.localLocators(pe.data.DefaultUnicast); len(a) > 0 {
		return a
	}
	return pe.metatraffic
}

// replyTo is where an answer to a peer's writer goes. Called with mu held.
func (p *Participant) replyTo(pe *peer, writer rtps.GUID) []netip.AddrPort {
	if writer.Entity.Kind()&0xc0 == 0xc0 { // built-in
		return pe.metatraffic
	}
	if ep, ok := p.endpoints[writer]; ok {
		return p.userAddrs(pe, ep)
	}
	return pe.metatraffic
}

// peerList returns the peers. Called with mu held.
func (p *Participant) peerList() []*peer {
	out := make([]*peer, 0, len(p.peers))
	for _, pe := range p.peers {
		out = append(out, pe)
	}
	return out
}

// Our SEDP subscriptions writer: a minimal reliable writer whose history is
// one sample per reader (SN 1, 2, ...), never removed, sent to each peer's
// subscriptions reader until it acknowledges them.

// sendSubscriptions sends a peer the announcements it asks for (nil: all of
// them) and a heartbeat for the whole history.
func (p *Participant) sendSubscriptions(pe *peer, want []rtps.SequenceNumber) {
	p.mu.Lock()
	subs := append([][]byte(nil), p.subs...)
	p.subHB++
	count := p.subHB
	p.mu.Unlock()
	if len(subs) == 0 {
		return
	}
	if want == nil {
		for i := range subs {
			want = append(want, rtps.SequenceNumber(i+1))
		}
	}
	last := rtps.SequenceNumber(len(subs))
	b := rtps.NewBuilder(p.prefix, VendorID).InfoDst(pe.data.GUID.Prefix).InfoTS(p.cfg.Now())
	n := 0
	for _, sn := range want {
		if sn < 1 || sn > last {
			continue
		}
		b.Data(rtps.EntitySEDPSubscriptionsReader, rtps.EntitySEDPSubscriptionsWriter, sn, nil, subs[sn-1], false)
		n++
		if n%8 == 0 { // keep datagrams small
			p.send(pe.metatraffic, b.Bytes())
			b = rtps.NewBuilder(p.prefix, VendorID).InfoDst(pe.data.GUID.Prefix).InfoTS(p.cfg.Now())
		}
	}
	b.Heartbeat(rtps.EntitySEDPSubscriptionsReader, rtps.EntitySEDPSubscriptionsWriter, 1, last, count, false)
	p.send(pe.metatraffic, b.Bytes())
}

// heartbeatSubscriptions renews the heartbeat to a peer that has not
// acknowledged every announcement.
func (p *Participant) heartbeatSubscriptions(pe *peer) {
	p.mu.Lock()
	last := rtps.SequenceNumber(len(p.subs))
	acked := pe.subsAcked
	p.mu.Unlock()
	if last == 0 || acked > last {
		return
	}
	p.sendSubscriptions(pe, nil)
}

// onAckNack takes an ACKNACK for our SEDP subscriptions writer: what the
// peer has, and what it asks for again.
func (p *Participant) onAckNack(src rtps.GUIDPrefix, a rtps.AckNack) {
	if a.Writer != rtps.EntitySEDPSubscriptionsWriter {
		return
	}
	p.mu.Lock()
	pe, ok := p.peers[src]
	if ok && a.State.Base > pe.subsAcked {
		pe.subsAcked = a.State.Base
	}
	last := rtps.SequenceNumber(len(p.subs))
	p.mu.Unlock()
	if !ok {
		return
	}
	want := a.State.Members()
	if len(want) == 0 && a.State.Base <= last {
		// It acknowledged less than we have and asked for nothing: it is
		// a preemptive ACKNACK, or it lost the heartbeat. Send what is past it.
		for sn := a.State.Base; sn <= last; sn++ {
			want = append(want, sn)
		}
	}
	if len(want) > 0 {
		p.sendSubscriptions(pe, want)
	}
}
