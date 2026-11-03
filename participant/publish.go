package participant

import (
	"errors"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/rtps"
)

// WriterQoS is the QoS a writer offers. A reader matches if it asks for no
// more: a reliable writer serves reliable and best-effort readers, a
// transient-local one transient-local and volatile ones.
type WriterQoS struct {
	// Reliability is Reliable when zero, DDS's default for a writer.
	Reliability rtps.ReliabilityKind
	Durability  rtps.DurabilityKind
	History     rtps.HistoryKind
	// Depth is how many samples KEEP_LAST keeps; one when zero.
	Depth int32
	// MaxSamples bounds what KEEP_ALL keeps; 1000 when zero.
	MaxSamples int32
}

// Publication is a topic to write.
type Publication struct {
	Topic string
	Type  string
	QoS   WriterQoS
	// UserData is announced with the writer (USER_DATA QoS).
	UserData []byte
}

// Writer writes samples of a topic to the readers matched with it.
type Writer struct {
	p      *Participant
	id     rtps.EntityID
	pub    Publication
	w      *stateWriter
	closed bool
}

// ErrClosed is what a closed writer returns.
var ErrClosed = errors.New("participant: the writer is closed")

// Publish adds a writer. It is announced to every peer (SEDP), and matched
// with the readers of the topic and type that ask no more than it offers.
// Call it before Run, or while running.
func (p *Participant) Publish(pub Publication) (*Writer, error) {
	if pub.Topic == "" || pub.Type == "" {
		return nil, errors.New("participant: a publication needs a topic and a type")
	}
	q := &pub.QoS
	if q.Reliability == 0 {
		q.Reliability = rtps.Reliable
	}
	if q.Depth <= 0 {
		q.Depth = 1
	}
	if q.MaxSamples <= 0 {
		q.MaxSamples = 1000
	}
	p.mu.Lock()
	k := p.nextKey
	p.nextKey++
	id := rtps.EntityID{byte(k >> 16), byte(k >> 8), byte(k), 0x03} // user-defined writer, no key
	w := &Writer{p: p, id: id, pub: pub, w: newStateWriter(p.prefix, writerConfig{
		id:             id,
		reliable:       q.Reliability == rtps.Reliable,
		transientLocal: q.Durability >= rtps.TransientLocal,
		keepAll:        q.History == rtps.KeepAll,
		depth:          int(q.Depth),
		maxSamples:     int(q.MaxSamples),
	})}
	p.writers[id] = w
	now := p.cfg.Now()
	_, out, _ := p.pubsW.write(change{at: now, payload: p.writerData(w), key: w.GUID()})
	for _, ep := range p.endpoints {
		out = append(out, p.matchOneReader(w, ep, now)...)
	}
	p.mu.Unlock()
	p.sendAll(out)
	return w, nil
}

// GUID is the writer's.
func (w *Writer) GUID() rtps.GUID { return rtps.GUID{Prefix: w.p.prefix, Entity: w.id} }

// Write sends a sample: payload is its serialized data, encapsulation
// header first (00 01 00 00 for CDR little endian), as it goes on the wire.
// It returns the sample's sequence number. It does not wait for readers to
// acknowledge it; a KEEP_ALL writer whose history is full of samples a
// reliable reader lacks returns ErrHistoryFull.
func (w *Writer) Write(payload []byte) (rtps.SequenceNumber, error) {
	if payload == nil {
		return 0, errors.New("participant: no payload")
	}
	p := w.p
	p.mu.Lock()
	if w.closed {
		p.mu.Unlock()
		return 0, ErrClosed
	}
	sn, out, err := w.w.write(change{at: p.cfg.Now(), payload: append([]byte(nil), payload...)})
	p.mu.Unlock()
	p.sendAll(out)
	return sn, err
}

// Matched returns the readers matched with the writer.
func (w *Writer) Matched() []rtps.GUID {
	w.p.mu.Lock()
	defer w.p.mu.Unlock()
	return w.w.matched()
}

// Ready returns the matched readers known to have matched the writer too.
// Matching happens on each side as each learns of the other: a reader
// ignores what a writer sends before it has. A reliable reader is known to
// once it has acknowledged anything; a best-effort one never says, and is
// taken as ready as soon as it is matched.
func (w *Writer) Ready() []rtps.GUID {
	w.p.mu.Lock()
	defer w.p.mu.Unlock()
	return w.w.ready()
}

// Close removes the writer: its readers are told it is gone (an SEDP
// dispose).
func (w *Writer) Close() error {
	p := w.p
	p.mu.Lock()
	if w.closed {
		p.mu.Unlock()
		return nil
	}
	w.closed = true
	delete(p.writers, w.id)
	_, out, _ := p.pubsW.write(disposal(w.GUID(), p.cfg.Now()))
	p.mu.Unlock()
	p.sendAll(out)
	return nil
}

// disposal is the SEDP change that disposes of an endpoint of ours.
func disposal(g rtps.GUID, at time.Time) change {
	key := (&rtps.ParameterListWriter{}).GUID(rtps.PIDEndpointGUID, g).Payload()
	qos := (&rtps.ParameterListWriter{}).
		Raw(rtps.PIDKeyHash, append(g.Prefix[:], g.Entity[:]...)).
		Raw(rtps.PIDStatusInfo, []byte{0, 0, 0, rtps.StatusDisposed | rtps.StatusUnregistered}).
		List()
	return change{at: at, payload: key, inlineQoS: qos, keyOnly: true, key: g}
}

// writerData is the SEDP announcement of one of our writers.
func (p *Participant) writerData(w *Writer) []byte {
	q := w.pub.QoS
	// max_blocking_time: 100 ms, the default, as a Duration_t.
	blocking := append(le32(0), le32(uint32(uint64(100*time.Millisecond)<<32/uint64(time.Second)))...)
	pl := (&rtps.ParameterListWriter{}).
		GUID(rtps.PIDEndpointGUID, w.GUID()).
		GUID(rtps.PIDParticipantGUID, rtps.GUID{Prefix: p.prefix, Entity: rtps.EntityParticipant}).
		String(rtps.PIDTopicName, w.pub.Topic).
		String(rtps.PIDTypeName, w.pub.Type).
		Raw(rtps.PIDReliability, append(le32(uint32(q.Reliability)), blocking...)).
		Uint32(rtps.PIDDurability, uint32(q.Durability)).
		Raw(rtps.PIDHistory, append(le32(uint32(q.History)), le32(uint32(q.Depth))...))
	if w.pub.UserData != nil {
		pl.Octets(rtps.PIDUserData, w.pub.UserData)
	}
	return pl.Payload()
}

// offers reports whether a writer offers what a reader asks for (DDS 1.4,
// 2.2.3: RELIABILITY and DURABILITY are requested/offered).
func offers(w WriterQoS, r rtps.QoS) bool {
	return w.Reliability >= r.Reliability && w.Durability >= r.Durability
}

// matchOneReader matches a discovered reader with one of our writers.
// Called with mu held.
func (p *Participant) matchOneReader(w *Writer, ep *discovery.Endpoint, at time.Time) []outbound {
	if ep.Kind != rtps.Reader || ep.Topic != w.pub.Topic || ep.Type != w.pub.Type || !offers(w.pub.QoS, ep.QoS) {
		return nil
	}
	pe, ok := p.peers[ep.Participant]
	if !ok {
		return nil
	}
	return w.w.addReader(ep.GUID, p.userAddrs(pe, ep), ep.QoS.Reliability == rtps.Reliable, at)
}

// matchReader matches a newly discovered reader with every writer. Called
// with mu held; what it returns is for after it is released.
func (p *Participant) matchReader(ep *discovery.Endpoint) []outbound {
	var out []outbound
	now := p.cfg.Now()
	for _, w := range p.writers {
		out = append(out, p.matchOneReader(w, ep, now)...)
	}
	return out
}

// matchBuiltin matches a new peer's SEDP readers with our SEDP writers: it is
// sent our endpoints.
func (p *Participant) matchBuiltin(pe *peer) {
	now := p.cfg.Now()
	var out []outbound
	p.mu.Lock()
	prefix := pe.data.GUID.Prefix
	if pe.data.BuiltinEndpoints&rtps.BuiltinPublicationsDetector != 0 {
		out = append(out, p.pubsW.addReader(rtps.GUID{Prefix: prefix, Entity: rtps.EntitySEDPPublicationsReader}, pe.metatraffic, true, now)...)
	}
	if pe.data.BuiltinEndpoints&rtps.BuiltinSubscriptionsDetector != 0 {
		out = append(out, p.subsW.addReader(rtps.GUID{Prefix: prefix, Entity: rtps.EntitySEDPSubscriptionsReader}, pe.metatraffic, true, now)...)
	}
	p.mu.Unlock()
	p.sendAll(out)
}

// stateWriterFor is our writer an entity is: SEDP, or a user writer. Called
// with mu held.
func (p *Participant) stateWriterFor(id rtps.EntityID) *stateWriter {
	switch id {
	case rtps.EntitySEDPPublicationsWriter:
		return p.pubsW
	case rtps.EntitySEDPSubscriptionsWriter:
		return p.subsW
	}
	if w, ok := p.writers[id]; ok {
		return w.w
	}
	return nil
}

// onAckNack takes a reader's ACKNACK for one of our writers.
func (p *Participant) onAckNack(src rtps.GUIDPrefix, a rtps.AckNack) {
	p.mu.Lock()
	var out []outbound
	if w := p.stateWriterFor(a.Writer); w != nil {
		out = w.onAckNack(rtps.GUID{Prefix: src, Entity: a.Reader}, a, p.cfg.Now())
	}
	p.mu.Unlock()
	p.sendAll(out)
}

// onNackFrag takes a reader's NACK_FRAG for one of our writers.
func (p *Participant) onNackFrag(src rtps.GUIDPrefix, n rtps.NackFrag) {
	p.mu.Lock()
	var out []outbound
	if w := p.stateWriterFor(n.Writer); w != nil {
		out = w.onNackFrag(rtps.GUID{Prefix: src, Entity: n.Reader}, n)
	}
	p.mu.Unlock()
	p.sendAll(out)
}

// heartbeatWriters heartbeats the readers that lack something.
func (p *Participant) heartbeatWriters() {
	now := p.cfg.Now()
	p.mu.Lock()
	out := append(p.pubsW.tick(now), p.subsW.tick(now)...)
	for _, w := range p.writers {
		out = append(out, w.w.tick(now)...)
	}
	p.mu.Unlock()
	p.sendAll(out)
}

// sendAll sends what writers returned. Called with mu released.
func (p *Participant) sendAll(out []outbound) {
	for _, o := range out {
		p.send(o.to, o.msg)
	}
}
