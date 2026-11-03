// Package participant is a DDS participant that observes: it discovers the
// participants it is allowed to talk to (SPDP), learns their endpoints
// (SEDP), and subscribes to the topics it is asked to, reading reliably.
//
// It talks only to participants its filter keeps, and only by unicast to
// addresses of this host: it announces itself to no one else. It publishes no
// user data.
package participant

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/transport"
)

// Config configures a Participant.
type Config struct {
	Domain uint32
	// Interface is the network interface DDS runs on, and Addr its address
	// the participant listens on for unicast.
	Interface *net.Interface
	Addr      netip.Addr

	// Filter keeps the participants to talk to. It must keep only
	// participants on this host: the participant refuses to send anywhere
	// else whatever the filter says. Nil keeps the local ones
	// (discovery.LocalOnly).
	Filter discovery.Filter

	// LeaseDuration is what the participant announces; zero is 10 s, Cyclone
	// DDS's default. It is renewed every AnnounceInterval (zero: a third of
	// the lease).
	LeaseDuration    time.Duration
	AnnounceInterval time.Duration

	// Name is the entity name announced; zero is "dds-observer".
	Name string

	Logger *slog.Logger
	Now    func() time.Time
}

// VendorID is what this implementation sends in its messages. It has no
// vendor ID assigned by the OMG, and uses the one meaning "unknown".
var VendorID = rtps.VendorUnknown

// Participant is a running observer participant.
type Participant struct {
	cfg    Config
	prefix rtps.GUIDPrefix
	log    *slog.Logger
	events func(discovery.Event)

	conn  *net.UDPConn
	mcast *transport.MulticastListener
	addr  netip.AddrPort
	local map[netip.Addr]bool
	spdp  *discovery.SPDPObserver

	mu        sync.Mutex
	peers     map[rtps.GUIDPrefix]*peer
	endpoints map[rtps.GUID]*discovery.Endpoint
	readers   map[rtps.EntityID]*userReader
	sent      map[netip.AddrPort]int64
	nextKey   uint32
	// subs is our SEDP subscriptions writer's history: the announcement of
	// reader i is sequence number i+1. subHB counts its heartbeats.
	subs  [][]byte
	subHB int32
}

// peer is a remote participant we talk to.
type peer struct {
	data discovery.Participant
	// metatraffic is where its discovery traffic goes: its metatraffic
	// unicast locators that are addresses of this host.
	metatraffic []netip.AddrPort
	// proxies are our readers' views of its writers, by writer.
	proxies map[rtps.GUID]*writerProxy
	// announced: we have told it of ourselves at least once.
	announced bool
	// subsAcked: it has every reader announcement before this number.
	subsAcked rtps.SequenceNumber
}

// New opens the participant's sockets. Run starts it.
func New(cfg Config, events func(discovery.Event)) (*Participant, error) {
	if err := transport.CheckDomain(cfg.Domain); err != nil {
		return nil, err
	}
	if cfg.Interface == nil || !cfg.Addr.IsValid() {
		return nil, errors.New("participant: an interface and its address are required")
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 10 * time.Second
	}
	if cfg.AnnounceInterval <= 0 {
		cfg.AnnounceInterval = cfg.LeaseDuration / 3
	}
	if cfg.Name == "" {
		cfg.Name = "dds-observer"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	local, err := transport.LocalAddrs()
	if err != nil {
		return nil, err
	}
	if cfg.Filter == nil {
		cfg.Filter = discovery.LocalOnly(local)
	}

	p := &Participant{
		cfg:       cfg,
		log:       cfg.Logger,
		events:    events,
		local:     local,
		peers:     map[rtps.GUIDPrefix]*peer{},
		endpoints: map[rtps.GUID]*discovery.Endpoint{},
		readers:   map[rtps.EntityID]*userReader{},
		sent:      map[netip.AddrPort]int64{},
		nextKey:   1,
	}
	if _, err := rand.Read(p.prefix[:]); err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(cfg.Addr, 0)))
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(1 << 20)
	p.conn = conn
	p.addr = conn.LocalAddr().(*net.UDPAddr).AddrPort()
	p.addr = netip.AddrPortFrom(p.addr.Addr().Unmap(), p.addr.Port())

	mcast, err := transport.ListenMulticast(cfg.Interface, transport.DefaultMulticastGroup, transport.SPDPMulticastPort(cfg.Domain))
	if err != nil {
		conn.Close()
		return nil, err
	}
	p.mcast = mcast
	p.spdp = discovery.NewSPDPObserver(discovery.SPDPConfig{
		Domain: cfg.Domain,
		Filter: cfg.Filter,
		Now:    cfg.Now,
	}, p.onSPDP)
	return p, nil
}

// GUIDPrefix is the participant's own.
func (p *Participant) GUIDPrefix() rtps.GUIDPrefix { return p.prefix }

// Addr is where it receives unicast, and sends from.
func (p *Participant) Addr() netip.AddrPort { return p.addr }

// Participants returns the remote participants known now.
func (p *Participant) Participants() []discovery.Participant { return p.spdp.Participants() }

// Endpoints returns the remote endpoints known now.
func (p *Participant) Endpoints() []discovery.Endpoint {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]discovery.Endpoint, 0, len(p.endpoints))
	for _, e := range p.endpoints {
		out = append(out, *e)
	}
	return out
}

// Sent returns how many datagrams went to each destination.
func (p *Participant) Sent() map[netip.AddrPort]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[netip.AddrPort]int64, len(p.sent))
	for k, v := range p.sent {
		out[k] = v
	}
	return out
}

// Run runs the participant until the context ends, then tells its peers it is
// gone and closes its sockets.
func (p *Participant) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(3)
	go func() {
		defer wg.Done()
		errs <- p.mcast.Receive(ctx, func(pk transport.Packet) { p.spdp.Handle(pk.From, pk.Data) })
	}()
	go func() {
		defer wg.Done()
		errs <- p.receiveUnicast(ctx)
	}()
	go func() {
		defer wg.Done()
		p.tick(ctx)
	}()

	var err error
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	p.sayGoodbye()
	cancel()
	_ = p.conn.Close()
	wg.Wait()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (p *Participant) receiveUnicast(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() { _ = p.conn.Close() })
	defer stop()
	buf := make([]byte, 65507)
	for {
		n, from, err := p.conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		p.handleUnicast(netip.AddrPortFrom(from.Addr().Unmap(), from.Port()), buf[:n])
	}
}

func (p *Participant) tick(ctx context.Context) {
	announce := time.NewTicker(p.cfg.AnnounceInterval)
	lease := time.NewTicker(time.Second)
	defer announce.Stop()
	defer lease.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-lease.C:
			p.spdp.CheckLeases()
		case <-announce.C:
			p.announceAll()
		}
	}
}

// onSPDP follows the participants SPDP reports: a new one is told of us and
// asked for its endpoints; a lost one takes its endpoints with it.
func (p *Participant) onSPDP(e discovery.Event) {
	switch e := e.(type) {
	case discovery.ParticipantDiscovered:
		p.emit(e)
		p.mu.Lock()
		pe := p.addPeer(e.Participant)
		p.mu.Unlock()
		if pe != nil {
			p.announce(pe)
			p.greet(pe)
			p.sendSubscriptions(pe, nil)
		}
	case discovery.ParticipantUpdated:
		p.emit(e)
		p.mu.Lock()
		if pe, ok := p.peers[e.Participant.GUID.Prefix]; ok {
			pe.data = e.Participant
			pe.metatraffic = p.localLocators(e.Participant.MetatrafficUnicast)
		}
		p.mu.Unlock()
	case discovery.ParticipantLost:
		p.mu.Lock()
		delete(p.peers, e.Participant.GUID.Prefix)
		var lost []discovery.Event
		for g, ep := range p.endpoints {
			if ep.Participant == e.Participant.GUID.Prefix {
				lost = append(lost, discovery.EndpointLost{Endpoint: *ep, Reason: discovery.EndpointParticipantLost, At: e.At})
				p.unmatch(ep)
				delete(p.endpoints, g)
			}
		}
		p.mu.Unlock()
		for _, l := range lost {
			p.emit(l)
		}
		p.emit(e)
	}
}

// addPeer registers a discovered participant; nil if it cannot be talked to
// (no metatraffic address on this host). Called with mu held.
func (p *Participant) addPeer(d discovery.Participant) *peer {
	meta := p.localLocators(d.MetatrafficUnicast)
	if len(meta) == 0 {
		p.log.Debug("participant has no local metatraffic address; not talking to it", "guid", d.GUID.Prefix)
		return nil
	}
	pe := &peer{data: d, metatraffic: meta, proxies: map[rtps.GUID]*writerProxy{}, subsAcked: 1}
	p.peers[d.GUID.Prefix] = pe
	return pe
}

// localLocators keeps the UDPv4 locators that are addresses of this host.
func (p *Participant) localLocators(ls []rtps.Locator) []netip.AddrPort {
	var out []netip.AddrPort
	for _, l := range ls {
		if l.Kind == rtps.LocatorUDPv4 && p.local[l.Addr] && l.Port > 0 && l.Port < 65536 {
			out = append(out, netip.AddrPortFrom(l.Addr, uint16(l.Port)))
		}
	}
	return out
}

// send writes a message to every address, if it is of this host. It is the
// one place datagrams leave.
func (p *Participant) send(to []netip.AddrPort, b []byte) {
	for _, a := range to {
		if !p.local[a.Addr()] {
			p.log.Error("refusing to send off this host", "to", a)
			continue
		}
		if _, err := p.conn.WriteToUDPAddrPort(b, a); err != nil {
			p.log.Debug("send", "to", a, "err", err)
			continue
		}
		p.mu.Lock()
		p.sent[a]++
		p.mu.Unlock()
	}
}

func (p *Participant) emit(e discovery.Event) {
	if p.events != nil {
		p.events(e)
	}
}

// builtinEndpoints is what we have: SPDP both ways, the SEDP detectors, and
// the subscriptions announcer once there are readers to announce.
func (p *Participant) builtinEndpoints() uint32 {
	return rtps.BuiltinParticipantAnnouncer | rtps.BuiltinParticipantDetector |
		rtps.BuiltinPublicationsDetector | rtps.BuiltinSubscriptionsDetector |
		rtps.BuiltinSubscriptionsAnnouncer
}

// spdpPayload is what we announce of ourselves.
func (p *Participant) spdpPayload() []byte {
	loc := rtps.Locator{Kind: rtps.LocatorUDPv4, Port: uint32(p.addr.Port()), Addr: p.addr.Addr()}
	host, _ := os.Hostname()
	exe, _ := os.Executable()
	return (&rtps.ParameterListWriter{}).
		GUID(rtps.PIDParticipantGUID, rtps.GUID{Prefix: p.prefix, Entity: rtps.EntityParticipant}).
		Raw(rtps.PIDProtocolVersion, []byte{2, 1}).
		Raw(rtps.PIDVendorID, VendorID[:]).
		Uint32(rtps.PIDDomainID, p.cfg.Domain).
		Duration(rtps.PIDParticipantLease, p.cfg.LeaseDuration).
		Locator(rtps.PIDMetatrafficUnicastLoc, loc).
		Locator(rtps.PIDDefaultUnicastLoc, loc).
		Uint32(rtps.PIDBuiltinEndpointSet, p.builtinEndpoints()).
		String(rtps.PIDEntityName, p.cfg.Name).
		Properties(
			[2]string{"__Hostname", host},
			[2]string{"__Pid", strconv.Itoa(os.Getpid())},
			[2]string{"__ProcessName", filepath.Base(exe)},
		).
		Payload()
}

func (p *Participant) spdpMessage() []byte {
	return rtps.NewBuilder(p.prefix, VendorID).
		InfoTS(p.cfg.Now()).
		Data(rtps.EntitySPDPReader, rtps.EntitySPDPWriter, 1, nil, p.spdpPayload(), false).
		Bytes()
}

// announce sends our SPDP to one peer.
func (p *Participant) announce(pe *peer) {
	p.send(pe.metatraffic, p.spdpMessage())
	p.mu.Lock()
	pe.announced = true
	p.mu.Unlock()
}

// announceAll renews our lease with every peer, and nudges the writers that
// have not started talking to us.
func (p *Participant) announceAll() {
	msg := p.spdpMessage()
	p.mu.Lock()
	peers := make([]*peer, 0, len(p.peers))
	for _, pe := range p.peers {
		peers = append(peers, pe)
	}
	p.mu.Unlock()
	for _, pe := range peers {
		p.send(pe.metatraffic, msg)
		p.greet(pe)
		p.heartbeatSubscriptions(pe)
	}
}

// sayGoodbye tells every peer we are gone (an SPDP dispose), so that it
// forgets us now rather than when our lease runs out.
func (p *Participant) sayGoodbye() {
	key := (&rtps.ParameterListWriter{}).
		GUID(rtps.PIDParticipantGUID, rtps.GUID{Prefix: p.prefix, Entity: rtps.EntityParticipant}).
		Payload()
	qos := (&rtps.ParameterListWriter{}).
		Raw(rtps.PIDStatusInfo, []byte{0, 0, 0, rtps.StatusDisposed | rtps.StatusUnregistered}).
		List()
	msg := rtps.NewBuilder(p.prefix, VendorID).
		InfoTS(p.cfg.Now()).
		Data(rtps.EntitySPDPReader, rtps.EntitySPDPWriter, 2, qos, key, true).
		Bytes()
	p.mu.Lock()
	var to []netip.AddrPort
	for _, pe := range p.peers {
		to = append(to, pe.metatraffic...)
	}
	p.mu.Unlock()
	p.send(to, msg)
}

// sedpReaders are our SEDP readers and the writers they read.
var sedpReaders = []struct {
	reader, writer rtps.EntityID
	kind           rtps.EndpointKind
	announcer      uint32
}{
	{rtps.EntitySEDPPublicationsReader, rtps.EntitySEDPPublicationsWriter, rtps.Writer, rtps.BuiltinPublicationsAnnouncer},
	{rtps.EntitySEDPSubscriptionsReader, rtps.EntitySEDPSubscriptionsWriter, rtps.Reader, rtps.BuiltinSubscriptionsAnnouncer},
}

// greet sends a preemptive ACKNACK to each of the peer's SEDP writers we have
// not heard a heartbeat from: we are here, send us what you have.
func (p *Participant) greet(pe *peer) {
	b := rtps.NewBuilder(p.prefix, VendorID).InfoDst(pe.data.GUID.Prefix)
	any := false
	p.mu.Lock()
	for _, s := range sedpReaders {
		if pe.data.BuiltinEndpoints&s.announcer == 0 {
			continue
		}
		w := p.proxy(pe, rtps.GUID{Prefix: pe.data.GUID.Prefix, Entity: s.writer})
		if w.haveHB {
			continue
		}
		b.AckNack(s.reader, s.writer, w.preemptive(), w.nextAckCount(), false)
		any = true
	}
	p.mu.Unlock()
	if any {
		p.send(pe.metatraffic, b.Bytes())
	}
}

// proxy returns our view of a peer's writer. Called with mu held.
func (p *Participant) proxy(pe *peer, writer rtps.GUID) *writerProxy {
	w, ok := pe.proxies[writer]
	if !ok {
		w = newWriterProxy()
		pe.proxies[writer] = w
	}
	return w
}

// handleUnicast processes a datagram sent to us.
func (p *Participant) handleUnicast(from netip.AddrPort, b []byte) {
	msg, err := rtps.Parse(b)
	if err != nil && len(msg.Submessages) == 0 {
		return
	}
	src := msg.Header.Prefix
	toUs := true
	spdp := false
	for _, s := range msg.Submessages {
		switch s.ID {
		case rtps.SubInfoDst:
			dst, err := rtps.ParseInfoDst(s)
			if err == nil {
				toUs = dst.IsZero() || dst == p.prefix
			}
			continue
		case rtps.SubInfoSrc:
			// Relayed traffic is not something an observer on one host sees.
			return
		}
		if !toUs {
			continue
		}
		switch s.ID {
		case rtps.SubData:
			d, err := rtps.ParseData(s)
			if err != nil {
				continue
			}
			if d.Writer == rtps.EntitySPDPWriter {
				spdp = true
				continue
			}
			p.onData(src, d)
		case rtps.SubDataFrag:
			if f, err := rtps.ParseDataFrag(s); err == nil {
				p.onDataFrag(src, f)
			}
		case rtps.SubHeartbeat:
			if h, err := rtps.ParseHeartbeat(s); err == nil {
				p.onHeartbeat(src, h)
			}
		case rtps.SubGap:
			if g, err := rtps.ParseGap(s); err == nil {
				p.onGap(src, g)
			}
		case rtps.SubHeartbeatFrg:
			if h, err := rtps.ParseHeartbeatFrag(s); err == nil {
				p.onHeartbeatFrag(src, h)
			}
		case rtps.SubAckNack:
			if a, err := rtps.ParseAckNack(s); err == nil {
				p.onAckNack(src, a)
			}
		}
	}
	if spdp {
		p.spdp.Handle(from, b)
	}
}

// route finds the peer and our reader a writer's traffic is for. Called
// with mu held.
func (p *Participant) route(src rtps.GUIDPrefix, writer rtps.EntityID) (*peer, *writerProxy, func([]Sample)) {
	pe, ok := p.peers[src]
	if !ok {
		return nil, nil, nil
	}
	for _, s := range sedpReaders {
		if writer == s.writer {
			kind := s.kind
			return pe, p.proxy(pe, rtps.GUID{Prefix: src, Entity: writer}), func(ss []Sample) { p.onSEDP(pe, kind, ss) }
		}
	}
	g := rtps.GUID{Prefix: src, Entity: writer}
	for _, r := range p.readers {
		if r.matched[g] {
			return pe, p.proxy(pe, g), func(ss []Sample) { r.deliver(g, ss) }
		}
	}
	return nil, nil, nil
}

func (p *Participant) onData(src rtps.GUIDPrefix, d rtps.Data) {
	p.mu.Lock()
	_, w, deliver := p.route(src, d.Writer)
	var ss []Sample
	if w != nil {
		ss = w.data(d)
	}
	p.mu.Unlock()
	if len(ss) > 0 {
		deliver(ss)
	}
}

func (p *Participant) onDataFrag(src rtps.GUIDPrefix, f rtps.DataFrag) {
	p.mu.Lock()
	_, w, deliver := p.route(src, f.Writer)
	var ss []Sample
	if w != nil {
		ss = w.fragment(f)
	}
	p.mu.Unlock()
	if len(ss) > 0 {
		deliver(ss)
	}
}

func (p *Participant) onGap(src rtps.GUIDPrefix, g rtps.Gap) {
	p.mu.Lock()
	_, w, deliver := p.route(src, g.Writer)
	var ss []Sample
	if w != nil {
		ss = w.gap(g)
	}
	p.mu.Unlock()
	if len(ss) > 0 {
		deliver(ss)
	}
}

func (p *Participant) onHeartbeat(src rtps.GUIDPrefix, h rtps.Heartbeat) {
	p.mu.Lock()
	pe, w, deliver := p.route(src, h.Writer)
	var ss []Sample
	var ack *rtps.SequenceNumberSet
	var nacks []fragNack
	var count int32
	var nackCounts []int32
	reader := h.Reader
	if w != nil {
		ss, ack, nacks = w.heartbeat(h)
		if ack != nil {
			count = w.nextAckCount()
		}
		for range nacks {
			nackCounts = append(nackCounts, w.nextNackCount())
		}
		reader = p.readerFor(src, h.Writer, h.Reader)
	}
	p.mu.Unlock()
	if len(ss) > 0 {
		deliver(ss)
	}
	if ack != nil {
		p.mu.Lock()
		to := p.replyTo(pe, rtps.GUID{Prefix: src, Entity: h.Writer})
		p.mu.Unlock()
		b := rtps.NewBuilder(p.prefix, VendorID).InfoDst(src)
		for i, n := range nacks {
			b.NackFrag(reader, h.Writer, n.SN, n.Missing, nackCounts[i])
		}
		b.AckNack(reader, h.Writer, *ack, count, false)
		p.send(to, b.Bytes())
	}
}

func (p *Participant) onHeartbeatFrag(src rtps.GUIDPrefix, h rtps.HeartbeatFrag) {
	p.mu.Lock()
	pe, w, _ := p.route(src, h.Writer)
	if w == nil {
		p.mu.Unlock()
		return
	}
	n := w.heartbeatFrag(h)
	if n == nil {
		p.mu.Unlock()
		return
	}
	count := w.nextNackCount()
	reader := p.readerFor(src, h.Writer, h.Reader)
	to := p.replyTo(pe, rtps.GUID{Prefix: src, Entity: h.Writer})
	p.mu.Unlock()
	msg := rtps.NewBuilder(p.prefix, VendorID).InfoDst(src).NackFrag(reader, h.Writer, n.SN, n.Missing, count).Bytes()
	p.send(to, msg)
}

// readerFor is our reader that reads a writer: an SEDP reader, or a user
// reader matched with it. Called with mu held.
func (p *Participant) readerFor(src rtps.GUIDPrefix, writer, addressed rtps.EntityID) rtps.EntityID {
	for _, s := range sedpReaders {
		if writer == s.writer {
			return s.reader
		}
	}
	g := rtps.GUID{Prefix: src, Entity: writer}
	for id, r := range p.readers {
		if r.matched[g] {
			return id
		}
	}
	return addressed
}

// onSEDP takes delivered SEDP samples from a peer: endpoints announced,
// changed, or disposed.
func (p *Participant) onSEDP(pe *peer, kind rtps.EndpointKind, ss []Sample) {
	now := p.cfg.Now()
	var evs []discovery.Event
	var nudges []matchNudge
	p.mu.Lock()
	for _, s := range ss {
		if guid, ok := disposedEndpoint(s); ok {
			if ep, ok := p.endpoints[guid]; ok {
				delete(p.endpoints, guid)
				evs = append(evs, discovery.EndpointLost{Endpoint: *ep, Reason: discovery.EndpointDisposed, At: now})
				p.unmatch(ep)
			}
			continue
		}
		if s.Payload == nil || s.KeyOnly {
			continue
		}
		pl, err := rtps.ReadParameterListPayload(s.Payload)
		if err != nil {
			continue
		}
		ed, err := rtps.ParseEndpointData(kind, pl)
		if err != nil || ed.Participant != pe.data.GUID.Prefix {
			continue
		}
		prev, ok := p.endpoints[ed.GUID]
		ep := &discovery.Endpoint{EndpointData: ed, FirstSeen: now, LastSeen: now}
		if ok {
			ep.FirstSeen = prev.FirstSeen
		}
		p.endpoints[ed.GUID] = ep
		switch {
		case !ok:
			evs = append(evs, discovery.EndpointDiscovered{Endpoint: *ep})
			nudges = append(nudges, p.match(ep)...)
		case !discovery.SameEndpoint(&prev.EndpointData, &ed):
			evs = append(evs, discovery.EndpointUpdated{Endpoint: *ep})
		}
	}
	p.mu.Unlock()
	for _, e := range evs {
		p.emit(e)
	}
	p.nudge(nudges)
}

// disposedEndpoint reports the endpoint a disposing or unregistering sample
// is about.
func disposedEndpoint(s Sample) (rtps.GUID, bool) {
	v, ok := s.InlineQoS.Get(rtps.PIDStatusInfo)
	if !ok {
		return rtps.GUID{}, false
	}
	st, err := rtps.ReadStatusInfo(v)
	if err != nil || st&(rtps.StatusDisposed|rtps.StatusUnregistered) == 0 {
		return rtps.GUID{}, false
	}
	if s.Payload != nil {
		if pl, err := rtps.ReadParameterListPayload(s.Payload); err == nil {
			if v, ok := pl.Get(rtps.PIDEndpointGUID); ok {
				if g, err := rtps.ReadGUID(v); err == nil {
					return g, true
				}
			}
		}
	}
	if kh, ok := s.InlineQoS.Get(rtps.PIDKeyHash); ok {
		if g, err := rtps.ReadGUID(kh); err == nil {
			return g, true
		}
	}
	return rtps.GUID{}, false
}
