// Package participant is a DDS participant: it discovers the participants
// it is allowed to talk to (SPDP), learns their endpoints (SEDP), subscribes
// to the topics it is asked to and publishes on those it is asked to,
// reliably or not.
//
// Its Scope says who it talks to. By default (ScopeHost) that is only the
// participants on this host, only by unicast to addresses of this host: it
// announces itself to no one else. ScopeNetwork reaches the DDS interface's
// network too. A participant that only subscribes publishes nothing but its
// discovery.
//
// Payloads are the user's: serialized, encapsulation header first, as they
// go on the wire. Topics are keyless.
package participant

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
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
	"github.com/lesomnus/dds/transport/udp"
)

// Config configures a Participant.
type Config struct {
	Domain uint32
	// Interface is the network interface DDS runs on, and Addr its address
	// the participant listens on for unicast: one interface. Interfaces are
	// several, in order of preference: a participant reachable on more than
	// one is sent to on the first.
	Interface  *net.Interface
	Addr       netip.Addr
	Interfaces []udp.Interface
	// Transports replace the UDP transport the interfaces would make: a
	// test's network in memory (transport/memtest), say.
	Transports []transport.Transport

	// Scope is who it talks to: by default (ScopeHost) only the
	// participants on this host.
	Scope Scope
	// Filter keeps the participants to talk to, within the scope: the
	// participant refuses to send outside its scope whatever the filter
	// says. Nil keeps those of the scope: the local ones (discovery.LocalOnly)
	// in ScopeHost, every one in ScopeNetwork.
	Filter discovery.Filter
	// Peers are addresses SPDP is sent to by unicast, in ScopeNetwork:
	// participants on a network multicast does not cross, or with
	// NoMulticast.
	Peers []netip.Addr
	// NoMulticast keeps ScopeNetwork from announcing to the SPDP multicast
	// group: only Peers, and the participants that announce themselves,
	// learn of it.
	NoMulticast bool
	// Ports is how the unicast port is picked.
	Ports PortPolicy

	// MaxMessageSize bounds a datagram, and FragmentSize the fragments of a
	// sample too large for one. Zero is 60064 and 16000 in ScopeHost (the
	// loopback carries 64 KiB), Cyclone DDS's 14720 and 1344 in ScopeNetwork.
	MaxMessageSize int
	FragmentSize   int
	// MaxSendRate bounds what is sent, in bytes a second; zero does not.
	// Datagrams wait in a queue; one that finds it full is dropped (see
	// Dropped), for reliable writers to send again.
	MaxSendRate int

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

	transports []transport.Transport
	unicast    []rtps.Locator // where we receive, as we announce it
	local      map[netip.Addr]bool
	nets       []netip.Prefix // the subnets the transports reach, in order of preference
	peerAddrs  map[netip.Addr]bool
	spdp       *discovery.SPDPObserver

	mu        sync.Mutex
	peers     map[rtps.GUIDPrefix]*peer
	endpoints map[rtps.GUID]*discovery.Endpoint
	readers   map[rtps.EntityID]*userReader
	writers   map[rtps.EntityID]*Writer
	sent      map[netip.AddrPort]int64
	nextKey   uint32
	// Our SEDP writers: the announcements of our writers and readers.
	pubsW, subsW *stateWriter
}

// peer is a remote participant we talk to.
type peer struct {
	data discovery.Participant
	// metatraffic is where its discovery traffic goes: its metatraffic
	// unicast locators that are addresses of this host.
	metatraffic []netip.AddrPort
	// proxies are our readers' views of its writers: one for each pair of
	// our reader and its writer.
	proxies map[proxyKey]*writerProxy
	// announced: we have told it of ourselves at least once.
	announced bool
}

// New opens the participant's sockets. Run starts it.
func New(cfg Config, events func(discovery.Event)) (*Participant, error) {
	if err := transport.CheckDomain(cfg.Domain); err != nil {
		return nil, err
	}
	if cfg.Interface != nil && cfg.Addr.IsValid() {
		cfg.Interfaces = append([]udp.Interface{{Interface: cfg.Interface, Addr: cfg.Addr}}, cfg.Interfaces...)
	}
	if len(cfg.Interfaces) == 0 && len(cfg.Transports) == 0 {
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
	if cfg.Scope != ScopeHost && cfg.Scope != ScopeNetwork {
		return nil, fmt.Errorf("participant: scope %s", cfg.Scope)
	}
	if cfg.MaxMessageSize <= 0 {
		cfg.MaxMessageSize = hostMaxMessage
		if cfg.Scope == ScopeNetwork {
			cfg.MaxMessageSize = networkMaxMessage
		}
	}
	if cfg.FragmentSize <= 0 {
		cfg.FragmentSize = hostFragment
		if cfg.Scope == ScopeNetwork {
			cfg.FragmentSize = networkFragment
		}
	}
	if cfg.MaxMessageSize > 65507 || cfg.MaxMessageSize < messageOverhead+64 {
		return nil, fmt.Errorf("participant: a message size of %d", cfg.MaxMessageSize)
	}
	if cfg.FragmentSize > cfg.MaxMessageSize-messageOverhead || cfg.FragmentSize < 64 {
		return nil, fmt.Errorf("participant: a fragment size of %d for messages of %d", cfg.FragmentSize, cfg.MaxMessageSize)
	}
	transports := cfg.Transports
	if len(transports) == 0 {
		ports := cfg.Ports
		if ports == PortsDefault {
			ports = PortsEphemeral
			if cfg.Scope == ScopeNetwork {
				ports = PortsWellKnown
			}
		}
		t, err := udp.Open(udp.Config{
			Domain:         cfg.Domain,
			Interfaces:     cfg.Interfaces,
			WellKnownPorts: ports == PortsWellKnown,
			SendMulticast:  cfg.Scope == ScopeNetwork && !cfg.NoMulticast,
		})
		if err != nil {
			return nil, err
		}
		transports = []transport.Transport{t}
	}
	if cfg.MaxSendRate > 0 {
		for i, t := range transports {
			transports[i] = transport.NewPaced(t, cfg.MaxSendRate, 4*cfg.MaxMessageSize)
		}
	}
	local := map[netip.Addr]bool{}
	var nets []netip.Prefix
	var unicast []rtps.Locator
	for _, t := range transports {
		for _, a := range t.LocalAddrs() {
			local[a] = true
		}
		nets = append(nets, t.Networks()...)
		unicast = append(unicast, t.Unicast()...)
	}
	if cfg.Filter == nil {
		if cfg.Scope == ScopeHost {
			cfg.Filter = discovery.LocalOnly(local)
		} else {
			cfg.Filter = func(*discovery.Participant) bool { return true }
		}
	}

	p := &Participant{
		cfg:        cfg,
		log:        cfg.Logger,
		events:     events,
		local:      local,
		peers:      map[rtps.GUIDPrefix]*peer{},
		endpoints:  map[rtps.GUID]*discovery.Endpoint{},
		readers:    map[rtps.EntityID]*userReader{},
		writers:    map[rtps.EntityID]*Writer{},
		sent:       map[netip.AddrPort]int64{},
		nextKey:    1,
		transports: transports,
		unicast:    unicast,
		nets:       nets,
		peerAddrs:  map[netip.Addr]bool{},
	}
	for _, a := range cfg.Peers {
		p.peerAddrs[a.Unmap()] = true
	}
	if _, err := rand.Read(p.prefix[:]); err != nil {
		p.closeTransports()
		return nil, err
	}
	sedp := p.writerConfig(writerConfig{reliable: true, transientLocal: true, keyed: true})
	sedp.id = rtps.EntitySEDPPublicationsWriter
	p.pubsW = newStateWriter(p.prefix, sedp)
	sedp.id = rtps.EntitySEDPSubscriptionsWriter
	p.subsW = newStateWriter(p.prefix, sedp)

	filter := cfg.Filter
	p.spdp = discovery.NewSPDPObserver(discovery.SPDPConfig{
		Domain: cfg.Domain,
		// Not ourselves: our multicast announcements come back to us.
		Filter: func(d *discovery.Participant) bool { return d.GUID.Prefix != p.prefix && filter(d) },
		Now:    cfg.Now,
	}, p.onSPDP)
	return p, nil
}

// GUIDPrefix is the participant's own.
func (p *Participant) GUIDPrefix() rtps.GUIDPrefix { return p.prefix }

// Addr is where it receives unicast (the first of its addresses), and sends
// from.
func (p *Participant) Addr() netip.AddrPort {
	for _, l := range p.unicast {
		if a, ok := transport.AddrPort(l); ok {
			return a
		}
	}
	return netip.AddrPort{}
}

// Locators are where it receives unicast, as it announces them.
func (p *Participant) Locators() []rtps.Locator { return append([]rtps.Locator(nil), p.unicast...) }

func (p *Participant) closeTransports() {
	for _, t := range p.transports {
		_ = t.Close()
	}
}

// deliver takes what a transport received.
func (p *Participant) deliver(pk transport.Packet) {
	if pk.Multicast {
		p.spdp.Handle(pk.From, pk.Data)
		return
	}
	p.handleUnicast(pk.From, pk.Data)
}

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
	errs := make(chan error, len(p.transports))

	for _, t := range p.transports {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := t.Run(ctx, p.deliver); err != nil {
				errs <- err
			}
		}()
	}
	// Announce ourselves at once to whoever the scope lets us find.
	p.sendSPDP(p.spdpTargets())

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.tick(ctx)
	}()

	var err error
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	cancel()
	wg.Wait()
	// The transports stopped receiving, and send still: say goodbye, then
	// close them.
	p.sayGoodbye()
	p.closeTransports()
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func (p *Participant) tick(ctx context.Context) {
	announce := time.NewTicker(p.cfg.AnnounceInterval)
	lease := time.NewTicker(time.Second)
	heartbeat := time.NewTicker(heartbeatPeriod / 2)
	defer announce.Stop()
	defer lease.Stop()
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-lease.C:
			p.spdp.CheckLeases()
		case <-announce.C:
			p.announceAll()
		case <-heartbeat.C:
			p.heartbeatWriters()
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
			p.matchBuiltin(pe)
		}
	case discovery.ParticipantUpdated:
		p.emit(e)
		p.mu.Lock()
		if pe, ok := p.peers[e.Participant.GUID.Prefix]; ok {
			pe.data = e.Participant
			pe.metatraffic = p.reachable(e.Participant.From.Addr(), e.Participant.MetatrafficUnicast)
		}
		p.mu.Unlock()
	case discovery.ParticipantLost:
		p.mu.Lock()
		delete(p.peers, e.Participant.GUID.Prefix)
		p.pubsW.removeParticipant(e.Participant.GUID.Prefix)
		p.subsW.removeParticipant(e.Participant.GUID.Prefix)
		for _, w := range p.writers {
			w.w.removeParticipant(e.Participant.GUID.Prefix)
		}
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
// (no metatraffic address the scope lets us reach). Called with mu held.
func (p *Participant) addPeer(d discovery.Participant) *peer {
	meta := p.reachable(d.From.Addr(), d.MetatrafficUnicast)
	if len(meta) == 0 {
		p.log.Debug("participant has no metatraffic address in scope; not talking to it", "guid", d.GUID.Prefix, "scope", p.cfg.Scope)
		return nil
	}
	pe := &peer{data: d, metatraffic: meta, proxies: map[proxyKey]*writerProxy{}}
	p.peers[d.GUID.Prefix] = pe
	return pe
}

// send writes a message to every address the scope allows, through the
// first transport that reaches it.
func (p *Participant) send(to []netip.AddrPort, b []byte) {
	for _, a := range to {
		p.sendVia(rtps.Locator{}, a, b)
	}
}

// sendVia writes a message to an address the scope allows, multicasting
// only out of the interface of one of our locators if via is one and the
// transport can. It is the one place messages leave.
func (p *Participant) sendVia(via rtps.Locator, a netip.AddrPort, b []byte) {
	{
		if !p.allowed(a.Addr()) {
			p.log.Error("refusing to send out of scope", "to", a, "scope", p.cfg.Scope)
			return
		}
		l := transport.UDPv4(a)
		var t transport.Transport
		for _, c := range p.transports {
			if c.Reaches(l) {
				t = c
				break
			}
		}
		if t == nil {
			p.log.Debug("no transport reaches", "to", a)
			return
		}
		var err error
		if vt, ok := t.(transport.Via); ok && via.Kind != 0 {
			err = vt.SendVia(via, l, b)
		} else {
			err = t.Send(l, b)
		}
		if err != nil {
			p.log.Debug("send", "to", a, "err", err)
			return
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

// builtinEndpoints is what we have: SPDP and SEDP, both ways.
func (p *Participant) builtinEndpoints() uint32 {
	return rtps.BuiltinParticipantAnnouncer | rtps.BuiltinParticipantDetector |
		rtps.BuiltinPublicationsDetector | rtps.BuiltinSubscriptionsDetector |
		rtps.BuiltinPublicationsAnnouncer | rtps.BuiltinSubscriptionsAnnouncer
}

// spdpPayload is what we announce of ourselves: our locator at an address
// (one of ours) alone, or every one if it is none of ours.
func (p *Participant) spdpPayload(only netip.Addr) []byte {
	host, _ := os.Hostname()
	exe, _ := os.Executable()
	pl := (&rtps.ParameterListWriter{}).
		GUID(rtps.PIDParticipantGUID, rtps.GUID{Prefix: p.prefix, Entity: rtps.EntityParticipant}).
		Raw(rtps.PIDProtocolVersion, []byte{2, 1}).
		Raw(rtps.PIDVendorID, VendorID[:]).
		Uint32(rtps.PIDDomainID, p.cfg.Domain).
		Duration(rtps.PIDParticipantLease, p.cfg.LeaseDuration)
	// One socket for both: discovery and user traffic. A peer may send to
	// every locator we list, reachable or not (Cyclone DDS does): it is told
	// of the one on the interface that reaches it, as if we had no other.
	var ls []rtps.Locator
	for _, l := range p.unicast {
		if l.Addr == only {
			ls = append(ls, l)
		}
	}
	if len(ls) == 0 {
		ls = p.unicast
	}
	for _, l := range ls {
		pl.Locator(rtps.PIDMetatrafficUnicastLoc, l)
	}
	for _, l := range ls {
		pl.Locator(rtps.PIDDefaultUnicastLoc, l)
	}
	return pl.
		Uint32(rtps.PIDBuiltinEndpointSet, p.builtinEndpoints()).
		String(rtps.PIDEntityName, p.cfg.Name).
		Properties(
			[2]string{"__Hostname", host},
			[2]string{"__Pid", strconv.Itoa(os.Getpid())},
			[2]string{"__ProcessName", filepath.Base(exe)},
		).
		Payload()
}

func (p *Participant) spdpMessage(only netip.Addr) []byte {
	return rtps.NewBuilder(p.prefix, VendorID).
		InfoTS(p.cfg.Now()).
		Data(rtps.EntitySPDPReader, rtps.EntitySPDPWriter, 1, nil, p.spdpPayload(only), false).
		Bytes()
}

// ourAddrFor is our address on the network of an address: the one a
// message to it goes out of. Zero if none of ours is.
func (p *Participant) ourAddrFor(dst netip.Addr) netip.Addr {
	for _, n := range p.nets {
		if !n.Contains(dst) {
			continue
		}
		for _, l := range p.unicast {
			if n.Contains(l.Addr) {
				return l.Addr
			}
		}
	}
	return netip.Addr{}
}

// sendSPDP announces us to each address: to a unicast one with our locator
// on its network; to the multicast group out of each interface, with that
// interface's locator.
func (p *Participant) sendSPDP(to []netip.AddrPort) {
	for _, a := range to {
		if !a.Addr().IsMulticast() {
			p.send([]netip.AddrPort{a}, p.spdpMessage(p.ourAddrFor(a.Addr())))
			continue
		}
		for _, l := range p.unicast {
			p.sendVia(l, a, p.spdpMessage(l.Addr))
		}
	}
}

// announce sends our SPDP to one peer.
func (p *Participant) announce(pe *peer) {
	p.sendSPDP(pe.metatraffic)
	p.mu.Lock()
	pe.announced = true
	p.mu.Unlock()
}

// announceAll renews our lease with every peer, and nudges the writers that
// have not started talking to us.
func (p *Participant) announceAll() {
	p.mu.Lock()
	peers := make([]*peer, 0, len(p.peers))
	for _, pe := range p.peers {
		peers = append(peers, pe)
	}
	p.mu.Unlock()
	p.sendSPDP(p.spdpTargets())
	for _, pe := range peers {
		p.sendSPDP(pe.metatraffic)
		p.greet(pe)
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
	p.send(append(to, p.spdpTargets()...), msg)
}

// writerConfig is a writer's configuration with the participant's sizes.
func (p *Participant) writerConfig(c writerConfig) writerConfig {
	c.maxMessage, c.fragSize = p.cfg.MaxMessageSize, p.cfg.FragmentSize
	return c
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
		w := p.proxy(pe, s.reader, rtps.GUID{Prefix: pe.data.GUID.Prefix, Entity: s.writer})
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

// proxyKey is one of our readers and a writer it reads.
type proxyKey struct {
	reader rtps.EntityID
	writer rtps.GUID
}

// proxy returns a reader's view of a peer's writer. Called with mu held.
func (p *Participant) proxy(pe *peer, reader rtps.EntityID, writer rtps.GUID) *writerProxy {
	k := proxyKey{reader, writer}
	w, ok := pe.proxies[k]
	if !ok {
		w = newWriterProxy()
		pe.proxies[k] = w
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
		case rtps.SubNackFrag:
			if n, err := rtps.ParseNackFrag(s); err == nil {
				p.onNackFrag(src, n)
			}
		}
	}
	if spdp {
		p.spdp.Handle(from, b)
	}
}

// target is one of our readers a writer's submessage is for, and its view
// of the writer.
type target struct {
	reader  rtps.EntityID
	w       *writerProxy
	deliver func([]Sample)
}

// route finds the peer and our readers a writer's submessage is for: the
// one it is addressed to, or every one matched with the writer when it is
// addressed to none (ENTITYID_UNKNOWN). Called with mu held.
func (p *Participant) route(src rtps.GUIDPrefix, writer, addressed rtps.EntityID) (*peer, []target) {
	pe, ok := p.peers[src]
	if !ok {
		return nil, nil
	}
	g := rtps.GUID{Prefix: src, Entity: writer}
	for _, s := range sedpReaders {
		if writer == s.writer {
			kind := s.kind
			return pe, []target{{s.reader, p.proxy(pe, s.reader, g), func(ss []Sample) { p.onSEDP(pe, kind, ss) }}}
		}
	}
	var out []target
	for _, r := range p.readers {
		if !r.matched[g] || (addressed != (rtps.EntityID{}) && addressed != r.id) {
			continue
		}
		out = append(out, target{r.id, p.proxy(pe, r.id, g), func(ss []Sample) { r.deliver(g, ss) }})
	}
	return pe, out
}

// delivery is samples for a reader, to hand it with mu released.
type delivery struct {
	deliver func([]Sample)
	ss      []Sample
}

func deliverAll(ds []delivery) {
	for _, d := range ds {
		if len(d.ss) > 0 {
			d.deliver(d.ss)
		}
	}
}

// feed gives each reader a submessage is for its part, and hands them what
// they can deliver.
func (p *Participant) feed(src rtps.GUIDPrefix, writer, addressed rtps.EntityID, f func(*writerProxy) []Sample) {
	p.mu.Lock()
	_, ts := p.route(src, writer, addressed)
	var ds []delivery
	for _, t := range ts {
		ds = append(ds, delivery{t.deliver, f(t.w)})
	}
	p.mu.Unlock()
	deliverAll(ds)
}

func (p *Participant) onData(src rtps.GUIDPrefix, d rtps.Data) {
	p.feed(src, d.Writer, d.Reader, func(w *writerProxy) []Sample { return w.data(d) })
}

func (p *Participant) onDataFrag(src rtps.GUIDPrefix, f rtps.DataFrag) {
	p.feed(src, f.Writer, f.Reader, func(w *writerProxy) []Sample { return w.fragment(f) })
}

func (p *Participant) onGap(src rtps.GUIDPrefix, g rtps.Gap) {
	p.feed(src, g.Writer, g.Reader, func(w *writerProxy) []Sample { return w.gap(g) })
}

// onHeartbeat answers a writer's heartbeat for each reader it is for: what
// it has, what it lacks.
func (p *Participant) onHeartbeat(src rtps.GUIDPrefix, h rtps.Heartbeat) {
	p.mu.Lock()
	pe, ts := p.route(src, h.Writer, h.Reader)
	var ds []delivery
	var answer *rtps.Builder
	for _, t := range ts {
		ss, ack, nacks := t.w.heartbeat(h)
		ds = append(ds, delivery{t.deliver, ss})
		if ack == nil {
			continue
		}
		if answer == nil {
			answer = rtps.NewBuilder(p.prefix, VendorID).InfoDst(src)
		}
		for _, n := range nacks {
			answer.NackFrag(t.reader, h.Writer, n.SN, n.Missing, t.w.nextNackCount())
		}
		answer.AckNack(t.reader, h.Writer, *ack, t.w.nextAckCount(), false)
	}
	var to []netip.AddrPort
	if answer != nil {
		to = p.replyTo(pe, rtps.GUID{Prefix: src, Entity: h.Writer})
	}
	p.mu.Unlock()
	deliverAll(ds)
	if answer != nil {
		p.send(to, answer.Bytes())
	}
}

func (p *Participant) onHeartbeatFrag(src rtps.GUIDPrefix, h rtps.HeartbeatFrag) {
	p.mu.Lock()
	pe, ts := p.route(src, h.Writer, h.Reader)
	var answer *rtps.Builder
	for _, t := range ts {
		n := t.w.heartbeatFrag(h)
		if n == nil {
			continue
		}
		if answer == nil {
			answer = rtps.NewBuilder(p.prefix, VendorID).InfoDst(src)
		}
		answer.NackFrag(t.reader, h.Writer, n.SN, n.Missing, t.w.nextNackCount())
	}
	var to []netip.AddrPort
	if answer != nil {
		to = p.replyTo(pe, rtps.GUID{Prefix: src, Entity: h.Writer})
	}
	p.mu.Unlock()
	if answer != nil {
		p.send(to, answer.Bytes())
	}
}

// onSEDP takes delivered SEDP samples from a peer: endpoints announced,
// changed, or disposed.
func (p *Participant) onSEDP(pe *peer, kind rtps.EndpointKind, ss []Sample) {
	now := p.cfg.Now()
	var evs []discovery.Event
	var nudges []matchNudge
	var out []outbound
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
			out = append(out, p.matchReader(ep)...)
		case !discovery.SameEndpoint(&prev.EndpointData, &ed):
			evs = append(evs, discovery.EndpointUpdated{Endpoint: *ep})
		}
	}
	p.mu.Unlock()
	for _, e := range evs {
		p.emit(e)
	}
	p.nudge(nudges)
	p.sendAll(out)
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
