// Package discovery tracks what the DDS discovery protocols announce:
// participants (SPDP) and, later, their endpoints (SEDP).
package discovery

import (
	"context"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/transport"
)

// Participant is a remote participant as last announced.
type Participant struct {
	rtps.ParticipantData
	// From is where its last announcement came from.
	From netip.AddrPort
	// FirstSeen and LastSeen bound when it has been heard from.
	FirstSeen time.Time
	LastSeen  time.Time
}

// Hostname, PID and ProcessName read the properties Cyclone DDS announces
// (__Hostname, __Pid, __ProcessName). PID is in the sender's PID namespace,
// which in a container is not the host's. Each is zero if not announced.
func (p Participant) Hostname() string    { return p.Properties["__Hostname"] }
func (p Participant) ProcessName() string { return p.Properties["__ProcessName"] }
func (p Participant) PID() int {
	n, _ := strconv.Atoi(p.Properties["__Pid"])
	return n
}

// LostReason says how a participant went away.
type LostReason int

const (
	// Disposed: it said so -- an SPDP dispose, which a participant sends when
	// it is deleted.
	Disposed LostReason = iota + 1
	// LeaseExpired: it went unheard for its lease duration -- a crash, a
	// kill, or the network.
	LeaseExpired
)

func (r LostReason) String() string {
	switch r {
	case Disposed:
		return "disposed"
	case LeaseExpired:
		return "lease_expired"
	}
	return "unknown"
}

// Event is something that happened to a participant.
type Event interface{ isEvent() }

type (
	// ParticipantDiscovered: heard from for the first time.
	ParticipantDiscovered struct{ Participant Participant }
	// ParticipantUpdated: it announced something different from before (the
	// lease renewing is not a change).
	ParticipantUpdated struct{ Participant Participant }
	// ParticipantLost: gone, and why.
	ParticipantLost struct {
		Participant Participant
		Reason      LostReason
		At          time.Time
	}
)

func (ParticipantDiscovered) isEvent() {}
func (ParticipantUpdated) isEvent()    {}
func (ParticipantLost) isEvent()       {}

// Filter decides which participants are of interest. Those it rejects are
// neither tracked nor reported.
type Filter func(*Participant) bool

// SPDPConfig configures an SPDPObserver.
type SPDPConfig struct {
	Domain uint32
	// DomainTag, if set, keeps only participants announcing it.
	DomainTag string
	// Filter keeps the participants of interest; nil keeps all.
	Filter Filter
	// Now is the clock; nil is time.Now.
	Now func() time.Time
	// CheckInterval is how often leases are checked; zero is one second.
	CheckInterval time.Duration
}

// SPDPObserver tracks participants from their SPDP announcements. It only
// listens.
type SPDPObserver struct {
	cfg    SPDPConfig
	events func(Event)

	mu    sync.Mutex
	known map[rtps.GUIDPrefix]*Participant
}

// NewSPDPObserver returns an observer that reports to events. events is
// called with no lock held, from the goroutine that runs the observer.
func NewSPDPObserver(cfg SPDPConfig, events func(Event)) *SPDPObserver {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = time.Second
	}
	return &SPDPObserver{cfg: cfg, events: events, known: map[rtps.GUIDPrefix]*Participant{}}
}

// Participants returns the participants known now.
func (o *SPDPObserver) Participants() []Participant {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]Participant, 0, len(o.known))
	for _, p := range o.known {
		out = append(out, *p)
	}
	return out
}

// Run listens on l until the context ends, checking leases meanwhile.
func (o *SPDPObserver) Run(ctx context.Context, l *transport.MulticastListener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(o.cfg.CheckInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				o.CheckLeases()
			}
		}
	}()
	err := l.Receive(ctx, func(p transport.Packet) { o.Handle(p.From, p.Data) })
	cancel()
	wg.Wait()
	return err
}

// Handle processes one received datagram. Anything that is not SPDP, or does
// not decode, is ignored.
func (o *SPDPObserver) Handle(from netip.AddrPort, b []byte) {
	msg, err := rtps.Parse(b)
	if err != nil && len(msg.Submessages) == 0 {
		return
	}
	for _, s := range msg.Submessages {
		if s.ID != rtps.SubData {
			continue
		}
		d, err := rtps.ParseData(s)
		if err != nil || d.Writer != rtps.EntitySPDPWriter {
			continue
		}
		o.handleSPDP(from, msg.Header, d)
	}
}

func (o *SPDPObserver) handleSPDP(from netip.AddrPort, h rtps.Header, d rtps.Data) {
	now := o.cfg.Now()

	// A dispose or unregister: inline QoS carries the status, and the key
	// (the participant's GUID) in a key hash or a key-only payload.
	if status, ok := d.InlineQoS.Get(rtps.PIDStatusInfo); ok {
		if st, err := rtps.ReadStatusInfo(status); err == nil && st&(rtps.StatusDisposed|rtps.StatusUnregistered) != 0 {
			o.lose(disposedPrefix(h, d), Disposed, now)
			return
		}
	}
	if d.Payload == nil || d.KeyOnly {
		return
	}
	pl, err := rtps.ReadParameterListPayload(d.Payload)
	if err != nil {
		return
	}
	pd, err := rtps.ParseParticipantData(pl, h.Vendor)
	if err != nil {
		return
	}
	if pd.DomainID != nil && *pd.DomainID != o.cfg.Domain {
		return
	}
	if pd.DomainTag != o.cfg.DomainTag {
		return
	}
	p := Participant{ParticipantData: pd, From: from, FirstSeen: now, LastSeen: now}
	if o.cfg.Filter != nil && !o.cfg.Filter(&p) {
		return
	}

	o.mu.Lock()
	prev, ok := o.known[pd.GUID.Prefix]
	if ok {
		p.FirstSeen = prev.FirstSeen
	}
	changed := ok && !sameAnnouncement(prev, &p)
	o.known[pd.GUID.Prefix] = &p
	o.mu.Unlock()

	switch {
	case !ok:
		o.emit(ParticipantDiscovered{Participant: p})
	case changed:
		o.emit(ParticipantUpdated{Participant: p})
	}
}

// disposedPrefix is the participant a dispose is about: the GUID in its
// key-only payload (as Cyclone DDS sends it), else its key hash, else the
// sender.
func disposedPrefix(h rtps.Header, d rtps.Data) rtps.GUIDPrefix {
	if d.Payload != nil {
		if pl, err := rtps.ReadParameterListPayload(d.Payload); err == nil {
			if v, ok := pl.Get(rtps.PIDParticipantGUID); ok {
				if g, err := rtps.ReadGUID(v); err == nil {
					return g.Prefix
				}
			}
		}
	}
	if kh, ok := d.InlineQoS.Get(rtps.PIDKeyHash); ok && len(kh) >= 12 {
		return rtps.GUIDPrefix(kh[:12])
	}
	return h.Prefix
}

func (o *SPDPObserver) lose(prefix rtps.GUIDPrefix, why LostReason, at time.Time) {
	o.mu.Lock()
	p, ok := o.known[prefix]
	if ok {
		delete(o.known, prefix)
	}
	o.mu.Unlock()
	if ok {
		o.emit(ParticipantLost{Participant: *p, Reason: why, At: at})
	}
}

// CheckLeases reports participants not heard from for their lease duration.
// Run calls it every CheckInterval.
func (o *SPDPObserver) CheckLeases() {
	now := o.cfg.Now()
	var lost []Participant
	o.mu.Lock()
	for k, p := range o.known {
		if p.LeaseDuration == rtps.DurationInfinite {
			continue
		}
		if now.Sub(p.LastSeen) > p.LeaseDuration {
			lost = append(lost, *p)
			delete(o.known, k)
		}
	}
	o.mu.Unlock()
	for _, p := range lost {
		o.emit(ParticipantLost{Participant: p, Reason: LeaseExpired, At: now})
	}
}

func (o *SPDPObserver) emit(e Event) {
	if o.events != nil {
		o.events(e)
	}
}

// sameAnnouncement compares what matters of two announcements: what a
// participant says of itself, not when it said it.
func sameAnnouncement(a, b *Participant) bool {
	if a.LeaseDuration != b.LeaseDuration || a.BuiltinEndpoints != b.BuiltinEndpoints ||
		a.EntityName != b.EntityName || string(a.UserData) != string(b.UserData) ||
		a.CycloneInternals != b.CycloneInternals || len(a.Properties) != len(b.Properties) {
		return false
	}
	for k, v := range a.Properties {
		if b.Properties[k] != v {
			return false
		}
	}
	return sameLocators(a.MetatrafficUnicast, b.MetatrafficUnicast) &&
		sameLocators(a.DefaultUnicast, b.DefaultUnicast) &&
		sameLocators(a.MetatrafficMulticast, b.MetatrafficMulticast) &&
		sameLocators(a.DefaultMulticast, b.DefaultMulticast)
}

func sameLocators(a, b []rtps.Locator) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// LocalOnly keeps the participants whose announcements come from an address
// of this host. Containers on the host network count as local. What a
// participant claims in its locators is not trusted for this: where its
// packets come from is.
func LocalOnly(local map[netip.Addr]bool) Filter {
	return func(p *Participant) bool { return local[p.From.Addr()] }
}
