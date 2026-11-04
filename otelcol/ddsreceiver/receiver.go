package ddsreceiver

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/participant"
	"github.com/lesomnus/dds/ros2"
	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/traffic"
	"github.com/lesomnus/dds/transport"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/lesomnus/dds/otelcol/ddsreceiver/internal/procmap"
)

// ddsReceiver is what the logs and metrics pipelines a receiver of this ID
// is in share: the domains it collects, and the captures.
type ddsReceiver struct {
	cfg    *Config
	logger *zap.Logger

	mu      sync.Mutex
	logs    consumer.Logs
	metrics consumer.Metrics
	starts  int

	ifi    *net.Interface
	addr   netip.Addr
	local  map[netip.Addr]bool
	ignore map[uint32]bool
	procs  *procmap.Resolver
	queue  chan queued
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // every goroutine but send
	done   chan struct{}  // send's

	domainsMu sync.Mutex
	domains   map[uint32]*domain

	// The traffic; nil when off, or when it could not be captured.
	stats      *traffic.Stats
	captureCfg traffic.CaptureConfig
	began      time.Time
	drops      uint64 // by the sampling goroutine

	// newParticipant is participant.New; tests replace it.
	newParticipant func(participant.Config, func(discovery.Event)) (runner, error)
	// openCapture is traffic.Open; tests replace it.
	openCapture func(traffic.CaptureConfig) (capturer, error)
	now         func() time.Time
	// leaveAfter is how long a discovered domain is kept with no participant
	// of this host's.
	leaveAfter time.Duration
}

// domain is a DDS domain the receiver collects.
type domain struct {
	id     uint32
	mode   string // active, passive
	pinned bool   // configured: never left
	state  *state
	spdp   *discovery.SPDPObserver // passive: fed from the discovery capture
	cancel context.CancelFunc
	done   chan struct{}
	// emptySince is when it last had no participant; zero while it has.
	emptySince time.Time
}

func (d *domain) resource() resource { return resource{domain: d.id, mode: d.mode} }

// queued is an event of a domain.
type queued struct {
	d *domain
	e any
}

// capturer is what the receiver needs of a capture.
type capturer interface {
	Run(context.Context, func(traffic.Packet)) error
	Drops() uint64
	Close() error
}

// runner is what the receiver needs of a participant.
type runner interface {
	Run(context.Context) error
	Subscribe(participant.Subscription) error
}

// queueSize bounds the events waiting to be sent: past it, events are
// dropped (and counted) rather than the participants' receiving held up.
const queueSize = 4096

var (
	sharedMu sync.Mutex
	shared   = map[component.ID]*ddsReceiver{}
)

// receiverFor returns the receiver of an ID, made once.
func receiverFor(id component.ID, cfg *Config, logger *zap.Logger) *ddsReceiver {
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if r, ok := shared[id]; ok {
		return r
	}
	r := &ddsReceiver{
		cfg:    cfg,
		logger: logger,
		newParticipant: func(c participant.Config, ev func(discovery.Event)) (runner, error) {
			return participant.New(c, ev)
		},
		openCapture: func(c traffic.CaptureConfig) (capturer, error) {
			return traffic.Open(c)
		},
		now:        time.Now,
		leaveAfter: 2 * time.Minute,
	}
	shared[id] = r
	return r
}

// handle is the receiver as one pipeline holds it: Start and Shutdown count.
type handle struct {
	id component.ID
	r  *ddsReceiver
}

func (h handle) Start(ctx context.Context, host component.Host) error { return h.r.start() }

func (h handle) Shutdown(ctx context.Context) error {
	last, err := h.r.shutdown(ctx)
	if last {
		sharedMu.Lock()
		delete(shared, h.id)
		sharedMu.Unlock()
	}
	return err
}

func (r *ddsReceiver) start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.starts++
	if r.starts > 1 {
		return nil
	}

	prefix, err := netip.ParsePrefix(r.cfg.Interface)
	if err != nil {
		return err
	}
	r.ifi, r.addr, err = transport.InterfaceByAddr(prefix)
	if err != nil {
		return err
	}
	if r.local, err = transport.LocalAddrs(); err != nil {
		return err
	}
	if procs, err := procmap.New(r.cfg.ProcRoot); err == nil {
		r.procs = procs
	} else {
		r.logger.Warn("participants will not be tied to host processes", zap.Error(err))
	}
	r.ignore = map[uint32]bool{}
	for _, d := range r.cfg.Domains.Ignore {
		r.ignore[d] = true
	}
	r.queue = make(chan queued, queueSize)
	r.done = make(chan struct{})
	r.domains = map[uint32]*domain{}
	r.ctx, r.cancel = context.WithCancel(context.Background())

	if r.cfg.Traffic.Enabled {
		r.openTraffic()
	}
	for _, id := range r.cfg.active() {
		if _, err := r.join(id, modeActive, true); err != nil {
			r.cancel()
			return err
		}
	}
	if r.cfg.Domains.Discover {
		r.startDiscovery()
	}
	if r.stats != nil {
		r.wg.Add(1)
		go r.sampleTraffic(r.ctx)
	}
	r.wg.Add(1)
	go r.maintain(r.ctx)
	go r.send(r.ctx)
	r.logger.Info("observing DDS",
		zap.Uint32s("domains", r.cfg.active()), zap.Bool("discover", r.cfg.Domains.Discover),
		zap.String("interface", r.ifi.Name), zap.Stringer("addr", r.addr))
	return nil
}

// captureInterfaces are what the captures listen on: the configured ones,
// else the DDS interface and the loopback ones.
func (r *ddsReceiver) captureInterfaces() []string {
	if names := r.cfg.Traffic.Interfaces; len(names) > 0 {
		return names
	}
	names := []string{r.ifi.Name}
	if ifs, err := net.Interfaces(); err == nil {
		for _, i := range ifs {
			if i.Flags&net.FlagLoopback != 0 && i.Flags&net.FlagUp != 0 && i.Name != r.ifi.Name {
				names = append(names, i.Name)
			}
		}
	}
	return names
}

// join starts collecting a domain, actively (a participant) or passively
// (an SPDP observer fed by the discovery capture). Called with r.mu held,
// or from the discovery capture.
func (r *ddsReceiver) join(id uint32, mode string, pinned bool) (*domain, error) {
	r.domainsMu.Lock()
	defer r.domainsMu.Unlock()
	if d, ok := r.domains[id]; ok {
		return d, nil
	}
	d := &domain{id: id, mode: mode, pinned: pinned, state: newState(), done: make(chan struct{})}
	ctx, cancel := context.WithCancel(r.ctx)
	d.cancel = cancel
	events := func(e discovery.Event) {
		r.enqueue(queued{d, e})
		if l, ok := e.(discovery.ParticipantLost); ok && r.stats != nil {
			r.stats.ForgetParticipant(l.Participant.GUID.Prefix)
		}
	}
	if mode == modePassive {
		d.spdp = discovery.NewSPDPObserver(discovery.SPDPConfig{Domain: id, Filter: discovery.LocalOnly(r.local), Now: r.now}, events)
		close(d.done) // nothing runs: the discovery capture feeds it
		r.domains[id] = d
		r.logger.Info("collecting a domain", zap.Uint32("domain", id), zap.String("mode", mode))
		return d, nil
	}
	var graph *ros2.Graph
	if r.cfg.ROS {
		graph = ros2.NewGraph(func(e any) { r.enqueue(queued{d, e}) })
	}
	p, err := r.newParticipant(participant.Config{
		Domain:    id,
		Interface: r.ifi,
		Addr:      r.addr,
		Name:      "otelcol-dds",
	}, func(e discovery.Event) {
		events(e)
		if graph != nil {
			graph.HandleDiscovery(e)
		}
	})
	if err != nil {
		cancel()
		return nil, err
	}
	if graph != nil {
		// What ros2.Graph.Subscribe asks for, on the runner.
		err := p.Subscribe(participant.Subscription{
			Topic: ros2.DiscoveryTopic,
			Type:  ros2.DiscoveryType,
			QoS: participant.ReaderQoS{
				Reliability: rtps.Reliable,
				Durability:  rtps.TransientLocal,
				History:     rtps.KeepLast,
				Depth:       1,
			},
			Handle: func(w rtps.GUID, s participant.Sample) { graph.HandleSample(w, s.Payload) },
		})
		if err != nil {
			cancel()
			return nil, err
		}
	}
	r.domains[id] = d
	go func() {
		defer close(d.done)
		if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("DDS participant stopped", zap.Uint32("domain", id), zap.Error(err))
		}
	}()
	r.logger.Info("collecting a domain", zap.Uint32("domain", id), zap.String("mode", mode))
	return d, nil
}

// leave stops collecting a domain.
func (r *ddsReceiver) leave(d *domain) {
	r.domainsMu.Lock()
	delete(r.domains, d.id)
	r.domainsMu.Unlock()
	d.cancel()
	<-d.done
	r.logger.Info("left a domain: no participant of this host's in it", zap.Uint32("domain", d.id))
}

// domainList is the domains collected, by ID.
func (r *ddsReceiver) domainList() []*domain {
	r.domainsMu.Lock()
	defer r.domainsMu.Unlock()
	out := make([]*domain, 0, len(r.domains))
	for _, d := range r.domains {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func (r *ddsReceiver) domain(id uint32) *domain {
	r.domainsMu.Lock()
	defer r.domainsMu.Unlock()
	return r.domains[id]
}

// startDiscovery opens the discovery capture: SPDP announcements alone, all
// along. Every other packet is dropped in the kernel. Failing (no
// CAP_NET_RAW), only the configured domains are collected.
func (r *ddsReceiver) startDiscovery() {
	cfg := traffic.CaptureConfig{
		Interfaces: r.captureInterfaces(),
		Snaplen:    traffic.SPDPSnaplen,
		Filter:     traffic.SPDPFilter(traffic.SPDPSnaplen),
	}
	c, err := r.openCapture(cfg)
	if err != nil {
		r.logger.Warn("domains will not be discovered: the traffic cannot be captured", zap.Error(err))
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer c.Close()
		if err := c.Run(r.ctx, r.discovered); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("discovery capture stopped", zap.Error(err))
		}
	}()
	r.logger.Info("discovering domains", zap.Strings("interfaces", cfg.Interfaces), zap.String("discovered", r.cfg.Domains.Discovered))
}

// discovered takes an SPDP announcement the discovery capture saw: one of
// this host's participants in a domain not collected yet is a domain to
// join; in a passive domain it is what the domain's observer reads.
func (r *ddsReceiver) discovered(p traffic.Packet) {
	id, ok := traffic.SPDPDomain(p.Dst.Port())
	if !ok || r.ignore[id] || !r.local[p.Src.Addr()] {
		return
	}
	d := r.domain(id)
	if d == nil {
		if r.ctx.Err() != nil {
			return
		}
		var err error
		if d, err = r.join(id, r.cfg.Domains.Discovered, false); err != nil {
			r.logger.Warn("domain not joined", zap.Uint32("domain", id), zap.Error(err))
			return
		}
	}
	if d.spdp != nil {
		d.spdp.Handle(p.Src, p.Message)
	}
}

// maintain expires passive domains' participants, and leaves the
// discovered domains that have had no participant of this host's for a
// while.
func (r *ddsReceiver) maintain(ctx context.Context) {
	defer r.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := r.now()
		for _, d := range r.domainList() {
			if d.spdp != nil {
				d.spdp.CheckLeases()
			}
			if d.pinned {
				continue
			}
			if d.state.size() > 0 || len(d.spdpParticipants()) > 0 {
				d.emptySince = time.Time{}
				continue
			}
			if d.emptySince.IsZero() {
				d.emptySince = now
			} else if now.Sub(d.emptySince) >= r.leaveAfter {
				r.leave(d)
			}
		}
	}
}

func (d *domain) spdpParticipants() []discovery.Participant {
	if d.spdp == nil {
		return nil
	}
	return d.spdp.Participants()
}

// openTraffic finds out whether the traffic can be captured. Failing (no
// CAP_NET_RAW, say), the receiver goes on without.
func (r *ddsReceiver) openTraffic() {
	cfg := traffic.CaptureConfig{Interfaces: r.captureInterfaces(), Buffer: captureBuffer}
	// Opened once now to know it can be; then for each window.
	c, err := r.openCapture(cfg)
	if err != nil {
		r.logger.Warn("no traffic metrics: the traffic cannot be captured", zap.Error(err))
		return
	}
	_ = c.Close()
	r.captureCfg, r.stats, r.began = cfg, traffic.NewStats(), r.now()
	r.logger.Info("capturing the RTPS traffic",
		zap.Strings("interfaces", cfg.Interfaces),
		zap.Duration("window", r.cfg.Traffic.Window),
		zap.Duration("every", r.cfg.Traffic.CollectionInterval))
}

// captureBuffer is a capture's receive buffer. The kernel charges a queued
// packet at its size, not at the bytes the filter keeps: a burst of large
// samples on the loopback fills a small one at once.
const captureBuffer = 32 << 20

// sampleTraffic captures the traffic a window long every collection
// interval, and reports it. Between windows the capture is closed: the
// kernel hands it nothing, and it costs nothing. Counting by sequence
// numbers, seeing a writer now and then is enough to know how much it
// wrote. A window as long as the interval captures all along.
func (r *ddsReceiver) sampleTraffic(ctx context.Context) {
	defer r.wg.Done()
	every, window := r.cfg.Traffic.CollectionInterval, r.cfg.Traffic.Window
	observe := func(p traffic.Packet) { r.stats.Observe(p.Message, p.At) }
	t := time.NewTicker(every)
	defer t.Stop()
	if window >= every {
		c, err := r.openCapture(r.captureCfg)
		if err != nil {
			r.logger.Error("capture failed", zap.Error(err))
			return
		}
		defer c.Close()
		go func() {
			if err := c.Run(ctx, observe); err != nil && !errors.Is(err, context.Canceled) {
				r.logger.Error("capture stopped", zap.Error(err))
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				r.drops = c.Drops()
				r.sendTraffic(ctx)
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c, err := r.openCapture(r.captureCfg)
		if err != nil {
			r.logger.Warn("capture failed this window", zap.Error(err))
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, window)
		err = c.Run(wctx, observe)
		cancel()
		r.drops += c.Drops()
		_ = c.Close()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			r.logger.Warn("capture stopped this window", zap.Error(err))
		}
		if ctx.Err() != nil {
			return
		}
		r.sendTraffic(ctx)
	}
}

func (r *ddsReceiver) shutdown(ctx context.Context) (bool, error) {
	r.mu.Lock()
	r.starts--
	last := r.starts <= 0
	cancel, done := r.cancel, r.done
	r.mu.Unlock()
	if !last || cancel == nil {
		return last, nil
	}
	cancel()
	stopped := make(chan struct{})
	go func() {
		r.wg.Wait()
		for _, d := range r.domainList() {
			<-d.done
		}
		<-done
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		return true, ctx.Err()
	}
	return true, nil
}

func (r *ddsReceiver) enqueue(q queued) {
	select {
	case r.queue <- q:
	default:
		r.logger.Warn("event dropped: the queue is full")
	}
}

// batch is the log records of one domain waiting to be sent.
type batch struct {
	ld plog.Logs
	rs plog.LogRecordSlice
}

// send drains the queue into the pipelines: events as log records, batched
// up to a second; each domain's graph every snapshot interval, as state
// records and metrics.
func (r *ddsReceiver) send(ctx context.Context) {
	defer close(r.done)
	flush := time.NewTicker(time.Second)
	snap := time.NewTicker(r.cfg.SnapshotInterval)
	defer flush.Stop()
	defer snap.Stop()

	batches := map[*domain]*batch{}
	of := func(d *domain) *batch {
		b, ok := batches[d]
		if !ok {
			b = &batch{}
			b.ld, b.rs = newLogs(d.resource())
			batches[d] = b
		}
		return b
	}
	push := func() {
		logs := r.logsConsumer()
		for d, b := range batches {
			if b.rs.Len() > 0 && logs != nil {
				if err := logs.ConsumeLogs(ctx, b.ld); err != nil {
					r.logger.Debug("logs not consumed", zap.Error(err))
				}
			}
			delete(batches, d)
		}
	}
	for {
		select {
		case <-ctx.Done():
			push()
			return
		case q := <-r.queue:
			p, _ := q.d.state.apply(q.e, r.resolve)
			b := of(q.d)
			appendEvent(b.rs, q.e, p, r.now(), r.cfg.ROS)
			if b.rs.Len() >= 512 {
				push()
			}
		case <-flush.C:
			push()
		case <-snap.C:
			now := r.now()
			m := r.metricsConsumer()
			for _, d := range r.domainList() {
				s := d.state.snapshot(now)
				if r.cfg.ROS && d.mode == modeActive {
					appendSnapshot(of(d).rs, s)
				}
				if m != nil {
					if err := m.ConsumeMetrics(ctx, snapshotMetrics(d.resource(), s, r.cfg.ROS && d.mode == modeActive)); err != nil {
						r.logger.Debug("metrics not consumed", zap.Error(err))
					}
				}
			}
			push()
		}
	}
}

// sendTraffic reports each domain's local writers' traffic, and forgets
// writers of other hosts' participants that have gone quiet: their goodbyes
// are not heard here.
func (r *ddsReceiver) sendTraffic(ctx context.Context) {
	now := r.now()
	quiet := now.Add(-2 * time.Minute)
	ws := r.stats.Writers()
	domains := r.domainList()
	for _, w := range ws {
		if w.LastSeen.Before(quiet) && !slices.ContainsFunc(domains, func(d *domain) bool { return d.state.isLocal(w.GUID.Prefix) }) {
			r.stats.Forget(w.GUID)
		}
	}
	m := r.metricsConsumer()
	if m == nil {
		return
	}
	for _, d := range domains {
		lw := d.state.localWriters(ws)
		if len(lw) == 0 {
			continue
		}
		if err := m.ConsumeMetrics(ctx, trafficMetrics(d.resource(), now, lw, r.cfg.ROS && d.mode == modeActive)); err != nil {
			r.logger.Debug("metrics not consumed", zap.Error(err))
		}
	}
	if err := m.ConsumeMetrics(ctx, dropsMetrics(now, r.began, r.drops)); err != nil {
		r.logger.Debug("metrics not consumed", zap.Error(err))
	}
}

// resolve ties a participant to the host process holding its sockets.
func (r *ddsReceiver) resolve(p discovery.Participant) procmap.Process {
	if r.procs == nil {
		return procmap.Process{}
	}
	proc, ok := r.procs.ByPorts(localPorts(p)...)
	if !ok {
		r.logger.Debug("no host process holds the participant's ports", zap.Stringer("guid", p.GUID.Prefix))
	}
	return proc
}

func (r *ddsReceiver) logsConsumer() consumer.Logs {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.logs
}

func (r *ddsReceiver) metricsConsumer() consumer.Metrics {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.metrics
}
