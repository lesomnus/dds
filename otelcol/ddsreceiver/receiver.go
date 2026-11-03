package ddsreceiver

import (
	"context"
	"errors"
	"net"
	"net/netip"
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
	"go.uber.org/zap"

	"github.com/lesomnus/dds/otelcol/ddsreceiver/internal/procmap"
)

// ddsReceiver is one DDS participant (and capture) shared by the logs and
// metrics pipelines a receiver of this ID is in.
type ddsReceiver struct {
	cfg    *Config
	logger *zap.Logger

	mu      sync.Mutex
	logs    consumer.Logs
	metrics consumer.Metrics
	starts  int

	state  *state
	procs  *procmap.Resolver
	queue  chan any
	cancel context.CancelFunc
	done   chan struct{}

	// The traffic; nil when off, or when it could not be captured.
	stats   *traffic.Stats
	capture capturer
	began   time.Time

	// newParticipant is participant.New; tests replace it.
	newParticipant func(participant.Config, func(discovery.Event)) (runner, error)
	// openCapture is traffic.Open; tests replace it.
	openCapture func(traffic.CaptureConfig) (capturer, error)
	now         func() time.Time
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
// dropped (and counted) rather than the participant's receiving held up.
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
		now: time.Now,
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
	ifi, addr, err := transport.InterfaceByAddr(prefix)
	if err != nil {
		return err
	}
	if procs, err := procmap.New(r.cfg.ProcRoot); err == nil {
		r.procs = procs
	} else {
		r.logger.Warn("participants will not be tied to host processes", zap.Error(err))
	}

	r.state = newState()
	r.queue = make(chan any, queueSize)
	r.done = make(chan struct{})

	var graph *ros2.Graph
	if r.cfg.ROS {
		graph = ros2.NewGraph(r.enqueue)
	}
	p, err := r.newParticipant(participant.Config{
		Domain:    r.cfg.Domain,
		Interface: ifi,
		Addr:      addr,
		Name:      "otelcol-dds",
	}, func(e discovery.Event) {
		r.enqueue(e)
		if graph != nil {
			graph.HandleDiscovery(e)
		}
		if l, ok := e.(discovery.ParticipantLost); ok && r.stats != nil {
			r.stats.ForgetParticipant(l.Participant.GUID.Prefix)
		}
	})
	if err != nil {
		return err
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
			return err
		}
	}

	if r.cfg.Traffic.Enabled {
		r.openTraffic(ifi)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	if r.capture != nil {
		c, stats := r.capture, r.stats
		go func() {
			err := c.Run(ctx, func(p traffic.Packet) { stats.Observe(p.Message, p.At) })
			if err != nil && !errors.Is(err, context.Canceled) {
				r.logger.Error("capture stopped", zap.Error(err))
			}
			_ = c.Close()
		}()
	}
	go r.send(ctx)
	go func() {
		if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("DDS participant stopped", zap.Error(err))
		}
	}()
	r.logger.Info("observing DDS", zap.Uint32("domain", r.cfg.Domain), zap.String("interface", ifi.Name), zap.Stringer("addr", addr))
	return nil
}

// openTraffic opens the capture of the traffic, on the DDS interface and the
// loopback ones unless told which. Failing (no CAP_NET_RAW, say), the
// receiver goes on without.
func (r *ddsReceiver) openTraffic(ifi *net.Interface) {
	names := r.cfg.Traffic.Interfaces
	if len(names) == 0 {
		names = []string{ifi.Name}
		ifs, err := net.Interfaces()
		if err == nil {
			for _, i := range ifs {
				if i.Flags&net.FlagLoopback != 0 && i.Flags&net.FlagUp != 0 && i.Name != ifi.Name {
					names = append(names, i.Name)
				}
			}
		}
	}
	c, err := r.openCapture(traffic.CaptureConfig{Interfaces: names})
	if err != nil {
		r.logger.Warn("no traffic metrics: the traffic cannot be captured", zap.Error(err))
		return
	}
	r.capture, r.stats, r.began = c, traffic.NewStats(), r.now()
	r.logger.Info("capturing the RTPS traffic", zap.Strings("interfaces", names))
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
	select {
	case <-done:
	case <-ctx.Done():
		return true, ctx.Err()
	}
	return true, nil
}

func (r *ddsReceiver) enqueue(e any) {
	select {
	case r.queue <- e:
	default:
		r.logger.Warn("event dropped: the queue is full")
	}
}

// send drains the queue into the pipelines: events as log records, batched
// up to a second; the whole graph every snapshot interval, as state records
// and metrics.
func (r *ddsReceiver) send(ctx context.Context) {
	defer close(r.done)
	flush := time.NewTicker(time.Second)
	snap := time.NewTicker(r.cfg.SnapshotInterval)
	defer flush.Stop()
	defer snap.Stop()

	var tick <-chan time.Time
	if r.stats != nil {
		t := time.NewTicker(r.cfg.Traffic.CollectionInterval)
		defer t.Stop()
		tick = t.C
	}

	ld, rs := newLogs()
	push := func() {
		if rs.Len() == 0 {
			return
		}
		if logs := r.logsConsumer(); logs != nil {
			if err := logs.ConsumeLogs(ctx, ld); err != nil {
				r.logger.Debug("logs not consumed", zap.Error(err))
			}
		}
		ld, rs = newLogs()
	}
	for {
		select {
		case <-ctx.Done():
			push()
			return
		case e := <-r.queue:
			p, _ := r.state.apply(e, r.resolve)
			appendEvent(rs, e, p, r.now(), r.cfg.ROS)
			if rs.Len() >= 512 {
				push()
			}
		case <-flush.C:
			push()
		case <-snap.C:
			s := r.state.snapshot(r.now())
			if r.cfg.ROS {
				appendSnapshot(rs, s)
			}
			push()
			if m := r.metricsConsumer(); m != nil {
				if err := m.ConsumeMetrics(ctx, snapshotMetrics(s, r.cfg.ROS)); err != nil {
					r.logger.Debug("metrics not consumed", zap.Error(err))
				}
			}
		case <-tick:
			r.sendTraffic(ctx)
		}
	}
}

// sendTraffic reports the local writers' traffic, and forgets writers of
// other hosts' participants that have gone quiet: their goodbyes are not
// heard here.
func (r *ddsReceiver) sendTraffic(ctx context.Context) {
	now := r.now()
	quiet := now.Add(-2 * time.Minute)
	ws := r.stats.Writers()
	for _, w := range ws {
		if w.LastSeen.Before(quiet) && !r.state.isLocal(w.GUID.Prefix) {
			r.stats.Forget(w.GUID)
		}
	}
	m := r.metricsConsumer()
	if m == nil {
		return
	}
	if err := m.ConsumeMetrics(ctx, trafficMetrics(now, r.began, r.state.localWriters(ws), r.capture.Drops(), r.cfg.ROS)); err != nil {
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
