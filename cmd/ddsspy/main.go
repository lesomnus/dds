// Command ddsspy listens to SPDP in a DDS domain and prints what it hears:
// participants appearing, changing and going away. It sends nothing.
//
//	ddsspy -domain 138 -iface 192.168.10.0/24 [-local] [-json] [-dump dir]
//	ddsspy -domain 138 -iface 192.168.10.0/24 -sedp [-ros] [-json]
//	ddsspy -domain 138 -iface 192.168.10.0/24 -sedp -traffic 5s [-json]
//
// With -sedp it takes part: it announces itself to the participants on this
// host -- and only to them -- and prints their endpoints too. With -ros it
// also reads ros_discovery_info and prints ROS nodes as they come and go.
// With -traffic it also captures the RTPS traffic of the interface and the
// loopback one (CAP_NET_RAW) and prints, every interval, how much each writer
// on this host has written.
//
// With -json every event is one JSON object per line on stdout, which is how
// the E2E tests read it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/participant"
	"github.com/lesomnus/dds/ros2"
	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/traffic"
	"github.com/lesomnus/dds/transport"
)

func main() {
	domain := flag.Uint("domain", 0, "DDS domain ID")
	iface := flag.String("iface", "", "interface to listen on, by an address prefix it has (e.g. 192.168.10.0/24 or 10.0.0.5/32)")
	local := flag.Bool("local", false, "only participants on this host")
	asJSON := flag.Bool("json", false, "print events as JSON lines")
	dump := flag.String("dump", "", "also write every datagram received to this directory")
	sedp := flag.Bool("sedp", false, "take part (on this host only) and print endpoints too")
	ros := flag.Bool("ros", false, "with -sedp: print ROS nodes too")
	every := flag.Duration("traffic", 0, "with -sedp: print each local writer's traffic this often")
	flag.Parse()

	if err := transport.CheckDomain(uint32(*domain)); err != nil {
		log.Fatal(err)
	}
	prefix, err := netip.ParsePrefix(*iface)
	if err != nil {
		log.Fatalf("-iface: %v", err)
	}
	ifi, addr, err := transport.InterfaceByAddr(prefix)
	if err != nil {
		log.Fatal(err)
	}
	out := &printer{json: *asJSON, enc: json.NewEncoder(os.Stdout)}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *sedp {
		if err := runParticipant(ctx, uint32(*domain), ifi, addr, out, *ros, *every); err != nil {
			log.Fatal(err)
		}
		return
	}

	port := transport.SPDPMulticastPort(uint32(*domain))
	l, err := transport.ListenMulticast(ifi, transport.DefaultMulticastGroup, port)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s (%s), %s:%d", ifi.Name, addr, transport.DefaultMulticastGroup, port)

	cfg := discovery.SPDPConfig{Domain: uint32(*domain)}
	if *local {
		addrs, err := transport.LocalAddrs()
		if err != nil {
			log.Fatal(err)
		}
		cfg.Filter = discovery.LocalOnly(addrs)
	}
	obs := discovery.NewSPDPObserver(cfg, out.print)

	if *dump == "" {
		err = obs.Run(ctx, l)
	} else {
		err = runDumping(ctx, obs, l, *dump)
	}
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

// runParticipant takes part in the domain and prints participants and
// endpoints until the context ends. On the way out it logs where it sent.
func runParticipant(ctx context.Context, domain uint32, ifi *net.Interface, addr netip.Addr, out *printer, ros bool, every time.Duration) error {
	var graph *ros2.Graph
	var stats *traffic.Stats
	if every > 0 {
		stats = traffic.NewStats()
	}
	if ros {
		graph = ros2.NewGraph(out.node)
	}
	events := func(e discovery.Event) {
		out.print(e)
		if graph != nil {
			graph.HandleDiscovery(e)
		}
		if l, ok := e.(discovery.ParticipantLost); ok && stats != nil {
			stats.ForgetParticipant(l.Participant.GUID.Prefix)
		}
	}
	p, err := participant.New(participant.Config{Domain: domain, Interface: ifi, Addr: addr, Name: "ddsspy"}, events)
	if err != nil {
		return err
	}
	if graph != nil {
		if err := graph.Subscribe(p); err != nil {
			return err
		}
	}
	log.Printf("participant %s at %s", p.GUIDPrefix(), p.Addr())
	if stats != nil {
		c, err := openCapture(ifi)
		if err != nil {
			return err
		}
		go func() {
			if err := c.Run(ctx, func(pk traffic.Packet) { stats.Observe(pk.Message, pk.At) }); err != nil && ctx.Err() == nil {
				log.Printf("capture: %v", err)
			}
			c.Close()
		}()
		go reportTraffic(ctx, p, stats, c, every, out)
	}
	err = p.Run(ctx)
	for to, n := range p.Sent() {
		log.Printf("sent %d datagrams to %s", n, to)
	}
	return err
}

func openCapture(ifi *net.Interface) (*traffic.Capture, error) {
	names := []string{ifi.Name}
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, i := range ifs {
		if i.Flags&net.FlagLoopback != 0 && i.Name != ifi.Name {
			names = append(names, i.Name)
		}
	}
	c, err := traffic.Open(traffic.CaptureConfig{Interfaces: names})
	if err != nil {
		return nil, err
	}
	log.Printf("capturing on %s", strings.Join(names, ", "))
	return c, nil
}

// reportTraffic prints, every interval, what each writer of a participant on
// this host has written since the last time.
func reportTraffic(ctx context.Context, p *participant.Participant, stats *traffic.Stats, c *traffic.Capture, every time.Duration, out *printer) {
	t := time.NewTicker(every)
	defer t.Stop()
	last := map[rtps.GUID]traffic.Writer{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		local := map[rtps.GUIDPrefix]bool{}
		for _, q := range p.Participants() {
			local[q.GUID.Prefix] = true
		}
		topics := map[rtps.GUID]discovery.Endpoint{}
		for _, e := range p.Endpoints() {
			topics[e.GUID] = e
		}
		seen := map[rtps.GUID]traffic.Writer{}
		for _, w := range stats.Writers() {
			if !local[w.GUID.Prefix] {
				continue
			}
			seen[w.GUID] = w
			prev, ok := last[w.GUID]
			if !ok {
				continue // a rate needs two looks
			}
			samples := w.Samples() - prev.Samples()
			l := Line{
				Event: "writer_traffic", At: w.LastSeen, GUIDPrefix: w.GUID.Prefix.String(), GUID: w.GUID.String(),
				Samples: samples, Bytes: w.Bytes() - prev.Bytes(),
				RetransmitRequests: w.RetransmitRequests - prev.RetransmitRequests,
				FragmentRequests:   w.FragmentRequests - prev.FragmentRequests,
				Interval:           every.String(), Drops: c.Drops(),
			}
			if e, ok := topics[w.GUID]; ok {
				l.Topic, l.Type = e.Topic, e.Type
			}
			l.Rate = float64(samples) / every.Seconds()
			out.line(l, func() string {
				return fmt.Sprintf("  > %s %s: %d samples (%.2f/s), %d bytes, %d+%d asked again", l.GUID, l.Topic, l.Samples, l.Rate, l.Bytes, l.RetransmitRequests, l.FragmentRequests)
			})
		}
		last = seen
	}
}

// runDumping is Run, writing every datagram to dir as it goes.
func runDumping(ctx context.Context, obs *discovery.SPDPObserver, l *transport.MulticastListener, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				obs.CheckLeases()
			}
		}
	}()
	var n atomic.Int64
	return l.Receive(ctx, func(p transport.Packet) {
		name := filepath.Join(dir, fmt.Sprintf("%06d-%s.rtps", n.Add(1), strings.ReplaceAll(p.From.String(), ":", "_")))
		_ = os.WriteFile(name, p.Data, 0o644)
		obs.Handle(p.From, p.Data)
	})
}

type printer struct {
	json bool
	mu   sync.Mutex
	enc  *json.Encoder
}

// Line is one event as -json prints it.
type Line struct {
	Event       string    `json:"event"` // discovered, updated, lost
	Reason      string    `json:"reason,omitempty"`
	At          time.Time `json:"at"`
	GUIDPrefix  string    `json:"guid_prefix"`
	Vendor      string    `json:"vendor"`
	Hostname    string    `json:"hostname,omitempty"`
	PID         int       `json:"pid,omitempty"`
	ProcessName string    `json:"process_name,omitempty"`
	Lease       string    `json:"lease"`
	From        string    `json:"from"`
	Cyclone     string    `json:"cyclone,omitempty"`
	UserData    string    `json:"user_data,omitempty"`

	// Endpoint events (endpoint_discovered, endpoint_updated, endpoint_lost).
	GUID        string `json:"guid,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Topic       string `json:"topic,omitempty"`
	Type        string `json:"type,omitempty"`
	Reliability string `json:"reliability,omitempty"`
	Durability  string `json:"durability,omitempty"`
	History     string `json:"history,omitempty"`
	Depth       int32  `json:"depth,omitempty"`

	// Node events (node_appeared, node_updated, node_disappeared).
	Node    string `json:"node,omitempty"`
	Readers int    `json:"readers,omitempty"`
	Writers int    `json:"writers,omitempty"`

	// Traffic (writer_traffic): of one writer, over the interval.
	Interval           string  `json:"interval,omitempty"`
	Samples            int64   `json:"samples,omitempty"`
	Rate               float64 `json:"rate,omitempty"`
	Bytes              int64   `json:"bytes,omitempty"`
	RetransmitRequests int64   `json:"retransmit_requests,omitempty"`
	FragmentRequests   int64   `json:"fragment_requests,omitempty"`
	Drops              uint64  `json:"drops,omitempty"`
}

func (o *printer) line(l Line, text func() string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.json {
		_ = o.enc.Encode(l)
		return
	}
	fmt.Println(text())
}

func (o *printer) print(e discovery.Event) {
	var l Line
	var p discovery.Participant
	switch e := e.(type) {
	case discovery.ParticipantDiscovered:
		l.Event, p, l.At = "discovered", e.Participant, e.Participant.LastSeen
	case discovery.ParticipantUpdated:
		l.Event, p, l.At = "updated", e.Participant, e.Participant.LastSeen
	case discovery.ParticipantLost:
		l.Event, l.Reason, p, l.At = "lost", e.Reason.String(), e.Participant, e.At
	case discovery.EndpointDiscovered:
		o.endpoint("endpoint_discovered", "", e.Endpoint, e.Endpoint.LastSeen)
		return
	case discovery.EndpointUpdated:
		o.endpoint("endpoint_updated", "", e.Endpoint, e.Endpoint.LastSeen)
		return
	case discovery.EndpointLost:
		o.endpoint("endpoint_lost", e.Reason.String(), e.Endpoint, e.At)
		return
	default:
		return
	}
	l.GUIDPrefix, l.Vendor = p.GUID.Prefix.String(), p.Vendor.String()
	l.Hostname, l.PID, l.ProcessName = p.Hostname(), p.PID(), p.ProcessName()
	l.Lease, l.From, l.Cyclone, l.UserData = p.LeaseDuration.String(), p.From.String(), p.CycloneInternals, string(p.UserData)

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.json {
		_ = o.enc.Encode(l)
		return
	}
	mark := map[string]string{"discovered": "+", "updated": "~", "lost": "-"}[l.Event]
	reason := ""
	if l.Reason != "" {
		reason = " (" + l.Reason + ")"
	}
	fmt.Printf("%s %s %s host=%s pid=%d proc=%s lease=%s from=%s cyclone=%q user_data=%q%s\n",
		mark, l.GUIDPrefix, l.Vendor, l.Hostname, l.PID, l.ProcessName, l.Lease, l.From, l.Cyclone, l.UserData, reason)
}

func (o *printer) endpoint(event, reason string, e discovery.Endpoint, at time.Time) {
	q := e.QoS
	l := Line{
		Event: event, Reason: reason, At: at, GUIDPrefix: e.Participant.String(),
		GUID: e.GUID.String(), Kind: e.Kind.String(), Topic: e.Topic, Type: e.Type,
		Reliability: q.Reliability.String(), Durability: q.Durability.String(), History: q.History.String(), Depth: q.Depth,
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.json {
		_ = o.enc.Encode(l)
		return
	}
	mark := map[string]string{"endpoint_discovered": "+", "endpoint_updated": "~", "endpoint_lost": "-"}[event]
	if reason != "" {
		reason = " (" + reason + ")"
	}
	fmt.Printf("  %s %s %s %s [%s] %s %s %s/%d%s\n", mark, l.GUID, l.Kind, l.Topic, l.Type, l.Reliability, l.Durability, l.History, l.Depth, reason)
}

func (o *printer) node(e any) {
	var l Line
	var n ros2.Node
	switch e := e.(type) {
	case ros2.NodeAppeared:
		l.Event, n, l.At = "node_appeared", e.Node, e.At
	case ros2.NodeUpdated:
		l.Event, n, l.At = "node_updated", e.Node, e.At
	case ros2.NodeDisappeared:
		l.Event, l.Reason, n, l.At = "node_disappeared", e.Reason.String(), e.Node, e.At
	default:
		return
	}
	l.Node, l.GUIDPrefix, l.Readers, l.Writers = n.FullName(), n.Participant.String(), len(n.Readers), len(n.Writers)
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.json {
		_ = o.enc.Encode(l)
		return
	}
	mark := map[string]string{"node_appeared": "+", "node_updated": "~", "node_disappeared": "-"}[l.Event]
	reason := ""
	if l.Reason != "" {
		reason = " (" + l.Reason + ")"
	}
	fmt.Printf("  %s node %s in %s: %d readers, %d writers%s\n", mark, l.Node, l.GUIDPrefix, l.Readers, l.Writers, reason)
}
