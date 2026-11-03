// Command ddsspy listens to SPDP in a DDS domain and prints what it hears:
// participants appearing, changing and going away. It sends nothing.
//
//	ddsspy -domain 138 -iface 192.168.10.0/24 [-local] [-json] [-dump dir]
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
	"github.com/lesomnus/dds/transport"
)

func main() {
	domain := flag.Uint("domain", 0, "DDS domain ID")
	iface := flag.String("iface", "", "interface to listen on, by an address prefix it has (e.g. 192.168.10.0/24 or 10.0.0.5/32)")
	local := flag.Bool("local", false, "only participants on this host")
	asJSON := flag.Bool("json", false, "print events as JSON lines")
	dump := flag.String("dump", "", "also write every datagram received to this directory")
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
	out := &printer{json: *asJSON, enc: json.NewEncoder(os.Stdout)}
	obs := discovery.NewSPDPObserver(cfg, out.print)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *dump == "" {
		err = obs.Run(ctx, l)
	} else {
		err = runDumping(ctx, obs, l, *dump)
	}
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
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
