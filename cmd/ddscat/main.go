// Command ddscat publishes to a DDS topic, or prints what is published on
// one, on this host. Payloads are serialized by ddscat itself: a CDR string
// (a type whose one member is a string), or raw octets.
//
//	ddscat pub -domain 138 -iface 192.168.10.0/24 -topic rt/chatter -type std_msgs::msg::dds_::String_ -string 'hello {n}' -rate 100ms -count 10
//	ddscat pub ... -size 100000              # a string of 100000 'x'
//	ddscat sub -domain 138 -iface 192.168.10.0/24 -topic rt/chatter -type std_msgs::msg::dds_::String_
//
// It prints one JSON object per line: what it wrote (pub), or each sample
// it read (sub). The topic and type names are the user's: for ROS 2, "rt/"
// and "pkg::msg::dds_::Name_".
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lesomnus/dds/participant"
	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/transport"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: ddscat pub|sub [flags]")
	os.Exit(2)
}

// Line is one thing ddscat prints.
type Line struct {
	Event   string `json:"event"` // written, matched, sample
	SN      int64  `json:"sn,omitempty"`
	Writer  string `json:"writer,omitempty"`
	Size    int    `json:"size,omitempty"`
	String  string `json:"string,omitempty"`
	Readers int    `json:"readers,omitempty"`
	Error   string `json:"error,omitempty"`
}

var (
	outMu sync.Mutex
	enc   = json.NewEncoder(os.Stdout)
)

func print(l Line) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = enc.Encode(l)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	mode := os.Args[1]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	domain := fs.Uint("domain", 0, "DDS domain ID")
	iface := fs.String("iface", "", "interface, by an address prefix it has (e.g. 192.168.10.0/24)")
	topic := fs.String("topic", "", "topic name")
	typ := fs.String("type", "", "type name")
	reliability := fs.String("reliability", "reliable", "reliable or best_effort")
	durability := fs.String("durability", "volatile", "volatile or transient_local")
	history := fs.String("history", "keep_last", "keep_last or keep_all")
	depth := fs.Int("depth", 10, "KEEP_LAST depth")
	text := fs.String("string", "hello {n}", "pub: the string to publish; {n} is the sample's number")
	size := fs.Int("size", 0, "pub: publish a string of this many 'x' instead")
	rate := fs.Duration("rate", time.Second, "pub: one sample this often")
	count := fs.Int("count", 0, "pub: stop after this many (0: never)")
	wait := fs.Int("wait", 0, "pub: wait for this many readers to be ready (matched both ways) before the first sample")
	linger := fs.Duration("linger", 2*time.Second, "pub: stay this long after the last sample, for reliable readers to catch up")
	_ = fs.Parse(os.Args[2:])
	if *topic == "" || *typ == "" {
		log.Fatal("-topic and -type are required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	prefix, err := netip.ParsePrefix(*iface)
	if err != nil {
		log.Fatalf("-iface: %v", err)
	}
	ifi, addr, err := transport.InterfaceByAddr(prefix)
	if err != nil {
		log.Fatal(err)
	}
	p, err := participant.New(participant.Config{Domain: uint32(*domain), Interface: ifi, Addr: addr, Name: "ddscat"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	rel := map[string]rtps.ReliabilityKind{"reliable": rtps.Reliable, "best_effort": rtps.BestEffort}[*reliability]
	dur := map[string]rtps.DurabilityKind{"volatile": rtps.Volatile, "transient_local": rtps.TransientLocal}[*durability]
	hist := map[string]rtps.HistoryKind{"keep_last": rtps.KeepLast, "keep_all": rtps.KeepAll}[*history]
	if rel == 0 {
		log.Fatalf("-reliability %q", *reliability)
	}

	done := make(chan error, 1)
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { done <- p.Run(run) }()
	log.Printf("participant %s at %s", p.GUIDPrefix(), p.Addr())

	switch mode {
	case "sub":
		err := p.Subscribe(participant.Subscription{
			Topic: *topic, Type: *typ,
			QoS: participant.ReaderQoS{Reliability: rel, Durability: dur, History: hist, Depth: int32(*depth)},
			Handle: func(w rtps.GUID, s participant.Sample) {
				l := Line{Event: "sample", SN: int64(s.SN), Writer: w.String(), Size: len(s.Payload)}
				l.String, _ = cdrString(s.Payload)
				if len(l.String) > 200 {
					l.String = l.String[:200]
				}
				print(l)
			},
		})
		if err != nil {
			log.Fatal(err)
		}
	case "pub":
		w, err := p.Publish(participant.Publication{
			Topic: *topic, Type: *typ,
			QoS: participant.WriterQoS{Reliability: rel, Durability: dur, History: hist, Depth: int32(*depth)},
		})
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			defer cancel()
			for len(w.Ready()) < *wait {
				select {
				case <-run.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
			if *wait > 0 {
				print(Line{Event: "matched", Readers: len(w.Ready())})
			}
			t := time.NewTicker(*rate)
			defer t.Stop()
			for n := 1; *count == 0 || n <= *count; n++ {
				s := strings.ReplaceAll(*text, "{n}", strconv.Itoa(n))
				if *size > 0 {
					s = strings.Repeat("x", *size)
				}
				sn, err := w.Write(cdrPayload(s))
				l := Line{Event: "written", SN: int64(sn), Size: len(s), Readers: len(w.Matched())}
				if err != nil {
					l.Error = err.Error()
				}
				print(l)
				select {
				case <-run.Done():
					return
				case <-t.C:
				}
			}
			select {
			case <-run.Done():
			case <-time.After(*linger):
			}
		}()
	default:
		usage()
	}
	if err := <-done; err != nil {
		log.Fatal(err)
	}
}

// cdrPayload is a CDR little-endian serialized string: the encapsulation
// header, the length with the terminating NUL, the octets, the NUL.
func cdrPayload(s string) []byte {
	b := []byte{0x00, 0x01, 0x00, 0x00}
	b = binary.LittleEndian.AppendUint32(b, uint32(len(s)+1))
	b = append(b, s...)
	return append(b, 0)
}

// cdrString reads what cdrPayload writes.
func cdrString(b []byte) (string, bool) {
	if len(b) < 9 || b[0] != 0 || b[1] != 1 {
		return "", false
	}
	n := int(binary.LittleEndian.Uint32(b[4:8]))
	if n < 1 || 8+n > len(b) {
		return "", false
	}
	return string(b[8 : 8+n-1]), true
}
