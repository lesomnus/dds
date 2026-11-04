package ddsreceiver

import (
	"context"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/dds/discovery"
	"github.com/lesomnus/dds/participant"
	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/traffic"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// spdp is an SPDP announcement of a participant at an address, leasing for
// lease.
func spdp(prefix rtps.GUIDPrefix, at netip.AddrPort, lease time.Duration) []byte {
	loc := rtps.Locator{Kind: rtps.LocatorUDPv4, Addr: at.Addr(), Port: uint32(at.Port())}
	pl := (&rtps.ParameterListWriter{}).
		GUID(rtps.PIDParticipantGUID, rtps.GUID{Prefix: prefix, Entity: rtps.EntityParticipant}).
		Raw(rtps.PIDProtocolVersion, []byte{2, 1}).
		Raw(rtps.PIDVendorID, rtps.VendorCyclone[:]).
		Duration(rtps.PIDParticipantLease, lease).
		Locator(rtps.PIDMetatrafficUnicastLoc, loc).
		Locator(rtps.PIDDefaultUnicastLoc, loc).
		Uint32(rtps.PIDBuiltinEndpointSet, rtps.BuiltinParticipantAnnouncer|rtps.BuiltinParticipantDetector).
		Payload()
	return rtps.NewBuilder(prefix, rtps.VendorCyclone).Data(rtps.EntitySPDPReader, rtps.EntitySPDPWriter, 1, nil, pl, false).Bytes()
}

// announcement is an SPDP packet as the discovery capture sees it.
func announcement(prefix rtps.GUIDPrefix, from string, domain uint32, lease time.Duration) traffic.Packet {
	src := netip.MustParseAddrPort(from)
	return traffic.Packet{
		Message: spdp(prefix, src, lease),
		Src:     src,
		Dst:     netip.AddrPortFrom(netip.MustParseAddr("239.255.0.1"), uint16(7400+250*domain)),
		At:      time.Now(),
	}
}

// discoveryCapture is the discovery capture: it sees its packets, then
// waits. A traffic capture sees nothing.
type discoveryCapture struct{ packets []traffic.Packet }

func (c *discoveryCapture) Run(ctx context.Context, fn func(traffic.Packet)) error {
	for _, p := range c.packets {
		fn(p)
	}
	<-ctx.Done()
	return ctx.Err()
}
func (c *discoveryCapture) Drops() uint64 { return 0 }
func (c *discoveryCapture) Close() error  { return nil }

type domainsRig struct {
	r       *ddsReceiver
	mr      interface{ Shutdown(context.Context) error }
	metrics *consumertest.MetricsSink
	mu      sync.Mutex
	joined  []uint32
}

func startDomains(t *testing.T, cfg *Config, seen []traffic.Packet) *domainsRig {
	t.Helper()
	f := NewFactory()
	cfg.Interface = "127.0.0.0/8"
	cfg.ProcRoot = t.TempDir()
	cfg.SnapshotInterval = 100 * time.Millisecond
	cfg.Traffic.Enabled = false
	set := receivertest.NewNopSettings(f.Type())
	set.ID = component.NewIDWithName(f.Type(), t.Name())
	g := &domainsRig{metrics: new(consumertest.MetricsSink)}
	mr, err := f.CreateMetrics(context.Background(), set, cfg, g.metrics)
	if err != nil {
		t.Fatal(err)
	}
	g.r, g.mr = mr.(handle).r, mr
	g.r.leaveAfter = 200 * time.Millisecond
	g.r.openCapture = func(c traffic.CaptureConfig) (capturer, error) {
		if c.Snaplen != traffic.SPDPSnaplen {
			t.Errorf("a capture other than discovery's: %+v", c)
		}
		return &discoveryCapture{packets: seen}, nil
	}
	g.r.newParticipant = func(c participant.Config, ev func(discovery.Event)) (runner, error) {
		g.mu.Lock()
		g.joined = append(g.joined, c.Domain)
		g.mu.Unlock()
		return &fakeParticipant{events: ev}, nil
	}
	if err := mr.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mr.Shutdown(context.Background()) })
	return g
}

func (g *domainsRig) domains() []uint32 {
	var out []uint32
	for _, d := range g.r.domainList() {
		out = append(out, d.id)
	}
	return out
}

// participantsIn is the latest dds.participants of a domain, and its mode.
func (g *domainsRig) participantsIn(domain uint32) (int64, string, bool) {
	var v int64 = -1
	var mode string
	for _, md := range g.metrics.AllMetrics() {
		for i := range md.ResourceMetrics().Len() {
			rm := md.ResourceMetrics().At(i)
			a := rm.Resource().Attributes().AsRaw()
			if a[attrDomain] != int64(domain) {
				continue
			}
			mode, _ = a[attrMode].(string)
			ms := rm.ScopeMetrics().At(0).Metrics()
			for j := range ms.Len() {
				if m := ms.At(j); m.Name() == mParticipants && m.Type() == pmetric.MetricTypeGauge {
					v = m.Gauge().DataPoints().At(0).IntValue()
				}
			}
		}
	}
	return v, mode, v >= 0
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

var (
	pLocal  = rtps.GUIDPrefix{0x01, 0x10, 0x05}
	pRemote = rtps.GUIDPrefix{0x01, 0x10, 0x06}
	pIgnore = rtps.GUIDPrefix{0x01, 0x10, 0x07}
)

// TestDiscoveredDomainsJoined: by default, the domains this host's
// participants announce themselves in are taken part in -- not those only
// another host's are in, nor the ignored.
func TestDiscoveredDomainsJoined(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Domains.Active = []uint32{138}
	cfg.Domains.Ignore = []uint32{7}
	g := startDomains(t, cfg, []traffic.Packet{
		announcement(pLocal, "127.0.0.1:7410", 5, 10*time.Second),
		announcement(pRemote, "10.99.0.1:7410", 6, 10*time.Second), // another host's
		announcement(pIgnore, "127.0.0.1:7412", 7, 10*time.Second), // ignored
		announcement(pLocal, "127.0.0.1:7410", 5, 10*time.Second),  // again
	})
	waitFor(t, "domain 5 joined", func() bool { return slices.Contains(g.domains(), 5) })
	time.Sleep(100 * time.Millisecond)
	if got := g.domains(); !slices.Equal(got, []uint32{5, 138}) {
		t.Errorf("domains %v, want [5 138]", got)
	}
	g.mu.Lock()
	joined := slices.Clone(g.joined)
	g.mu.Unlock()
	slices.Sort(joined)
	if !slices.Equal(joined, []uint32{5, 138}) {
		t.Errorf("participants in %v, want one in 5 and one in 138", joined)
	}
	waitFor(t, "domain 5's metrics", func() bool { _, m, ok := g.participantsIn(5); return ok && m == modeActive })
}

// TestPassiveDomain: a domain discovered passively is listened to: its
// participants are reported, no participant of ours takes part, and once
// they are gone the domain is left.
func TestPassiveDomain(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Domains.Discovered = modePassive
	g := startDomains(t, cfg, []traffic.Packet{announcement(pLocal, "127.0.0.1:7410", 5, time.Second)})
	waitFor(t, "a participant in domain 5", func() bool {
		n, m, ok := g.participantsIn(5)
		return ok && n == 1 && m == modePassive
	})
	g.mu.Lock()
	if len(g.joined) != 0 {
		t.Errorf("took part in %v passively", g.joined)
	}
	g.mu.Unlock()
	// Its lease runs out (it is heard of no more), then the domain is left.
	waitFor(t, "domain 5 left", func() bool { return len(g.domains()) == 0 })
}

// TestActiveDomainsStay: a configured domain is kept, participants or not.
func TestActiveDomainsStay(t *testing.T) {
	cfg := createDefaultConfig()
	cfg.Domain = u32(9)
	cfg.Domains.Discover = false
	g := startDomains(t, cfg, nil)
	time.Sleep(1500 * time.Millisecond)
	if got := g.domains(); !slices.Equal(got, []uint32{9}) {
		t.Errorf("domains %v", got)
	}
}
