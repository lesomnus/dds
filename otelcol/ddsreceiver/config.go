package ddsreceiver

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"time"
)

// Config is the `dds` receiver's configuration.
type Config struct {
	// Domain is one domain to take part in: shorthand for
	// domains.active: [Domain].
	Domain *uint32 `mapstructure:"domain"`
	// Domains are the DDS domains collected.
	Domains DomainsConfig `mapstructure:"domains"`
	// Interface is the network DDS runs on, named by an address prefix one
	// interface has -- as Cyclone DDS configurations do
	// (<NetworkInterface address="192.168.10.0"/> is 192.168.10.0/24).
	Interface string `mapstructure:"interface"`
	// ROS reads ros_discovery_info for the nodes, and names endpoints by
	// their ROS names. Off, only DDS is reported.
	ROS bool `mapstructure:"ros"`
	// SnapshotInterval is how often the whole graph is reported (state logs
	// and metrics), besides the events as they happen.
	SnapshotInterval time.Duration `mapstructure:"snapshot_interval"`
	// ProcRoot is where /proc of the host is: the receiver ties participants
	// to host processes through it. "/proc" when the collector shares the
	// host's PID and network namespaces.
	ProcRoot string `mapstructure:"proc_root"`
	// Traffic is what each writer on this host writes, read off the wire.
	Traffic TrafficConfig `mapstructure:"traffic"`
}

// DomainsConfig says which domains are collected, and how.
//
// A domain is collected actively by taking part in it, as a participant that
// only reads and talks only to this host's participants: what is learned is
// everything -- participants, endpoints (topics, types, QoS), ROS nodes,
// traffic by topic. It is collected passively by listening alone, sending
// nothing: participants, and traffic by writer, without topics.
type DomainsConfig struct {
	// Active are domains always taken part in.
	Active []uint32 `mapstructure:"active"`
	// Discover collects the domains this host's participants announce
	// themselves in, as they appear: an SPDP announcement from an address of
	// this host, captured (CAP_NET_RAW). A discovered domain is left two
	// minutes after its last participant here is gone.
	Discover bool `mapstructure:"discover"`
	// Discovered is how a discovered domain is collected: active or
	// passive.
	Discovered string `mapstructure:"discovered"`
	// Ignore are domains not collected, discovered or not.
	Ignore []uint32 `mapstructure:"ignore"`
}

// TrafficConfig configures the traffic metrics.
type TrafficConfig struct {
	// Enabled captures the RTPS traffic. It needs CAP_NET_RAW in the host's
	// network namespace; without it the receiver goes on without.
	Enabled bool `mapstructure:"enabled"`
	// CollectionInterval is how often the traffic is captured and the
	// writers' counters reported.
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	// Window is how long the traffic is captured each time. Writers are
	// counted by sequence number, so a writer seen now and then is counted
	// whole; what is seen of sizes and retransmit requests is a sample.
	// Outside the windows capturing costs nothing. A window as long as the
	// interval captures all along.
	Window time.Duration `mapstructure:"window"`
	// Interfaces to capture on, by name. By default the DDS interface and
	// the loopback one: participants on one host reach each other over
	// loopback.
	Interfaces []string `mapstructure:"interfaces"`
}

func createDefaultConfig() *Config {
	return &Config{
		Domains: DomainsConfig{
			Discover:   true,
			Discovered: modeActive,
		},
		ROS:              true,
		SnapshotInterval: time.Minute,
		ProcRoot:         "/proc",
		Traffic: TrafficConfig{
			Enabled:            true,
			CollectionInterval: 5 * time.Second,
			Window:             250 * time.Millisecond,
		},
	}
}

// maxDomain is the largest domain the default port mapping fits.
const maxDomain = 232

// active is the domains always taken part in: domains.active and domain.
func (c *Config) active() []uint32 {
	out := slices.Clone(c.Domains.Active)
	if c.Domain != nil && !slices.Contains(out, *c.Domain) {
		out = append(out, *c.Domain)
	}
	return out
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	active := c.active()
	for _, d := range append(slices.Clone(active), c.Domains.Ignore...) {
		if d > maxDomain {
			return fmt.Errorf("domain %d does not fit the default port mapping (max %d)", d, maxDomain)
		}
	}
	for _, d := range active {
		if slices.Contains(c.Domains.Ignore, d) {
			return fmt.Errorf("domain %d is both active and ignored", d)
		}
	}
	if len(active) == 0 && !c.Domains.Discover {
		return errors.New("no domain to collect: none active, and discover is off")
	}
	if c.Domains.Discovered != modeActive && c.Domains.Discovered != modePassive {
		return fmt.Errorf("domains.discovered: %q is neither %q nor %q", c.Domains.Discovered, modeActive, modePassive)
	}
	if c.Interface == "" {
		return errors.New("interface is required: an address prefix of the network DDS runs on")
	}
	if _, err := netip.ParsePrefix(c.Interface); err != nil {
		return fmt.Errorf("interface: %w", err)
	}
	if c.SnapshotInterval <= 0 {
		return errors.New("snapshot_interval must be positive")
	}
	if c.Traffic.Enabled && c.Traffic.CollectionInterval <= 0 {
		return errors.New("traffic.collection_interval must be positive")
	}
	if c.Traffic.Enabled && c.Traffic.Window <= 0 {
		return errors.New("traffic.window must be positive")
	}
	return nil
}
