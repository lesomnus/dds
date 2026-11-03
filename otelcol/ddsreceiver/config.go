package ddsreceiver

import (
	"errors"
	"fmt"
	"net/netip"
	"time"
)

// Config is the `dds` receiver's configuration.
type Config struct {
	// Domain is the DDS domain (ROS_DOMAIN_ID).
	Domain uint32 `mapstructure:"domain"`
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

// TrafficConfig configures the traffic metrics.
type TrafficConfig struct {
	// Enabled captures the RTPS traffic. It needs CAP_NET_RAW in the host's
	// network namespace; without it the receiver goes on without traffic.
	Enabled bool `mapstructure:"enabled"`
	// CollectionInterval is how often the writers' counters are reported.
	CollectionInterval time.Duration `mapstructure:"collection_interval"`
	// Interfaces to capture on, by name. By default the DDS interface and
	// the loopback one: participants on one host reach each other over
	// loopback.
	Interfaces []string `mapstructure:"interfaces"`
}

func createDefaultConfig() *Config {
	return &Config{
		ROS:              true,
		SnapshotInterval: time.Minute,
		ProcRoot:         "/proc",
		Traffic: TrafficConfig{
			Enabled:            true,
			CollectionInterval: 10 * time.Second,
		},
	}
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.Domain > 232 {
		return fmt.Errorf("domain %d does not fit the default port mapping (max 232)", c.Domain)
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
	return nil
}
