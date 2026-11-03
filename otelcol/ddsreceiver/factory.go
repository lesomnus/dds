// Package ddsreceiver is an OpenTelemetry Collector receiver that reports
// the DDS system on this host: participants and the host processes they
// are, their endpoints with topic, type and QoS, how much each writer
// writes, and -- with ROS -- the ROS nodes they host. Changes are events
// (logs); the state is reported every snapshot interval (logs and metrics),
// the traffic every collection interval (metrics).
//
// It takes part in the DDS domain read-only, talking only to participants on
// this host, and is never a ROS node itself. The traffic it reads off the
// wire (github.com/lesomnus/dds/traffic), without taking part.
package ddsreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

var typeStr = component.MustNewType("dds")

// NewFactory returns the factory of the `dds` receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		func() component.Config { return createDefaultConfig() },
		receiver.WithLogs(createLogs, component.StabilityLevelAlpha),
		receiver.WithMetrics(createMetrics, component.StabilityLevelAlpha),
	)
}

// One receiver of an ID is one DDS participant, whichever pipelines it is
// in: the logs and metrics ones share it.

func createLogs(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Logs) (receiver.Logs, error) {
	r := receiverFor(set.ID, cfg.(*Config), set.Logger)
	r.mu.Lock()
	r.logs = next
	r.mu.Unlock()
	return handle{id: set.ID, r: r}, nil
}

func createMetrics(_ context.Context, set receiver.Settings, cfg component.Config, next consumer.Metrics) (receiver.Metrics, error) {
	r := receiverFor(set.ID, cfg.(*Config), set.Logger)
	r.mu.Lock()
	r.metrics = next
	r.mu.Unlock()
	return handle{id: set.ID, r: r}, nil
}
