//go:build !linux

package traffic

import (
	"context"
	"errors"
)

// Capture reads the RTPS traffic of network interfaces. It is only
// implemented on Linux.
type Capture struct{}

// Open fails: capturing is only implemented on Linux.
func Open(CaptureConfig) (*Capture, error) {
	return nil, errors.New("traffic: capturing is only implemented on Linux")
}

func (c *Capture) Run(context.Context, func(Packet)) error { return errors.ErrUnsupported }
func (c *Capture) Drops() uint64                           { return 0 }
func (c *Capture) Close() error                            { return nil }
