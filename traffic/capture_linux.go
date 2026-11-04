package traffic

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/bpf"
	"golang.org/x/sys/unix"
)

// Capture reads the RTPS traffic of network interfaces, as a packet socket
// (AF_PACKET) with a filter in the kernel: only the first bytes of RTPS
// datagrams reach it. It needs CAP_NET_RAW in the interfaces' network
// namespace.
//
// What one host's participants send each other goes over the loopback
// interface (to its own addresses); what they send others, and receive,
// over the network's. Capture both to see a host's writers whole.
type Capture struct {
	socks   []*sock
	snaplen int
	closed  atomic.Bool
}

type sock struct {
	fd    int
	iface string
	lo    bool
	drops atomic.Uint64
	mu    sync.Mutex // the fd's statistics
}

func htons(v uint16) uint16 { return v<<8 | v>>8 }

// Open opens a capture on the interfaces.
func Open(cfg CaptureConfig) (*Capture, error) {
	if cfg.Snaplen <= 0 {
		cfg.Snaplen = 256
	}
	if cfg.Buffer <= 0 {
		cfg.Buffer = 4 << 20
	}
	if len(cfg.Interfaces) == 0 {
		return nil, errors.New("traffic: no interface to capture on")
	}
	prog := cfg.Filter
	if prog == nil {
		prog = Filter(uint32(cfg.Snaplen))
	}
	raw, err := bpf.Assemble(prog)
	if err != nil {
		return nil, err
	}
	filter := make([]unix.SockFilter, len(raw))
	for i, r := range raw {
		filter[i] = unix.SockFilter{Code: r.Op, Jt: r.Jt, Jf: r.Jf, K: r.K}
	}
	c := &Capture{snaplen: cfg.Snaplen}
	for _, name := range cfg.Interfaces {
		ifi, err := net.InterfaceByName(name)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("traffic: %w", err)
		}
		s, err := openSock(ifi, filter, cfg.Buffer)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("traffic: %s: %w", name, err)
		}
		c.socks = append(c.socks, s)
	}
	return c, nil
}

func openSock(ifi *net.Interface, filter []unix.SockFilter, buffer int) (*sock, error) {
	// Protocol 0 receives nothing until bound: the filter is in place by
	// then, so nothing unfiltered is queued.
	fd, err := unix.Socket(unix.AF_PACKET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	prog := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := unix.SetsockoptSockFprog(fd, unix.SOL_SOCKET, unix.SO_ATTACH_FILTER, &prog); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("attach filter: %w", err)
	}
	if unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUFFORCE, buffer) != nil {
		_ = unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, buffer)
	}
	// Wake up now and then to see whether to stop.
	tv := unix.NsecToTimeval(int64(250 * time.Millisecond))
	if err := unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv); err != nil {
		unix.Close(fd)
		return nil, err
	}
	// ETH_P_ALL, not ETH_P_IP: a socket of one protocol is not given what
	// the host sends, only what it receives. The filter keeps IPv4.
	if err := unix.Bind(fd, &unix.SockaddrLinklayer{Protocol: htons(unix.ETH_P_ALL), Ifindex: ifi.Index}); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("bind: %w", err)
	}
	return &sock{fd: fd, iface: ifi.Name, lo: ifi.Flags&net.FlagLoopback != 0}, nil
}

// Run reads packets until the context is done, calling fn with each, from
// one goroutine per interface: fn must be safe for concurrent use. The
// Packet's Message is only valid during the call.
func (c *Capture) Run(ctx context.Context, fn func(Packet)) error {
	var wg sync.WaitGroup
	errs := make([]error, len(c.socks))
	for i, s := range c.socks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = c.read(ctx, s, fn)
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.Join(errs...)
}

func (c *Capture) read(ctx context.Context, s *sock, fn func(Packet)) error {
	buf := make([]byte, c.snaplen)
	for ctx.Err() == nil {
		n, from, err := unix.Recvfrom(s.fd, buf, 0)
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
				continue
			}
			if c.closed.Load() {
				return nil
			}
			return fmt.Errorf("traffic: %s: %w", s.iface, err)
		}
		ll, _ := from.(*unix.SockaddrLinklayer)
		if ll == nil || ll.Protocol != htons(unix.ETH_P_IP) {
			continue
		}
		switch ll.Pkttype {
		case unix.PACKET_LOOPBACK:
			// A copy of what this host multicast, looped back to it: the
			// outgoing one is seen already.
			continue
		case unix.PACKET_OUTGOING:
			if s.lo {
				continue // seen again as it comes in
			}
		}
		msg, src, dst, ok := rtpsPayload(buf[:min(n, len(buf))])
		if !ok {
			continue
		}
		fn(Packet{
			Message: msg, Src: src, Dst: dst, SrcPort: src.Port(), DstPort: dst.Port(),
			Interface: s.iface, Outgoing: ll.Pkttype == unix.PACKET_OUTGOING,
			At: time.Now(),
		})
	}
	return nil
}

// Drops returns how many packets the kernel has dropped, since the capture
// was opened, for want of room in the receive buffers: the capture fell
// behind.
func (c *Capture) Drops() uint64 {
	var total uint64
	for _, s := range c.socks {
		s.mu.Lock()
		// Reading the statistics resets them.
		if st, err := unix.GetsockoptTpacketStats(s.fd, unix.SOL_PACKET, unix.PACKET_STATISTICS); err == nil {
			s.drops.Add(uint64(st.Drops))
		}
		s.mu.Unlock()
		total += s.drops.Load()
	}
	return total
}

// Close closes the capture. Run must have returned.
func (c *Capture) Close() error {
	c.closed.Store(true)
	var errs []error
	for _, s := range c.socks {
		errs = append(errs, unix.Close(s.fd))
	}
	c.socks = nil
	return errors.Join(errs...)
}
