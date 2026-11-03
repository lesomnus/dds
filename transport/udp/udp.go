// Package udp is the UDPv4 transport: one unicast socket for everything a
// participant sends and receives directly, and one that listens to the SPDP
// multicast group, on one network interface or several.
package udp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"

	"github.com/lesomnus/dds/rtps"
	"github.com/lesomnus/dds/transport"
)

// Interface is a network interface the transport uses, and its address.
type Interface struct {
	Interface *net.Interface
	Addr      netip.Addr
	// NoMulticast keeps the SPDP multicast group off this interface: it is
	// neither listened to nor sent to here.
	NoMulticast bool
}

// Config configures the transport.
type Config struct {
	Domain     uint32
	Interfaces []Interface
	// WellKnownPorts takes the first free participant index of the domain
	// and its metatraffic unicast port (7410 + 250·domain + 2·index), the
	// range a firewall opens for a domain. Otherwise the system picks a
	// port.
	WellKnownPorts bool
	// SendMulticast lets Send reach the SPDP multicast group, out of each
	// interface that multicasts, to this link only (TTL 1). The group is
	// listened to either way.
	SendMulticast bool
	// ReadBuffer is the sockets' receive buffer; 4 MiB when zero.
	ReadBuffer int
}

// maxParticipantIndex bounds the indexes of a domain: their ports stay
// below the next domain's.
const maxParticipantIndex = (transport.DomainGain - 10) / transport.ParticipantGain

// Transport is the UDPv4 transport.
type Transport struct {
	cfg     Config
	conn    *net.UDPConn
	pc      *ipv4.PacketConn
	mcast   *net.UDPConn // nil if no interface multicasts
	port    uint16
	index   int
	local   []netip.Addr
	nets    []netip.Prefix
	unicast []rtps.Locator

	mu sync.Mutex // the multicast interface of pc
}

var _ transport.Transport = (*Transport)(nil)

// Open opens the transport's sockets.
func Open(cfg Config) (*Transport, error) {
	if len(cfg.Interfaces) == 0 {
		return nil, errors.New("udp: no interface")
	}
	if cfg.ReadBuffer <= 0 {
		cfg.ReadBuffer = 4 << 20
	}
	t := &Transport{cfg: cfg, index: -1}
	// One interface: its address. Several: all of them.
	bind := netip.IPv4Unspecified()
	if len(cfg.Interfaces) == 1 {
		bind = cfg.Interfaces[0].Addr
	}
	conn, err := t.listen(bind)
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(cfg.ReadBuffer)
	t.conn, t.pc = conn, ipv4.NewPacketConn(conn)
	t.port = uint16(conn.LocalAddr().(*net.UDPAddr).Port)
	if cfg.SendMulticast {
		if err := t.pc.SetMulticastTTL(1); err != nil {
			conn.Close()
			return nil, err
		}
		if err := t.pc.SetMulticastLoopback(true); err != nil {
			conn.Close()
			return nil, err
		}
	}
	for _, i := range cfg.Interfaces {
		t.unicast = append(t.unicast, transport.UDPv4(netip.AddrPortFrom(i.Addr, t.port)))
		t.nets = append(t.nets, interfaceNets(i.Interface)...)
	}
	if t.mcast, err = listenMulticast(cfg); err != nil {
		conn.Close()
		return nil, err
	}
	local, err := transport.LocalAddrs()
	if err != nil {
		t.Close()
		return nil, err
	}
	for a := range local {
		t.local = append(t.local, a)
	}
	return t, nil
}

func (t *Transport) listen(bind netip.Addr) (*net.UDPConn, error) {
	if !t.cfg.WellKnownPorts {
		return net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(bind, 0)))
	}
	for i := 0; i < maxParticipantIndex; i++ {
		port := transport.MetatrafficUnicastPort(t.cfg.Domain, uint32(i))
		c, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(bind, port)))
		if err == nil {
			t.index = i
			return c, nil
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("udp: every participant index of domain %d is taken", t.cfg.Domain)
}

// listenMulticast joins the SPDP group on each interface that multicasts.
func listenMulticast(cfg Config) (*net.UDPConn, error) {
	group := transport.DefaultMulticastGroup
	port := transport.SPDPMulticastPort(cfg.Domain)
	var c *net.UDPConn
	for _, i := range cfg.Interfaces {
		if i.NoMulticast {
			continue
		}
		if c == nil {
			l, err := transport.ListenMulticast(i.Interface, group, port)
			if err != nil {
				return nil, err
			}
			c = l.Conn()
			_ = c.SetReadBuffer(cfg.ReadBuffer)
			continue
		}
		if err := ipv4.NewPacketConn(c).JoinGroup(i.Interface, &net.UDPAddr{IP: group.AsSlice()}); err != nil {
			c.Close()
			return nil, fmt.Errorf("udp: joining %s on %s: %w", group, i.Interface.Name, err)
		}
	}
	return c, nil
}

// interfaceNets are the subnets of an interface's IPv4 addresses.
func interfaceNets(ifi *net.Interface) []netip.Prefix {
	addrs, err := ifi.Addrs()
	if err != nil {
		return nil
	}
	var out []netip.Prefix
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(n.IP)
		if !ok || !ip.Unmap().Is4() {
			continue
		}
		bits, _ := n.Mask.Size()
		out = append(out, netip.PrefixFrom(ip.Unmap(), bits).Masked())
	}
	return out
}

// Index is the participant index whose port it took, or -1.
func (t *Transport) Index() int { return t.index }

func (t *Transport) Unicast() []rtps.Locator  { return t.unicast }
func (t *Transport) LocalAddrs() []netip.Addr { return t.local }
func (t *Transport) Networks() []netip.Prefix { return t.nets }

func (t *Transport) Reaches(l rtps.Locator) bool {
	_, ok := transport.AddrPort(l)
	return ok
}

// Send sends a message: to a unicast address, or to the SPDP group out of
// every interface that multicasts.
func (t *Transport) Send(to rtps.Locator, msg []byte) error {
	a, ok := transport.AddrPort(to)
	if !ok {
		return fmt.Errorf("udp: cannot send to %v", to)
	}
	if !a.Addr().IsMulticast() {
		_, err := t.conn.WriteToUDPAddrPort(msg, a)
		return err
	}
	if !t.cfg.SendMulticast {
		return errors.New("udp: multicast is off")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	var errs []error
	for _, i := range t.cfg.Interfaces {
		if i.NoMulticast {
			continue
		}
		if err := t.pc.SetMulticastInterface(i.Interface); err != nil {
			errs = append(errs, err)
			continue
		}
		if _, err := t.conn.WriteToUDPAddrPort(msg, a); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SendVia sends a message to the SPDP group out of the interface of one of
// its locators alone; to a unicast address as Send does.
func (t *Transport) SendVia(via, to rtps.Locator, msg []byte) error {
	a, ok := transport.AddrPort(to)
	if !ok || !a.Addr().IsMulticast() {
		return t.Send(to, msg)
	}
	if !t.cfg.SendMulticast {
		return errors.New("udp: multicast is off")
	}
	for _, i := range t.cfg.Interfaces {
		if i.Addr != via.Addr {
			continue
		}
		if i.NoMulticast {
			return nil
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if err := t.pc.SetMulticastInterface(i.Interface); err != nil {
			return err
		}
		_, err := t.conn.WriteToUDPAddrPort(msg, a)
		return err
	}
	return fmt.Errorf("udp: no interface at %s", via.Addr)
}

// Run receives on both sockets until the context ends.
func (t *Transport) Run(ctx context.Context, deliver func(transport.Packet)) error {
	conns := []*net.UDPConn{t.conn}
	if t.mcast != nil {
		conns = append(conns, t.mcast)
	}
	errs := make([]error, len(conns))
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = receive(ctx, c, c == t.mcast, deliver)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// receive reads a socket until the context ends: then it wakes the read
// with a deadline, and leaves the socket open for sending.
func receive(ctx context.Context, c *net.UDPConn, multicast bool, deliver func(transport.Packet)) error {
	stop := context.AfterFunc(ctx, func() { _ = c.SetReadDeadline(time.Now()) })
	defer stop()
	buf := make([]byte, 65507)
	for {
		n, from, err := c.ReadFromUDPAddrPort(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		deliver(transport.Packet{From: netip.AddrPortFrom(from.Addr().Unmap(), from.Port()), Multicast: multicast, Data: buf[:n]})
	}
}

// Close closes the sockets.
func (t *Transport) Close() error {
	var errs []error
	if t.conn != nil {
		errs = append(errs, t.conn.Close())
	}
	if t.mcast != nil {
		errs = append(errs, t.mcast.Close())
	}
	return errors.Join(errs...)
}

// ParseInterfaces finds the interfaces a comma-separated list of address
// prefixes names, in order: "192.168.10.0/24,10.0.0.0/16" is the interface
// with an address in the first, then the one in the second (see
// transport.InterfaceByAddr).
func ParseInterfaces(list string) ([]Interface, error) {
	var out []Interface
	for _, s := range strings.Split(list, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(s)
		if err != nil {
			return nil, err
		}
		ifi, addr, err := transport.InterfaceByAddr(prefix)
		if err != nil {
			return nil, err
		}
		out = append(out, Interface{Interface: ifi, Addr: addr})
	}
	if len(out) == 0 {
		return nil, errors.New("udp: no interface named")
	}
	return out, nil
}
