// Package procmap finds the host process that owns a UDP port, through
// /proc: the socket bound to the port (/proc/net/udp), then the process that
// holds it (/proc/<pid>/fd). It is how a DDS participant, which names itself
// only by a PID in its own namespace, is tied to a host process.
//
// It needs the host's PID and network namespaces and the right to read other
// processes' fds: in hday-os the collector runs privileged with pid_mode and
// network_mode host.
package procmap

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Process is a host process.
type Process struct {
	PID int
	// Comm is the kernel's name for it (15 characters at most).
	Comm string
	// Start is when it started, from /proc/<pid>/stat and the boot time;
	// with PID it identifies the process across PID reuse.
	Start time.Time
	// Cgroup is /proc/<pid>/cgroup, as hostmetrics reports it.
	Cgroup string
}

// Resolver looks processes up under a /proc root.
type Resolver struct {
	Root string // "/proc", or "/hostfs/proc"

	clockTicks int64
	boot       time.Time
}

// New returns a resolver over root. It reads the boot time once.
func New(root string) (*Resolver, error) {
	r := &Resolver{Root: root, clockTicks: 100} // USER_HZ on every Linux this runs on
	b, err := os.ReadFile(filepath.Join(root, "stat"))
	if err != nil {
		return nil, err
	}
	for _, l := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(l, "btime "); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err == nil {
				r.boot = time.Unix(n, 0)
			}
		}
	}
	if r.boot.IsZero() {
		return nil, fmt.Errorf("procmap: no btime in %s/stat", root)
	}
	return r, nil
}

// SocketInodes returns the inodes of the UDP sockets bound to any of the
// local ports, by port.
func (r *Resolver) SocketInodes(ports ...uint16) (map[uint16]uint64, error) {
	want := make(map[uint16]bool, len(ports))
	for _, p := range ports {
		want[p] = true
	}
	out := map[uint16]uint64{}
	for _, name := range []string{"udp", "udp6"} {
		f, err := os.Open(filepath.Join(r.Root, "net", name))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return out, err
		}
		sc := bufio.NewScanner(f)
		sc.Scan() // header
		for sc.Scan() {
			// sl local_address rem_address st tx:rx tr:tm retrnsmt uid timeout inode ...
			fs := strings.Fields(sc.Text())
			if len(fs) < 10 {
				continue
			}
			_, portHex, ok := strings.Cut(fs[1], ":")
			if !ok {
				continue
			}
			port, err := strconv.ParseUint(portHex, 16, 16)
			if err != nil || !want[uint16(port)] {
				continue
			}
			inode, err := strconv.ParseUint(fs[9], 10, 64)
			if err != nil || inode == 0 {
				continue
			}
			out[uint16(port)] = inode
		}
		f.Close()
	}
	return out, nil
}

// Owners returns the process holding each socket inode, scanning every
// process's fds once.
func (r *Resolver) Owners(inodes map[uint64]bool) (map[uint64]int, error) {
	out := map[uint64]int{}
	if len(inodes) == 0 {
		return out, nil
	}
	ents, err := os.ReadDir(r.Root)
	if err != nil {
		return out, err
	}
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		dir := filepath.Join(r.Root, e.Name(), "fd")
		fds, err := os.ReadDir(dir)
		if err != nil {
			continue // gone, or not ours to read
		}
		for _, fd := range fds {
			link, err := os.Readlink(filepath.Join(dir, fd.Name()))
			if err != nil {
				continue
			}
			s, ok := strings.CutPrefix(link, "socket:[")
			if !ok {
				continue
			}
			inode, err := strconv.ParseUint(strings.TrimSuffix(s, "]"), 10, 64)
			if err == nil && inodes[inode] {
				if _, seen := out[inode]; !seen {
					out[inode] = pid
				}
			}
		}
		if len(out) == len(inodes) {
			break
		}
	}
	return out, nil
}

// Process reads what identifies a process.
func (r *Resolver) Process(pid int) (Process, error) {
	p := Process{PID: pid}
	stat, err := os.ReadFile(filepath.Join(r.Root, strconv.Itoa(pid), "stat"))
	if err != nil {
		return p, err
	}
	// pid (comm) state ppid ... starttime is field 22, the 20th after comm.
	open, close := bytes.IndexByte(stat, '('), bytes.LastIndexByte(stat, ')')
	if open < 0 || close < open {
		return p, fmt.Errorf("procmap: %d/stat unreadable", pid)
	}
	p.Comm = string(stat[open+1 : close])
	fs := strings.Fields(string(stat[close+1:]))
	if len(fs) >= 20 {
		if ticks, err := strconv.ParseInt(fs[19], 10, 64); err == nil {
			p.Start = r.boot.Add(time.Duration(ticks) * time.Second / time.Duration(r.clockTicks))
		}
	}
	if cg, err := os.ReadFile(filepath.Join(r.Root, strconv.Itoa(pid), "cgroup")); err == nil {
		p.Cgroup = string(cg)
	}
	return p, nil
}

// ByPorts resolves the process that owns any of the ports: the first port
// with an owner wins (a participant's discovery and user ports are one
// process's).
func (r *Resolver) ByPorts(ports ...uint16) (Process, bool) {
	inodes, err := r.SocketInodes(ports...)
	if err != nil || len(inodes) == 0 {
		return Process{}, false
	}
	set := make(map[uint64]bool, len(inodes))
	for _, i := range inodes {
		set[i] = true
	}
	owners, err := r.Owners(set)
	if err != nil {
		return Process{}, false
	}
	for _, port := range ports {
		if pid, ok := owners[inodes[port]]; ok && inodes[port] != 0 {
			p, err := r.Process(pid)
			return p, err == nil
		}
	}
	return Process{}, false
}
