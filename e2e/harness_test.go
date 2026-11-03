//go:build e2e

// Package e2e checks this module against Cyclone DDS 0.10.x as ROS 2 Jazzy
// runs it (rmw_cyclonedds_cpp), in Docker.
//
// A Host is a network namespace -- one "machine" -- that ROS 2 processes and
// the observer share; Hosts on one Docker network are machines on one LAN.
// Everything runs in containers, so the Docker daemon may be elsewhere than
// the test.
//
//	go test -tags e2e ./e2e/ -v
//
// $DDS_E2E_IMAGE names the ROS 2 image (default dds-e2e-ros:jazzy, built from
// e2e/image if missing).
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func image() string {
	if v := os.Getenv("DDS_E2E_IMAGE"); v != "" {
		return v
	}
	return "dds-e2e-ros:jazzy"
}

func docker(t testing.TB, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func dockerQuiet(args ...string) { _ = exec.Command("docker", args...).Run() }

var (
	setupOnce sync.Once
	setupErr  error
	spyBin    string // ddsspy built for the daemon's architecture
)

// setup builds the image if it is missing and ddsspy for the daemon.
func setup(t testing.TB) {
	t.Helper()
	setupOnce.Do(func() {
		if exec.Command("docker", "image", "inspect", image()).Run() != nil {
			if out, err := exec.Command("docker", "build", "-t", image(), "image").CombinedOutput(); err != nil {
				setupErr = fmt.Errorf("build %s: %v\n%s", image(), err, out)
				return
			}
		}
		arch, err := exec.Command("docker", "info", "-f", "{{.Architecture}}").Output()
		if err != nil {
			setupErr = err
			return
		}
		goarch := map[string]string{"x86_64": "amd64", "aarch64": "arm64"}[strings.TrimSpace(string(arch))]
		if goarch == "" {
			goarch = runtime.GOARCH
		}
		dir, err := os.MkdirTemp("", "dds-e2e")
		if err != nil {
			setupErr = err
			return
		}
		spyBin = filepath.Join(dir, "ddsspy")
		build := exec.Command("go", "build", "-o", spyBin, "../cmd/ddsspy")
		build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+goarch)
		if out, err := build.CombinedOutput(); err != nil {
			setupErr = fmt.Errorf("build ddsspy: %v\n%s", err, out)
		}
	})
	if setupErr != nil {
		t.Fatal(setupErr)
	}
}

func randomName(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}

// randomDomain keeps concurrent runs on one daemon apart.
func randomDomain() uint32 {
	n, _ := rand.Int(rand.Reader, big.NewInt(200))
	return uint32(n.Int64()) + 10
}

// LAN is a Docker network: machines on it see each other's multicast.
type LAN struct {
	t      testing.TB
	name   string
	subnet string
}

func NewLAN(t testing.TB) *LAN {
	t.Helper()
	setup(t)
	l := &LAN{t: t, name: randomName("dds-e2e")}
	docker(t, "network", "create", l.name)
	t.Cleanup(func() { dockerQuiet("network", "rm", l.name) })
	l.subnet = docker(t, "network", "inspect", l.name, "-f", "{{(index .IPAM.Config 0).Subnet}}")
	return l
}

// Host is one machine: a network namespace kept by a container that sleeps.
type Host struct {
	t    testing.TB
	lan  *LAN
	name string
}

func (l *LAN) NewHost(t testing.TB) *Host {
	t.Helper()
	h := &Host{t: t, lan: l, name: randomName("host")}
	// NET_ADMIN for tc netem, NET_RAW for tcpdump: on the namespace, which
	// the host's processes share.
	docker(t, "run", "-d", "--name", h.name, "--network", l.name, "--cap-add", "NET_ADMIN", "--cap-add", "NET_RAW",
		"--entrypoint", "sleep", image(), "infinity")
	t.Cleanup(func() { dockerQuiet("rm", "-f", h.name) })
	docker(t, "cp", spyBin, h.name+":/ddsspy")
	return h
}

// Process is a ROS 2 process on a Host, in its own container.
type Process struct {
	t    testing.TB
	name string
}

// Run starts a ROS 2 executable on the host, in the domain, under tini so
// that a stop is a SIGTERM it gets (a graceful shutdown).
func (h *Host) Run(t testing.TB, domain uint32, exe string, args ...string) *Process {
	t.Helper()
	p := &Process{t: t, name: randomName("proc")}
	cmd := []string{"run", "-d", "--init", "--name", p.name, "--network", "container:" + h.name,
		"-e", fmt.Sprintf("ROS_DOMAIN_ID=%d", domain), image(), exe}
	docker(t, append(cmd, args...)...)
	t.Cleanup(func() { dockerQuiet("rm", "-f", p.name) })
	return p
}

// Stop shuts it down gracefully (SIGTERM).
func (p *Process) Stop() { docker(p.t, "stop", "-t", "10", p.name) }

// Kill kills it (SIGKILL): it gets no chance to say goodbye.
func (p *Process) Kill() { docker(p.t, "kill", "-s", "KILL", p.name) }

// Executables used by the tests.
const (
	Talker            = "/opt/ros/jazzy/lib/demo_nodes_cpp/talker"
	Listener          = "/opt/ros/jazzy/lib/demo_nodes_cpp/listener"
	ManualComposition = "/opt/ros/jazzy/lib/composition/manual_composition"
)

// SpyLine is one event ddsspy -json prints.
type SpyLine struct {
	Event       string    `json:"event"`
	Reason      string    `json:"reason"`
	At          time.Time `json:"at"`
	GUIDPrefix  string    `json:"guid_prefix"`
	Vendor      string    `json:"vendor"`
	Hostname    string    `json:"hostname"`
	PID         int       `json:"pid"`
	ProcessName string    `json:"process_name"`
	Lease       string    `json:"lease"`
	From        string    `json:"from"`
	Cyclone     string    `json:"cyclone"`

	GUID        string `json:"guid"`
	Kind        string `json:"kind"`
	Topic       string `json:"topic"`
	Type        string `json:"type"`
	Reliability string `json:"reliability"`
	Durability  string `json:"durability"`
	History     string `json:"history"`
	Depth       int32  `json:"depth"`

	Node    string `json:"node"`
	Readers int    `json:"readers"`
	Writers int    `json:"writers"`
}

// Spy is ddsspy running on a host.
type Spy struct {
	t      testing.TB
	cancel context.CancelFunc
	mu     sync.Mutex
	lines  []SpyLine
	stderr lockedBuffer
	done   chan struct{}
}

// lockedBuffer is written by exec's copying goroutine and read by the test.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Spy starts ddsspy on the host. local keeps only the host's participants.
func (h *Host) Spy(t testing.TB, domain uint32, local bool) *Spy {
	t.Helper()
	var flags []string
	if local {
		flags = append(flags, "-local")
	}
	return h.spy(t, domain, flags...)
}

// Participant starts ddsspy -sedp -ros on the host: it takes part, and
// reports endpoints and ROS nodes.
func (h *Host) Participant(t testing.TB, domain uint32) *Spy {
	t.Helper()
	return h.spy(t, domain, "-sedp", "-ros")
}

func (h *Host) spy(t testing.TB, domain uint32, flags ...string) *Spy {
	t.Helper()
	args := append([]string{"exec", h.name, "/ddsspy", "-json", "-domain", fmt.Sprint(domain), "-iface", h.lan.subnet}, flags...)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "docker", args...)
	s := &Spy{t: t, cancel: cancel, done: make(chan struct{})}
	cmd.Stderr = &s.stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(s.done)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			var l SpyLine
			if json.Unmarshal(sc.Bytes(), &l) == nil {
				s.mu.Lock()
				s.lines = append(s.lines, l)
				s.mu.Unlock()
			}
		}
		_ = cmd.Wait()
	}()
	t.Cleanup(func() {
		cancel()
		<-s.done
		if t.Failed() {
			t.Logf("ddsspy on %s:\n%s", h.name, s.stderr.String())
		}
	})
	// Listening before the test starts anything.
	s.WaitFor(10*time.Second, "ddsspy to listen", func([]SpyLine) bool {
		log := s.stderr.String()
		return strings.Contains(log, "listening on") || strings.Contains(log, "participant ")
	})
	return s
}

// Lines returns what it has printed so far.
func (s *Spy) Lines() []SpyLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SpyLine(nil), s.lines...)
}

// WaitFor polls until cond holds of the lines printed so far.
func (s *Spy) WaitFor(d time.Duration, what string, cond func([]SpyLine) bool) []SpyLine {
	s.t.Helper()
	deadline := time.Now().Add(d)
	for {
		lines := s.Lines()
		if cond(lines) {
			return lines
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out after %s waiting for %s; got %+v", d, what, lines)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Find returns the first line of the event for the process.
func Find(lines []SpyLine, event, process string) (SpyLine, bool) {
	for _, l := range lines {
		if l.Event == event && l.ProcessName == process {
			return l, true
		}
	}
	return SpyLine{}, false
}

// Stderr is what ddsspy has logged.
func (s *Spy) Stderr() string { return s.stderr.String() }

// Stop ends ddsspy gracefully and waits for it.
func (s *Spy) Stop(h *Host) {
	_ = exec.Command("docker", "exec", h.name, "pkill", "-INT", "-f", "/ddsspy").Run()
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
	}
}

// Python runs a Python program with rclpy on the host, as a ROS 2 process.
func (h *Host) Python(t testing.TB, domain uint32, program string) *Process {
	t.Helper()
	return h.Run(t, domain, "python3", "-u", "-c", program)
}

// CLI runs the ros2 command line on the host, as its own short-lived
// process, and returns what it prints.
func (h *Host) CLI(t testing.TB, domain uint32, args ...string) string {
	t.Helper()
	cmd := []string{"run", "--rm", "--network", "container:" + h.name,
		"-e", fmt.Sprintf("ROS_DOMAIN_ID=%d", domain), image(), "ros2"}
	return docker(t, append(cmd, args...)...)
}

// Capture runs tcpdump on the host until stop is called, then returns the
// packets it saw, one line each.
func (h *Host) Capture(t testing.TB, filter string) (stop func() []string) {
	t.Helper()
	docker(t, "exec", "-d", h.name, "sh", "-c", "tcpdump -i any -nn -l "+filter+" > /capture.txt 2>/capture.err")
	time.Sleep(time.Second) // tcpdump attaching
	return func() []string {
		_ = exec.Command("docker", "exec", h.name, "pkill", "-INT", "tcpdump").Run()
		time.Sleep(500 * time.Millisecond)
		out := docker(t, "exec", h.name, "cat", "/capture.txt")
		if out == "" {
			return nil
		}
		return strings.Split(out, "\n")
	}
}

// Addr is the host's address on its LAN.
func (h *Host) Addr(t testing.TB) string {
	t.Helper()
	return docker(t, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", h.name)
}

// FindTopic returns the lines of the event for a topic and endpoint kind.
func FindTopic(lines []SpyLine, event, topic, kind string) []SpyLine {
	var out []SpyLine
	for _, l := range lines {
		if l.Event == event && l.Topic == topic && l.Kind == kind {
			out = append(out, l)
		}
	}
	return out
}

// Nodes returns the nodes appeared and not disappeared, by full name.
func Nodes(lines []SpyLine) map[string]SpyLine {
	out := map[string]SpyLine{}
	for _, l := range lines {
		key := l.Node + " " + l.GUIDPrefix
		switch l.Event {
		case "node_appeared", "node_updated":
			out[key] = l
		case "node_disappeared":
			delete(out, key)
		}
	}
	names := map[string]SpyLine{}
	for _, l := range out {
		names[l.Node] = l
	}
	return names
}

// Degrade makes the host's loopback -- the path between its processes --
// lose, duplicate, reorder and delay packets (tc netem arguments).
func (h *Host) Degrade(t testing.TB, netem string) {
	t.Helper()
	docker(t, append([]string{"exec", h.name, "tc", "qdisc", "add", "dev", "lo", "root", "netem"}, strings.Fields(netem)...)...)
}

// RSS is a process's resident memory on the host, in KiB, by command name.
func (h *Host) RSS(t testing.TB, comm string) (rss, peak int) {
	t.Helper()
	out := docker(t, "exec", h.name, "sh", "-c", "grep -E '^(VmRSS|VmHWM):' /proc/$(pgrep -o -x "+comm+")/status")
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		if len(f) < 2 {
			continue
		}
		n, _ := strconv.Atoi(f[1])
		switch f[0] {
		case "VmRSS:":
			rss = n
		case "VmHWM:":
			peak = n
		}
	}
	return rss, peak
}
