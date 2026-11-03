package procmap

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeProc writes a /proc with two processes, 100 holding socket 5001 on
// port 0x1d4c (7500) and 200 holding socket 5002 on 7501.
func fakeProc(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write := func(name, data string) {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("stat", "cpu  1 2 3\nbtime 1800000000\nprocesses 9\n")
	write("net/udp", strings.Join([]string{
		"  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops",
		"  1: 210AA8C0:1D4C 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 5001 2 0 0",
		"  2: 210AA8C0:1D4D 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 5002 2 0 0",
		"",
	}, "\n"))
	for pid, inode := range map[string]string{"100": "5001", "200": "5002"} {
		dir := filepath.Join(root, pid, "fd")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("socket:["+inode+"]", filepath.Join(dir, "7")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("/dev/null", filepath.Join(dir, "0")); err != nil {
			t.Fatal(err)
		}
	}
	// starttime 500 ticks = 5 s after boot; comm with a space and a paren.
	write("100/stat", "100 (planner (cpp) node) S 1 100 100 0 -1 4194560 1 0 0 0 0 0 0 0 20 0 1 0 500 1000 10\n")
	write("100/cgroup", "0::/system.slice/docker-abc.scope\n")
	write("200/stat", "200 (talker) S 1 200 200 0 -1 4194560 1 0 0 0 0 0 0 0 20 0 1 0 900 1000 10\n")
	return root
}

func TestByPorts(t *testing.T) {
	r, err := New(fakeProc(t))
	if err != nil {
		t.Fatal(err)
	}
	p, ok := r.ByPorts(9999, 7500)
	if !ok {
		t.Fatal("not found")
	}
	if p.PID != 100 || p.Comm != "planner (cpp) node" || p.Cgroup != "0::/system.slice/docker-abc.scope\n" {
		t.Errorf("%+v", p)
	}
	if want := time.Unix(1_800_000_005, 0); !p.Start.Equal(want) {
		t.Errorf("start %s, want %s", p.Start, want)
	}
	if p, ok := r.ByPorts(7501); !ok || p.PID != 200 {
		t.Errorf("7501: %+v %v", p, ok)
	}
	if _, ok := r.ByPorts(7502); ok {
		t.Error("an unbound port resolved")
	}
}

// TestThisProcessOwnsItsSocket runs against the real /proc: a socket this
// test opens resolves to this test.
func TestThisProcessOwnsItsSocket(t *testing.T) {
	if _, err := os.Stat("/proc/net/udp"); err != nil {
		t.Skip("no /proc")
	}
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := New("/proc")
	if err != nil {
		t.Fatal(err)
	}
	p, ok := r.ByPorts(uint16(c.LocalAddr().(*net.UDPAddr).Port))
	if !ok || p.PID != os.Getpid() {
		t.Errorf("resolved %+v %v, want pid %d", p, ok, os.Getpid())
	}
	if time.Since(p.Start) > time.Hour || time.Since(p.Start) < 0 {
		t.Errorf("start %s", p.Start)
	}
}
