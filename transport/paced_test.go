package transport

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/dds/rtps"
)

type recorder struct {
	mu   sync.Mutex
	now  *time.Time
	sent []time.Time
}

func (r *recorder) Unicast() []rtps.Locator   { return nil }
func (r *recorder) LocalAddrs() []netip.Addr  { return nil }
func (r *recorder) Networks() []netip.Prefix  { return nil }
func (r *recorder) Reaches(rtps.Locator) bool { return true }
func (r *recorder) Close() error              { return nil }
func (r *recorder) Send(rtps.Locator, []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, *r.now)
	return nil
}
func (r *recorder) Run(ctx context.Context, _ func(Packet)) error {
	<-ctx.Done()
	return nil
}

func TestPaced(t *testing.T) {
	now := time.Unix(0, 0)
	r := &recorder{now: &now}
	q := NewPaced(r, 1000, 500)
	q.now = func() time.Time { r.mu.Lock(); defer r.mu.Unlock(); return now }
	q.sleep = func(d time.Duration) { r.mu.Lock(); now = now.Add(d); r.mu.Unlock() }
	to := UDPv4(netip.MustParseAddrPort("127.0.0.1:1"))

	// Before Run, at once.
	_ = q.Send(to, make([]byte, 500))
	if len(r.sent) != 1 {
		t.Fatal("not sent at once before Run")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = q.Run(ctx, nil); close(done) }()
	for !q.running.Load() {
		time.Sleep(time.Millisecond)
	}
	for range 4 {
		_ = q.Send(to, make([]byte, 500))
	}
	deadline := time.Now().Add(time.Second)
	for {
		r.mu.Lock()
		n := len(r.sent)
		r.mu.Unlock()
		if n == 5 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	r.mu.Lock()
	// 2000 bytes at 1000 a second with a burst of 500: one at once, then
	// one every half second.
	if len(r.sent) != 5 {
		t.Fatalf("%d sent", len(r.sent))
	}
	if d := r.sent[4].Sub(r.sent[1]); d < 1400*time.Millisecond || d > 1600*time.Millisecond {
		t.Errorf("four sent over %s", d)
	}
	r.mu.Unlock()

	for range 5000 {
		_ = q.Send(to, []byte{1})
	}
	if q.Dropped() == 0 {
		t.Error("a full queue dropped nothing")
	}
	cancel()
	<-done
	// After Run, at once again.
	r.mu.Lock()
	n := len(r.sent)
	r.mu.Unlock()
	_ = q.Send(to, []byte{1})
	if len(r.sent) != n+1 {
		t.Error("not sent at once after Run")
	}
}
