package transport

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesomnus/dds/rtps"
)

// Paced is a transport that sends at most rate bytes a second, in bursts of
// up to burst. What is sent waits in a queue; what finds it full is dropped
// and counted, for the reliable protocol to send again. Until Run starts and
// after it returns, Send sends at once.
type Paced struct {
	Transport
	rate, burst float64
	queue       chan paced
	running     atomic.Bool
	dropped     atomic.Int64

	// now and sleep are the clock; tests replace them.
	now   func() time.Time
	sleep func(time.Duration)
}

type paced struct {
	to  rtps.Locator
	msg []byte
}

// NewPaced paces t.
func NewPaced(t Transport, rate, burst int) *Paced {
	return &Paced{
		Transport: t,
		rate:      float64(rate), burst: float64(max(burst, 1)),
		queue: make(chan paced, 4096),
		now:   time.Now, sleep: time.Sleep,
	}
}

// Dropped is how many messages the queue had no room for.
func (q *Paced) Dropped() int64 { return q.dropped.Load() }

// Send queues a message.
func (q *Paced) Send(to rtps.Locator, msg []byte) error {
	if !q.running.Load() {
		return q.Transport.Send(to, msg)
	}
	select {
	case q.queue <- paced{to, msg}:
	default:
		q.dropped.Add(1)
	}
	return nil
}

// Run receives, and sends the queue at the rate.
func (q *Paced) Run(ctx context.Context, deliver func(Packet)) error {
	q.running.Store(true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		q.drain(ctx)
	}()
	err := q.Transport.Run(ctx, deliver)
	wg.Wait()
	q.running.Store(false)
	// What is still queued goes now.
	for {
		select {
		case m := <-q.queue:
			_ = q.Transport.Send(m.to, m.msg)
		default:
			return err
		}
	}
}

func (q *Paced) drain(ctx context.Context) {
	tokens, last := q.burst, q.now()
	for {
		var m paced
		select {
		case <-ctx.Done():
			return
		case m = <-q.queue:
		}
		need := float64(len(m.msg))
		for {
			now := q.now()
			tokens = min(q.burst, tokens+now.Sub(last).Seconds()*q.rate)
			last = now
			if tokens >= need || tokens >= q.burst {
				break
			}
			q.sleep(time.Duration((min(need, q.burst) - tokens) / q.rate * float64(time.Second)))
		}
		tokens -= need
		_ = q.Transport.Send(m.to, m.msg)
	}
}
