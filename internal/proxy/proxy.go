// Package proxy is a plain TCP passthrough to whichever instance the
// controller currently routes to. It never parses the stream, so TLS and
// AUTH go straight through to kvrocks. It fails closed: with no route it
// refuses new connections, and whenever the route changes it closes every
// proxied connection so clients reconnect to the new target.
package proxy

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Route returns the address to dial and a key for the routing decision.
// addr == "" means refuse. Connections are closed when key changes.
type Route func() (addr, key, reason string)

// Counters is the metrics surface (all optional).
type Counters struct {
	Active  func(delta float64)
	Dropped func(n int)
	Refused func()
	// Reset is called when an established route is replaced or withdrawn
	// (leader/epoch change).
	Reset func()
	// Accepted counts connections that reached an upstream.
	Accepted func()
	// Bytes counts forwarded bytes; dir is "up" (client to upstream) or
	// "down". Called per write, so long-lived streams are visible.
	Bytes func(dir string, n int)
}

// Stats is a point-in-time view of a proxy, for status pages.
type Stats struct {
	Target    string `json:"target"`
	Reason    string `json:"reason,omitempty"`
	Active    int64  `json:"active"`
	Total     uint64 `json:"total"`
	BytesUp   uint64 `json:"bytes_up"`
	BytesDown uint64 `json:"bytes_down"`
	Resets    uint64 `json:"resets"`
	Dropped   uint64 `json:"dropped"`
	Refused   uint64 `json:"refused"`
}

// Proxy forwards connections accepted on one listener.
type Proxy struct {
	Name        string
	Route       Route
	Notify      <-chan struct{} // optional: re-evaluate the route now
	Poll        time.Duration   // re-evaluation interval (default 200ms)
	DialTimeout time.Duration   // default 2s
	// Hold, when > 0, keeps a new connection waiting up to this long for
	// a route instead of refusing it at once (e.g. during a failover).
	Hold     time.Duration
	Counters Counters
	Logf     func(msg string, fields map[string]any)

	mu     sync.Mutex
	key    string
	conns  map[*pair]struct{}
	ln     net.Listener
	closed bool
	done   chan struct{}
	once   sync.Once

	target, reason                            string
	active                                    atomic.Int64
	total, up, down, resets, dropped, refused atomic.Uint64
}

// Stats returns counters and the current target.
func (p *Proxy) Stats() Stats {
	p.mu.Lock()
	t, r := p.target, p.reason
	p.mu.Unlock()
	return Stats{Target: t, Reason: r, Active: p.active.Load(), Total: p.total.Load(), BytesUp: p.up.Load(), BytesDown: p.down.Load(),
		Resets: p.resets.Load(), Dropped: p.dropped.Load(), Refused: p.refused.Load()}
}

type pair struct {
	client, upstream net.Conn
	once             sync.Once
}

func (p *pair) close() {
	p.once.Do(func() {
		p.client.Close()
		if p.upstream != nil {
			p.upstream.Close()
		}
	})
}

func (p *Proxy) init() {
	p.once.Do(func() {
		p.conns = map[*pair]struct{}{}
		p.done = make(chan struct{})
		if p.Poll <= 0 {
			p.Poll = 200 * time.Millisecond
		}
		if p.DialTimeout <= 0 {
			p.DialTimeout = 2 * time.Second
		}
		if p.Logf == nil {
			p.Logf = func(string, map[string]any) {}
		}
		c := &p.Counters
		if c.Active == nil {
			c.Active = func(float64) {}
		}
		if c.Dropped == nil {
			c.Dropped = func(int) {}
		}
		if c.Refused == nil {
			c.Refused = func() {}
		}
		if c.Reset == nil {
			c.Reset = func() {}
		}
		if c.Accepted == nil {
			c.Accepted = func() {}
		}
		if c.Bytes == nil {
			c.Bytes = func(string, int) {}
		}
	})
}

// Serve accepts until Close.
func (p *Proxy) Serve(ln net.Listener) error {
	p.init()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return net.ErrClosed
	}
	p.ln = ln
	p.mu.Unlock()
	p.sync()
	go p.watch()
	for {
		c, err := ln.Accept()
		if err != nil {
			p.mu.Lock()
			closed := p.closed
			p.mu.Unlock()
			if closed {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		go p.handle(c)
	}
}

// Close stops accepting and closes all proxied connections.
func (p *Proxy) Close() error {
	p.init()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.done)
	ln := p.ln
	victims := p.takeAllLocked()
	p.mu.Unlock()
	for _, v := range victims {
		v.close()
	}
	if ln != nil {
		return ln.Close()
	}
	return nil
}

func (p *Proxy) takeAllLocked() []*pair {
	out := make([]*pair, 0, len(p.conns))
	for c := range p.conns {
		out = append(out, c)
	}
	p.conns = map[*pair]struct{}{}
	return out
}

func (p *Proxy) watch() {
	t := time.NewTicker(p.Poll)
	defer t.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
		case <-p.Notify: // nil channel blocks forever: fine
		}
		p.sync()
	}
}

// sync re-evaluates the route and drops connections if it changed.
func (p *Proxy) sync() (addr, key, reason string) {
	addr, key, reason = p.Route()
	if addr == "" {
		key = ""
	}
	p.mu.Lock()
	p.target, p.reason = addr, reason
	if key == p.key {
		p.mu.Unlock()
		return
	}
	old := p.key
	p.key = key
	victims := p.takeAllLocked()
	p.mu.Unlock()
	for _, v := range victims {
		v.close()
	}
	if old != "" {
		p.resets.Add(1)
		p.Counters.Reset()
	}
	if len(victims) > 0 {
		p.dropped.Add(uint64(len(victims)))
		p.Counters.Dropped(len(victims))
	}
	p.Logf("proxy route changed", map[string]any{"listener": p.Name, "target": addr, "reason": reason, "dropped": len(victims), "had_route": old != ""})
	return
}

func (p *Proxy) refuse(c net.Conn, why string) {
	p.refused.Add(1)
	p.Counters.Refused()
	p.Logf("proxy refused connection", map[string]any{"listener": p.Name, "client": c.RemoteAddr().String(), "reason": why})
	c.Close()
}

func (p *Proxy) handle(c net.Conn) {
	addr, key, reason := p.sync()
	if addr == "" && p.Hold > 0 {
		// Hold the client briefly: a failover usually resolves in well
		// under Hold. Poll instead of subscribing; the cost is tiny.
		deadline := time.Now().Add(p.Hold)
		for addr == "" && time.Now().Before(deadline) {
			select {
			case <-p.done:
				c.Close()
				return
			case <-time.After(25 * time.Millisecond):
			}
			addr, key, reason = p.sync()
		}
	}
	if addr == "" {
		p.refuse(c, reason)
		return
	}
	up, err := net.DialTimeout("tcp", addr, p.DialTimeout)
	if err != nil {
		p.refuse(c, "dial "+addr+": "+err.Error())
		return
	}
	pr := &pair{client: c, upstream: up}
	p.mu.Lock()
	if p.closed || p.key != key {
		p.mu.Unlock()
		pr.close()
		p.refused.Add(1)
		p.Counters.Refused()
		return
	}
	p.conns[pr] = struct{}{}
	p.mu.Unlock()
	p.total.Add(1)
	p.Counters.Accepted()
	p.active.Add(1)
	p.Counters.Active(1)
	defer func() { p.active.Add(-1); p.Counters.Active(-1) }()

	var wg sync.WaitGroup
	wg.Add(2)
	cp := func(dst, src net.Conn, dir string, ctr *atomic.Uint64) {
		defer wg.Done()
		_, _ = io.Copy(&countWriter{w: dst, f: func(n int) { ctr.Add(uint64(n)); p.Counters.Bytes(dir, n) }}, src)
		pr.close()
	}
	go cp(up, c, "up", &p.up)
	go cp(c, up, "down", &p.down)
	wg.Wait()
	p.mu.Lock()
	delete(p.conns, pr)
	p.mu.Unlock()
}

type countWriter struct {
	w io.Writer
	f func(int)
}

func (c *countWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	if n > 0 {
		c.f(n)
	}
	return n, err
}
