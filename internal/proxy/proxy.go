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
}

// Proxy forwards connections accepted on one listener.
type Proxy struct {
	Name        string
	Route       Route
	Notify      <-chan struct{} // optional: re-evaluate the route now
	Poll        time.Duration   // re-evaluation interval (default 200ms)
	DialTimeout time.Duration   // default 2s
	Counters    Counters
	Logf        func(msg string, fields map[string]any)

	mu     sync.Mutex
	key    string
	conns  map[*pair]struct{}
	ln     net.Listener
	closed bool
	done   chan struct{}
	once   sync.Once
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
	if len(victims) > 0 {
		p.Counters.Dropped(len(victims))
	}
	p.Logf("proxy route changed", map[string]any{"listener": p.Name, "target": addr, "reason": reason, "dropped": len(victims), "had_route": old != ""})
	return
}

func (p *Proxy) refuse(c net.Conn, why string) {
	p.Counters.Refused()
	p.Logf("proxy refused connection", map[string]any{"listener": p.Name, "client": c.RemoteAddr().String(), "reason": why})
	c.Close()
}

func (p *Proxy) handle(c net.Conn) {
	addr, key, reason := p.sync()
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
		p.Counters.Refused()
		return
	}
	p.conns[pr] = struct{}{}
	p.mu.Unlock()
	p.Counters.Active(1)
	defer p.Counters.Active(-1)

	var wg sync.WaitGroup
	wg.Add(2)
	cp := func(dst, src net.Conn) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		pr.close()
	}
	go cp(up, c)
	go cp(c, up)
	wg.Wait()
	p.mu.Lock()
	delete(p.conns, pr)
	p.mu.Unlock()
}
