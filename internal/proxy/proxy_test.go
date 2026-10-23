package proxy

import (
	"bufio"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// echo starts a server that echoes lines prefixed with its tag.
func echo(t *testing.T, tag string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					l, err := r.ReadString('\n')
					if err != nil {
						return
					}
					c.Write([]byte(tag + ":" + l))
				}
			}()
		}
	}()
	return ln.Addr().String()
}

type router struct {
	mu        sync.Mutex
	addr, key string
}

func (r *router) set(a, k string) { r.mu.Lock(); r.addr, r.key = a, k; r.mu.Unlock() }
func (r *router) route() (string, string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addr, r.key, "test"
}

func startProxy(t *testing.T, r *router, notify chan struct{}) (*Proxy, string, *int64, *int64, *int64) {
	t.Helper()
	var active, dropped, refused int64
	p := &Proxy{Name: "write", Route: r.route, Notify: notify, Poll: time.Hour, Counters: Counters{
		Active:  func(d float64) { atomic.AddInt64(&active, int64(d)) },
		Dropped: func(n int) { atomic.AddInt64(&dropped, int64(n)) },
		Refused: func() { atomic.AddInt64(&refused, 1) },
	}}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go p.Serve(ln)
	t.Cleanup(func() { p.Close() })
	return p, ln.Addr().String(), &active, &dropped, &refused
}

func roundTrip(t *testing.T, c net.Conn, r *bufio.Reader) string {
	t.Helper()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write([]byte("hi\n")); err != nil {
		t.Fatal(err)
	}
	l, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func waitClosed(t *testing.T, c net.Conn) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadAll(c); err != nil {
		t.Fatalf("connection should be closed, got %v", err)
	}
}

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func TestProxiesAndDropsOnRouteChange(t *testing.T) {
	a, b := echo(t, "A"), echo(t, "B")
	r := &router{addr: a, key: "A|1"}
	notify := make(chan struct{}, 1)
	_, addr, active, dropped, _ := startProxy(t, r, notify)

	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	br := bufio.NewReader(c)
	if got := roundTrip(t, c, br); got != "A:hi\n" {
		t.Fatalf("got %q", got)
	}
	eventually(t, func() bool { return atomic.LoadInt64(active) == 1 })

	r.set(b, "B|2")
	notify <- struct{}{}
	waitClosed(t, c)
	eventually(t, func() bool { return atomic.LoadInt64(dropped) == 1 && atomic.LoadInt64(active) == 0 })

	c2, _ := net.Dial("tcp", addr)
	defer c2.Close()
	if got := roundTrip(t, c2, bufio.NewReader(c2)); got != "B:hi\n" {
		t.Fatalf("after failover got %q", got)
	}
}

func TestSameAddressNewKeyStillDrops(t *testing.T) {
	a := echo(t, "A")
	r := &router{addr: a, key: "A|gen1"}
	notify := make(chan struct{}, 1)
	_, addr, _, dropped, _ := startProxy(t, r, notify)
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	roundTrip(t, c, bufio.NewReader(c))
	r.set(a, "A|gen2") // e.g. local fence + re-promotion
	notify <- struct{}{}
	waitClosed(t, c)
	eventually(t, func() bool { return atomic.LoadInt64(dropped) == 1 })
}

func TestRefusesWithoutRouteAndDropsWhenRouteLost(t *testing.T) {
	a := echo(t, "A")
	r := &router{}
	notify := make(chan struct{}, 1)
	_, addr, _, dropped, refused := startProxy(t, r, notify)

	c, _ := net.Dial("tcp", addr)
	waitClosed(t, c)
	c.Close()
	eventually(t, func() bool { return atomic.LoadInt64(refused) == 1 })

	r.set(a, "A")
	c2, _ := net.Dial("tcp", addr) // new connections re-check the route
	defer c2.Close()
	roundTrip(t, c2, bufio.NewReader(c2))

	r.set("", "") // e.g. Consul unconfirmed
	notify <- struct{}{}
	waitClosed(t, c2)
	eventually(t, func() bool { return atomic.LoadInt64(dropped) == 1 })
}

func TestRefusesWhenDialFails(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()
	r := &router{addr: dead, key: "x"}
	_, addr, _, _, refused := startProxy(t, r, nil)
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	waitClosed(t, c)
	eventually(t, func() bool { return atomic.LoadInt64(refused) == 1 })
}

func TestPollingPicksUpChangesWithoutNotify(t *testing.T) {
	a, b := echo(t, "A"), echo(t, "B")
	r := &router{addr: a, key: "A"}
	p := &Proxy{Name: "read", Route: r.route, Poll: 10 * time.Millisecond}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go p.Serve(ln)
	defer p.Close()
	c, _ := net.Dial("tcp", ln.Addr().String())
	defer c.Close()
	roundTrip(t, c, bufio.NewReader(c))
	r.set(b, "B")
	waitClosed(t, c)
}

func TestCloseDropsEverything(t *testing.T) {
	a := echo(t, "A")
	r := &router{addr: a, key: "A"}
	p, addr, _, _, _ := startProxy(t, r, nil)
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	roundTrip(t, c, bufio.NewReader(c))
	p.Close()
	waitClosed(t, c)
}
