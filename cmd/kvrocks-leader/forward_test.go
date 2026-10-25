package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/logx"
	"github.com/p1u3o/kvlock/internal/metrics"
)

// backend is a fake kvrocks leader: it answers each line with its name.
func backend(t *testing.T, name string) string {
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
					fmt.Fprintf(c, "%s %s", name, l)
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// leaderView mimics Controller.ForwardView: addr|session|epoch keys.
type leaderView struct {
	mu                 sync.Mutex
	addr, sess, reason string
	epoch              int
}

func (v *leaderView) set(addr, sess string, epoch int) {
	v.mu.Lock()
	v.addr, v.sess, v.epoch = addr, sess, epoch
	v.mu.Unlock()
}
func (v *leaderView) ForwardView() (string, string, string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.addr == "" {
		return "", "", "no confirmed leader"
	}
	return v.addr, fmt.Sprintf("%s|%s|%d", v.addr, v.sess, v.epoch), ""
}

func ask(t *testing.T, c net.Conn, r *bufio.Reader) string {
	t.Helper()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(c, "ping\n"); err != nil {
		t.Fatal(err)
	}
	l, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadAll(c); err != nil {
		t.Fatalf("expected the forwarded connection to be cut, got %v", err)
	}
}

// End to end through the real forward listener: a long-lived replication
// link to leader A is cut on the epoch change, a connection opened while
// no leader is known is held and then reaches B, and so does any new one.
func TestForwarderEpochChangeCutsAndReroutes(t *testing.T) {
	a, b := backend(t, "A"), backend(t, "B")
	v := &leaderView{}
	fwdAddr := freePort(t)
	v.set(a, "sess-a", 7)
	notify := make(chan struct{}, 1)
	kick := func() {
		select {
		case notify <- struct{}{}:
		default:
		}
	}
	p, err := startProxyHold("forward", fwdAddr, forwardRoute(v), notify, 3*time.Second, metrics.New(), logx.New("kv02", "active"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	addr := fwdAddr

	link, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	lr := bufio.NewReader(link)
	if got := ask(t, link, lr); got != "A ping\n" {
		t.Fatalf("before failover got %q", got)
	}

	// Leader A is lost: route withdrawn, link must be cut at once.
	v.set("", "", 7)
	kick()
	expectClosed(t, link)

	// A reconnect during the gap is held, not refused...
	held, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	time.Sleep(200 * time.Millisecond)
	// ...and reaches B once B wins epoch 8.
	v.set(b, "sess-b", 8)
	kick()
	if got := ask(t, held, bufio.NewReader(held)); got != "B ping\n" {
		t.Fatalf("held connection got %q", got)
	}
	fresh, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if got := ask(t, fresh, bufio.NewReader(fresh)); got != "B ping\n" {
		t.Fatalf("new connection got %q", got)
	}

	// Same address, new epoch (B re-elected): still cut.
	v.set(b, "sess-b2", 9)
	kick()
	expectClosed(t, fresh)
	expectClosed(t, held)

	deadline := time.Now().Add(2 * time.Second)
	for {
		st := p.Stats()
		if st.Resets == 2 && st.Dropped == 3 && st.Total == 3 && st.Active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Direct switch A -> B without a gap also cuts existing links.
func TestForwarderDirectSwitch(t *testing.T) {
	a, b := backend(t, "A"), backend(t, "B")
	v := &leaderView{}
	fwdAddr := freePort(t)
	v.set(a, "s1", 1)
	notify := make(chan struct{}, 1)
	p, err := startProxyHold("forward", fwdAddr, forwardRoute(v), notify, time.Second, metrics.New(), logx.New("kv02", "active"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c, _ := net.Dial("tcp", fwdAddr)
	defer c.Close()
	if got := ask(t, c, bufio.NewReader(c)); got != "A ping\n" {
		t.Fatalf("got %q", got)
	}
	v.set(b, "s2", 2)
	notify <- struct{}{}
	expectClosed(t, c)
	c2, _ := net.Dial("tcp", fwdAddr)
	defer c2.Close()
	if got := ask(t, c2, bufio.NewReader(c2)); got != "B ping\n" {
		t.Fatalf("got %q", got)
	}
}

// fakePeers mimics the controller's peer table for three instances.
type fakePeers struct {
	mu     sync.Mutex
	routes map[int]string
}

func (f *fakePeers) RefreshPeers(context.Context) error { return nil }
func (f *fakePeers) PeerIndexes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []int
	for i := range f.routes {
		out = append(out, i)
	}
	return out
}
func (f *fakePeers) PeerRoute(i int) (string, string, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.routes[i]
	if a == "" {
		return "", "", "gone"
	}
	return a, a, ""
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// Three instances, each reachable at its own stable address; a replaced
// alloc (same index, new address) is re-routed on the same address.
func TestPeerListenersThreeInstances(t *testing.T) {
	be := []string{backend(t, "N0"), backend(t, "N1"), backend(t, "N2")}
	addrs := []string{freePort(t), freePort(t), freePort(t)}
	src := &fakePeers{routes: map[int]string{0: be[0], 1: be[1], 2: be[2]}}
	pl := newPeerListeners(src, func(i int) (string, error) { return addrs[i], nil }, metrics.New(), logx.New("kv01", "active"))
	defer pl.Close()
	pl.sync(context.Background())
	for i, want := range []string{"N0", "N1", "N2"} {
		c, err := net.Dial("tcp", addrs[i])
		if err != nil {
			t.Fatal(err)
		}
		if got := ask(t, c, bufio.NewReader(c)); got != want+" ping\n" {
			t.Fatalf("index %d got %q", i, got)
		}
		c.Close()
	}
	// Alloc 2 replaced by a new instance elsewhere.
	nb := backend(t, "N2b")
	src.mu.Lock()
	src.routes[2] = nb
	src.mu.Unlock()
	pl.sync(context.Background())
	c, _ := net.Dial("tcp", addrs[2])
	defer c.Close()
	if got := ask(t, c, bufio.NewReader(c)); got != "N2b ping\n" {
		t.Fatalf("replaced alloc got %q", got)
	}
}

func (v *leaderView) Subscribe() <-chan struct{} { return make(chan struct{}) }
