package proxy

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func startHold(t *testing.T, r *router, notify chan struct{}, hold time.Duration) (*Proxy, string) {
	t.Helper()
	p := &Proxy{Name: "forward", Route: r.route, Notify: notify, Poll: time.Hour, Hold: hold}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go p.Serve(ln)
	t.Cleanup(func() { p.Close() })
	return p, ln.Addr().String()
}

// A connection that arrives with no route waits and is forwarded once a
// route appears within Hold.
func TestHoldThenRoute(t *testing.T) {
	b := echo(t, "B")
	r := &router{}
	notify := make(chan struct{}, 1)
	_, addr := startHold(t, r, notify, 2*time.Second)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	time.Sleep(100 * time.Millisecond)
	r.set(b, "B|5")
	notify <- struct{}{}
	if got := roundTrip(t, c, bufio.NewReader(c)); got != "B:hi\n" {
		t.Fatalf("held connection got %q", got)
	}
}

// With no route for longer than Hold, the connection is closed.
func TestHoldExpiresAndRefuses(t *testing.T) {
	r := &router{}
	p, addr := startHold(t, r, nil, 150*time.Millisecond)
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	start := time.Now()
	waitClosed(t, c)
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Fatalf("closed before hold expired: %v", d)
	}
	eventually(t, func() bool { return p.Stats().Refused == 1 })
}

// Stats track target, active/total, bytes and resets across a switch.
func TestStatsAcrossSwitch(t *testing.T) {
	a, b := echo(t, "A"), echo(t, "B")
	r := &router{addr: a, key: "A|1"}
	notify := make(chan struct{}, 1)
	p, addr := startHold(t, r, notify, time.Second)
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	roundTrip(t, c, bufio.NewReader(c))
	eventually(t, func() bool { s := p.Stats(); return s.Active == 1 && s.BytesUp == 3 && s.BytesDown == 5 })
	if s := p.Stats(); s.Target != a || s.Total != 1 {
		t.Fatalf("stats: %+v", s)
	}
	r.set(b, "B|2")
	notify <- struct{}{}
	waitClosed(t, c)
	eventually(t, func() bool { s := p.Stats(); return s.Resets == 1 && s.Active == 0 && s.Dropped == 1 && s.Target == b })
}

// A definitive refusal closes the client immediately instead of holding.
func TestRefuseSkipsHold(t *testing.T) {
	r := &router{refuse: true}
	p, addr := startHold(t, r, nil, 3*time.Second)
	c, _ := net.Dial("tcp", addr)
	defer c.Close()
	start := time.Now()
	waitClosed(t, c)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("refused connection was held for %v", d)
	}
	eventually(t, func() bool { return p.Stats().Refused == 1 })
}
