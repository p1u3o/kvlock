package controller_test

import (
	"errors"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

func setLockErr(l *fakeLock, err error) {
	l.mu.Lock()
	l.getErr = err
	l.mu.Unlock()
}

// The leader's own forwarder refuses with a definitive state (no hold).
func TestForwardViewLeaderIsRefuse(t *testing.T) {
	cfg := forwardConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	waitFor(t, 2*time.Second, func() bool { return c.Snapshot().Role == elect.RoleLeader })
	if r := c.ForwardView(); r.Addr != "" || r.State != controller.RouteRefuse {
		t.Fatalf("leader forward view: %+v", r)
	}
}

func gapFollower(t *testing.T, ttl time.Duration) (*controller.Controller, *fakeLock, *fakeKV, func()) {
	cfg := forwardConfig("kv02")
	cfg.SessionTTL = ttl
	kv := &fakeKV{up: true, role: "slave", lag: 10}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, "sess-x", 3, time.Now()), sess: "sess-x"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	waitFor(t, 2*time.Second, func() bool { return c.ForwardView().State == controller.RouteOK })
	return c, lock, kv, stop
}

// A Consul gap longer than the confirm grace keeps the route while the
// leader is verified as master of the same epoch.
func TestForwardKeepsRouteThroughConsulGap(t *testing.T) {
	c, lock, _, stop := gapFollower(t, 10*time.Second)
	defer stop()
	k := c.ForwardView().Key
	setLockErr(lock, errors.New("consul timeout"))
	time.Sleep(2500 * time.Millisecond) // > 1s grace
	if r := c.ForwardView(); r.Addr != "10.0.0.1:23001" || r.Key != k {
		t.Fatalf("route dropped during gap: %+v", r)
	}
}

// If the leader can't be verified during the gap, the route is cut
// (transient: the forwarder may hold).
func TestForwardCutsWhenLeaderUnverifiedInGap(t *testing.T) {
	c, lock, kv, stop := gapFollower(t, 10*time.Second)
	defer stop()
	setLockErr(lock, errors.New("consul timeout"))
	kv.mu.Lock()
	kv.peerErr = errors.New("unreachable")
	kv.mu.Unlock()
	waitFor(t, 4*time.Second, func() bool { r := c.ForwardView(); return r.Addr == "" && r.State == controller.RouteTransient })
}

// The gap tolerance is bounded by the session TTL.
func TestForwardCutsAfterSessionTTL(t *testing.T) {
	c, lock, _, stop := gapFollower(t, 1500*time.Millisecond)
	defer stop()
	setLockErr(lock, errors.New("consul timeout"))
	waitFor(t, 4*time.Second, func() bool { return c.ForwardView().Addr == "" })
}
