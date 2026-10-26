package controller_test

import (
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

func setDelay(kv *fakeKV, d time.Duration) {
	kv.mu.Lock()
	kv.delay = d
	kv.mu.Unlock()
}

func setLeaderVal(l *fakeLock, sess string, epoch uint64) {
	l.mu.Lock()
	l.val = elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, sess, epoch, time.Now())
	l.sess = sess
	l.mu.Unlock()
}

// A replica catching up answers local kvrocks calls slowly, so a tick
// takes longer than the session TTL before it reaches the leader key.
// Nothing changed in Consul, so the route must stay up.
func TestForwardKeepsRouteWhileCatchUpDelaysTick(t *testing.T) {
	c, _, kv, stop := gapFollower(t, 600*time.Millisecond)
	defer stop()
	k := c.ForwardView().Key
	setDelay(kv, 900*time.Millisecond)
	end := time.Now().Add(3 * time.Second)
	for time.Now().Before(end) {
		if r := c.ForwardView(); r.State != controller.RouteOK || r.Key != k {
			t.Fatalf("route dropped while tick was delayed: %+v", r)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// An epoch change seen in Consul still cuts the route at once, even while
// tick is stuck behind slow kvrocks calls.
func TestForwardCutsOnEpochChangeDuringCatchUp(t *testing.T) {
	c, lock, kv, stop := gapFollower(t, 600*time.Millisecond)
	defer stop()
	k := c.ForwardView().Key
	setDelay(kv, 5*time.Second)
	time.Sleep(100 * time.Millisecond)
	setLeaderVal(lock, "sess-x", 4)
	waitFor(t, 300*time.Millisecond, func() bool { r := c.ForwardView(); return r.Key != k || r.State != controller.RouteOK })
}

// A new holder session cuts the route, and the new leader is not routed
// to until it is verified as master of its epoch.
func TestForwardCutsOnNewHolderDuringCatchUp(t *testing.T) {
	c, lock, kv, stop := gapFollower(t, 600*time.Millisecond)
	defer stop()
	k := c.ForwardView().Key
	setDelay(kv, 5*time.Second)
	time.Sleep(100 * time.Millisecond)
	setLeaderVal(lock, "sess-y", 4)
	waitFor(t, 300*time.Millisecond, func() bool { r := c.ForwardView(); return r.Key != k || r.State != controller.RouteOK })
}

// A lost lock (no holder) cuts the route during catch-up.
func TestForwardCutsOnLockLossDuringCatchUp(t *testing.T) {
	c, lock, kv, stop := gapFollower(t, 600*time.Millisecond)
	defer stop()
	setDelay(kv, 5*time.Second)
	time.Sleep(100 * time.Millisecond)
	lock.mu.Lock()
	lock.sess = ""
	lock.mu.Unlock()
	waitFor(t, 300*time.Millisecond, func() bool { return c.ForwardView().Addr == "" })
}
