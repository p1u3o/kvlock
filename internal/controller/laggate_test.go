package controller_test

import (
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

// A replica far behind refuses at first, then promotes once the old leader
// has been gone for --lag-gate-max-wait.
func TestLagGateMaxWaitPromotesEventually(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.HoldDown = time.Hour
	cfg.LagGateMaxWait = 300 * time.Millisecond
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 10, peerOffset: 1_000_000}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-kv01", 2, time.Now()), sess: "sess-kv01"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(80 * time.Millisecond)
	kv.mu.Lock()
	kv.peerErr = errString("connection refused")
	kv.mu.Unlock()
	lock.mu.Lock()
	lock.sess = ""
	lock.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("must refuse before max wait")
	}
	time.Sleep(400 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("must promote after max wait")
	}
}

// If the leader stays visible in Consul but its kvrocks stops answering,
// the last good sample ages; once older than --lag-sample-max-age when the
// leader disappears, a small-looking lag is treated as unknown.
func TestLagGateStaleSampleWaitsOutHoldDown(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.HoldDown = time.Hour
	cfg.LagSampleMaxAge = 100 * time.Millisecond
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 990, peerOffset: 1000}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-kv01", 2, time.Now()), sess: "sess-kv01"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(80 * time.Millisecond)
	kv.mu.Lock()
	kv.peerErr = errString("i/o timeout")
	kv.mu.Unlock()
	time.Sleep(300 * time.Millisecond) // leader still holds the lock; sample ages
	lock.mu.Lock()
	lock.sess = ""
	lock.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("stale sample must not count as within bound")
	}
}

// The gate re-samples the departed leader when its kvrocks still answers:
// a replica that looked close by an old sample but is now far behind the
// still-running old master refuses.
func TestLagGateResamplesReachableOldLeader(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.HoldDown = time.Hour
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 990, peerOffset: 1000}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-kv01", 2, time.Now()), sess: "sess-kv01"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(80 * time.Millisecond)
	// Following ticks can no longer sample, so the 1000 sample stays.
	kv.mu.Lock()
	kv.peerErr = errString("i/o timeout")
	kv.mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	// Leader's session goes away while its kvrocks answers again, ahead.
	kv.mu.Lock()
	kv.peerErr = nil
	kv.peerOffset = 1_000_000
	kv.mu.Unlock()
	lock.mu.Lock()
	lock.sess = ""
	lock.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatalf("must refuse after a fresh sample shows lag %d", c.Snapshot().LagBytes)
	}
}
