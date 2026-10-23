package controller_test

import (
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

// Observe mode runs next to production instances, so it must never change
// them: no REPLICAOF/SLAVEOF (Promote and ReplicaOf), no CLIENT KILL, no
// CONFIG SET (read-only flag, use-rsid-psync) and no service registrations.
func assertNoSideEffects(t *testing.T, kv *fakeKV, reg *fakeRegistrar) {
	t.Helper()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if kv.promotions != 0 || kv.follows != 0 {
		t.Errorf("observe sent REPLICAOF: promotions=%d follows=%d", kv.promotions, kv.follows)
	}
	if kv.hbSets != 0 {
		t.Errorf("observe wrote the heartbeat key %d times", kv.hbSets)
	}
	if kv.kills != 0 {
		t.Errorf("observe killed clients: %d", kv.kills)
	}
	if kv.setRO != 0 || kv.rsidCalls != 0 {
		t.Errorf("observe ran CONFIG SET: readonly=%d rsid=%d", kv.setRO, kv.rsidCalls)
	}
	if reg.master+reg.replica+reg.passed+reg.failed+reg.dereg != 0 {
		t.Errorf("observe touched service registrations: master=%d replica=%d pass=%d fail=%d dereg=%d", reg.master, reg.replica, reg.passed, reg.failed, reg.dereg)
	}
}

func TestObserveLeaderHasNoSideEffects(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.Mode = "observe"
	cfg.ShutdownHandover = time.Second
	kv := &fakeKV{up: true, role: "master", local: 500}
	reg := &fakeRegistrar{}
	c, _, _, _ := runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: reg, KV: kv}, 200*time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatalf("expected observe leader, got %v", c.Snapshot().Role)
	}
	assertNoSideEffects(t, kv, reg)
}

func TestObserveFollowerWithWrongMasterHasNoSideEffects(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.Mode = "observe"
	// Writable local instance while another node holds the lock: active
	// mode would fence and repoint it; observe must only log.
	kv := &fakeKV{up: true, role: "master", local: 10}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.9", "", 6666, "sess-x", 3, time.Now()), sess: "sess-x"}
	reg := &fakeRegistrar{}
	c, _, _, _ := runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: reg, KV: kv}, 200*time.Millisecond)
	if c.Snapshot().Role != elect.RoleFollower {
		t.Fatalf("expected follower, got %v", c.Snapshot().Role)
	}
	assertNoSideEffects(t, kv, reg)
}

func TestObserveReplicaOfOtherHostHasNoSideEffects(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.Mode = "observe"
	kv := &fakeKV{up: true, role: "slave", masterHost: "elsewhere", masterPort: 1, lag: 5}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.9", "", 6666, "sess-x", 3, time.Now()), sess: "sess-x"}
	reg := &fakeRegistrar{}
	runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: reg, KV: kv}, 200*time.Millisecond)
	assertNoSideEffects(t, kv, reg)
}

// A linked replica must not take a never-held lock: the master's node
// should win, otherwise observe reports (and active would do) a promotion
// of the replica.
func TestLinkedReplicaDoesNotBootstrapLeadership(t *testing.T) {
	for _, mode := range []string{"observe", "active"} {
		cfg := testConfig("kv02")
		cfg.Mode = mode
		kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.9", masterPort: 6666}
		lock := &fakeLock{}
		c, _, _, _ := runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}, 200*time.Millisecond)
		if c.Snapshot().Role == elect.RoleLeader {
			t.Fatalf("%s: linked replica took the lock", mode)
		}
		lock.mu.Lock()
		held := lock.sess != ""
		lock.mu.Unlock()
		if held {
			t.Fatalf("%s: lock held", mode)
		}
	}
}
