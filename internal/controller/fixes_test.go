package controller_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/example/kvrocks-leader/internal/consulstore"
	"github.com/example/kvrocks-leader/internal/controller"
	"github.com/example/kvrocks-leader/internal/elect"
	"github.com/example/kvrocks-leader/internal/logx"
	"github.com/example/kvrocks-leader/internal/metrics"
)

func epochOf(t *testing.T, raw []byte) uint64 {
	t.Helper()
	var lv consulstore.LeaderValue
	if err := json.Unmarshal(raw, &lv); err != nil {
		t.Fatal(err)
	}
	return lv.Epoch
}

// A leader that is shut down (SIGTERM) must stop accepting writes before
// the lock is released, otherwise the next leader overlaps with it.
func TestShutdownFencesBeforeRelease(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	lock := &fakeLock{}
	roleAtRelease := ""
	lock.onRelease = func() {
		kv.mu.Lock()
		roleAtRelease = kv.role
		kv.mu.Unlock()
	}
	deps := controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}
	_, _, _, fkv := runCtl(t, cfg, deps, 200*time.Millisecond)
	if roleAtRelease != "slave" {
		t.Fatalf("lock released while instance role was %q", roleAtRelease)
	}
	lock.mu.Lock()
	sess := lock.sess
	lock.mu.Unlock()
	if sess != "" {
		t.Fatal("expected lock released after fencing")
	}
	fkv.mu.Lock()
	defer fkv.mu.Unlock()
	if fkv.masterPort != cfg.FencePort || fkv.kills == 0 || fkv.killedWhileMaster {
		t.Fatalf("want fenced to fence target and clients killed after: port=%d kills=%d whileMaster=%v", fkv.masterPort, fkv.kills, fkv.killedWhileMaster)
	}
}

// If the instance cannot be fenced on shutdown the lock must not be
// handed out early; it expires with the session instead.
func TestShutdownKeepsLockWhenFenceFails(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	lock := &fakeLock{}
	deps := controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}
	c := controller.New(cfg, deps, logx.New(cfg.NodeName, cfg.Mode), metrics.New())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	time.Sleep(200 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("expected leader before shutdown")
	}
	kv.mu.Lock()
	kv.failReplicaOf = true
	kv.mu.Unlock()
	cancel()
	<-done
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.sess == "" {
		t.Fatal("lock released although the instance was still a writable master")
	}
}

// The epoch must keep rising even when the leader value was wiped.
func TestEpochMonotonicWhenValueLost(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	lock := &fakeLock{epoch: 5} // value empty, counter remembers 5
	deps := controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}
	runCtl(t, cfg, deps, 200*time.Millisecond)
	if e := epochOf(t, lock.val); e != 6 {
		t.Fatalf("epoch %d, want 6", e)
	}
}

// Releasing keeps the value, so the next leader continues from it.
func TestEpochSurvivesRelease(t *testing.T) {
	cfg := testConfig("kv01")
	lock := &fakeLock{}
	for i := 1; i <= 3; i++ {
		kv := &fakeKV{up: true, role: "master"}
		deps := controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}
		runCtl(t, cfg, deps, 150*time.Millisecond)
		if e := epochOf(t, lock.val); e != uint64(i) {
			t.Fatalf("round %d: epoch %d", i, e)
		}
	}
}

// After a restart the leader value still names this node with an old
// session. That is our own earlier tenure, not an old master to wait for.
func TestOwnPreviousRecordIsNotOldMaster(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.HoldDown = time.Hour
	kv := &fakeKV{up: true, role: "slave", masterHost: "127.0.0.1", masterPort: 1}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "old-sess", 4, time.Now())}
	deps := controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}
	c, _, _, _ := runCtl(t, cfg, deps, 300*time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatalf("expected to take over again, got %v", c.Snapshot().Role)
	}

	// Someone else's record with unknown lag must still wait out hold-down.
	kv2 := &fakeKV{up: true, role: "slave", masterHost: "127.0.0.1", masterPort: 1}
	lock2 := &fakeLock{val: elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "old-sess", 4, time.Now())}
	deps2 := controller.Deps{Session: &fakeSession{}, Lock: lock2, Registrar: &fakeRegistrar{}, KV: kv2}
	c2, _, _, _ := runCtl(t, cfg, deps2, 300*time.Millisecond)
	if c2.Snapshot().Role == elect.RoleLeader {
		t.Fatal("must not promote over another node's record inside hold-down")
	}
}

// A restarted instance comes back as a writable master; a follower must
// fence it straight away, not on the next regular check.
func TestRefenceAsSoonAsBackendReturns(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.CheckInterval = 5 * time.Second
	kv := &fakeKV{up: false}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.9", "", 6666, "sess-x", 3, time.Now()), sess: "sess-x"}
	deps := controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}
	c := controller.New(cfg, deps, logx.New(cfg.NodeName, cfg.Mode), metrics.New())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	time.Sleep(100 * time.Millisecond)
	kv.mu.Lock()
	kv.up, kv.role = true, "master"
	kv.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		kv.mu.Lock()
		r := kv.role
		kv.mu.Unlock()
		if r == "slave" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.role != "slave" {
		t.Fatalf("instance still %q 900ms after returning", kv.role)
	}
}
