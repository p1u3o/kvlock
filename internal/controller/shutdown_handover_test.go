package controller_test

import (
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/kvrocks"
)

func shutdownWithReplicas(t *testing.T, wait time.Duration, local int64, replicas []kvrocks.ReplicaState) (*fakeLock, *fakeKV, time.Duration) {
	t.Helper()
	cfg := testConfig("kv01")
	cfg.ShutdownHandover = wait
	kv := &fakeKV{up: true, role: "master", local: local, replicas: replicas}
	lock := &fakeLock{}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(80 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		stop()
		t.Fatal("precondition: leader")
	}
	start := time.Now()
	stop()
	return lock, kv, time.Since(start)
}

func hint(lock *fakeLock) (held bool, to string) {
	lock.mu.Lock()
	defer lock.mu.Unlock()
	lv := elect.ParseLeader(lock.val)
	if lv != nil {
		to = lv.HandoverTo
	}
	return lock.sess != "", to
}

func TestShutdownHandsOverToCaughtUpReplica(t *testing.T) {
	lock, kv, took := shutdownWithReplicas(t, time.Second, 500, []kvrocks.ReplicaState{{IP: "10.0.0.2", Port: 6666, Offset: 500}})
	held, to := hint(lock)
	if held || to != "10.0.0.2:6666" {
		t.Fatalf("want released lock with hint for the replica, held=%v to=%q", held, to)
	}
	if role, _, _ := kv.state(); role != "slave" {
		t.Fatalf("instance must be fenced, role %s", role)
	}
	if took > 900*time.Millisecond {
		t.Fatalf("handover on shutdown took %v", took)
	}
}

func TestShutdownFallsBackWhenReplicaLags(t *testing.T) {
	lock, kv, took := shutdownWithReplicas(t, 200*time.Millisecond, 1_000_000, []kvrocks.ReplicaState{{IP: "10.0.0.2", Port: 6666, Offset: 10}})
	_ = took
	held, to := hint(lock)
	if held || to != "" {
		t.Fatalf("want plain fence-then-release, held=%v to=%q", held, to)
	}
	if role, _, _ := kv.state(); role != "slave" {
		t.Fatalf("instance must be fenced, role %s", role)
	}
}

func TestShutdownFallsBackWithoutReplica(t *testing.T) {
	lock, _, took := shutdownWithReplicas(t, 200*time.Millisecond, 500, nil)
	if held, to := hint(lock); held || to != "" {
		t.Fatalf("want plain fence-then-release, held=%v to=%q", held, to)
	}
	if took > 3*time.Second {
		t.Fatalf("fallback took %v", took)
	}
}

func TestShutdownHandoverDisabled(t *testing.T) {
	lock, _, _ := shutdownWithReplicas(t, 0, 500, []kvrocks.ReplicaState{{IP: "10.0.0.2", Port: 6666, Offset: 500}})
	if held, to := hint(lock); held || to != "" {
		t.Fatalf("disabled: want plain release, held=%v to=%q", held, to)
	}
}
