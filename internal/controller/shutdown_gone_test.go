package controller_test

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/example/kvrocks-leader/internal/controller"
	"github.com/example/kvrocks-leader/internal/elect"
	"github.com/example/kvrocks-leader/internal/logx"
	"github.com/example/kvrocks-leader/internal/metrics"
)

// shutdownLeaderWith runs a leader, applies change to the backend, shuts the
// sidecar down and reports whether the lock is still held afterwards.
func shutdownLeaderWith(t *testing.T, change func(kv *fakeKV)) (held bool, took time.Duration) {
	t.Helper()
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
	change(kv)
	kv.mu.Unlock()
	start := time.Now()
	cancel()
	<-done
	took = time.Since(start)
	lock.mu.Lock()
	defer lock.mu.Unlock()
	return lock.sess != "", took
}

func TestShutdownReleasesWhenBackendRefuses(t *testing.T) {
	held, took := shutdownLeaderWith(t, func(kv *fakeKV) {
		kv.up = false
		kv.downErr = fmt.Errorf("dial tcp 127.0.0.1:8379: %w", syscall.ECONNREFUSED)
	})
	if held {
		t.Fatal("lock kept although nothing is listening on the local address")
	}
	if took > 3*time.Second {
		t.Fatalf("shutdown took %v", took)
	}
}

func TestShutdownReleasesWhenBackendDoesNotAnswer(t *testing.T) {
	held, took := shutdownLeaderWith(t, func(kv *fakeKV) { kv.hang = true })
	if held {
		t.Fatal("lock kept although the local instance never answered")
	}
	if took > 5*time.Second {
		t.Fatalf("shutdown took %v", took)
	}
}

// Any other error (the instance answered something) is not proof that it
// is gone, so the lock must stay with the session.
func TestShutdownKeepsLockOnOtherBackendErrors(t *testing.T) {
	held, _ := shutdownLeaderWith(t, func(kv *fakeKV) { kv.up = false })
	if !held {
		t.Fatal("lock released on an unclassified backend error")
	}
}

func TestRsidPsyncRetriedWhenBackendComesUp(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: false, role: "slave", downErr: fmt.Errorf("dial: %w", syscall.ECONNREFUSED)}
	deps := controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv}
	c := controller.New(cfg, deps, logx.New(cfg.NodeName, cfg.Mode), metrics.New())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	time.Sleep(150 * time.Millisecond)
	kv.mu.Lock()
	if kv.rsidOK != 0 {
		t.Fatal("applied while down")
	}
	kv.up = true
	kv.mu.Unlock()
	time.Sleep(600 * time.Millisecond)
	cancel()
	<-done
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rsidOK != 1 {
		t.Fatalf("use-rsid-psync applied %d times after the backend came up, want 1 (calls %d)", kv.rsidOK, kv.rsidCalls)
	}
}
