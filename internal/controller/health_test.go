package controller_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/config"
	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/kvrocks"
)

func (f *fakeKV) setHang(v bool) { f.mu.Lock(); f.hang = v; f.mu.Unlock() }

func slowCfg() *config.Config {
	cfg := testConfig("kv01")
	cfg.HealthTimeout = 30 * time.Millisecond
	cfg.SlowThreshold = 4
	cfg.UnhealthyThreshold = 2
	return cfg
}

// A couple of slow probes (kvrocks busy) must not cost leadership.
func TestSlowProbesKeepLeadership(t *testing.T) {
	cfg := slowCfg()
	kv := &fakeKV{up: true, role: "master"}
	lock := &fakeLock{}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(200 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		stop()
		t.Fatalf("setup: not leader")
	}
	epoch := lock.epoch
	kv.mu.Lock()
	kills := kv.kills
	kv.mu.Unlock()
	kv.setHang(true)
	time.Sleep(90 * time.Millisecond) // < 4 probes of 30ms + interval
	kv.setHang(false)
	time.Sleep(150 * time.Millisecond)
	role := c.Snapshot().Role
	kv.mu.Lock()
	killsAfter := kv.kills
	kv.mu.Unlock()
	stop()
	if role != elect.RoleLeader || lock.epoch != epoch || killsAfter != kills {
		t.Fatalf("slow probes must keep leadership: role=%v epoch=%d->%d kills=%d->%d", role, epoch, lock.epoch, kills, killsAfter)
	}
}

// A kvrocks that stays slow past the slow threshold counts as down.
func TestPersistentlySlowProbesDemote(t *testing.T) {
	cfg := slowCfg()
	kv := &fakeKV{up: true, role: "master"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(200 * time.Millisecond)
	kv.setHang(true)
	time.Sleep(600 * time.Millisecond)
	stop()
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatalf("persistently slow kvrocks must lose leadership")
	}
}

// A dead process (refused) still demotes at the normal threshold, well
// before the slow threshold would.
func TestRefusedProbesDemoteAtUnhealthyThreshold(t *testing.T) {
	cfg := slowCfg()
	cfg.SlowThreshold = 1000
	kv := &fakeKV{up: true, role: "master", downErr: errors.New("dial tcp 127.0.0.1:6666: connect: connection refused")}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(200 * time.Millisecond)
	kv.mu.Lock()
	kv.up = false
	kv.mu.Unlock()
	time.Sleep(250 * time.Millisecond)
	stop()
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatalf("refused probes must demote at --unhealthy-threshold")
	}
}

func TestIsTimeout(t *testing.T) {
	if !kvrocks.IsTimeout(contextDeadline()) || kvrocks.IsTimeout(errors.New("connection refused")) || kvrocks.IsTimeout(nil) {
		t.Fatal("IsTimeout misclassifies")
	}
}

func contextDeadline() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	return ctx.Err()
}
