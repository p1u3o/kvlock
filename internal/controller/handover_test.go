package controller_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/example/kvrocks-leader/internal/consulstore"
	"github.com/example/kvrocks-leader/internal/controller"
	"github.com/example/kvrocks-leader/internal/elect"
	"github.com/example/kvrocks-leader/internal/httpapi"
	"github.com/example/kvrocks-leader/internal/kvrocks"
	"github.com/example/kvrocks-leader/internal/metrics"
)

func leaderWith(t *testing.T, replicas []kvrocks.ReplicaState, local int64) (*controller.Controller, *fakeLock, *fakeKV, func()) {
	t.Helper()
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master", local: local, replicas: replicas}
	lock := &fakeLock{}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(80 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		stop()
		t.Fatal("precondition: leader")
	}
	return c, lock, kv, stop
}

func assertStillLeader(t *testing.T, c *controller.Controller, lock *fakeLock, kv *fakeKV, res httpapi.HandoverResult, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected the handover to abort")
	}
	if !res.StillLeader || c.Snapshot().Role != elect.RoleLeader {
		t.Fatalf("must stay leader after abort: res=%+v role=%v", res, c.Snapshot().Role)
	}
	if role, _, _ := kv.state(); role != "master" {
		t.Fatalf("instance must stay master, got %s", role)
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.sess == "" {
		t.Fatal("lock must still be held")
	}
	if lv := elect.ParseLeader(lock.val); lv == nil || lv.HandoverTo != "" {
		t.Fatalf("no hint may be written on abort: %+v", lv)
	}
}

func TestHandoverAbortsWithoutReplica(t *testing.T) {
	c, lock, kv, stop := leaderWith(t, nil, 500)
	defer stop()
	res, err := c.Handover(context.Background(), httpapi.HandoverRequest{Timeout: "150ms"})
	assertStillLeader(t, c, lock, kv, res, err)
	if got := testutil.ToFloat64(c.Metrics().Handovers.WithLabelValues("aborted")); got != 1 {
		t.Fatalf("handovers_total{result=aborted}=%v", got)
	}
}

func TestHandoverAbortsWhenReplicaTooFarBehind(t *testing.T) {
	c, lock, kv, stop := leaderWith(t, []kvrocks.ReplicaState{{IP: "10.0.0.2", Port: 6666, Offset: 10}}, 1_000_000)
	defer stop()
	res, err := c.Handover(context.Background(), httpapi.HandoverRequest{Timeout: "150ms"})
	assertStillLeader(t, c, lock, kv, res, err)
	if res.Target != "10.0.0.2:6666" || res.Lag != 999_990 {
		t.Fatalf("result should report the lagging replica: %+v", res)
	}
}

func TestHandoverAbortsForUnknownTarget(t *testing.T) {
	c, lock, kv, stop := leaderWith(t, []kvrocks.ReplicaState{{IP: "10.0.0.2", Port: 6666, Offset: 500}}, 500)
	defer stop()
	res, err := c.Handover(context.Background(), httpapi.HandoverRequest{To: "10.0.0.3:6666", Timeout: "100ms"})
	assertStillLeader(t, c, lock, kv, res, err)
}

func TestHandoverRejectedOnFollower(t *testing.T) {
	cfg := testConfig("kv02")
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.9", "", 6666, "sess-x", 3, time.Now()), sess: "sess-x"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: &fakeKV{up: true, role: "slave"}})
	defer stop()
	time.Sleep(60 * time.Millisecond)
	if _, err := c.Handover(context.Background(), httpapi.HandoverRequest{}); err == nil {
		t.Fatal("a follower must refuse to hand over")
	}
}

func TestHandoverCommitsAndReleases(t *testing.T) {
	c, lock, kv, stop := leaderWith(t, []kvrocks.ReplicaState{{IP: "10.0.0.2", Port: 6666, Offset: 480}}, 500)
	defer stop()

	// Play the successor: once the lock is free, take it with the hint
	// value recorded first.
	var hint *consulstore.LeaderValue
	go func() {
		for i := 0; i < 200; i++ {
			lock.mu.Lock()
			if lock.sess == "" && lock.val != nil {
				hint = elect.ParseLeader(lock.val)
				lock.sess = "sess-kv02"
				lock.val = elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-kv02", 9, time.Now())
				lock.mu.Unlock()
				return
			}
			lock.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	res, err := c.Handover(context.Background(), httpapi.HandoverRequest{})
	if err != nil {
		t.Fatalf("handover failed: %v (%+v)", err, res)
	}
	if res.Stage != "done" || res.NewLeader != "kv02" || res.Target != "10.0.0.2:6666" || res.StillLeader {
		t.Fatalf("unexpected result %+v", res)
	}
	lock.mu.Lock()
	h := hint
	lock.mu.Unlock()
	if h == nil || h.HandoverTo != "10.0.0.2:6666" || h.HandoverOffset != 500 || h.Node != "kv01" {
		t.Fatalf("hint not written before release: %+v", h)
	}
	if !elect.HandoverActive(h, time.Now()) {
		t.Fatalf("hint should still be active: %+v", h)
	}
	kv.mu.Lock()
	kills, early, role := kv.kills, kv.killedWhileMaster, kv.role
	kv.mu.Unlock()
	if role != "slave" || kills != 1 || early {
		t.Fatalf("expected fenced replica with one client kill after fencing: role=%s kills=%d early=%v", role, kills, early)
	}
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("must not be leader after handover")
	}
	if got := testutil.ToFloat64(c.Metrics().StepDowns.WithLabelValues(metrics.ReasonHandover)); got != 1 {
		t.Fatalf("step_downs_total{reason=handover}=%v", got)
	}
	if got := testutil.ToFloat64(c.Metrics().Handovers.WithLabelValues("ok")); got != 1 {
		t.Fatalf("handovers_total{result=ok}=%v", got)
	}
	// The old leader then follows the new one.
	time.Sleep(80 * time.Millisecond)
	if _, host, _ := kv.state(); host != "10.0.0.2" {
		t.Fatalf("old leader should follow kv02, follows %s", host)
	}
}

func hinted(to string, offset int64, until time.Time) []byte {
	lv := elect.ParseLeader(elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()))
	lv.HandoverTo, lv.HandoverOffset = to, offset
	lv.HandoverUntil = until.UTC().Format(time.RFC3339Nano)
	return mustJSON(lv)
}

func TestHandoverTargetWaitsForCatchUpThenAcquires(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.AdvertiseAddr = "10.0.0.2"
	cfg.HandoverTimeout = time.Hour // stay in the strict (lag 0) phase
	kv := &fakeKV{up: true, role: "slave", masterHost: "127.0.0.1", masterPort: 1, local: 90, peerErr: errString("down")}
	lock := &fakeLock{val: hinted("10.0.0.2:6666", 100, time.Now().Add(time.Hour))}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(100 * time.Millisecond)
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("target must wait until it has the old leader's final offset")
	}
	kv.mu.Lock()
	kv.local = 100
	kv.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("caught-up target should acquire")
	}
	if lv := elect.ParseLeader(lock.val); lv.Node != "kv02" || lv.HandoverTo != "" || lv.Epoch != 5 {
		t.Fatalf("new value should be a clean kv02 value, got %+v", lv)
	}
}

func TestNonTargetDoesNotContendDuringHandover(t *testing.T) {
	cfg := testConfig("kv03")
	cfg.AdvertiseAddr = "10.0.0.3"
	kv := &fakeKV{up: true, role: "master", local: 100}
	lock := &fakeLock{val: hinted("10.0.0.2:6666", 100, time.Now().Add(time.Hour))}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(120 * time.Millisecond)
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("non-target must not acquire during an active handover")
	}
	if role, host, port := kv.state(); role != "slave" || host != "127.0.0.1" || port != 1 {
		t.Fatalf("writable non-target must be fenced, got %s %s:%d", role, host, port)
	}
}

func TestExpiredHintIsIgnored(t *testing.T) {
	cfg := testConfig("kv03")
	cfg.AdvertiseAddr = "10.0.0.3"
	kv := &fakeKV{up: true, role: "master", local: 100}
	lock := &fakeLock{val: hinted("10.0.0.2:6666", 100, time.Now().Add(-time.Second))}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(150 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("an expired hint must not block election")
	}
}
