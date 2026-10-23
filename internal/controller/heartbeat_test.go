package controller_test

import (
	"strings"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/config"
	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

func hbAt(t time.Time, epoch uint64) string { return elect.FormatHeartbeat(t, epoch) }

func TestLeaderWritesHeartbeat(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	c, _, _, _ := runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv}, 250*time.Millisecond)
	kv.mu.Lock()
	sets, v := kv.hbSets, kv.hbLocal
	kv.mu.Unlock()
	if sets < 3 {
		t.Fatalf("leader wrote %d heartbeats", sets)
	}
	hb, ok := elect.ParseHeartbeat(v)
	if !ok || hb.Epoch == 0 || time.Since(time.UnixMilli(hb.UnixMs)) > time.Second {
		t.Fatalf("bad heartbeat %q", v)
	}
	if s := c.Snapshot(); s.LagSeconds != 0 || s.Role != elect.RoleLeader {
		t.Fatalf("leader lag_seconds=%v role=%v", s.LagSeconds, s.Role)
	}
}

func TestObserveNeverWritesHeartbeat(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.Mode = "observe"
	kv := &fakeKV{up: true, role: "master"}
	reg := &fakeRegistrar{}
	runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: reg, KV: kv}, 200*time.Millisecond)
	assertNoSideEffects(t, kv, reg)
}

// An observer following a leader may read an existing heartbeat key.
func TestObserveReadsExistingHeartbeat(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.Mode = "observe"
	now := time.Now()
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, hbPeer: hbAt(now, 4), hbLocal: hbAt(now.Add(-700*time.Millisecond), 4)}
	reg := &fakeRegistrar{}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-kv01", 4, now), sess: "sess-kv01"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: reg, KV: kv})
	time.Sleep(150 * time.Millisecond)
	s := c.Snapshot()
	stop()
	if s.LagSeconds < 0.69 || s.LagSeconds > 0.71 || s.LagGate != "time" {
		t.Fatalf("lag_seconds=%v gate=%s", s.LagSeconds, s.LagGate)
	}
	assertNoSideEffects(t, kv, reg)
}

// followThenVanish runs a follower of kv01 with the given heartbeats, then
// makes kv01's session and instance go away.
func followThenVanish(t *testing.T, kv *fakeKV, cfgMod func(*config.Config)) (*controller.Controller, func()) {
	t.Helper()
	cfg := testConfig("kv02")
	cfg.HoldDown = time.Hour
	if cfgMod != nil {
		cfgMod(cfg)
	}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-kv01", 2, time.Now()), sess: "sess-kv01"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(100 * time.Millisecond)
	kv.mu.Lock()
	kv.peerErr = errString("connection refused")
	kv.mu.Unlock()
	lock.mu.Lock()
	lock.sess = ""
	lock.mu.Unlock()
	time.Sleep(200 * time.Millisecond)
	return c, stop
}

func TestTimeGateRefusesBeyondMaxLagTime(t *testing.T) {
	now := time.Now()
	// Offsets look identical, but the replica's heartbeat is 5s old.
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 1000, peerOffset: 1000,
		hbPeer: hbAt(now, 2), hbLocal: hbAt(now.Add(-5*time.Second), 2)}
	c, stop := followThenVanish(t, kv, nil)
	defer stop()
	s := c.Snapshot()
	if s.Role == elect.RoleLeader {
		t.Fatal("5s behind with --max-lag-time 2s must refuse")
	}
	if s.LagSeconds < 4.9 || s.LagGate != "time" {
		t.Fatalf("lag_seconds=%v gate=%s", s.LagSeconds, s.LagGate)
	}
	found := false
	for _, e := range s.Events {
		if e.Kind == "lag_gate" && strings.Contains(e.Message, "time gate") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no lag_gate event: %+v", s.Events)
	}
}

// Within the time bound the replica promotes even though the offset gap
// is far over --max-lag: the time gate replaces the offset gate.
func TestTimeGateAllowsWithinMaxLagTime(t *testing.T) {
	now := time.Now()
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 10, peerOffset: 1_000_000,
		hbPeer: hbAt(now, 2), hbLocal: hbAt(now.Add(-500*time.Millisecond), 2)}
	c, stop := followThenVanish(t, kv, nil)
	defer stop()
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("0.5s behind must promote")
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.promotions == 0 {
		t.Fatal("no REPLICAOF NO ONE")
	}
}

// A leader that has a heartbeat while the replica has none: the replica
// never received one, so it is treated as far behind.
func TestTimeGateMissingLocalHeartbeatRefuses(t *testing.T) {
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 1000, peerOffset: 1000, hbPeer: hbAt(time.Now(), 2)}
	c, stop := followThenVanish(t, kv, nil)
	defer stop()
	if c.Snapshot().Role == elect.RoleLeader {
		t.Fatal("missing local heartbeat must refuse")
	}
}

// No heartbeat anywhere (first start, or an older leader): the offset gate
// decides, as before.
func TestNoHeartbeatFallsBackToSeqGate(t *testing.T) {
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 990, peerOffset: 1000}
	c, stop := followThenVanish(t, kv, nil)
	defer stop()
	s := c.Snapshot()
	if s.Role != elect.RoleLeader {
		t.Fatalf("seq lag 10 must promote; gate=%s", s.LagGate)
	}
	kv2 := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 10, peerOffset: 1_000_000}
	c2, stop2 := followThenVanish(t, kv2, nil)
	defer stop2()
	if c2.Snapshot().Role == elect.RoleLeader {
		t.Fatal("seq lag 999990 must refuse")
	}
}

// With heartbeats disabled the time gate is never used.
func TestHeartbeatDisabled(t *testing.T) {
	now := time.Now()
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666, local: 1000, peerOffset: 1000,
		hbPeer: hbAt(now, 2), hbLocal: hbAt(now.Add(-time.Minute), 2)}
	c, stop := followThenVanish(t, kv, func(cfg *config.Config) { cfg.HeartbeatInterval = 0 })
	defer stop()
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("heartbeat disabled: seq lag 0 must promote")
	}
}

func AddEventForTestRingBounded(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	c, _, _, _ := runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv}, 100*time.Millisecond)
	ev := c.Events()
	if len(ev) == 0 || ev[0].Kind != "election" {
		t.Fatalf("events %+v", ev)
	}
	for i := 0; i < 200; i++ {
		controller.AddEventForTest(c, "fence", "x")
	}
	if n := len(c.Events()); n != 50 {
		t.Fatalf("ring holds %d", n)
	}
}
