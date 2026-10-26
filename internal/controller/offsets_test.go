package controller_test

import (
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/consulstore"
	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

func offsetLock(cfg interface {
	MembersPrefix() string
	OffsetsPrefix() string
}, leader []byte, epoch uint64, peers map[string]int64) (*presenceLock, *fakeLock) {
	base := &fakeLock{val: leader, epoch: epoch}
	m := map[string]string{}
	for n, off := range peers {
		m[cfg.MembersPrefix()+n] = n
		if off >= 0 {
			m[cfg.OffsetsPrefix()+n] = string(elect.MarshalOffset(n, off, time.Now()))
		}
	}
	return &presenceLock{fakeLock: base, members: m}, base
}

// The 2026-10-25 deadlock: kv02 (last leader, newest data) restarted and
// deferred to kv01 forever (lag-gate-max-wait 0), while kv01 was refused
// by the lag gate. With offsets published, kv02 sees it is ahead of the
// only live peer and promotes.
func TestOffsetElectBreaksRejoinDeadlock(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.OffsetElect = true
	cfg.LagGateMaxWait = 0
	cfg.HoldDown = time.Hour
	kv := fencedKV()
	kv.local = 500
	lock, base := offsetLock(cfg, elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-old", 147, time.Now()), 147, map[string]int64{"kv01": 400})
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(300 * time.Millisecond)
	stop()
	if c.Snapshot().Role != elect.RoleLeader || base.epoch != 148 {
		t.Fatalf("most up-to-date node must promote: role=%v epoch=%d", c.Snapshot().Role, base.epoch)
	}
	lock.pmu.Lock()
	own, ok := elect.ParseOffset(lock.members[cfg.OffsetsPrefix()+"kv02"])
	lock.pmu.Unlock()
	if !ok || own.Offset != 500 {
		t.Fatalf("own offset not published: %+v", own)
	}
}

// The behind node of the same pair keeps waiting.
func TestOffsetElectNeverPromotesBehindLiveNode(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.OffsetElect = true
	cfg.HoldDown = time.Hour
	kv := fencedKV()
	kv.local = 400
	lock, base := offsetLock(cfg, elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-old", 147, time.Now()), 147, map[string]int64{"kv01": 500})
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(300 * time.Millisecond)
	stop()
	if c.Snapshot().Role == elect.RoleLeader || kv.promotions != 0 || base.epoch != 147 {
		t.Fatalf("must not promote behind a live peer: role=%v promotions=%d epoch=%d", c.Snapshot().Role, kv.promotions, base.epoch)
	}
}

// A healthy catalog peer with no published offset blocks: no visibility.
func TestOffsetElectNeedsEveryKnownPeer(t *testing.T) {
	cfg := testConfig("kv02")
	cfg.OffsetElect = true
	cfg.HoldDown = time.Hour
	kv := fencedKV()
	kv.local = 500
	lock, base := offsetLock(cfg, elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-old", 147, time.Now()), 147, map[string]int64{"kv01": 400})
	reg := &fakeRegistrar{peers: []consulstore.Instance{{ID: "kvrocks-kv03", Node: "n3"}}}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: reg, KV: kv})
	time.Sleep(300 * time.Millisecond)
	stop()
	if c.Snapshot().Role == elect.RoleLeader || base.epoch != 147 {
		t.Fatalf("must wait for kv03's offset: role=%v epoch=%d", c.Snapshot().Role, base.epoch)
	}
}

// Lag unknown behind a departed leader whose sidecar is gone but which
// is still live and publishing: kv01 is ahead, so it takes over without
// the hold-down.
func TestOffsetElectOverridesUnknownLagWhenOldLeaderIsLiveAndBehind(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.OffsetElect = true
	cfg.HoldDown = time.Hour
	kv := fencedKV()
	kv.local = 900
	lock, base := offsetLock(cfg, elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-gone", 7, time.Now()), 7, map[string]int64{"kv02": 900})
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(300 * time.Millisecond)
	stop()
	// Tie at 900: kv01 < kv02 wins.
	if c.Snapshot().Role != elect.RoleLeader || base.epoch != 8 {
		t.Fatalf("tie must go to kv01: role=%v epoch=%d", c.Snapshot().Role, base.epoch)
	}
}

// A dead old leader (no member, no offset) may hold writes nobody else
// has: the hold-down still applies.
func TestOffsetElectKeepsHoldDownForDeadOldLeader(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.OffsetElect = true
	cfg.HoldDown = time.Hour
	kv := fencedKV()
	kv.local = 900
	lock, base := offsetLock(cfg, elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-gone", 7, time.Now()), 7, map[string]int64{"kv03": 100})
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(300 * time.Millisecond)
	stop()
	if c.Snapshot().Role == elect.RoleLeader || base.epoch != 7 {
		t.Fatalf("dead old leader: hold-down must apply: role=%v epoch=%d", c.Snapshot().Role, base.epoch)
	}
}
