package controller_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/consulstore"
	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

// A sidecar killed while leader comes back: its kvrocks was fenced by the
// watchdog and the key value still names its own node.
func fencedKV() *fakeKV {
	return &fakeKV{up: true, role: "slave", masterHost: "127.0.0.1", masterPort: 1}
}

func peerKV02() []consulstore.Instance {
	return []consulstore.Instance{{ID: "kvrocks-kv02", Node: "kv02", Addr: "10.0.0.2", Port: 6666}}
}

func TestRestartedLeaderDefersToHealthyPeer(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.LagGateMaxWait = time.Minute
	kv := fencedKV()
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()), epoch: 4}
	reg := &fakeRegistrar{peers: peerKV02()}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: reg, KV: kv})
	time.Sleep(250 * time.Millisecond)
	stop()
	if c.Snapshot().Role == elect.RoleLeader || kv.promotions != 0 {
		t.Fatalf("restarted former leader must leave the lock to the healthy peer: role=%v promotions=%d", c.Snapshot().Role, kv.promotions)
	}
	if lock.acquires != 0 || lock.epoch != 4 {
		t.Fatalf("expected no acquire and no epoch change, got acquires=%d epoch=%d", lock.acquires, lock.epoch)
	}
}

func TestRestartedLeaderPromotesWithoutPeer(t *testing.T) {
	cfg := testConfig("kv01")
	kv := fencedKV()
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()), epoch: 4}
	reg := &fakeRegistrar{peers: []consulstore.Instance{{ID: "kvrocks-kv01", Node: "kv01"}}} // only its own stale entry
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: reg, KV: kv})
	time.Sleep(250 * time.Millisecond)
	stop()
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatalf("with no healthy peer the former leader must take over, got %v", c.Snapshot().Role)
	}
	if lv := elect.ParseLeader(lock.val); lv == nil || lv.Epoch != 5 || lock.epoch != 5 || lock.bumps != 1 {
		t.Fatalf("expected epoch 5 stored once, got value=%+v counter=%d bumps=%d", lv, lock.epoch, lock.bumps)
	}
}

func TestRestartedLeaderReleasesStaleSession(t *testing.T) {
	// The old session has not expired yet. Waiting for it costs the TTL
	// plus lock-delay; releasing it costs neither.
	cfg := testConfig("kv01")
	cfg.LagGateMaxWait = time.Minute
	kv := fencedKV()
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()), sess: "sess-old", epoch: 4}
	sess := &fakeSession{}
	reg := &fakeRegistrar{peers: peerKV02()}
	c, stop := startCtl(t, cfg, controller.Deps{Session: sess, Lock: lock, Registrar: reg, KV: kv})
	time.Sleep(250 * time.Millisecond)
	stop()
	if lock.sess != "" {
		t.Fatalf("stale session still holds the key: %q", lock.sess)
	}
	if len(sess.other) == 0 || sess.other[0] != "sess-old" {
		t.Fatalf("stale session not destroyed: %v", sess.other)
	}
	if c.Snapshot().Role == elect.RoleLeader || lock.acquires != 0 {
		t.Fatalf("must defer to the healthy peer after releasing, role=%v acquires=%d", c.Snapshot().Role, lock.acquires)
	}
}

func TestRejoinWaitIsBounded(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.LagGateMaxWait = 100 * time.Millisecond
	kv := fencedKV()
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()), epoch: 4}
	reg := &fakeRegistrar{peers: peerKV02()}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: reg, KV: kv})
	time.Sleep(400 * time.Millisecond)
	stop()
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatalf("after the max wait the former leader must run, got %v", c.Snapshot().Role)
	}
}

func TestStaleOwnSessionOnWritableMasterIsNotReleased(t *testing.T) {
	// Observe mode never touches the real lock.
	cfg := testConfig("kv01")
	cfg.Mode = "observe"
	kv := fencedKV()
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()), sess: "sess-old"}
	_, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{peers: peerKV02()}, KV: kv})
	time.Sleep(150 * time.Millisecond)
	stop()
	if lock.sess != "sess-old" {
		t.Fatal("observe mode must not release the lock")
	}
}

func TestRefusedAcquireDoesNotBumpEpoch(t *testing.T) {
	cfg := testConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.1", masterPort: 6666}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 10, time.Now()), epoch: 10, refuse: true}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(300 * time.Millisecond)
	lock.mu.Lock()
	tries, ep, bumps := lock.acquires, lock.epoch, lock.bumps
	lock.refuse = false
	lock.mu.Unlock()
	if tries < 2 || ep != 10 || bumps != 0 {
		t.Fatalf("refused acquires must not move the counter: tries=%d epoch=%d bumps=%d", tries, ep, bumps)
	}
	time.Sleep(300 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatalf("expected leader once lock-delay is over, got %v", c.Snapshot().Role)
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lv := elect.ParseLeader(lock.val); lv == nil || lv.Epoch != 11 || lock.epoch != 11 || lock.bumps != 1 {
		t.Fatalf("expected epoch 11 bumped once, got value=%+v counter=%d bumps=%d", lv, lock.epoch, lock.bumps)
	}
}

// presenceLock adds session-held member keys to fakeLock.
type presenceLock struct {
	*fakeLock
	pmu     sync.Mutex
	members map[string]string // key -> value, all held
}

func (p *presenceLock) Acquire(ctx context.Context, key, sess string, v []byte) (bool, error) {
	if strings.Contains(key, "/members/") {
		p.pmu.Lock()
		defer p.pmu.Unlock()
		p.members[key] = string(v)
		return true, nil
	}
	return p.fakeLock.Acquire(ctx, key, sess, v)
}

func (p *presenceLock) Members(ctx context.Context, prefix string) ([]string, error) {
	p.pmu.Lock()
	defer p.pmu.Unlock()
	var out []string
	for k, v := range p.members {
		if strings.HasPrefix(k, prefix) {
			out = append(out, v)
		}
	}
	return out, nil
}

// The catalog filters out peers whose node the token cannot read, so the
// health API can show only this node. The member key still shows the peer.
func TestRestartedLeaderDefersToPeerMemberKey(t *testing.T) {
	cfg := testConfig("kv01")
	cfg.LagGateMaxWait = time.Minute
	kv := fencedKV()
	base := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()), epoch: 4}
	lock := &presenceLock{fakeLock: base, members: map[string]string{cfg.MembersPrefix() + "kv02": "kv02"}}
	reg := &fakeRegistrar{peers: []consulstore.Instance{{ID: "kvrocks-kv01", Node: "kv01"}}}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: reg, KV: kv})
	time.Sleep(250 * time.Millisecond)
	stop()
	if c.Snapshot().Role == elect.RoleLeader || kv.promotions != 0 || base.epoch != 4 {
		t.Fatalf("must defer to the peer's member key: role=%v promotions=%d epoch=%d", c.Snapshot().Role, kv.promotions, base.epoch)
	}
	lock.pmu.Lock()
	own := lock.members[cfg.MembersPrefix()+"kv01"]
	lock.pmu.Unlock()
	if own != "kv01" {
		t.Fatalf("own member key not published: %q", own)
	}
}

func TestOwnMemberKeyIsNotAPeer(t *testing.T) {
	cfg := testConfig("kv01")
	kv := fencedKV()
	base := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess-old", 4, time.Now()), epoch: 4}
	lock := &presenceLock{fakeLock: base, members: map[string]string{}}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	time.Sleep(250 * time.Millisecond)
	stop()
	if c.Snapshot().Role != elect.RoleLeader || base.epoch != 5 {
		t.Fatalf("alone, the former leader must take over: role=%v epoch=%d", c.Snapshot().Role, base.epoch)
	}
}
