package controller_test

import (
	"strings"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/config"
	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

func forwardConfig(node string) *config.Config {
	cfg := testConfig(node)
	cfg.ForwardEnabled = true
	cfg.ForwardAddr = "127.0.1.1:18379"
	return cfg
}

func waitFor(t *testing.T, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

// A follower points kvrocks at the forwarder once; a later leader change
// moves the forwarder target (new key) without another REPLICAOF.
func TestForwardFollowerReplicatesViaForwarderOnce(t *testing.T) {
	cfg := forwardConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", masterHost: "other", masterPort: 1, lag: 10}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, "sess-x", 3, time.Now()), sess: "sess-x"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()

	waitFor(t, 2*time.Second, func() bool {
		_, h, p := kv.state()
		return h == "127.0.1.1" && p == 18379
	})
	addr, key1, _ := fwd(c)
	if addr != "10.0.0.1:23001" {
		t.Fatalf("forward target: %q", addr)
	}
	kv.mu.Lock()
	follows := kv.follows
	kv.mu.Unlock()

	// Failover to kv03 on another dynamic port, higher epoch.
	lock.mu.Lock()
	lock.val = elect.MarshalLeader("kv03", "10.0.0.3", "", 27777, "sess-y", 4, time.Now())
	lock.sess = "sess-y"
	lock.idx++
	lock.mu.Unlock()
	waitFor(t, 2*time.Second, func() bool { a, _, _ := fwd(c); return a == "10.0.0.3:27777" })
	_, key2, _ := fwd(c)
	if key1 == key2 {
		t.Fatal("key must change with the leader")
	}
	time.Sleep(100 * time.Millisecond)
	kv.mu.Lock()
	after := kv.follows
	kv.mu.Unlock()
	if after != follows {
		t.Fatalf("leader change must not issue another REPLICAOF (before %d after %d)", follows, after)
	}
	if _, h, p := kv.state(); h != "127.0.1.1" || p != 18379 {
		t.Fatalf("replica moved off the forwarder: %s:%d", h, p)
	}
}

// Same leader address, new epoch (e.g. the same node re-elected): the key
// still changes so stale links are cut.
func TestForwardKeyIncludesEpoch(t *testing.T) {
	cfg := forwardConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", lag: 10}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, "sess-x", 3, time.Now()), sess: "sess-x"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	waitFor(t, 2*time.Second, func() bool { a, _, _ := fwd(c); return a != "" })
	_, k1, _ := fwd(c)
	lock.mu.Lock()
	lock.val = elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, "sess-x", 4, time.Now())
	lock.idx++
	lock.mu.Unlock()
	waitFor(t, 2*time.Second, func() bool { _, k, _ := fwd(c); return k != "" && k != k1 })
}

// The leader's forwarder refuses: forwarding to itself would loop.
func TestForwardLeaderRefuses(t *testing.T) {
	cfg := forwardConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	waitFor(t, 2*time.Second, func() bool { return c.Snapshot().Role == elect.RoleLeader })
	addr, key, reason := fwd(c)
	if addr != "" || key != "" || !strings.Contains(reason, "leader") {
		t.Fatalf("leader must not forward: %q %q %q", addr, key, reason)
	}
	if _, h, _ := kv.state(); h == "127.0.1.1" {
		t.Fatal("leader must not replicate from the forwarder")
	}
}

// Losing the leader (key released) withdraws the target.
func TestForwardRefusesWhenLeaderUnknown(t *testing.T) {
	cfg := forwardConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", lag: 10}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, "sess-x", 3, time.Now()), sess: "sess-x", refuse: true}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	waitFor(t, 2*time.Second, func() bool { a, _, _ := fwd(c); return a != "" })
	lock.mu.Lock()
	lock.sess = ""
	lock.idx++
	lock.mu.Unlock()
	waitFor(t, 2*time.Second, func() bool { a, _, _ := fwd(c); return a == "" })
}

// Disabled by default: REPLICAOF goes to the leader directly.
func TestForwardDisabledUsesLeaderAddr(t *testing.T) {
	cfg := testConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", masterHost: "other", masterPort: 1, lag: 10}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, "sess-x", 3, time.Now()), sess: "sess-x"}
	_, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	waitFor(t, 2*time.Second, func() bool { _, h, p := kv.state(); return h == "10.0.0.1" && p == 23001 })
}

func fwd(c *controller.Controller) (addr, key, reason string) {
	r := c.ForwardView()
	return r.Addr, r.Key, r.Reason
}
