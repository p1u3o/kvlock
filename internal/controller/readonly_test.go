package controller_test

import (
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

func roState(kv *fakeKV) (bool, []bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.readonly, append([]bool(nil), kv.roAtReplicaOf...)
}

// Every move to replica (follow or fence) must set read-only before the
// REPLICAOF; only promotion turns it off.
func TestReadOnlySetBeforeEveryReplicaOf(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master"}
	lock := &fakeLock{}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(100 * time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatal("precondition: leader")
	}
	if ro, _ := roState(kv); ro {
		t.Fatal("leader must be writable (promotion turns read-only off)")
	}
	lock.mu.Lock()
	lock.sess = "sess-kv02"
	lock.val = elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-kv02", 9, time.Now())
	lock.mu.Unlock()
	time.Sleep(150 * time.Millisecond)
	ro, at := roState(kv)
	if !ro || len(at) == 0 {
		t.Fatalf("after demotion: readonly=%v replicaofs=%v", ro, at)
	}
	for i, v := range at {
		if !v {
			t.Fatalf("REPLICAOF #%d ran while replica-read-only was off", i)
		}
	}
}

// A node that is already replicating the leader when the sidecar starts
// still gets read-only asserted.
func TestReadOnlyAssertedWhenAlreadyFollowing(t *testing.T) {
	cfg := testConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.9", masterPort: 6666}
	lock := &fakeLock{}
	lock.val = elect.MarshalLeader("kv01", "10.0.0.9", "", 6666, "sess-x", 3, time.Now())
	lock.sess = "sess-x"
	_, _, _, fkv := runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv}, 200*time.Millisecond)
	ro, at := roState(fkv)
	if !ro || len(at) != 0 {
		t.Fatalf("readonly=%v replicaofs=%v (want read-only, no REPLICAOF)", ro, at)
	}
}
