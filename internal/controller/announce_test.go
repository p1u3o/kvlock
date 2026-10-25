package controller_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
)

// annKV adds replica-announce support to fakeKV and records call order.
type annKV struct {
	*fakeKV
	amu    sync.Mutex
	events []string
	ip     string
	port   int
}

func (a *annKV) SetReplicaAnnounce(_ context.Context, ip string, port int) error {
	a.amu.Lock()
	defer a.amu.Unlock()
	a.events = append(a.events, "announce")
	a.ip, a.port = ip, port
	return nil
}

func (a *annKV) ReplicaAnnounce(context.Context) (string, int, error) {
	a.amu.Lock()
	defer a.amu.Unlock()
	return a.ip, a.port, nil
}

func (a *annKV) ReplicaOf(ctx context.Context, h string, p int) error {
	a.amu.Lock()
	a.events = append(a.events, "replicaof")
	a.amu.Unlock()
	return a.fakeKV.ReplicaOf(ctx, h, p)
}

func (a *annKV) count(ev string) int {
	a.amu.Lock()
	defer a.amu.Unlock()
	n := 0
	for _, e := range a.events {
		if e == ev {
			n++
		}
	}
	return n
}

func TestAnnounceBeforeReplicaOfAndAfterRestart(t *testing.T) {
	cfg := forwardConfig("kv02")
	cfg.ReplicaAnnounce = true
	cfg.ReplicaAnnounceIP, cfg.ReplicaAnnouncePort = "10.0.0.2", 24567
	kv := &annKV{fakeKV: &fakeKV{up: true, role: "slave", masterHost: "other", masterPort: 1, lag: 10}}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, "sess-x", 3, time.Now()), sess: "sess-x"}
	_, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()

	waitFor(t, 2*time.Second, func() bool { _, h, _ := kv.state(); return h == "127.0.1.1" })
	kv.amu.Lock()
	first := append([]string(nil), kv.events...)
	ip, port := kv.ip, kv.port
	kv.amu.Unlock()
	if len(first) < 2 || first[0] != "announce" || first[1] != "replicaof" {
		t.Fatalf("announce must precede REPLICAOF: %v", first)
	}
	if ip != "10.0.0.2" || port != 24567 {
		t.Fatalf("announced %s:%d", ip, port)
	}
	time.Sleep(100 * time.Millisecond)
	if n := kv.count("announce"); n != 1 {
		t.Fatalf("announce must be applied once while healthy, got %d", n)
	}

	// kvrocks restart: down for a tick, back up (CONFIG SET values lost).
	kv.mu.Lock()
	kv.up = false
	kv.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	kv.amu.Lock()
	kv.ip, kv.port = "", 0
	kv.amu.Unlock()
	kv.mu.Lock()
	kv.up = true
	kv.mu.Unlock()
	waitFor(t, 2*time.Second, func() bool { return kv.count("announce") >= 2 })
	if ip, port, _ := kv.ReplicaAnnounce(context.Background()); ip != "10.0.0.2" || port != 24567 {
		t.Fatalf("not re-applied: %s:%d", ip, port)
	}
}

func TestAnnounceOffWithoutForwarding(t *testing.T) {
	cfg := forwardConfig("kv02")
	cfg.ForwardEnabled = false
	cfg.ReplicaAnnounce = true
	cfg.ReplicaAnnounceIP, cfg.ReplicaAnnouncePort = "10.0.0.2", 24567
	kv := &annKV{fakeKV: &fakeKV{up: true, role: "slave", masterHost: "other", masterPort: 1, lag: 10}}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.1", "", 23001, "sess-x", 3, time.Now()), sess: "sess-x"}
	_, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	waitFor(t, 2*time.Second, func() bool { _, h, _ := kv.state(); return h == "10.0.0.1" })
	time.Sleep(100 * time.Millisecond)
	if n := kv.count("announce"); n != 0 {
		t.Fatalf("no announce without forwarding, got %d", n)
	}
}
