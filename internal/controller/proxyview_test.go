package controller_test

import (
	"testing"
	"time"

	"github.com/example/kvrocks-leader/internal/controller"
	"github.com/example/kvrocks-leader/internal/elect"
	"github.com/example/kvrocks-leader/internal/logx"
	"github.com/example/kvrocks-leader/internal/metrics"
)

func TestProxyViewLeaderRoutesLocally(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master", local: 10}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(80 * time.Millisecond)
	v := c.ProxyView()
	if v.Leader != "127.0.0.1:6666" || v.Key == "" || v.Replica != "" {
		t.Fatalf("leader view: %+v", v)
	}
}

func TestProxyViewFollowerRoutesToPeerPreferringPrivate(t *testing.T) {
	cfg := testConfig("kv02")
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "1.2.3.4", "100.64.0.1", 6666, "sess-x", 3, time.Now()), sess: "sess-x"}
	kv := &fakeKV{up: true, role: "slave"}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	time.Sleep(80 * time.Millisecond)
	v := c.ProxyView()
	if v.Leader != "100.64.0.1:6666" {
		t.Fatalf("follower view: %+v", v)
	}
	if v.Replica != "127.0.0.1:6666" {
		t.Fatalf("caught-up replica should be readable: %+v", v)
	}
}

func TestProxyViewKeyChangesOnDemotion(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master", local: 10}
	lock := &fakeLock{}
	c, stop := startCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv})
	defer stop()
	notify := c.Subscribe()
	time.Sleep(80 * time.Millisecond)
	before := c.ProxyView()
	// Someone else takes the lock (e.g. our session was invalidated).
	lock.mu.Lock()
	lock.sess = "sess-other"
	lock.val = elect.MarshalLeader("kv02", "10.0.0.2", "", 6666, "sess-other", 9, time.Now())
	lock.mu.Unlock()
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("expected a change notification")
	}
	time.Sleep(80 * time.Millisecond)
	after := c.ProxyView()
	if after.Key == before.Key || after.Leader == "127.0.0.1:6666" {
		t.Fatalf("route must change after losing the lock: before=%+v after=%+v", before, after)
	}
}

func TestProxyViewFailsClosedWithoutConsul(t *testing.T) {
	cfg := testConfig("kv01")
	c := controller.New(cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: &fakeKV{up: true, role: "master"}}, logx.New("kv01", "active"), metrics.New())
	if v := c.ProxyView(); v.Leader != "" || v.Reason == "" {
		t.Fatalf("no Consul contact yet must mean no route: %+v", v)
	}
}
