package main

import (
	"context"
	"testing"

	"github.com/example/kvrocks-leader/internal/config"
	"github.com/example/kvrocks-leader/internal/consulstore"
	"github.com/example/kvrocks-leader/internal/consulstore/live"
)

type fixedLeader struct {
	lv consulstore.LeaderValue
	ok bool
}

func (f fixedLeader) CurrentLeader() (consulstore.LeaderValue, bool) { return f.lv, f.ok }

type fakeCatalog struct {
	calls     int
	svc, tag  string
	instances []live.Instance
}

func (f *fakeCatalog) HealthyInstances(_ context.Context, svc, tag string) ([]live.Instance, error) {
	f.calls++
	f.svc, f.tag = svc, tag
	return f.instances, nil
}

func TestSentinelSource(t *testing.T) {
	cfg := &config.Config{ServiceName: "kvrocks", PrivateServiceName: "kvrocks-private", ReplicaTag: "replica"}
	cat := &fakeCatalog{instances: []live.Instance{{Node: "kv02", Addr: "10.0.0.2", Port: 6666}}}
	lv := consulstore.LeaderValue{Node: "kv01", Addr: "1.2.3.4", PrivateAddr: "100.64.0.1", Port: 6666, Epoch: 4}
	s := &sentinelSource{cfg: cfg, ctl: fixedLeader{lv: lv, ok: true}, cat: cat}

	m, ok := s.Master()
	if !ok || m.IP != "1.2.3.4" || m.Port != 6666 || m.Epoch != 4 {
		t.Fatalf("got %+v %v", m, ok)
	}
	reps, _ := s.Replicas(context.Background())
	_, _ = s.Replicas(context.Background())
	if len(reps) != 1 || reps[0].IP != "10.0.0.2" || cat.calls != 1 || cat.svc != "kvrocks" || cat.tag != "replica" {
		t.Fatalf("reps=%+v calls=%d svc=%s", reps, cat.calls, cat.svc)
	}

	cfg.SentinelPrivate = true
	s = &sentinelSource{cfg: cfg, ctl: fixedLeader{lv: lv, ok: true}, cat: cat}
	if m, _ := s.Master(); m.IP != "100.64.0.1" {
		t.Fatalf("private: %+v", m)
	}
	_, _ = s.Replicas(context.Background())
	if cat.svc != "kvrocks-private" {
		t.Fatalf("private service: %s", cat.svc)
	}

	s = &sentinelSource{cfg: cfg, ctl: fixedLeader{}, cat: cat}
	if _, ok := s.Master(); ok {
		t.Fatal("vacant lock must report no master")
	}
}
