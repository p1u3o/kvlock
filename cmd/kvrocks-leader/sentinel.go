package main

import (
	"context"
	"sync"
	"time"

	"github.com/example/kvrocks-leader/internal/config"
	"github.com/example/kvrocks-leader/internal/consulstore"
	"github.com/example/kvrocks-leader/internal/consulstore/live"
	"github.com/example/kvrocks-leader/internal/sentinel"
)

type leaderSource interface {
	CurrentLeader() (consulstore.LeaderValue, bool)
}

type catalog interface {
	HealthyInstances(ctx context.Context, service, tag string) ([]live.Instance, error)
}

// sentinelSource feeds the Sentinel endpoint: the master from the lock as
// tracked by the controller, replicas from Consul's passing replica
// registrations (cached briefly).
type sentinelSource struct {
	cfg *config.Config
	ctl leaderSource
	cat catalog

	mu      sync.Mutex
	cached  []sentinel.Instance
	fetched time.Time
}

func (s *sentinelSource) Master() (sentinel.Master, bool) {
	lv, ok := s.ctl.CurrentLeader()
	if !ok || lv.Addr == "" || lv.Port == 0 {
		return sentinel.Master{}, false
	}
	ip := lv.Addr
	if s.cfg.SentinelPrivate && lv.PrivateAddr != "" {
		ip = lv.PrivateAddr
	}
	return sentinel.Master{IP: ip, Port: lv.Port, Epoch: lv.Epoch, Node: lv.Node}, true
}

func (s *sentinelSource) Replicas(ctx context.Context) ([]sentinel.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.fetched) < 2*time.Second {
		return s.cached, nil
	}
	svc := s.cfg.ServiceName
	if s.cfg.SentinelPrivate && s.cfg.PrivateServiceName != "" {
		svc = s.cfg.PrivateServiceName
	}
	insts, err := s.cat.HealthyInstances(ctx, svc, s.cfg.ReplicaTag)
	if err != nil {
		return nil, err
	}
	out := make([]sentinel.Instance, 0, len(insts))
	for _, in := range insts {
		out = append(out, sentinel.Instance{Node: in.Node, IP: in.Addr, Port: in.Port})
	}
	s.cached, s.fetched = out, time.Now()
	return out, nil
}
