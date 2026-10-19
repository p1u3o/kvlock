// Package live implements consulstore interfaces with hashicorp consul/api.
package live

import (
	"context"
	"fmt"
	"time"

	consul "github.com/hashicorp/consul/api"
)

// Client bundles session, lock and registrar over one *consul.Client.
type Client struct {
	c *consul.Client
}

func New(addr, token, caFile string, skipVerify bool) (*Client, error) {
	cfg := consul.DefaultConfig()
	if addr != "" {
		cfg.Address = addr
	}
	if token != "" {
		cfg.Token = token
	}
	if caFile != "" || skipVerify {
		cfg.Scheme = "https"
		cfg.TLSConfig = consul.TLSConfig{
			CAFile:             caFile,
			InsecureSkipVerify: skipVerify,
		}
	}
	c, err := consul.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{c: c}, nil
}

// --- Session ---

func (l *Client) Create(ctx context.Context, ttl, lockDelay time.Duration) (string, error) {
	se := l.c.Session()
	id, _, err := se.Create(&consul.SessionEntry{
		Name:      "kvrocks-leader",
		Behavior:  consul.SessionBehaviorRelease,
		TTL:       ttl.String(),
		LockDelay: lockDelay,
	}, nil)
	if err != nil {
		return "", err
	}
	// Prime periodic renew via session renew goroutine? Caller renews explicitly.
	_ = ctx
	return id, nil
}

func (l *Client) Renew(ctx context.Context, id string) error {
	_, _, err := l.c.Session().Renew(id, nil)
	_ = ctx
	return err
}

func (l *Client) Destroy(ctx context.Context, id string) error {
	_, err := l.c.Session().Destroy(id, nil)
	_ = ctx
	return err
}

func (l *Client) Valid(id string) bool {
	if id == "" {
		return false
	}
	_, _, err := l.c.Session().Info(id, nil)
	return err == nil
}

// --- Lock ---

func (l *Client) Acquire(ctx context.Context, key, session string, value []byte) (bool, error) {
	kv := l.c.KV()
	ok, _, err := kv.Acquire(&consul.KVPair{Key: key, Session: session, Value: value}, nil)
	_ = ctx
	return ok, err
}

func (l *Client) Release(ctx context.Context, key, session string) error {
	kv := l.c.KV()
	p, _, err := kv.Get(key, nil)
	if err != nil {
		return err
	}
	if p == nil || p.Session != session {
		return nil // not ours
	}
	_, err = kv.Delete(key, nil)
	_ = ctx
	return err
}

func (l *Client) Get(ctx context.Context, key string) ([]byte, string, uint64, error) {
	p, meta, err := l.c.KV().Get(key, nil)
	_ = ctx
	if err != nil {
		return nil, "", 0, err
	}
	if p == nil {
		var idx uint64
		if meta != nil {
			idx = meta.LastIndex
		}
		return nil, "", idx, nil
	}
	return p.Value, p.Session, p.ModifyIndex, nil
}

func (l *Client) Watch(ctx context.Context, key string, waitIndex uint64, waitTime time.Duration) ([]byte, string, uint64, error) {
	q := &consul.QueryOptions{WaitIndex: waitIndex, WaitTime: waitTime}
	q = q.WithContext(ctx)
	p, meta, err := l.c.KV().Get(key, q)
	if err != nil {
		return nil, "", waitIndex, err
	}
	if p == nil {
		var idx uint64 = waitIndex
		if meta != nil {
			idx = meta.LastIndex
		}
		return nil, "", idx, nil
	}
	return p.Value, p.Session, p.ModifyIndex, nil
}

// --- Registrar ---

func serviceID(service, node string) string { return fmt.Sprintf("%s-%s", service, node) }

func (l *Client) RegisterMaster(ctx context.Context, service, node, addr string, port int, tags []string, ttl time.Duration) error {
	return l.register(ctx, serviceID(service, node), service, addr, port, tags, ttl)
}

func (l *Client) RegisterReplica(ctx context.Context, service, node, addr string, port int, tags []string, ttl time.Duration) error {
	return l.register(ctx, serviceID(service, node), service, addr, port, tags, ttl)
}

func (l *Client) register(ctx context.Context, id, service, addr string, port int, tags []string, ttl time.Duration) error {
	reg := &consul.AgentServiceRegistration{
		ID:      id,
		Name:    service,
		Address: addr,
		Port:    port,
		Tags:    tags,
		Check: &consul.AgentServiceCheck{
			TTL:    ttl.String(),
			Status: consul.HealthPassing,
		},
	}
	_ = ctx
	return l.c.Agent().ServiceRegister(reg)
}

func (l *Client) PassTTL(ctx context.Context, checkID string) error {
	_ = ctx
	return l.c.Agent().PassTTL(checkID, "")
}

func (l *Client) FailTTL(ctx context.Context, checkID string) error {
	_ = ctx
	return l.c.Agent().FailTTL(checkID, "")
}

func (l *Client) Deregister(ctx context.Context, serviceID string) error {
	_ = ctx
	return l.c.Agent().ServiceDeregister(serviceID)
}

func (l *Client) CheckID(serviceID string) string { return "service:" + serviceID }
