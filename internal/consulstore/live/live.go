// Package live implements consulstore interfaces with hashicorp consul/api.
package live

import (
	"context"
	"fmt"
	"time"

	consul "github.com/hashicorp/consul/api"

	"github.com/example/kvrocks-leader/internal/consulstore"
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

// Create makes a TTL session with behavior=release and NO health checks.
//
// Consul's default for a session with no explicit checks is to attach the
// node's serfHealth check, which means any node-level critical check (or a
// serf flap) would invalidate the session and drop the lock. We create the
// session with an explicitly empty NodeChecks list so the TTL, which we
// renew ourselves, is the only thing that can invalidate it.
func (l *Client) Create(ctx context.Context, ttl, lockDelay time.Duration) (string, error) {
	q := (&consul.WriteOptions{}).WithContext(ctx)
	id, _, err := l.c.Session().CreateNoChecks(&consul.SessionEntry{
		Name:      "kvrocks-leader",
		Behavior:  consul.SessionBehaviorRelease,
		TTL:       ttl.String(),
		LockDelay: lockDelay,
	}, q)
	if err != nil {
		return "", err
	}
	return id, nil
}

// Renew refreshes the session TTL. A 404 from Consul (session gone) is
// reported as consulstore.ErrSessionInvalid so callers can tell "the lock
// is definitely lost" apart from "Consul is temporarily unreachable".
func (l *Client) Renew(ctx context.Context, id string) error {
	q := (&consul.WriteOptions{}).WithContext(ctx)
	entry, _, err := l.c.Session().Renew(id, q)
	if err != nil {
		return err
	}
	if entry == nil {
		return consulstore.ErrSessionInvalid
	}
	return nil
}

func (l *Client) Destroy(ctx context.Context, id string) error {
	q := (&consul.WriteOptions{}).WithContext(ctx)
	_, err := l.c.Session().Destroy(id, q)
	return err
}

// Valid reports whether the session still exists. The error is non-nil
// only when Consul could not be asked.
func (l *Client) Valid(ctx context.Context, id string) (bool, error) {
	if id == "" {
		return false, nil
	}
	q := (&consul.QueryOptions{}).WithContext(ctx)
	entry, _, err := l.c.Session().Info(id, q)
	if err != nil {
		return false, err
	}
	return entry != nil, nil
}

// --- Lock ---

func (l *Client) Acquire(ctx context.Context, key, session string, value []byte) (bool, error) {
	q := (&consul.WriteOptions{}).WithContext(ctx)
	ok, _, err := l.c.KV().Acquire(&consul.KVPair{Key: key, Session: session, Value: value}, q)
	return ok, err
}

// Release uses Consul's ?release=<session> operation, which is atomic on
// the server: it only clears the lock if this session currently holds it.
// The value is left in place (it carries the epoch); holders are identified
// by the session field, not by the value.
func (l *Client) Release(ctx context.Context, key, session string) error {
	if session == "" {
		return nil
	}
	q := (&consul.WriteOptions{}).WithContext(ctx)
	_, _, err := l.c.KV().Release(&consul.KVPair{Key: key, Session: session}, q)
	return err
}

func (l *Client) Get(ctx context.Context, key string) ([]byte, string, uint64, error) {
	p, meta, err := l.c.KV().Get(key, (&consul.QueryOptions{}).WithContext(ctx))
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
