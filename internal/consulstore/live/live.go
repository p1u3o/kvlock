// Package live implements consulstore interfaces with hashicorp consul/api.
package live

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	consul "github.com/hashicorp/consul/api"

	"github.com/p1u3o/kvlock/internal/consulstore"
)

// Client bundles session, lock and registrar over one *consul.Client.
type Client struct {
	c *consul.Client

	privService, privAddr string
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
//
// Consul stores the request body of a release as the new value, so an
// empty body wipes the epoch, the previous-leader record and any handover
// hint. Read the current value first and send it back unchanged. Only the
// holding session can change the value, so it cannot move underneath us.
func (l *Client) Release(ctx context.Context, key, session string) error {
	if session == "" {
		return nil
	}
	p, _, err := l.c.KV().Get(key, (&consul.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return err
	}
	if p == nil || p.Session != session {
		return nil
	}
	q := (&consul.WriteOptions{}).WithContext(ctx)
	_, _, err = l.c.KV().Release(&consul.KVPair{Key: key, Session: session, Value: p.Value}, q)
	return err
}

// BumpEpoch stores and returns a counter value that is greater than the
// stored one and at least min, using check-and-set so concurrent bumps
// never hand out the same number.
func (l *Client) BumpEpoch(ctx context.Context, key string, min uint64) (uint64, error) {
	for i := 0; i < 5; i++ {
		p, _, err := l.c.KV().Get(key, (&consul.QueryOptions{}).WithContext(ctx))
		if err != nil {
			return 0, err
		}
		var cur, idx uint64
		if p != nil {
			idx = p.ModifyIndex
			if n, err := strconv.ParseUint(strings.TrimSpace(string(p.Value)), 10, 64); err == nil {
				cur = n
			}
		}
		next := cur + 1
		if next < min {
			next = min
		}
		ok, _, err := l.c.KV().CAS(&consul.KVPair{Key: key, Value: []byte(strconv.FormatUint(next, 10)), ModifyIndex: idx}, (&consul.WriteOptions{}).WithContext(ctx))
		if err != nil {
			return 0, err
		}
		if ok {
			return next, nil
		}
	}
	return 0, errors.New("epoch counter: check-and-set kept losing")
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

// SetPrivate makes every registration also publish a second service entry
// (privateService) carrying the private address, with the same tags and
// lifecycle. Both entries carry the other address in TaggedAddresses.
func (l *Client) SetPrivate(privateService, privateAddr string) {
	l.privService, l.privAddr = privateService, privateAddr
}

// privID maps a public service ID to its private twin, "" when disabled.
func (l *Client) privID(id string) string {
	if l.privAddr == "" || l.privService == "" {
		return ""
	}
	return "private:" + id
}

func (l *Client) RegisterMaster(ctx context.Context, service, node, addr string, port int, tags []string, ttl time.Duration) error {
	return l.registerBoth(ctx, serviceID(service, node), service, addr, port, tags, ttl)
}

func (l *Client) RegisterReplica(ctx context.Context, service, node, addr string, port int, tags []string, ttl time.Duration) error {
	return l.registerBoth(ctx, serviceID(service, node), service, addr, port, tags, ttl)
}

func (l *Client) registerBoth(ctx context.Context, id, service, addr string, port int, tags []string, ttl time.Duration) error {
	tagged := map[string]consul.ServiceAddress(nil)
	if l.privID(id) != "" {
		tagged = map[string]consul.ServiceAddress{
			"public":  {Address: addr, Port: port},
			"private": {Address: l.privAddr, Port: port},
		}
	}
	if err := l.register(ctx, id, service, addr, port, tags, ttl, tagged); err != nil {
		return err
	}
	if pid := l.privID(id); pid != "" {
		return l.register(ctx, pid, l.privService, l.privAddr, port, tags, ttl, tagged)
	}
	return nil
}

func (l *Client) register(ctx context.Context, id, service, addr string, port int, tags []string, ttl time.Duration, tagged map[string]consul.ServiceAddress) error {
	reg := &consul.AgentServiceRegistration{
		TaggedAddresses: tagged,
		ID:              id,
		Name:            service,
		Address:         addr,
		Port:            port,
		Tags:            tags,
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
	err := l.c.Agent().PassTTL(checkID, "")
	if pid := l.privID(strings.TrimPrefix(checkID, "service:")); pid != "" {
		err = errors.Join(err, l.c.Agent().PassTTL(l.CheckID(pid), ""))
	}
	return err
}

func (l *Client) FailTTL(ctx context.Context, checkID string) error {
	_ = ctx
	err := l.c.Agent().FailTTL(checkID, "")
	if pid := l.privID(strings.TrimPrefix(checkID, "service:")); pid != "" {
		err = errors.Join(err, l.c.Agent().FailTTL(l.CheckID(pid), ""))
	}
	return err
}

func (l *Client) Deregister(ctx context.Context, serviceID string) error {
	_ = ctx
	err := l.c.Agent().ServiceDeregister(serviceID)
	if pid := l.privID(serviceID); pid != "" {
		err = errors.Join(err, l.c.Agent().ServiceDeregister(pid))
	}
	return err
}

func (l *Client) CheckID(serviceID string) string { return "service:" + serviceID }

// Instance is a healthy catalog entry.
type Instance struct {
	Node string
	Addr string
	Port int
}

// HealthyInstances lists passing instances of service carrying tag.
func (l *Client) HealthyInstances(ctx context.Context, service, tag string) ([]Instance, error) {
	entries, _, err := l.c.Health().Service(service, tag, true, (&consul.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return nil, err
	}
	out := make([]Instance, 0, len(entries))
	for _, e := range entries {
		addr := e.Service.Address
		if addr == "" && e.Node != nil {
			addr = e.Node.Address
		}
		node := ""
		if e.Node != nil {
			node = e.Node.Node
		}
		out = append(out, Instance{Node: node, Addr: addr, Port: e.Service.Port})
	}
	return out, nil
}
