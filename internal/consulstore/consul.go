// Package consulstore abstracts Consul behind small interfaces so the
// election state machine is unit-testable.
package consulstore

import (
	"context"
	"errors"
	"time"
)

// ErrSessionInvalid means Consul answered and the session no longer exists
// (expired, destroyed, or invalidated). Any lock it held is gone.
var ErrSessionInvalid = errors.New("consul session invalid")

// LeaderValue is the JSON stored at kvrocks/<cluster>/leader.
type LeaderValue struct {
	Node    string `json:"node"`
	Addr    string `json:"addr"`
	Port    int    `json:"port"`
	Session string `json:"session"`
	Since   string `json:"since"`
	Epoch   uint64 `json:"epoch"`
}

// Session handles Consul session lifecycle.
type Session interface {
	// Create makes a session with TTL, behavior=release, lock-delay and no
	// health checks attached; returns session ID.
	Create(ctx context.Context, ttl, lockDelay time.Duration) (string, error)
	// Renew refreshes the TTL. ErrSessionInvalid means the session is gone;
	// any other error means Consul could not be reached.
	Renew(ctx context.Context, id string) error
	// Destroy releases the session.
	Destroy(ctx context.Context, id string) error
	// Valid reports whether the session still exists; err is set only when
	// Consul could not be asked.
	Valid(ctx context.Context, id string) (bool, error)
}

// Lock is the KV leader key with session-based acquire.
type Lock interface {
	// Acquire tries to hold key with session+value. Returns held?
	Acquire(ctx context.Context, key, session string, value []byte) (bool, error)
	// Release atomically gives up the lock if (and only if) session holds it.
	Release(ctx context.Context, key, session string) error
	// Get returns current value, session holder, modify index.
	Get(ctx context.Context, key string) (value []byte, session string, modifyIndex uint64, err error)
	// Watch blocks until key changes past waitIndex; returns new value/session/index.
	Watch(ctx context.Context, key string, waitIndex uint64, waitTime time.Duration) ([]byte, string, uint64, error)
}

// Registrar manages the Consul service catalog entries.
type Registrar interface {
	// RegisterMaster advertises this node as master with a TTL check.
	RegisterMaster(ctx context.Context, service, node, addr string, port int, tags []string, ttl time.Duration) error
	// RegisterReplica advertises this node as replica with a TTL check.
	RegisterReplica(ctx context.Context, service, node, addr string, port int, tags []string, ttl time.Duration) error
	// PassTTL marks the check passing.
	PassTTL(ctx context.Context, checkID string) error
	// FailTTL marks the check critical (fast demote on isolation).
	FailTTL(ctx context.Context, checkID string) error
	// Deregister removes serviceID.
	Deregister(ctx context.Context, serviceID string) error
	// CheckID returns the TTL check id for serviceID.
	CheckID(serviceID string) string
}
