// Package kvrocks abstracts the local kvrocks instance behind an interface
// so the election state machine is unit-testable without a real server.
//
// Supported-commands findings (kvrocks docs, Oct 2026):
//   - SLAVEOF host port / SLAVEOF NO ONE supported since v1.0.0.
//   - REPLICAOF host port / REPLICAOF NO ONE supported since v2.11.0.
//     We send REPLICAOF and fall back to SLAVEOF for older servers.
//   - ROLE supported since v1.1.30; INFO replication reports
//     role:master|slave plus master_repl_offset on masters.
//   - CONFIG GET/SET supported since v1.0.0; use-rsid-psync exists in
//     kvrocks.conf (default "no"). We set "yes" so a failover can PSYNC.
//   - PING, INFO supported since v1.0.0.
//
// UNVERIFIED (see README): the exact field names kvrocks reports for a
// replica's lag (Redis uses master_repl_offset vs slave_repl_offset and
// slave0:ip,port,offset). Kvrocks INFO replication documents role and
// master_repl_offset only; replica-side field names are parsed
// defensively and lag may be reported unknown. Likewise the read-only
// flag name (Redis replica-read-only vs slave-read-only) is applied
// best-effort and failures are logged, not fatal.
package kvrocks

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ReplicationInfo is the parsed subset of INFO replication + ROLE we need.
type ReplicationInfo struct {
	Role            string // "master" | "slave"
	MasterHost      string
	MasterPort      int
	MasterLinkUp    bool
	LocalOffset     int64
	MasterOffset    int64 // our copy of master's offset, -1 if unknown
	LagBytes        int64 // MasterOffset-LocalOffset, -1 if unknown
	ConnectedSlaves int
	Raw             string
}

// Client is the sidecar's view of the local kvrocks.
type Client interface {
	// Ping checks liveness.
	Ping(ctx context.Context) error
	// InfoReplication returns parsed replication state.
	InfoReplication(ctx context.Context) (*ReplicationInfo, error)
	// ReplicaOf points local at a master; pass ("NO","ONE") to promote.
	// Implementations must try REPLICAOF first, SLAVEOF as fallback.
	ReplicaOf(ctx context.Context, host string, port int) error
	// Promote makes local a writable master.
	Promote(ctx context.Context) error
	// EnsureRsIDPsync sets use-rsid-psync yes (best effort).
	EnsureRsIDPsync(ctx context.Context) error
	// SetReadOnly toggles write access (best effort across flag names).
	SetReadOnly(ctx context.Context, readonly bool) error
	// Close releases resources.
	Close() error
}

// --- go-redis implementation ---

// RedisClient is the production Client.
type RedisClient struct {
	rdb     *redis.Client
	timeout time.Duration
}

// New creates a client for addr:port with optional password.
func New(addr string, port int, password string, timeout time.Duration) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:%d", addr, port),
		Password:    password,
		DialTimeout: timeout,
		ReadTimeout: timeout,
	})
	return &RedisClient{rdb: rdb, timeout: timeout}
}

func (c *RedisClient) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

func (c *RedisClient) Ping(ctx context.Context) error {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	return c.rdb.Ping(ctx).Err()
}

func (c *RedisClient) InfoReplication(ctx context.Context) (*ReplicationInfo, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	s, err := c.rdb.Info(ctx, "replication").Result()
	if err != nil {
		return nil, err
	}
	return ParseInfoReplication(s), nil
}

func (c *RedisClient) ReplicaOf(ctx context.Context, host string, port int) error {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	var err error
	if port == 0 {
		// Promote: REPLICAOF NO ONE, fallback SLAVEOF NO ONE.
		err = c.rdb.Do(ctx, "REPLICAOF", "NO", "ONE").Err()
		if err != nil && isUnknownCommand(err) {
			err = c.rdb.Do(ctx, "SLAVEOF", "NO", "ONE").Err()
		}
		return err
	}
	err = c.rdb.Do(ctx, "REPLICAOF", host, strconv.Itoa(port)).Err()
	if err != nil && isUnknownCommand(err) {
		err = c.rdb.Do(ctx, "SLAVEOF", host, strconv.Itoa(port)).Err()
	}
	return err
}

func (c *RedisClient) Promote(ctx context.Context) error {
	if err := c.ReplicaOf(ctx, "NO", 0); err != nil {
		return err
	}
	// Best effort: make writable.
	_ = c.SetReadOnly(ctx, false)
	return nil
}

func (c *RedisClient) EnsureRsIDPsync(ctx context.Context) error {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	// CONFIG GET to check first; ignore errors (old versions may hide it).
	if v, err := c.rdb.Do(ctx, "CONFIG", "GET", "use-rsid-psync").Result(); err == nil {
		_ = v
	}
	if err := c.rdb.Do(ctx, "CONFIG", "SET", "use-rsid-psync", "yes").Err(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) SetReadOnly(ctx context.Context, readonly bool) error {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	val := "no"
	if readonly {
		val = "yes"
	}
	// Try modern name first, then legacy. Both best-effort.
	if err := c.rdb.Do(ctx, "CONFIG", "SET", "replica-read-only", val).Err(); err == nil {
		return nil
	}
	if err := c.rdb.Do(ctx, "CONFIG", "SET", "slave-read-only", val).Err(); err != nil {
		return err
	}
	return nil
}

func (c *RedisClient) Close() error { return c.rdb.Close() }

func isUnknownCommand(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown") || strings.Contains(s, "unsupported") || strings.Contains(s, "no such")
}

// ParseInfoReplication parses INFO replication output defensively.
// Unknown replica fields -> LagBytes = -1 (unknown).
func ParseInfoReplication(s string) *ReplicationInfo {
	ri := &ReplicationInfo{MasterOffset: -1, LagBytes: -1, Raw: s}
	kv := map[string]string{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		kv[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	ri.Role = kv["role"]
	if ri.Role == "" {
		ri.Role = "unknown"
	}
	ri.MasterHost = kv["master_host"]
	if p, err := strconv.Atoi(kv["master_port"]); err == nil {
		ri.MasterPort = p
	}
	if st := kv["master_link_status"]; st != "" {
		ri.MasterLinkUp = st == "up"
	}
	if o, err := strconv.ParseInt(kv["master_repl_offset"], 10, 64); err == nil {
		ri.MasterOffset = o
		ri.LocalOffset = o
	}
	// Replica-side offsets seen across Redis/kvrocks variants.
	for _, k := range []string{"slave_repl_offset", "replica_repl_offset", "repl_offset"} {
		if o, err := strconv.ParseInt(kv[k], 10, 64); err == nil {
			ri.LocalOffset = o
			break
		}
	}
	if n, err := strconv.Atoi(kv["connected_slaves"]); err == nil {
		ri.ConnectedSlaves = n
	}
	// slave0:ip,port,offset gives the master's view; prefer explicit offsets above.
	if ri.MasterOffset >= 0 && ri.Role == "slave" {
		// If master offset unknown on replica, try slave0 line? It isn't present on replicas.
		// Lag stays unknown unless master offset known.
		if _, ok := kv["master_repl_offset"]; ok {
			ri.LagBytes = ri.MasterOffset - ri.LocalOffset
			if ri.LagBytes < 0 {
				ri.LagBytes = 0
			}
		}
	}
	return ri
}
