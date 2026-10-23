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
// INFO replication (verified against kvrocks v2.11.0 src/server/server.cc,
// Server::GetReplicationInfo):
//
//	role:slave                      (replica only:)
//	master_host / master_port / master_link_status:up|down
//	master_sync_in_progress / master_last_io_seconds_ago
//	slave_repl_offset:<local rocksdb latest sequence number>
//	connected_slaves:N
//	slaveK:ip=..,port=..,offset=<replica seq>,lag=<master seq - replica seq>
//	master_repl_offset:<local rocksdb latest sequence number>
//
// On a kvrocks replica master_repl_offset is the replica's OWN sequence
// number (identical to slave_repl_offset), not the master's, so a replica
// cannot compute its lag from its own INFO. Redis behaves the same way in
// practice (both fields track the replica's position). Lag is therefore
// computed by the controller from the leader's master_repl_offset, sampled
// while the leader is reachable, minus our local offset. kvrocks offsets
// are sequence numbers (write batches), not bytes.
//
// Read-only flag: kvrocks only knows slave-read-only (default yes); Redis
// accepts both replica-read-only and slave-read-only. It only has an effect
// on replicas, which is why fencing uses REPLICAOF.
package kvrocks

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ReplicationInfo is the parsed subset of INFO replication + ROLE we need.
type ReplicationInfo struct {
	Role            string // "master" | "slave"
	MasterHost      string
	MasterPort      int
	MasterLinkUp    bool
	LocalOffset     int64 // this instance's replication offset / sequence
	MasterOffset    int64 // master_repl_offset when role=master, else -1
	LagBytes        int64 // 0 on a master; -1 (unknown) on a replica
	ConnectedSlaves int
	Replicas        []ReplicaState // slaveN lines (master only)
	Raw             string
}

// ReplicaState is one slaveN line as seen by a master.
type ReplicaState struct {
	IP     string
	Port   int
	Offset int64
	// State is Redis' state= field ("online", "wait_bgsave", ...); kvrocks
	// does not report one and leaves it empty.
	State string
}

// Client is the sidecar's view of the local kvrocks.
type Client interface {
	// Ping checks liveness.
	Ping(ctx context.Context) error
	// InfoReplication returns parsed replication state.
	InfoReplication(ctx context.Context) (*ReplicationInfo, error)
	// PeerInfoReplication reads INFO replication from another instance
	// (the current leader) using the same credentials.
	PeerInfoReplication(ctx context.Context, host string, port int) (*ReplicationInfo, error)
	// ReplicaOf points local at a master; pass ("NO","ONE") to promote.
	// Implementations must try REPLICAOF first, SLAVEOF as fallback.
	ReplicaOf(ctx context.Context, host string, port int) error
	// Promote makes local a writable master.
	Promote(ctx context.Context) error
	// EnsureRsIDPsync sets use-rsid-psync yes (best effort).
	EnsureRsIDPsync(ctx context.Context) error
	// SetReadOnly toggles write access (best effort across flag names).
	SetReadOnly(ctx context.Context, readonly bool) error
	// KillClients closes normal client connections on the local instance
	// (never replication links or the sidecar's own connection) and
	// returns how many were closed.
	KillClients(ctx context.Context) (int64, error)
	// Close releases resources.
	Close() error
}

// ClientName is set on the sidecar's own connections (CLIENT SETNAME) so
// they can be recognised in CLIENT LIST.
const ClientName = "kvlock-sidecar"

// --- go-redis implementation ---

// RedisClient is the production Client.
type RedisClient struct {
	rdb      *redis.Client
	timeout  time.Duration
	kind     string // "kvrocks" or "redis"
	password string

	peerMu   sync.Mutex
	peerAddr string
	peer     *redis.Client
}

// New creates a client for addr:port with optional password.
// kind is "kvrocks" (default) or "redis"; unknown kinds behave like kvrocks.
func New(addr string, port int, password string, timeout time.Duration, kind ...string) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:        fmt.Sprintf("%s:%d", addr, port),
		Password:    password,
		DialTimeout: timeout,
		ReadTimeout: timeout,
		ClientName:  ClientName,
		// One connection: the sidecar's load is tiny, and it means
		// CLIENT KILL ... SKIPME yes spares every sidecar connection.
		PoolSize: 1,
	})
	k := "kvrocks"
	if len(kind) > 0 && kind[0] != "" {
		k = kind[0]
	}
	return &RedisClient{rdb: rdb, timeout: timeout, kind: k, password: password}
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

func (c *RedisClient) PeerInfoReplication(ctx context.Context, host string, port int) (*ReplicationInfo, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	c.peerMu.Lock()
	if c.peer == nil || c.peerAddr != addr {
		if c.peer != nil {
			_ = c.peer.Close()
		}
		c.peer = redis.NewClient(&redis.Options{
			Addr: addr, Password: c.password,
			DialTimeout: c.timeout, ReadTimeout: c.timeout,
			PoolSize: 1, MaxRetries: -1,
		})
		c.peerAddr = addr
	}
	peer := c.peer
	c.peerMu.Unlock()
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	s, err := peer.Info(ctx, "replication").Result()
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
	// kvrocks-only setting for partial resync on failover; Redis has no
	// such config, so skip it there instead of logging a failure.
	if c.kind == "redis" {
		return nil
	}
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

// KillClients runs CLIENT KILL TYPE normal SKIPME yes. kvrocks (2.11:
// src/commands/cmd_server.cc, Worker::KillClient) and Redis >= 2.8.12
// support it; TYPE normal excludes replica and master links and pub/sub
// clients, and SKIPME yes spares the issuing connection (the sidecar's
// only one, PoolSize 1). kvrocks defaults SKIPME to no, so it is always
// sent explicitly. kvrocks ORs filters together, so no other filter is
// combined with TYPE.
//
// If the server rejects the TYPE form, fall back to CLIENT LIST and kill
// by ID every client that is not a replica/master/monitor link ("S"/"M"
// flags) and not named ClientName.
func (c *RedisClient) KillClients(ctx context.Context) (int64, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	n, err := c.rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Int64()
	if err == nil {
		return n, nil
	}
	if !isUnknownCommand(err) && !strings.Contains(strings.ToLower(err.Error()), "syntax") {
		return 0, err
	}
	list, lerr := c.rdb.Do(ctx, "CLIENT", "LIST").Text()
	if lerr != nil {
		return 0, fmt.Errorf("client kill type unsupported (%v) and client list failed: %w", err, lerr)
	}
	var killed int64
	var firstErr error
	for _, id := range KillableClientIDs(list) {
		if e := c.rdb.Do(ctx, "CLIENT", "KILL", "ID", id).Err(); e != nil {
			if firstErr == nil {
				firstErr = e
			}
			continue
		}
		killed++
	}
	return killed, firstErr
}

// KillableClientIDs parses CLIENT LIST output and returns the IDs of
// ordinary clients: not replicas or master links (flags S or M; on kvrocks
// M means MONITOR, also spared), not pub/sub (sub/psub > 0 where reported)
// and not the sidecar itself (name=ClientName).
func KillableClientIDs(list string) []string {
	var ids []string
	for _, line := range strings.Split(list, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		f := map[string]string{}
		for _, kv := range strings.Fields(line) {
			if k, v, ok := strings.Cut(kv, "="); ok {
				f[k] = v
			}
		}
		if f["id"] == "" || f["name"] == ClientName {
			continue
		}
		if strings.ContainsAny(f["flags"], "SM") {
			continue
		}
		if (f["sub"] != "" && f["sub"] != "0") || (f["psub"] != "" && f["psub"] != "0") {
			continue
		}
		ids = append(ids, f["id"])
	}
	return ids
}

func (c *RedisClient) Close() error {
	c.peerMu.Lock()
	if c.peer != nil {
		_ = c.peer.Close()
	}
	c.peerMu.Unlock()
	return c.rdb.Close()
}

func isUnknownCommand(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unknown") || strings.Contains(s, "unsupported") || strings.Contains(s, "no such")
}

// ParseInfoReplication parses INFO replication output from kvrocks or
// Redis. A replica's lag is always reported unknown (-1) here because its
// own INFO does not contain the master's offset; see the package comment.
func ParseInfoReplication(s string) *ReplicationInfo {
	ri := &ReplicationInfo{LocalOffset: -1, MasterOffset: -1, LagBytes: -1, Raw: s}
	kv := map[string]string{}
	var slaveLines []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		kv[k] = v
		if isSlaveN(k) {
			slaveLines = append(slaveLines, v)
		}
	}
	ri.Role = kv["role"]
	if ri.Role == "" {
		ri.Role = "unknown"
	}
	ri.MasterHost = kv["master_host"]
	if p, err := strconv.Atoi(kv["master_port"]); err == nil {
		ri.MasterPort = p
	}
	ri.MasterLinkUp = kv["master_link_status"] == "up"
	if n, err := strconv.Atoi(kv["connected_slaves"]); err == nil {
		ri.ConnectedSlaves = n
	}
	masterOff, masterOK := parseInt(kv["master_repl_offset"])
	switch ri.Role {
	case "master":
		if masterOK {
			ri.MasterOffset = masterOff
			ri.LocalOffset = masterOff
		}
		ri.LagBytes = 0
		for _, l := range slaveLines {
			if rs, ok := parseSlaveLine(l); ok {
				ri.Replicas = append(ri.Replicas, rs)
			}
		}
	case "slave":
		if o, ok := parseInt(kv["slave_repl_offset"]); ok {
			ri.LocalOffset = o
		} else if masterOK {
			ri.LocalOffset = masterOff
		}
	}
	return ri
}

func parseInt(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}

func isSlaveN(k string) bool {
	if !strings.HasPrefix(k, "slave") || len(k) == len("slave") {
		return false
	}
	_, err := strconv.Atoi(k[len("slave"):])
	return err == nil
}

// parseSlaveLine parses "ip=..,port=..,offset=..[,...]" (kvrocks and
// Redis >= 2.8 key=value form).
func parseSlaveLine(v string) (ReplicaState, bool) {
	var rs ReplicaState
	ok := false
	for _, part := range strings.Split(v, ",") {
		k, val, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		switch k {
		case "ip":
			rs.IP = val
		case "port":
			rs.Port, _ = strconv.Atoi(val)
		case "state":
			rs.State = val
		case "offset":
			if n, good := parseInt(val); good {
				rs.Offset = n
				ok = true
			}
		}
	}
	return rs, ok
}
