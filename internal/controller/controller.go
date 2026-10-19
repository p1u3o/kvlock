// Package controller runs the sidecar control loops.
package controller

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/example/kvrocks-leader/internal/config"
	"github.com/example/kvrocks-leader/internal/consulstore"
	"github.com/example/kvrocks-leader/internal/elect"
	"github.com/example/kvrocks-leader/internal/httpapi"
	"github.com/example/kvrocks-leader/internal/kvrocks"
	"github.com/example/kvrocks-leader/internal/logx"
	"github.com/example/kvrocks-leader/internal/metrics"
)

// Deps are the external systems behind interfaces.
type Deps struct {
	Session   consulstore.Session
	Lock      consulstore.Lock
	Registrar consulstore.Registrar
	KV        kvrocks.Client
}

// Controller owns runtime state.
type Controller struct {
	cfg  *config.Config
	deps Deps
	log  *logx.Logger
	reg  *metrics.Registry

	mu             sync.RWMutex
	role           elect.Role
	leader         *consulstore.LeaderValue
	lag            int64
	kvUp           bool
	sessionID      string
	sessionValid   bool
	unhealthy      int
	leaderSince    time.Time
	elections      uint64
	leaderChanges  uint64
	lastLeader     *consulstore.LeaderValue
	lastLeaderSeen time.Time

	rng *rand.Rand
}

// New builds a controller.
func New(cfg *config.Config, deps Deps, log *logx.Logger, reg *metrics.Registry) *Controller {
	return &Controller{cfg: cfg, deps: deps, log: log, reg: reg, role: elect.RoleUnknown, lag: -1, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (c *Controller) Snapshot() httpapi.Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := httpapi.Snapshot{Role: c.role, LagBytes: c.lag, KvrocksUp: c.kvUp, Mode: c.cfg.Mode, SessionValid: c.sessionValid, UnhealthyStreak: c.unhealthy}
	if c.leader != nil {
		s.Leader = c.leader.Node
		s.LeaderAddr = c.leader.Addr
		s.Epoch = c.leader.Epoch
	}
	return s
}

func (c *Controller) Metrics() *metrics.Registry { return c.reg }

func (c *Controller) setRole(r elect.Role) {
	c.mu.Lock()
	if c.role != r {
		c.role = r
		c.leaderChanges++
		c.reg.Inc("leader_changes_total")
	}
	if r == elect.RoleLeader {
		c.reg.SetGauge("is_leader", 1)
	} else {
		c.reg.SetGauge("is_leader", 0)
	}
	c.mu.Unlock()
}

// serviceID for our registration.
func (c *Controller) serviceID() string { return c.cfg.ServiceName + "-" + c.cfg.NodeName }

// Run blocks until ctx done; performs initial setup then loops.
func (c *Controller) Run(ctx context.Context) {
	c.log.Info("starting", map[string]any{"mode": c.cfg.Mode, "key": c.cfg.LeaderKey()})
	// Best effort: ensure partial-sync friendly config.
	func() {
		tctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
		defer cancel()
		if err := c.deps.KV.EnsureRsIDPsync(tctx); err != nil {
			c.log.Warn("ensure use-rsid-psync failed (non-fatal)", map[string]any{"error": err.Error()})
		}
	}()

	// Session management + health + election in one tick loop for simplicity
	// and testability; watch loop runs separately for followers.
	go c.watchLoop(ctx)
	ticker := time.NewTicker(c.cfg.CheckInterval)
	defer ticker.Stop()
	ttlTicker := time.NewTicker(c.cfg.SessionTTL / 3)
	defer ttlTicker.Stop()
	if c.cfg.SessionTTL/3 < time.Second {
		ttlTicker.Reset(time.Second)
	}

	for {
		select {
		case <-ctx.Done():
			c.shutdown(context.Background())
			return
		case <-ttlTicker.C:
			c.renewSession(ctx)
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Controller) ensureSession(ctx context.Context) {
	c.mu.RLock()
	sid := c.sessionID
	c.mu.RUnlock()
	if sid != "" && c.deps.Session.Valid(sid) {
		return
	}
	// (Re)create only while kvrocks healthy; checked in tick, but also gate here.
	id, err := c.deps.Session.Create(ctx, c.cfg.SessionTTL, c.cfg.LockDelay)
	if err != nil {
		c.log.Warn("session create failed", map[string]any{"error": err.Error()})
		return
	}
	c.mu.Lock()
	c.sessionID = id
	c.sessionValid = true
	c.mu.Unlock()
	c.log.Info("session created", map[string]any{"ttl": c.cfg.SessionTTL.String()})
}

func (c *Controller) renewSession(ctx context.Context) {
	c.mu.RLock()
	sid := c.sessionID
	c.mu.RUnlock()
	if sid == "" {
		return
	}
	// Renew only while local kvrocks is healthy.
	c.mu.RLock()
	up := c.kvUp
	c.mu.RUnlock()
	if !up {
		c.log.Warn("skipping session renew: kvrocks unhealthy", nil)
		return
	}
	if err := c.deps.Session.Renew(ctx, sid); err != nil {
		c.log.Warn("session renew failed", map[string]any{"error": err.Error()})
		c.mu.Lock()
		c.sessionValid = false
		c.mu.Unlock()
		if c.isLeader() {
			c.demote(ctx, "session renew failed")
		}
		return
	}
	c.mu.Lock()
	c.sessionValid = true
	c.mu.Unlock()
}

func (c *Controller) isLeader() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.role == elect.RoleLeader
}

// tick: health check, then election step.
func (c *Controller) tick(ctx context.Context) {
	// 1. Health: PING + INFO with timeout.
	hctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	ri, perr := c.probe(hctx)
	cancel()

	c.mu.Lock()
	if perr != nil {
		c.kvUp = false
		c.unhealthy++
	} else {
		c.kvUp = true
		c.unhealthy = 0
		c.lag = ri.LagBytes
	}
	unhealthy := c.unhealthy
	c.mu.Unlock()
	c.reg.SetGauge("kvrocks_up", boolToInt(c.Snapshot().KvrocksUp))
	c.reg.SetGauge("replication_lag_bytes", c.Snapshot().LagBytes)

	if perr != nil {
		c.log.Warn("kvrocks unhealthy", map[string]any{"streak": unhealthy, "error": perr.Error()})
		if unhealthy >= c.cfg.UnhealthyThreshold {
			// Stop renewing (renewSession gates on kvUp next round) and
			// proactively release + demote if leader.
			if c.isLeader() {
				c.demote(ctx, "kvrocks unhealthy")
				c.releaseSession(ctx)
			} else {
				c.releaseSession(ctx)
			}
		}
		return
	}

	// Healthy: ensure session exists.
	c.ensureSession(ctx)

	c.mu.RLock()
	sid := c.sessionID
	c.mu.RUnlock()
	if sid == "" {
		return
	}

	// 2. Read current leader.
	raw, sess, idx, err := c.deps.Lock.Get(ctx, c.cfg.LeaderKey())
	_ = idx
	if err != nil {
		c.onConsulError(ctx, err)
		return
	}
	lv := elect.ParseLeader(raw)
	c.mu.Lock()
	c.leader = lv
	if lv != nil && lv.Node != c.cfg.NodeName {
		c.lastLeader = lv
		c.lastLeaderSeen = time.Now()
	}
	c.mu.Unlock()

	if lv != nil && lv.Node == c.cfg.NodeName {
		c.onSelfLeader(ctx, ri)
		return
	}
	if lv == nil {
		c.mu.RLock()
		old := c.lastLeader
		seen := c.lastLeaderSeen
		c.mu.RUnlock()
		c.tryBecomeLeader(ctx, raw, ri, old, seen)
		return
	}
	// Someone else leads: follow.
	c.onFollow(ctx, ri, lv, sess)
}

func (c *Controller) probe(ctx context.Context) (*kvrocks.ReplicationInfo, error) {
	if err := c.deps.KV.Ping(ctx); err != nil {
		return nil, err
	}
	return c.deps.KV.InfoReplication(ctx)
}

func (c *Controller) onConsulError(ctx context.Context, err error) {
	c.log.Warn("consul unreachable", map[string]any{"error": err.Error()})
	// Track outage; if longer than TTL and we are leader, demote first.
	// Simple implementation: demote immediately on error while leader, since
	// an isolated old master must stop advertising within ~TTL. The TTL check
	// will also expire even if demote RPCs fail; FailTTL is best-effort.
	if c.isLeader() {
		c.demote(ctx, "consul unreachable")
	}
}

func (c *Controller) onSelfLeader(ctx context.Context, ri *kvrocks.ReplicationInfo) {
	c.setRole(elect.RoleLeader)
	c.mu.Lock()
	if c.leaderSince.IsZero() {
		c.leaderSince = time.Now()
	}
	since := c.leaderSince
	c.mu.Unlock()
	c.reg.SetGaugeFloat("lock_held_seconds", time.Since(since).Seconds())
	// Keep master registration TTL fresh.
	c.passCheck(ctx)
	// Ensure still promoted (idempotent).
	if ri != nil && ri.Role != "master" {
		c.promote(ctx)
	}
}

func (c *Controller) onFollow(ctx context.Context, ri *kvrocks.ReplicationInfo, lv *consulstore.LeaderValue, sess string) {
	wasLeader := c.isLeader()
	if wasLeader {
		// Lost the lock (another node holds it or session invalidated).
		c.demote(ctx, "lock lost")
	}
	c.setRole(elect.RoleFollower)
	c.reg.SetGauge("is_leader", 0)
	c.registerReplica(ctx)
	c.passCheck(ctx)
	// Point at leader only when not already following it.
	if ri != nil && !(ri.Role == "slave" && ri.MasterHost == lv.Addr && ri.MasterPort == lv.Port) {
		c.followLeader(ctx, lv)
	}
	// Track master's recency for the promotion gate: store last-seen in memory
	// via leader value? We approximate: leader key presence == recently seen.
	_ = sess
}

func (c *Controller) tryBecomeLeader(ctx context.Context, raw []byte, ri *kvrocks.ReplicationInfo, oldMaster *consulstore.LeaderValue, oldSeen time.Time) {
	// Flapping guard: backoff with jitter between attempts.
	c.mu.RLock()
	n := c.elections
	c.mu.RUnlock()
	backoff := elect.Backoff(int(n%6), c.cfg.ReelectBaseBackoff, c.cfg.ReelectMaxBackoff, c.rng)
	if backoff > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(minDur(backoff, c.cfg.CheckInterval/2)):
		}
	}
	epoch := elect.NextEpoch(raw)
	lag := int64(-1)
	if ri != nil {
		lag = ri.LagBytes
	}
	// Determine old-master recency: if key empty, no old master.
	haveOld := oldMaster != nil
	lastSeen := oldSeen
	if haveOld && lastSeen.IsZero() {
		// Key held by another node just vanished (failover/partition):
		// treat as just seen so the hold-down applies to unknown lag.
		// (If we genuinely never saw a leader, lastSeen stays zero and
		// the unknown-lag path requires the full hold-down — safe default.)
		lastSeen = time.Now()
	}
	dec := elect.ShouldPromote(lag, c.cfg.MaxLagBytes, lastSeen, c.cfg.HoldDown, time.Now(), haveOld)
	if !dec.Allow {
		c.log.Warn("promotion refused by lag gate", map[string]any{"lag": lag, "reason": dec.Reason})
		// Stay follower; keep replica registration.
		c.setRole(elect.RoleFollower)
		c.registerReplica(ctx)
		return
	}
	if dec.Reason != "lag within bound" {
		c.log.Warn("promoting with unknown lag", map[string]any{"reason": dec.Reason})
	}
	value := elect.MarshalLeader(c.cfg.NodeName, c.cfg.AdvertiseAddr, c.cfg.AdvertisePort, c.sessionIDValue(), epoch, time.Now())
	if c.cfg.Mode == "observe" {
		c.log.Info("observe: would acquire lock and promote", map[string]any{"epoch": epoch, "lag": lag})
		c.mu.Lock()
		c.elections++
		c.mu.Unlock()
		c.reg.Inc("elections_total")
		return
	}
	c.mu.Lock()
	sid := c.sessionID
	c.mu.Unlock()
	held, err := c.deps.Lock.Acquire(ctx, c.cfg.LeaderKey(), sid, value)
	c.mu.Lock()
	c.elections++
	c.mu.Unlock()
	c.reg.Inc("elections_total")
	if err != nil {
		c.log.Warn("acquire failed", map[string]any{"error": err.Error()})
		return
	}
	if !held {
		return
	}
	c.promote(ctx)
	c.mu.Lock()
	c.leader = &consulstore.LeaderValue{Node: c.cfg.NodeName, Addr: c.cfg.AdvertiseAddr, Port: c.cfg.AdvertisePort, Session: sid, Epoch: epoch}
	c.leaderSince = time.Now()
	c.mu.Unlock()
	c.setRole(elect.RoleLeader)
	c.log.Info("became leader", map[string]any{"epoch": epoch})
}

func (c *Controller) sessionIDValue() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionID
}

func (c *Controller) promote(ctx context.Context) {
	if c.cfg.Mode == "observe" {
		c.log.Info("observe: would REPLICAOF NO ONE + register master", nil)
		return
	}
	pctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	if err := c.deps.KV.Promote(pctx); err != nil {
		c.log.Error("promote REPLICAOF NO ONE failed", map[string]any{"error": err.Error()})
		return
	}
	c.registerMaster(ctx)
}

func (c *Controller) followLeader(ctx context.Context, lv *consulstore.LeaderValue) {
	if c.cfg.Mode == "observe" {
		c.log.Info("observe: would REPLICAOF leader + register replica", map[string]any{"leader": lv.Node, "addr": lv.Addr, "port": lv.Port})
		return
	}
	fctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	if err := c.deps.KV.ReplicaOf(fctx, lv.Addr, lv.Port); err != nil {
		c.log.Error("REPLICAOF leader failed", map[string]any{"error": err.Error(), "leader": lv.Node})
		return
	}
	_ = c.deps.KV.SetReadOnly(fctx, true)
	c.log.Info("following leader", map[string]any{"leader": lv.Node})
}

func (c *Controller) demote(ctx context.Context, reason string) {
	c.log.Warn("demoting", map[string]any{"reason": reason})
	if c.cfg.Mode == "observe" {
		c.log.Info("observe: would demote (remove master registration, REPLICAOF or read-only)", nil)
		c.setRole(elect.RoleFollower)
		return
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// 1. Remove master advertisement FIRST (fail TTL + deregister), then
	// make node non-writable so an isolated old master stops serving writes.
	_ = c.deps.Registrar.FailTTL(dctx, c.deps.Registrar.CheckID(c.serviceID()))
	_ = c.deps.Registrar.Deregister(dctx, c.serviceID())
	kctx, kcancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer kcancel()
	_ = c.deps.KV.SetReadOnly(kctx, true)
	c.setRole(elect.RoleFollower)
	c.mu.Lock()
	c.leaderSince = time.Time{}
	c.mu.Unlock()
	// Re-register as replica so clients stop routing writes here.
	c.registerReplica(ctx)
}

func (c *Controller) registerMaster(ctx context.Context) {
	if c.cfg.Mode == "observe" {
		c.log.Info("observe: would register master service", map[string]any{"service": c.cfg.ServiceName, "tag": c.cfg.MasterTag})
		return
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.deps.Registrar.RegisterMaster(rctx, c.cfg.ServiceName, c.cfg.NodeName, c.cfg.AdvertiseAddr, c.cfg.AdvertisePort, []string{c.cfg.MasterTag}, c.cfg.SessionTTL)
}

func (c *Controller) registerReplica(ctx context.Context) {
	if c.cfg.Mode == "observe" {
		return // observe never touches service catalog
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.deps.Registrar.RegisterReplica(rctx, c.cfg.ServiceName, c.cfg.NodeName, c.cfg.AdvertiseAddr, c.cfg.AdvertisePort, []string{c.cfg.ReplicaTag}, c.cfg.SessionTTL)
}

func (c *Controller) passCheck(ctx context.Context) {
	if c.cfg.Mode == "observe" {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.deps.Registrar.PassTTL(pctx, c.deps.Registrar.CheckID(c.serviceID()))
}

func (c *Controller) releaseSession(ctx context.Context) {
	c.mu.RLock()
	sid := c.sessionID
	c.mu.RUnlock()
	if sid == "" {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.deps.Lock.Release(rctx, c.cfg.LeaderKey(), sid)
	_ = c.deps.Session.Destroy(rctx, sid)
	c.mu.Lock()
	c.sessionID = ""
	c.sessionValid = false
	c.mu.Unlock()
}

// watchLoop gives followers fast (blocking-query) reaction to leader changes.
func (c *Controller) watchLoop(ctx context.Context) {
	var idx uint64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		raw, sess, nidx, err := c.deps.Lock.Watch(ctx, c.cfg.LeaderKey(), idx, 60*time.Second)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		idx = nidx
		lv := elect.ParseLeader(raw)
		c.mu.Lock()
		c.leader = lv
		c.mu.Unlock()
		if lv == nil {
			// Key vanished: next tick will attempt election.
			continue
		}
		if lv.Node == c.cfg.NodeName {
			continue // we lead; tick handles steady state
		}
		_ = sess
		// Nudge a follow check on change (tick also covers it).
	}
}

// shutdown: leader removes master registration + releases lock; follower deregisters.
func (c *Controller) shutdown(ctx context.Context) {
	wasLeader := c.isLeader()
	c.log.Info("shutting down", map[string]any{"was_leader": wasLeader})
	if c.cfg.Mode == "observe" {
		c.releaseSession(ctx)
		return
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.deps.Registrar.FailTTL(sctx, c.deps.Registrar.CheckID(c.serviceID()))
	_ = c.deps.Registrar.Deregister(sctx, c.serviceID())
	if wasLeader {
		c.releaseSession(sctx)
	} else {
		// Followers: destroy session too (harmless) and stop renewing.
		c.mu.RLock()
		sid := c.sessionID
		c.mu.RUnlock()
		if sid != "" {
			_ = c.deps.Session.Destroy(sctx, sid)
		}
	}
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
