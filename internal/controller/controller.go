// Package controller runs the sidecar control loops.
package controller

import (
	"context"
	"errors"
	"math"
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
	backendRole    string // role reported by the local instance's INFO
	sessionID      string
	sessionValid   bool
	unhealthy      int
	leaderSince    time.Time
	elections      uint64
	leaderChanges  uint64
	lastLeader     *consulstore.LeaderValue
	lastLeaderSeen time.Time
	// lastRenewOK is the last time Consul confirmed our session (create or
	// renew). The leader tolerates Consul errors until ~SessionTTL past it.
	lastRenewOK time.Time
	// lastConsulOK is the last time any Consul call succeeded.
	lastConsulOK time.Time
	// Leader's master_repl_offset as last sampled while it was reachable,
	// and which leader session it belongs to. Used by the lag gate after
	// the leader disappears.
	peerOffset  int64
	peerSampled bool
	peerSession string

	rng *rand.Rand
}

// New builds a controller.
func New(cfg *config.Config, deps Deps, log *logx.Logger, reg *metrics.Registry) *Controller {
	c := &Controller{cfg: cfg, deps: deps, log: log, reg: reg, role: elect.RoleUnknown, lag: -1, peerOffset: -1, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	reg.SetRenewAgeFunc(func() float64 {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.lastRenewOK.IsZero() {
			return math.Inf(1)
		}
		return time.Since(c.lastRenewOK).Seconds()
	})
	reg.SetLockHeldFunc(func() float64 {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.role != elect.RoleLeader || c.leaderSince.IsZero() {
			return 0
		}
		return time.Since(c.leaderSince).Seconds()
	})
	return c
}

func (c *Controller) Snapshot() httpapi.Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	consulOK := !c.lastConsulOK.IsZero() && time.Since(c.lastConsulOK) < c.cfg.SessionTTL
	s := httpapi.Snapshot{Role: c.role, LagBytes: c.lag, KvrocksUp: c.kvUp, Mode: c.cfg.Mode, SessionValid: c.sessionValid, UnhealthyStreak: c.unhealthy,
		ConsulOK: consulOK, BackendRole: c.backendRole}
	s.Problems = elect.HealthProblems(c.role, c.backendRole, c.kvUp, consulOK, c.cfg.Mode == "observe")
	s.Healthy = len(s.Problems) == 0
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
		c.reg.LeaderChanges.Inc()
	}
	metrics.SetBool(c.reg.IsLeader, r == elect.RoleLeader)
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
	if sid != "" {
		ok, err := c.deps.Session.Valid(ctx, sid)
		if err != nil || ok {
			// Still valid, or Consul unreachable: keep the current session
			// rather than churning a new one.
			return
		}
	}
	// (Re)create only while kvrocks healthy; checked in tick, but also gate here.
	id, err := c.deps.Session.Create(ctx, c.cfg.SessionTTL, c.cfg.LockDelay)
	if err != nil {
		c.reg.ConsulErrors.WithLabelValues("session").Inc()
		c.log.Warn("session create failed", map[string]any{"error": err.Error()})
		return
	}
	now := time.Now()
	c.mu.Lock()
	c.sessionID = id
	c.sessionValid = true
	c.lastRenewOK = now
	c.lastConsulOK = now
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
	// Renew only while local kvrocks is healthy (or a new leader is still
	// inside --min-leader-time).
	c.mu.RLock()
	up := c.kvUp
	c.mu.RUnlock()
	if !up && !c.inMinLeaderTime() {
		c.log.Warn("skipping session renew: kvrocks unhealthy", nil)
		return
	}
	if err := c.deps.Session.Renew(ctx, sid); err != nil {
		if errors.Is(err, consulstore.ErrSessionInvalid) {
			// Consul answered: the session (and any lock) is gone. No
			// point waiting; step down now and let the next tick make a
			// fresh session.
			c.log.Warn("session invalidated", nil)
			c.mu.Lock()
			if c.sessionID == sid {
				c.sessionID = ""
			}
			c.sessionValid = false
			c.lastConsulOK = time.Now()
			c.mu.Unlock()
			if c.isLeader() {
				c.demote(ctx, metrics.ReasonSessionInvalidated, "session invalidated", nil)
			}
			return
		}
		c.onConsulError(ctx, "renew", err)
		return
	}
	now := time.Now()
	c.mu.Lock()
	c.sessionValid = true
	c.lastRenewOK = now
	c.lastConsulOK = now
	c.mu.Unlock()
}

// inMinLeaderTime reports whether we are a leader that has not yet held the
// lock for --min-leader-time. During that window we do not voluntarily hand
// over because of local health failures (anti-flap). Involuntary loss
// (session invalid, Consul outage, lock taken, shutdown) is never delayed.
func (c *Controller) inMinLeaderTime() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.role == elect.RoleLeader && elect.WithinMinLeaderTime(c.leaderSince, time.Now(), c.cfg.MinLeaderTime)
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
		c.backendRole = ""
	} else {
		c.kvUp = true
		c.unhealthy = 0
		c.backendRole = ri.Role
	}
	unhealthy := c.unhealthy
	c.mu.Unlock()
	metrics.SetBool(c.reg.BackendUp, perr == nil)

	if perr != nil {
		c.log.Warn("kvrocks unhealthy", map[string]any{"streak": unhealthy, "error": perr.Error()})
		if unhealthy >= c.cfg.UnhealthyThreshold && c.inMinLeaderTime() {
			c.log.Warn("kvrocks unhealthy but within min-leader-time; holding lock", map[string]any{"min_leader_time": c.cfg.MinLeaderTime.String()})
		} else if unhealthy >= c.cfg.UnhealthyThreshold {
			// Stop renewing (renewSession gates on kvUp next round) and
			// proactively release + demote if leader.
			if c.isLeader() {
				c.demote(ctx, metrics.ReasonBackendUnhealthy, "kvrocks unhealthy", nil)
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

	// 2. Read current leader. Leadership is decided by which session holds
	// the key, never by the node name in the value: the value survives a
	// session release (behavior=release keeps it) and a restarted sidecar
	// on the same node has a different session.
	raw, holder, _, err := c.deps.Lock.Get(ctx, c.cfg.LeaderKey())
	if err != nil {
		c.onConsulError(ctx, "lock", err)
		return
	}
	c.mu.Lock()
	c.lastConsulOK = time.Now()
	c.mu.Unlock()
	c.evaluate(ctx, ri, raw, holder, sid)
	c.reg.ReplicationLag.Set(float64(c.Snapshot().LagBytes))
}

// sampleLeader records the leader's replication offset so the lag gate has
// something to compare against if the leader later disappears.
func (c *Controller) sampleLeader(ctx context.Context, holder string, target *consulstore.LeaderValue) {
	c.mu.Lock()
	if c.peerSession != holder {
		c.peerSession = holder
		c.peerSampled = false
		c.peerOffset = -1
	}
	c.mu.Unlock()
	if target == nil {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	pi, err := c.deps.KV.PeerInfoReplication(pctx, target.Addr, target.Port)
	if err != nil {
		c.log.Warn("could not sample leader replication offset", map[string]any{"error": err.Error(), "leader": target.Node})
		return
	}
	if pi.Role != "master" || pi.MasterOffset < 0 {
		return
	}
	c.mu.Lock()
	c.peerOffset = pi.MasterOffset
	c.peerSampled = true
	c.mu.Unlock()
}

// updateLag recomputes our lag behind the (last known) leader.
func (c *Controller) updateLag(ri *kvrocks.ReplicationInfo) {
	local := int64(-1)
	if ri != nil {
		local = ri.LocalOffset
	}
	c.mu.Lock()
	c.lag = elect.ComputeLag(c.peerOffset, c.peerSampled, local)
	c.mu.Unlock()
}

// evaluate applies one election step given the lock state read from Consul.
func (c *Controller) evaluate(ctx context.Context, ri *kvrocks.ReplicationInfo, raw []byte, holder, sid string) {
	lv := elect.ParseLeader(raw)
	switch elect.Classify(holder, sid) {
	case elect.LockMine:
		c.mu.Lock()
		c.leader = lv
		c.lag = 0
		c.mu.Unlock()
		c.onSelfLeader(ctx, ri)
	case elect.LockVacant:
		c.mu.Lock()
		c.leader = nil
		// The value outlives the session; it names the previous leader.
		if lv != nil && lv.Session != sid {
			c.lastLeader = lv
		}
		if c.lastLeader != nil && c.lastLeaderSeen.IsZero() {
			// We never saw it holding the lock (e.g. we just started), so
			// start the hold-down clock now, once.
			c.lastLeaderSeen = time.Now()
		}
		old := c.lastLeader
		seen := c.lastLeaderSeen
		c.mu.Unlock()
		if c.isLeader() {
			c.demote(ctx, metrics.ReasonLockLost, "lock no longer held", nil)
		}
		// Keep the last sample from the departed leader: that is exactly
		// what the lag gate needs now.
		c.updateLag(ri)
		c.tryBecomeLeader(ctx, raw, ri, old, seen)
	default: // held by another session
		c.mu.Lock()
		c.leader = lv
		if lv != nil {
			c.lastLeader = lv
			c.lastLeaderSeen = time.Now()
		}
		c.mu.Unlock()
		c.sampleLeader(ctx, holder, c.followTarget(lv))
		c.updateLag(ri)
		c.onFollow(ctx, ri, lv)
	}
}

func (c *Controller) probe(ctx context.Context) (*kvrocks.ReplicationInfo, error) {
	if err := c.deps.KV.Ping(ctx); err != nil {
		return nil, err
	}
	return c.deps.KV.InfoReplication(ctx)
}

// onConsulError tolerates Consul errors while the session can still be
// alive server-side. Consul only invalidates a session after its TTL
// elapses without a renew, so the leader keeps serving until roughly one
// TTL has passed since the last successful renewal, then steps down
// (before the server-side release lets the other node take over).
func (c *Controller) onConsulError(ctx context.Context, op string, err error) {
	c.reg.ConsulErrors.WithLabelValues(op).Inc()
	c.mu.RLock()
	last := c.lastRenewOK
	c.mu.RUnlock()
	since := time.Since(last)
	c.log.Warn("consul error", map[string]any{"error": err.Error(), "since_last_renew": since.String()})
	if !c.isLeader() {
		return
	}
	if elect.ConsulOutageExceeded(last, time.Now(), c.cfg.SessionTTL) {
		c.demote(ctx, metrics.ReasonConsulUnreachable, "consul unreachable for ~session TTL", nil)
	}
}

func (c *Controller) onSelfLeader(ctx context.Context, ri *kvrocks.ReplicationInfo) {
	c.setRole(elect.RoleLeader)
	c.mu.Lock()
	if c.leaderSince.IsZero() {
		c.leaderSince = time.Now()
	}
	c.mu.Unlock()
	// Keep master registration TTL fresh.
	c.passCheck(ctx)
	// Ensure still promoted (idempotent).
	if ri != nil && ri.Role != "master" {
		c.promote(ctx)
	}
}

func (c *Controller) onFollow(ctx context.Context, ri *kvrocks.ReplicationInfo, lv *consulstore.LeaderValue) {
	target := c.followTarget(lv)
	if c.isLeader() {
		// Lost the lock to another session.
		c.demote(ctx, metrics.ReasonLockTaken, "lock held by another session", lv)
	}
	c.setRole(elect.RoleFollower)
	c.registerReplica(ctx)
	c.passCheck(ctx)
	if target == nil {
		// Holder unknown or it is a stale session of our own node (e.g. we
		// restarted and the old session has not expired yet). We must not
		// REPLICAOF ourselves, but we also must not stay writable.
		if ri != nil && !c.isFenced(ri) {
			c.fence(ctx, nil)
		}
		return
	}
	if ri != nil && !(ri.Role == "slave" && ri.MasterHost == target.Addr && ri.MasterPort == target.Port) {
		c.followLeader(ctx, target)
	}
}

// followTarget returns where to replicate from, or nil when lv does not
// describe another node we can follow.
func (c *Controller) followTarget(lv *consulstore.LeaderValue) *consulstore.LeaderValue {
	if lv == nil || lv.Node == c.cfg.NodeName || lv.Port == 0 {
		return nil
	}
	t := *lv
	if t.PrivateAddr != "" {
		// Replicate over the private network when the leader has one.
		t.Addr = t.PrivateAddr
	}
	if t.Addr == "" {
		return nil
	}
	return &t
}

// isFenced reports whether the local instance already refuses writes:
// a replica of either some real leader or the fence target.
func (c *Controller) isFenced(ri *kvrocks.ReplicationInfo) bool {
	return ri.Role == "slave"
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
	c.mu.RLock()
	lag := c.lag
	c.mu.RUnlock()
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
		// Stay follower; keep replica registration, and make sure we are
		// not a writable master while nobody holds the lock.
		c.setRole(elect.RoleFollower)
		c.registerReplica(ctx)
		if ri != nil && !c.isFenced(ri) {
			c.fence(ctx, nil)
		}
		return
	}
	if dec.Reason != "lag within bound" {
		c.log.Warn("promoting with unknown lag", map[string]any{"reason": dec.Reason})
	}
	value := elect.MarshalLeader(c.cfg.NodeName, c.cfg.AdvertiseAddr, c.cfg.AdvertiseAddrPrivate, c.cfg.AdvertisePort, c.sessionIDValue(), epoch, time.Now())
	if c.cfg.Mode == "observe" {
		c.log.Info("observe: would acquire lock and promote", map[string]any{"epoch": epoch, "lag": lag})
		c.mu.Lock()
		c.elections++
		c.mu.Unlock()
		c.reg.Elections.Inc()
		return
	}
	c.mu.Lock()
	sid := c.sessionID
	c.mu.Unlock()
	held, err := c.deps.Lock.Acquire(ctx, c.cfg.LeaderKey(), sid, value)
	c.mu.Lock()
	c.elections++
	c.mu.Unlock()
	c.reg.Elections.Inc()
	if err != nil {
		c.log.Warn("acquire failed", map[string]any{"error": err.Error()})
		return
	}
	if !held {
		// Someone else won; the next tick follows them. Until then refuse
		// writes if we happen to be a master.
		if ri != nil && !c.isFenced(ri) {
			c.fence(ctx, nil)
		}
		return
	}
	c.promote(ctx)
	c.mu.Lock()
	c.leader = &consulstore.LeaderValue{Node: c.cfg.NodeName, Addr: c.cfg.AdvertiseAddr, PrivateAddr: c.cfg.AdvertiseAddrPrivate, Port: c.cfg.AdvertisePort, Session: sid, Epoch: epoch}
	c.leaderSince = time.Now()
	c.mu.Unlock()
	c.setRole(elect.RoleLeader)
	c.reg.LeaderAcquired.Inc()
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

// demote stops this node acting as master. target is the new leader if
// known (nil otherwise).
//
// Ordering: remove the master advertisement first, then fence. Fencing
// means turning the instance into a replica: replica-read-only /
// slave-read-only only applies to replicas, so a "read-only" master would
// still accept writes. We REPLICAOF the new leader when we know it, or a
// deliberately unreachable fence address otherwise; either way the
// instance becomes a read-only replica and keeps its data until it can
// sync from a real leader.
func (c *Controller) demote(ctx context.Context, label, reason string, target *consulstore.LeaderValue) {
	c.reg.StepDowns.WithLabelValues(label).Inc()
	c.log.Warn("demoting", map[string]any{"reason": reason})
	if c.cfg.Mode == "observe" {
		c.log.Info("observe: would demote (remove master registration, REPLICAOF leader or fence target)", nil)
		c.setRole(elect.RoleFollower)
		return
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.deps.Registrar.FailTTL(dctx, c.deps.Registrar.CheckID(c.serviceID()))
	_ = c.deps.Registrar.Deregister(dctx, c.serviceID())
	if c.fence(ctx, c.followTarget(target)) {
		c.killClients(ctx)
	}
	c.setRole(elect.RoleFollower)
	c.mu.Lock()
	c.leaderSince = time.Time{}
	c.mu.Unlock()
	c.registerReplica(ctx)
}

// fence makes the local instance refuse writes by making it a replica of
// target, or of the configured fence address when target is nil.
func (c *Controller) fence(ctx context.Context, target *consulstore.LeaderValue) bool {
	host, port, kind := c.cfg.FenceHost, c.cfg.FencePort, "fence"
	if target != nil {
		host, port, kind = target.Addr, target.Port, "leader"
	}
	if c.cfg.Mode == "observe" {
		c.log.Info("observe: would fence with REPLICAOF", map[string]any{"host": host, "port": port})
		return false
	}
	kctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	if err := c.deps.KV.ReplicaOf(kctx, host, port); err != nil {
		c.reg.Fencing.WithLabelValues(kind, "error").Inc()
		c.log.Error("fence REPLICAOF failed; instance may still accept writes", map[string]any{"error": err.Error(), "host": host, "port": port})
		return false
	}
	c.reg.Fencing.WithLabelValues(kind, "ok").Inc()
	_ = c.deps.KV.SetReadOnly(kctx, true)
	c.log.Info("fenced: now a read-only replica", map[string]any{"host": host, "port": port, "leader_known": target != nil})
	return true
}

// killClients drops normal client connections after the instance has been
// fenced, so clients reconnect and find the new master instead of sitting
// on a read-only replica. Replication links and our own connection are
// never touched (see kvrocks.RedisClient.KillClients).
func (c *Controller) killClients(ctx context.Context) {
	if !c.cfg.KillClientsOnDemote || c.cfg.Mode == "observe" {
		return
	}
	kctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	n, err := c.deps.KV.KillClients(kctx)
	if n > 0 {
		c.reg.ClientsKilled.Add(float64(n))
	}
	if err != nil {
		c.reg.ClientKills.WithLabelValues("error").Inc()
		c.log.Warn("killing client connections after demotion failed", map[string]any{"error": err.Error(), "killed": n})
		return
	}
	c.reg.ClientKills.WithLabelValues("ok").Inc()
	c.log.Info("killed client connections after demotion", map[string]any{"killed": n})
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
		c.mu.Lock()
		if sess == "" {
			c.leader = nil // vacant: next tick attempts election
		} else {
			c.leader = elect.ParseLeader(raw)
		}
		c.mu.Unlock()
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
		c.reg.StepDowns.WithLabelValues(metrics.ReasonShutdown).Inc()
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

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
