// Package controller runs the sidecar control loops.
package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/p1u3o/kvlock/internal/config"
	"github.com/p1u3o/kvlock/internal/consulstore"
	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/httpapi"
	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/logx"
	"github.com/p1u3o/kvlock/internal/metrics"
)

// Deps are the external systems behind interfaces.
type Deps struct {
	Session   consulstore.Session
	Lock      consulstore.Lock
	Registrar consulstore.Registrar
	KV        kvrocks.Client
	// Backup is optional; nil disables backups.
	Backup BackupRunner
}

// Controller owns runtime state.
type Controller struct {
	obsMu   sync.Mutex
	obsSeen map[string]time.Time // observe decisions already logged
	cfg     *config.Config
	deps    Deps
	log     *logx.Logger
	reg     *metrics.Registry

	mu            sync.RWMutex
	role          elect.Role
	leader        *consulstore.LeaderValue
	lag           int64
	kvUp          bool
	backendRole   string // role reported by the local instance's INFO
	sessionID     string
	sessionValid  bool
	unhealthy     int
	leaderSince   time.Time
	elections     uint64
	leaderChanges uint64
	lastLeader    *consulstore.LeaderValue
	// fencePending is set when a fence attempt failed; the return watcher
	// retries as soon as the instance answers.
	fencePending   bool
	rsidDone       bool // use-rsid-psync applied (or not applicable)
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
	peerAt      time.Time // when peerOffset was taken
	peerSession string

	hb     hbState
	probed bool // the first tick has probed the local instance
	lastRI *kvrocks.ReplicationInfo

	evMu          sync.Mutex
	events        []httpapi.Event
	lastRefusal   string
	lastRefusalAt time.Time

	infoMu  sync.RWMutex
	version string
	conns   func() map[string]int
	admin   bool

	rng *rand.Rand

	// cmds runs functions inside the control loop (planned handover).
	cmds         chan func(context.Context)
	handoverBusy atomic.Bool

	// Proxy support: fenceGen changes on every successful fence so proxied
	// connections are dropped even when the target address stays the same.
	fenceGen atomic.Uint64
	subsMu   sync.Mutex
	subs     []chan struct{}
}

// New builds a controller.
func New(cfg *config.Config, deps Deps, log *logx.Logger, reg *metrics.Registry) *Controller {
	c := &Controller{cfg: cfg, deps: deps, log: log, reg: reg, role: elect.RoleUnknown, lag: -1, peerOffset: -1, hb: hbState{lag: -1}, rng: rand.New(rand.NewSource(time.Now().UnixNano())), cmds: make(chan func(context.Context))}
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
		s.LeaderSince = c.leader.Since
		if t, err := time.Parse(time.RFC3339Nano, c.leader.Since); err == nil {
			s.LeaderAgeSeconds = time.Since(t).Seconds()
		}
	}
	s.Cluster = c.cfg.Cluster
	s.Node = c.cfg.NodeName
	s.NodeAddr = net.JoinHostPort(c.cfg.AdvertiseAddr, strconv.Itoa(c.cfg.AdvertisePort))
	s.MaxLagSeconds = c.cfg.MaxLagTime.Seconds()
	s.MaxLagSeq = c.cfg.MaxLagBytes
	s.LagSeconds = -1
	if c.hb.lag >= 0 {
		s.LagSeconds = c.hb.lag.Seconds()
	}
	s.HeartbeatAgeSeconds = -1
	if c.hb.haveLocal {
		s.HeartbeatAgeSeconds = time.Since(time.UnixMilli(c.hb.local.UnixMs)).Seconds()
	}
	s.LagGate = "seq"
	if c.role == elect.RoleLeader {
		s.LagGate = "-"
	} else if c.hb.havePeer {
		s.LagGate = "time"
	}
	s.LastRenewAgeSeconds = -1
	if !c.lastRenewOK.IsZero() {
		s.LastRenewAgeSeconds = time.Since(c.lastRenewOK).Seconds()
	}
	s.FencePending = c.fencePending
	if c.deps.Backup != nil {
		bs := c.deps.Backup.Status()
		if c.cfg.Mode == "observe" {
			bs.Enabled = false
		}
		s.Backup = &bs
	}
	if ri := c.lastRI; ri != nil {
		s.MasterLinkUp = ri.Role == "slave" && ri.MasterLinkUp
		if ri.Role == "slave" {
			s.MasterHost, s.MasterPort = ri.MasterHost, ri.MasterPort
		}
		for _, r := range ri.Replicas {
			s.Replicas = append(s.Replicas, net.JoinHostPort(r.IP, strconv.Itoa(r.Port)))
		}
	}
	s.FenceState = fenceState(c.role, c.backendRole, s.MasterHost, s.MasterPort, c.cfg.FenceHost, c.cfg.FencePort, c.fencePending, c.kvUp)
	c.infoMu.RLock()
	s.Version, s.AdminEnabled = c.version, c.admin
	conns := c.conns
	c.infoMu.RUnlock()
	if conns != nil {
		s.Connections = conns()
	}
	s.Events = c.Events()
	return s
}

// SetInfo sets what the status page shows besides controller state.
func (c *Controller) SetInfo(version string, adminEnabled bool, conns func() map[string]int) {
	c.infoMu.Lock()
	c.version, c.admin, c.conns = version, adminEnabled, conns
	c.infoMu.Unlock()
}

func fenceState(role elect.Role, backendRole, mh string, mp int, fh string, fp int, pending, up bool) string {
	switch {
	case !up:
		return "backend down"
	case pending:
		return "fence pending (retrying)"
	case backendRole == "master" && role == elect.RoleLeader:
		return "writable master (leader)"
	case backendRole == "master":
		return "WRITABLE master without the lock"
	case backendRole == "slave" && mh == fh && mp == fp:
		return "fenced (replica of fence target)"
	case backendRole == "slave":
		return "replica (read-only)"
	}
	return "unknown"
}

func (c *Controller) Metrics() *metrics.Registry { return c.reg }

// CurrentLeader returns a copy of the lock holder's value as last seen, or
// false while the lock is vacant or unknown.
func (c *Controller) CurrentLeader() (consulstore.LeaderValue, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.leader == nil {
		return consulstore.LeaderValue{}, false
	}
	return *c.leader, true
}

func (c *Controller) setRole(r elect.Role) {
	c.mu.Lock()
	if c.role != r {
		c.role = r
		c.leaderChanges++
		c.reg.LeaderChanges.Inc()
	}
	metrics.SetBool(c.reg.IsLeader, r == elect.RoleLeader)
	c.mu.Unlock()
	c.notify()
}

// serviceID for our registration.
func (c *Controller) serviceID() string { return c.cfg.ServiceName + "-" + c.cfg.NodeName }

// Run blocks until ctx done; performs initial setup then loops.
func (c *Controller) Run(ctx context.Context) {
	c.log.Info("starting", map[string]any{"mode": c.cfg.Mode, "key": c.cfg.LeaderKey(), "version": c.version})
	// Best effort: ensure partial-sync friendly config.
	// kvrocks is often not listening yet when the sidecar starts; then this
	// is retried when the backend first answers (onBackendBack).
	c.ensureRsIDPsync(ctx)

	// Session management + health + election in one tick loop for simplicity
	// and testability; watch loop runs separately for followers.
	go c.watchLoop(ctx)
	go c.backendReturnWatch(ctx)
	if c.cfg.Mode != "observe" {
		go c.heartbeatLoop(ctx)
		c.startBackups(ctx)
	}
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
		case f := <-c.cmds:
			f(ctx)
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
		c.probed = true
		c.unhealthy++
		c.backendRole = ""
	} else {
		c.kvUp = true
		c.probed = true
		c.unhealthy = 0
		c.backendRole = ri.Role
		c.lastRI = ri
	}
	unhealthy := c.unhealthy
	c.mu.Unlock()
	metrics.SetBool(c.reg.BackendUp, perr == nil)
	if perr == nil && !c.rsidDone {
		// The tick may see the instance back before backendReturnWatch.
		c.ensureRsIDPsync(ctx)
	}

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
	c.mu.RLock()
	lagSeq := c.lag
	c.mu.RUnlock()
	c.reg.ReplicationLag.Set(float64(lagSeq))
}

// sampleLeader records the leader's replication offset so the lag gate has
// something to compare against if the leader later disappears.
func (c *Controller) sampleLeader(ctx context.Context, holder string, target *consulstore.LeaderValue) {
	c.mu.Lock()
	if c.peerSession != holder {
		c.peerSession = holder
		c.peerSampled = false
		c.peerOffset = -1
		c.peerAt = time.Time{}
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
	c.peerAt = time.Now()
	c.mu.Unlock()
}

// resampleOld takes a fresh offset sample from the departed leader if its
// kvrocks still answers as a master. Failure keeps the previous sample.
// It only refreshes an existing sample of that leader: with no sample the
// lag stays unknown and the hold-down applies, as before.
func (c *Controller) resampleOld(ctx context.Context, old *consulstore.LeaderValue) {
	c.mu.RLock()
	have := c.peerSampled
	c.mu.RUnlock()
	if !have {
		return
	}
	target := c.followTarget(old)
	if target == nil {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, minDur(c.cfg.KvrocksTimeout, time.Second))
	defer cancel()
	pi, err := c.deps.KV.PeerInfoReplication(pctx, target.Addr, target.Port)
	if err != nil || pi.Role != "master" || pi.MasterOffset < 0 {
		return
	}
	c.mu.Lock()
	if c.peerSampled && pi.MasterOffset >= c.peerOffset {
		c.peerOffset, c.peerSampled, c.peerAt = pi.MasterOffset, true, time.Now()
	}
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
		// If that was this node (an earlier incarnation of this sidecar),
		// nobody newer than our own data was master, so there is no old
		// master to wait for.
		if lv != nil && lv.Session != sid {
			if lv.Node == c.cfg.NodeName {
				c.lastLeader = nil
				c.lastLeaderSeen = time.Time{}
			} else {
				c.lastLeader = lv
			}
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
		if elect.HandoverActive(lv, time.Now()) && c.onHandoverHint(ctx, ri, lv) {
			return
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
		if c.cfg.Mode == "observe" && c.cfg.HeartbeatInterval > 0 {
			c.readHeartbeats(ctx, nil) // GETs only, at the check interval
		}
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
	// Bootstrap next to a running pair: with no leader ever recorded, a
	// replica that is still linked to a live master must leave the lock to
	// the master's node instead of racing for it.
	if len(raw) == 0 && ri != nil && ri.Role == "slave" && ri.MasterLinkUp {
		c.setRole(elect.RoleFollower)
		c.observeLog("not running for leader: local instance replicates from a live master", map[string]any{"master_host": ri.MasterHost, "master_port": ri.MasterPort})
		return
	}
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
	// Fresh measurement: if the old leader's kvrocks still answers (only its
	// sidecar or session went away), re-sample it now rather than trusting
	// an older sample. Our own offset in ri is from this tick.
	if oldMaster != nil {
		c.resampleOld(ctx, oldMaster)
		c.updateLag(ri)
	}
	c.mu.RLock()
	lag := c.lag
	sampleAge := time.Duration(-1)
	if c.peerSampled && !c.peerAt.IsZero() {
		ref := oldSeen
		if ref.IsZero() || c.peerAt.After(ref) {
			ref = c.peerAt
		}
		sampleAge = ref.Sub(c.peerAt)
	}
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
	in := elect.LagGateInput{Lag: lag, MaxLag: c.cfg.MaxLagBytes, SampleAge: sampleAge, MaxSampleAge: c.cfg.LagSampleMaxAge,
		LastSeen: lastSeen, HaveOld: haveOld, HoldDown: c.cfg.HoldDown, MaxWait: c.cfg.LagGateMaxWait, Now: time.Now()}
	gate := "seq"
	if haveOld && c.cfg.HeartbeatInterval > 0 {
		// Fresh reads: the departed leader's heartbeat if its kvrocks still
		// answers, and our own copy.
		if t := c.followTarget(oldMaster); t != nil {
			c.readHeartbeats(ctx, &electTarget{addr: t.Addr, port: t.Port, session: oldMaster.Session})
		}
		if ms, age, ok := c.timeGateInput(oldSeen); ok {
			gate, in.Lag, in.MaxLag, in.SampleAge = "time", ms, c.cfg.MaxLagTime.Milliseconds(), age
		}
	}
	dec := elect.LagGate(in)
	lag = in.Lag
	if !dec.Allow {
		c.reg.LagRefusals.Inc()
		c.event("lag_gate", fmt.Sprintf("promotion refused (%s gate, lag %s): %s", gate, fmtLag(gate, lag), dec.Reason))
		c.log.Warn("promotion refused by lag gate", map[string]any{"gate": gate, "lag": lag, "sample_age": in.SampleAge.String(), "reason": dec.Reason})
		// Stay follower; keep replica registration, and make sure we are
		// not a writable master while nobody holds the lock.
		c.setRole(elect.RoleFollower)
		c.registerReplica(ctx)
		if ri != nil && !c.isFenced(ri) {
			c.fence(ctx, nil)
		}
		return
	}
	if lag > in.MaxLag {
		c.log.Warn("promoting despite lag", map[string]any{"gate": gate, "lag": lag, "reason": dec.Reason})
	} else if dec.Reason != "lag within bound" {
		c.log.Warn("promoting with unknown lag", map[string]any{"gate": gate, "reason": dec.Reason})
	}
	if ec, ok := c.deps.Lock.(consulstore.EpochCounter); ok && c.cfg.Mode != "observe" {
		// The counter key is never released or rewritten by anyone else,
		// so epochs keep rising even if the leader value was lost.
		n, err := ec.BumpEpoch(ctx, c.cfg.EpochKey(), epoch)
		if err != nil {
			c.log.Warn("epoch counter bump failed; using epoch from leader value", map[string]any{"error": err.Error()})
		} else {
			epoch = n
		}
	}
	value := elect.MarshalLeader(c.cfg.NodeName, c.cfg.AdvertiseAddr, c.cfg.AdvertiseAddrPrivate, c.cfg.AdvertisePort, c.sessionIDValue(), epoch, time.Now())
	if c.cfg.Mode == "observe" {
		// The observe lock lives under its own prefix, so taking it only
		// elects an observer; promote below just logs what it would do.
		c.observeLog("observe: would acquire the real lock and promote", map[string]any{"epoch": epoch, "lag": lag})
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
	c.event("election", fmt.Sprintf("became leader, epoch %d (%s gate, lag %s: %s)", epoch, gate, fmtLag(gate, lag), dec.Reason))
	c.log.Info("became leader", map[string]any{"epoch": epoch, "gate": gate, "lag": lag})
}

func (c *Controller) sessionIDValue() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.sessionID
}

func (c *Controller) promote(ctx context.Context) {
	if c.cfg.Mode == "observe" {
		c.observeLog("observe: would REPLICAOF NO ONE + register master", nil)
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
		c.observeLog("observe: would REPLICAOF leader + register replica", map[string]any{"leader": lv.Node, "addr": lv.Addr, "port": lv.Port})
		return
	}
	fctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	if err := c.deps.KV.ReplicaOf(fctx, lv.Addr, lv.Port); err != nil {
		if strings.Contains(err.Error(), "own replicas") {
			// The new leader still lists us as its replica for a moment
			// after a switch-over. Stay fenced and retry next tick.
			c.log.Info("new leader still lists us as its replica; will retry", map[string]any{"leader": lv.Node})
			if ri, ierr := c.deps.KV.InfoReplication(fctx); ierr == nil && ri.Role != "slave" {
				c.fence(ctx, nil)
			}
			return
		}
		c.log.Error("REPLICAOF leader failed", map[string]any{"error": err.Error(), "leader": lv.Node})
		return
	}
	_ = c.deps.KV.SetReadOnly(fctx, true)
	c.event("follow", "following leader "+lv.Node)
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
	c.event("step_down", label+": "+reason)
	c.log.Warn("demoting", map[string]any{"reason": reason})
	if c.cfg.Mode == "observe" {
		c.observeLog("observe: would demote (remove master registration, REPLICAOF leader or fence target)", nil)
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
		c.observeLog("observe: would fence with REPLICAOF", map[string]any{"host": host, "port": port})
		return false
	}
	kctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	if err := c.deps.KV.ReplicaOf(kctx, host, port); err != nil {
		c.reg.Fencing.WithLabelValues(kind, "error").Inc()
		c.setFencePending()
		c.event("fence", fmt.Sprintf("fence REPLICAOF %s:%d failed: %v", host, port, err))
		c.log.Error("fence REPLICAOF failed; instance may still accept writes", map[string]any{"error": err.Error(), "host": host, "port": port})
		return false
	}
	// Confirm: only a replica refuses writes.
	if ri, err := c.deps.KV.InfoReplication(kctx); err != nil || ri.Role != "slave" {
		role := ""
		if ri != nil {
			role = ri.Role
		}
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		c.reg.Fencing.WithLabelValues(kind, "error").Inc()
		c.setFencePending()
		c.event("fence", "fence not confirmed; role "+role)
		c.log.Error("fence not confirmed; instance may still accept writes", map[string]any{"error": msg, "role": role, "host": host, "port": port})
		return false
	}
	c.reg.Fencing.WithLabelValues(kind, "ok").Inc()
	c.mu.Lock()
	c.fencePending = false
	c.mu.Unlock()
	c.fenceGen.Add(1)
	c.notify()
	_ = c.deps.KV.SetReadOnly(kctx, true)
	c.event("fence", fmt.Sprintf("fenced: replica of %s:%d", host, port))
	c.log.Info("fenced: now a read-only replica", map[string]any{"host": host, "port": port, "leader_known": target != nil})
	return true
}

func (c *Controller) setFencePending() {
	c.mu.Lock()
	c.fencePending = true
	c.mu.Unlock()
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
		c.observeLog("observe: would register master service", map[string]any{"service": c.cfg.ServiceName, "tag": c.cfg.MasterTag})
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
		c.notify()
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
	if wasLeader && c.cfg.ShutdownHandover > 0 && c.shutdownHandover(ctx) {
		return
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.deps.Registrar.FailTTL(sctx, c.deps.Registrar.CheckID(c.serviceID()))
	_ = c.deps.Registrar.Deregister(sctx, c.serviceID())
	// Never leave a writable master behind: fence before the lock goes, so
	// the next leader can never overlap with us.
	fenced, gone := c.fenceForShutdown(sctx)
	if wasLeader {
		c.reg.StepDowns.WithLabelValues(metrics.ReasonShutdown).Inc()
		if gone {
			// Nothing is serving on the local address (it was stopped first,
			// e.g. on alloc stop). It cannot take writes, and it boots fenced
			// (replicaof in its config), so hand the lock over now instead of
			// leaving the cluster leaderless for TTL + lock-delay.
			c.log.Info("local kvrocks is gone; releasing the lock without fencing", nil)
			c.releaseSession(sctx)
			return
		}
		if !fenced {
			// Releasing now could create two masters. Leave the session to
			// expire; lock-delay then holds off the next leader a little
			// longer, and a restarted sidecar fences on start.
			c.log.Error("could not fence on shutdown; not releasing the lock, it will expire with the session TTL", nil)
			return
		}
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

// fenceForShutdown makes sure the local instance is a replica before the
// sidecar exits, retrying briefly. It reports whether the instance is now
// confirmed fenced (or already was). Clients are dropped after fencing so
// they reconnect to whoever is master next.
func (c *Controller) fenceForShutdown(ctx context.Context) (fenced, gone bool) {
	probe := minDur(c.cfg.KvrocksTimeout, time.Second)
	gone = true
	for attempt := 0; ; attempt++ {
		pctx, cancel := context.WithTimeout(ctx, probe)
		ri, err := c.deps.KV.InfoReplication(pctx)
		cancel()
		if err == nil && ri.Role == "slave" {
			return true, false
		}
		if err == nil && c.fence(ctx, nil) {
			c.killClients(ctx)
			return true, false
		}
		// gone only if every attempt found nothing answering; an instance
		// that answers but refuses REPLICAOF keeps the lock.
		if err == nil || !backendGone(err) {
			gone = false
		}
		if attempt >= 2 {
			return false, gone
		}
		select {
		case <-ctx.Done():
			return false, gone
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// backendReturnWatch polls the local instance quickly while it is down so
// that when it comes back (restart, or after a failed fence) it is fenced
// at once rather than on the next regular check: a restarted kvrocks
// without a replicaof line boots as a writable master.
func (c *Controller) backendReturnWatch(ctx context.Context) {
	every := minDur(c.cfg.CheckInterval, 250*time.Millisecond)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.mu.RLock()
		// Before the first probe kvUp is false only because nothing has
		// been checked yet; startup is handled by the first tick.
		watch := (c.probed && !c.kvUp) || c.fencePending
		c.mu.RUnlock()
		if !watch {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
		err := c.deps.KV.Ping(pctx)
		cancel()
		if err != nil {
			continue
		}
		done := make(chan struct{})
		select {
		case <-ctx.Done():
			return
		case c.cmds <- func(ctx context.Context) { defer close(done); c.onBackendBack(ctx) }:
		}
		select {
		case <-ctx.Done():
			return
		case <-done:
		}
	}
}

// onBackendBack runs on the control loop when the local instance answers
// again. A non-leader that finds it writable fences immediately, then a
// normal tick follows.
func (c *Controller) onBackendBack(ctx context.Context) {
	if !c.rsidDone {
		c.ensureRsIDPsync(ctx)
	}
	if c.cfg.Mode != "observe" && !c.isLeader() {
		pctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
		ri, err := c.deps.KV.InfoReplication(pctx)
		cancel()
		if err == nil && ri.Role != "slave" {
			c.mu.RLock()
			lv := c.leader
			c.mu.RUnlock()
			c.log.Warn("local instance is back as a writable master; fencing now", nil)
			if c.fence(ctx, c.followTarget(lv)) {
				c.killClients(ctx)
			}
		}
	}
	c.tick(ctx)
}

// ensureRsIDPsync applies use-rsid-psync once (best effort). If the local
// instance is not reachable yet it stays pending without a warning.
func (c *Controller) ensureRsIDPsync(ctx context.Context) {
	if c.cfg.Mode == "observe" {
		c.rsidDone = true
		c.observeLog("observe: would ensure use-rsid-psync", nil)
		return
	}
	tctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	err := c.deps.KV.EnsureRsIDPsync(tctx)
	switch {
	case err == nil:
		c.rsidDone = true
	case backendGone(err):
		// Not listening yet; retried when the backend first answers.
	default:
		c.rsidDone = true
		c.log.Warn("ensure use-rsid-psync failed (non-fatal)", map[string]any{"error": err.Error()})
	}
}

// backendGone reports whether err means nothing is answering on the local
// address: connection refused, or no answer within the timeout.
func backendGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// observeLog logs an observe-mode decision, repeating an identical one at
// most every observeRepeat so a steady state does not flood the logs.
func (c *Controller) observeLog(msg string, fields map[string]any) {
	key := msg + fmt.Sprint(fields)
	now := time.Now()
	c.obsMu.Lock()
	if c.obsSeen == nil {
		c.obsSeen = map[string]time.Time{}
	}
	last, seen := c.obsSeen[key]
	if seen && now.Sub(last) < observeRepeat {
		c.obsMu.Unlock()
		return
	}
	c.obsSeen[key] = now
	c.obsMu.Unlock()
	c.log.Info(msg, fields)
}

const observeRepeat = 5 * time.Minute

func fmtLag(gate string, lag int64) string {
	if lag < 0 {
		return "unknown"
	}
	if gate == "time" {
		return (time.Duration(lag) * time.Millisecond).String()
	}
	return strconv.FormatInt(lag, 10) + " seq"
}
