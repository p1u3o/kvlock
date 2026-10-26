package controller

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/p1u3o/kvlock/internal/consulstore"
	"github.com/p1u3o/kvlock/internal/elect"
)

// RouteState classifies a routing decision.
type RouteState int

const (
	// RouteOK: the decision has a target address.
	RouteOK RouteState = iota
	// RouteTransient: no target right now, but one is expected soon
	// (leader unknown, election or Consul confirmation in progress). A
	// proxy with a hold may keep new clients waiting.
	RouteTransient
	// RouteRefuse: waiting will not produce a target on this listener
	// (this node is the leader, the leader has no address). Refuse at once.
	RouteRefuse
)

func (s RouteState) String() string {
	switch s {
	case RouteOK:
		return "ok"
	case RouteTransient:
		return "transient"
	case RouteRefuse:
		return "refuse"
	}
	return "unknown"
}

// ProxyView is what the leader proxies route on.
type ProxyView struct {
	// Leader is the address to dial for writes, "" when no safe target is
	// known (Reason and State say why).
	Leader string
	// Key identifies the routing decision; proxied connections are closed
	// whenever it changes (new holder, role change, fencing).
	Key    string
	Reason string
	State  RouteState
	// Replica is the local instance's address when it is a healthy,
	// caught-up replica of a known leader, else "".
	Replica string
}

// ForwardRoute is the replication forwarder's routing decision.
type ForwardRoute struct {
	Addr   string
	Key    string
	Reason string
	State  RouteState
}

// Subscribe returns a channel signalled (coalesced) on leadership or
// fencing changes.
func (c *Controller) Subscribe() <-chan struct{} {
	ch := make(chan struct{}, 1)
	c.subsMu.Lock()
	c.subs = append(c.subs, ch)
	c.subsMu.Unlock()
	return ch
}

func (c *Controller) notify() {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	for _, ch := range c.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// confirmGrace is how recently Consul must have answered for a route to
// count as confirmed on that alone.
func (c *Controller) confirmGrace() time.Duration {
	g := 2 * c.cfg.CheckInterval
	if g < time.Second {
		g = time.Second
	}
	return g
}

// routeConfirmedLocked decides whether the known leader lv may still be
// routed to. Callers hold c.mu.
//
// Consul answered within confirmGrace: yes. A slow tick or a short Consul
// hiccup is not a leadership change, so after that the last known leader
// stays routable while all of these hold:
//   - Consul answered within one session TTL. Consul cannot have let
//     another session take the lock before the holder's TTL ran out
//     (plus lock-delay), unless the holder released it, and a holder
//     always fences itself before releasing.
//   - for a remote leader: this node read the leader's own kvrocks within
//     confirmGrace and it reported role master, for the same session and
//     epoch. A fenced old leader stops reporting master at once, so the
//     route never points at a node that is not the current master.
//   - for this node as leader: our session was renewed within TTL/2, so
//     nobody else can hold the lock.
//
// An epoch or session change seen in Consul replaces c.leader and so
// changes the key, which cuts every connection as before.
func (c *Controller) routeConfirmedLocked(lv *consulstore.LeaderValue, self bool) (bool, string) {
	if lv == nil {
		return false, "leader unknown"
	}
	if c.lastLockRead.IsZero() {
		return false, "leadership state not confirmed with Consul"
	}
	since := time.Since(c.lastLockRead)
	grace := c.confirmGrace()
	if since < grace {
		return true, ""
	}
	if since >= c.cfg.SessionTTL {
		return false, "leadership state not confirmed with Consul"
	}
	if self {
		if c.sessionValid && !c.lastRenewOK.IsZero() && time.Since(c.lastRenewOK) < c.cfg.SessionTTL/2 {
			return true, ""
		}
		return false, "leadership state not confirmed with Consul (session renewal stale)"
	}
	if c.verifiedAt.IsZero() || time.Since(c.verifiedAt) >= grace || c.verifiedSession != lv.Session || c.verifiedEpoch != lv.Epoch {
		return false, "leadership state not confirmed with Consul and leader not verified as master"
	}
	return true, ""
}

// ProxyView computes the current routing decision. It fails closed: no
// leader target unless the lock holder is known and confirmed (see
// routeConfirmedLocked).
func (c *Controller) ProxyView() ProxyView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	local := net.JoinHostPort(c.cfg.KvrocksAddr, strconv.Itoa(c.cfg.KvrocksPort))

	var v ProxyView
	lv := c.leader
	if lv != nil && c.role != elect.RoleLeader && c.kvUp && c.backendRole == "slave" && c.lag >= 0 && c.lag <= c.cfg.MaxLagBytes {
		v.Replica = local
	}
	self := lv != nil && lv.Session != "" && lv.Session == c.sessionID
	ok, why := c.routeConfirmedLocked(lv, self)
	v.State = RouteTransient
	switch {
	case !ok:
		v.Reason = why
	case self:
		if c.role != elect.RoleLeader || !c.kvUp || c.backendRole != "master" {
			v.Reason = "local leader not ready"
			break
		}
		v.Leader = local
	default:
		if c.role == elect.RoleLeader {
			v.Reason = "role in transition"
			break
		}
		host := lv.Addr
		if lv.PrivateAddr != "" {
			host = lv.PrivateAddr
		}
		if host == "" || lv.Port == 0 {
			v.Reason, v.State = "leader address unknown", RouteRefuse
			break
		}
		v.Leader = net.JoinHostPort(host, strconv.Itoa(lv.Port))
	}
	if v.Leader != "" {
		v.State = RouteOK
		// Connections are cut only on a leader or epoch change. On the
		// leader itself the key also carries role and fence generation,
		// so a local demotion or fence still cuts at once; on a replica
		// its own fencing or replica topology changes do not touch
		// connections to the remote leader.
		v.Key = v.Leader + "|" + lv.Session + "|" + strconv.FormatUint(lv.Epoch, 10)
		if self {
			v.Key += "|" + string(c.role) + "|" + strconv.FormatUint(c.fenceGen.Load(), 10)
		}
	}
	return v
}

// ForwardView is the replication forwarder's routing decision: the real
// address of the current lock holder, or no target. This node being the
// leader is RouteRefuse (the leader never replicates, so holding a client
// would only delay its error); an unknown or unconfirmed leader is
// RouteTransient. The key includes session and epoch, so any leadership
// change cuts every forwarded connection, even if the address is reused.
func (c *Controller) ForwardView() ForwardRoute {
	c.mu.RLock()
	defer c.mu.RUnlock()
	lv := c.leader
	if lv != nil && (lv.Node == c.cfg.NodeName || (lv.Session != "" && lv.Session == c.sessionID) || c.role == elect.RoleLeader) {
		// Checked first: whatever Consul says, a node that is or thinks
		// it is the leader must not forward to anyone, and its own
		// kvrocks must not wait on it.
		return ForwardRoute{Reason: "this node is the leader", State: RouteRefuse}
	}
	if lv == nil && c.role == elect.RoleLeader {
		return ForwardRoute{Reason: "this node is the leader", State: RouteRefuse}
	}
	if ok, why := c.routeConfirmedLocked(lv, false); !ok {
		return ForwardRoute{Reason: why, State: RouteTransient}
	}
	host := lv.Addr
	if lv.PrivateAddr != "" {
		host = lv.PrivateAddr
	}
	if host == "" || lv.Port == 0 {
		return ForwardRoute{Reason: "leader address unknown", State: RouteRefuse}
	}
	addr := net.JoinHostPort(host, strconv.Itoa(lv.Port))
	return ForwardRoute{Addr: addr, Key: addr + "|" + lv.Session + "|" + strconv.FormatUint(lv.Epoch, 10), Reason: "forwarding to leader " + lv.Node, State: RouteOK}
}

// leaderVerifyLoop reads the remote leader's kvrocks role independently
// of the tick, so a slow tick or a short Consul gap does not by itself
// cut routes to a leader that is still master (see routeConfirmedLocked).
func (c *Controller) leaderVerifyLoop(ctx context.Context) {
	every := c.cfg.CheckInterval
	if every < 100*time.Millisecond {
		every = 100 * time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.verifyLeaderOnce(ctx)
	}
}

func (c *Controller) verifyLeaderOnce(ctx context.Context) {
	c.mu.RLock()
	lv := c.leader
	self := c.role == elect.RoleLeader || (lv != nil && lv.Session != "" && lv.Session == c.sessionID)
	c.mu.RUnlock()
	target := c.followTarget(lv)
	if self || target == nil {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, minDur(c.cfg.KvrocksTimeout, c.confirmGrace()/2))
	defer cancel()
	ph, pp := c.probeAddr(target)
	pi, err := c.deps.KV.PeerInfoReplication(pctx, ph, pp)
	if err != nil || pi == nil || pi.Role != "master" {
		return
	}
	c.mu.Lock()
	if c.leader != nil && c.leader.Session == lv.Session && c.leader.Epoch == lv.Epoch {
		c.verifiedSession, c.verifiedEpoch, c.verifiedAt = lv.Session, lv.Epoch, time.Now()
	}
	c.mu.Unlock()
}

// leaderConfirmLoop re-reads the leader key from Consul on its own
// schedule, so route confirmation does not depend on tick. tick reads the
// key only after its local kvrocks calls (probe, PSYNC check, sampling,
// REPLICAOF), and on a replica catching up under heavy load those can
// together take longer than the session TTL; routes were then cut with
// "leadership state not confirmed with Consul" although nothing changed.
//
// It only refreshes lastLockRead when Consul shows the same holder session
// and the same epoch as the leader the routes are built from. Any
// difference is applied to c.leader at once (so ProxyView's key changes and
// routes are cut) and a tick is triggered to act on it.
func (c *Controller) leaderConfirmLoop(ctx context.Context) {
	every := c.cfg.CheckInterval / 2
	if every > time.Second {
		every = time.Second
	}
	if every < 50*time.Millisecond {
		every = 50 * time.Millisecond
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.confirmLeaderOnce(ctx)
	}
}

func (c *Controller) confirmLeaderOnce(ctx context.Context) {
	c.mu.RLock()
	lv := c.leader
	c.mu.RUnlock()
	if lv == nil || lv.Session == "" {
		return
	}
	timeout := c.cfg.SessionTTL / 4
	if timeout <= 0 || timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	gctx, cancel := context.WithTimeout(ctx, timeout)
	raw, holder, _, err := c.deps.Lock.Get(gctx, c.cfg.LeaderKey())
	cancel()
	if err != nil {
		return // no news; the TTL bound in routeConfirmedLocked still applies
	}
	now := time.Now()
	cur := elect.ParseLeader(raw)
	c.mu.Lock()
	if c.leader != lv {
		// tick or the watch replaced the leader meanwhile; this read may
		// predate that, so ignore it.
		c.mu.Unlock()
		return
	}
	if holder != "" && holder == lv.Session && cur != nil && cur.Session == lv.Session && cur.Epoch == lv.Epoch {
		c.lastLockRead = now
		c.mu.Unlock()
		return
	}
	// Changed or lost: stop routing to the old leader now.
	if holder == "" {
		c.leader = nil
	} else {
		c.leader = cur
	}
	c.mu.Unlock()
	c.notify()
	select {
	case c.cmds <- func(ctx context.Context) { c.tick(ctx) }:
	default:
	}
}
