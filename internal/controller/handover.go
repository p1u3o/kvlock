package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/example/kvrocks-leader/internal/consulstore"
	"github.com/example/kvrocks-leader/internal/elect"
	"github.com/example/kvrocks-leader/internal/httpapi"
	"github.com/example/kvrocks-leader/internal/kvrocks"
	"github.com/example/kvrocks-leader/internal/metrics"
)

var errNotLeader = errors.New("this node is not the leader")

// Handover performs a planned handover to a replica.
//
//  1. Checks: we are leader (active mode), our session was renewed
//     recently, and the local instance is a master with an online replica.
//  2. Wait (up to the timeout) until that replica's lag, as seen in our own
//     INFO replication, is within the bound. Writes are still accepted.
//  3. Commit, inside the control loop so no tick interleaves: re-check,
//     remove the master registration, fence (REPLICAOF the fence address),
//     record our final offset plus the chosen replica in the lock value,
//     kill client connections, release the lock.
//  4. Wait for another session to take the lock and report it.
//
// Any failure before the release leaves this node leader (re-promoting
// and re-registering if it had already fenced).
func (c *Controller) Handover(ctx context.Context, req httpapi.HandoverRequest) (httpapi.HandoverResult, error) {
	res := httpapi.HandoverResult{Stage: "checks", Lag: -1, StillLeader: c.isLeader()}
	if c.cfg.Mode == "observe" {
		return res, errors.New("handover is not available in observe mode")
	}
	if !c.isLeader() {
		return res, errNotLeader
	}
	if !c.handoverBusy.CompareAndSwap(false, true) {
		return res, errors.New("a handover is already in progress")
	}
	defer c.handoverBusy.Store(false)

	maxLag := c.cfg.HandoverMaxLag
	if req.MaxLag != nil && *req.MaxLag >= 0 {
		maxLag = *req.MaxLag
	}
	timeout := c.cfg.HandoverTimeout
	if d, err := time.ParseDuration(req.Timeout); err == nil && d > 0 {
		timeout = d
	}
	poll := minDur(c.cfg.CheckInterval, 500*time.Millisecond)
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	abort := func(r httpapi.HandoverResult, err error) (httpapi.HandoverResult, error) {
		c.reg.Handovers.WithLabelValues("aborted").Inc()
		c.log.Warn("handover aborted", map[string]any{"stage": r.Stage, "error": err.Error(), "still_leader": r.StillLeader})
		return r, err
	}
	c.log.Info("handover requested", map[string]any{"to": req.To, "max_lag": maxLag, "timeout": timeout.String()})

	res.Stage = "waiting"
	deadline := time.Now().Add(timeout)
	for {
		if !c.isLeader() {
			res.StillLeader = false
			return abort(res, errors.New("lost leadership while waiting for the replica"))
		}
		cand, _, why := c.handoverReady(ctx, req.To, maxLag)
		res.Target, res.Lag = cand.Addr, cand.Lag
		if why == "" {
			break
		}
		if !time.Now().Before(deadline) {
			return abort(res, fmt.Errorf("timed out after %v: %s", timeout, why))
		}
		select {
		case <-ctx.Done():
			return abort(res, fmt.Errorf("cancelled while waiting: %s", why))
		case <-time.After(poll):
		}
	}

	done := make(chan commitResult, 1)
	select {
	case c.cmds <- func(lctx context.Context) { done <- c.commitHandover(lctx, req.To, maxLag, timeout) }:
	case <-ctx.Done():
		return abort(res, errors.New("cancelled before commit"))
	}
	cr := <-done
	res.Target, res.Lag, res.StillLeader = cr.target, cr.lag, cr.stillLeader
	if cr.err != nil {
		res.Stage = "checks"
		return abort(res, cr.err)
	}
	res.Stage = "committed"

	// Report the successor. Our part is done either way.
	deadline = time.Now().Add(timeout)
	for {
		raw, holder, _, err := c.deps.Lock.Get(ctx, c.cfg.LeaderKey())
		if err == nil && holder != "" && holder != c.sessionIDValue() {
			if lv := elect.ParseLeader(raw); lv != nil {
				res.NewLeader = lv.Node
			}
			res.Stage = "done"
			c.reg.Handovers.WithLabelValues("ok").Inc()
			c.log.Info("handover complete", map[string]any{"new_leader": res.NewLeader})
			return res, nil
		}
		if !time.Now().Before(deadline) || ctx.Err() != nil {
			c.reg.Handovers.WithLabelValues("no_successor").Inc()
			return res, fmt.Errorf("lock released but no successor seen within %v; this node stays a fenced replica and normal election resumes when the hint expires", timeout)
		}
		select {
		case <-ctx.Done():
		case <-time.After(poll):
		}
	}
}

type commitResult struct {
	target      string
	lag         int64
	stillLeader bool
	err         error
}

// handoverReady checks the preconditions; why is "" when ready.
func (c *Controller) handoverReady(ctx context.Context, to string, maxLag int64) (elect.HandoverCandidate, *kvrocks.ReplicationInfo, string) {
	cand := elect.HandoverCandidate{Lag: -1}
	c.mu.RLock()
	last := c.lastRenewOK
	c.mu.RUnlock()
	if last.IsZero() || time.Since(last) > c.cfg.SessionTTL/2 {
		return cand, nil, "consul session not renewed recently"
	}
	kctx, cancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	defer cancel()
	ri, err := c.deps.KV.InfoReplication(kctx)
	if err != nil {
		return cand, nil, "local backend: " + err.Error()
	}
	if ri.Role != "master" {
		return cand, ri, "local instance is not a master (role " + ri.Role + ")"
	}
	found, ok := elect.PickHandoverTarget(ri, to)
	if !ok {
		if to != "" {
			return cand, ri, "no online replica at " + to
		}
		return cand, ri, "no online replica"
	}
	if found.Lag > maxLag {
		return found, ri, fmt.Sprintf("replica %s lag %d exceeds %d", found.Addr, found.Lag, maxLag)
	}
	return found, ri, ""
}

// commitHandover runs in the control loop goroutine.
func (c *Controller) commitHandover(ctx context.Context, to string, maxLag int64, window time.Duration) commitResult {
	stay := func(cand elect.HandoverCandidate, err error) commitResult {
		return commitResult{target: cand.Addr, lag: cand.Lag, stillLeader: c.isLeader(), err: err}
	}
	if !c.isLeader() {
		return stay(elect.HandoverCandidate{}, errNotLeader)
	}
	sid := c.sessionIDValue()
	raw, holder, _, err := c.deps.Lock.Get(ctx, c.cfg.LeaderKey())
	if err != nil {
		return stay(elect.HandoverCandidate{}, fmt.Errorf("reading lock: %w", err))
	}
	if sid == "" || holder != sid {
		return stay(elect.HandoverCandidate{}, errors.New("lock not held by our session"))
	}
	cand, pre, why := c.handoverReady(ctx, to, maxLag)
	if why != "" {
		return stay(cand, errors.New(why))
	}

	// Point of no return starts here; until the release, undo on failure.
	rollback := func(err error) commitResult {
		c.log.Error("handover failed after fencing; restoring leadership", map[string]any{"error": err.Error()})
		c.promote(ctx) // REPLICAOF NO ONE + register master
		return stay(cand, err)
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = c.deps.Registrar.FailTTL(dctx, c.deps.Registrar.CheckID(c.serviceID()))
	_ = c.deps.Registrar.Deregister(dctx, c.serviceID())
	if !c.fence(ctx, nil) {
		return rollback(errors.New("fencing the local instance failed"))
	}

	final := pre.MasterOffset
	kctx, kcancel := context.WithTimeout(ctx, c.cfg.KvrocksTimeout)
	if post, err := c.deps.KV.InfoReplication(kctx); err == nil && post.LocalOffset > final {
		final = post.LocalOffset
	}
	kcancel()

	lv := elect.ParseLeader(raw)
	if lv == nil {
		lv = &consulstore.LeaderValue{Node: c.cfg.NodeName, Addr: c.cfg.AdvertiseAddr, PrivateAddr: c.cfg.AdvertiseAddrPrivate, Port: c.cfg.AdvertisePort, Session: sid}
	}
	lv.HandoverTo = cand.Addr
	lv.HandoverUntil = time.Now().Add(window).UTC().Format(time.RFC3339Nano)
	lv.HandoverOffset = final
	value, _ := json.Marshal(lv)
	held, err := c.deps.Lock.Acquire(ctx, c.cfg.LeaderKey(), sid, value)
	if err != nil || !held {
		if err == nil {
			err = errors.New("lock no longer ours")
		}
		return rollback(fmt.Errorf("writing handover hint: %w", err))
	}

	c.killClients(ctx)
	c.releaseSession(ctx)
	c.reg.StepDowns.WithLabelValues(metrics.ReasonHandover).Inc()
	c.setRole(elect.RoleFollower)
	c.mu.Lock()
	c.leaderSince = time.Time{}
	c.mu.Unlock()
	c.registerReplica(ctx)
	c.log.Info("handover committed: lock released", map[string]any{"target": cand.Addr, "final_offset": final})
	return commitResult{target: cand.Addr, lag: cand.Lag}
}

// isHandoverTarget reports whether the hint names this node.
func (c *Controller) isHandoverTarget(lv *consulstore.LeaderValue) bool {
	addrs := []string{c.cfg.AdvertiseAddr, c.cfg.AdvertiseAddrPrivate, c.cfg.KvrocksAddr}
	if ifs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range ifs {
			if ipn, ok := a.(*net.IPNet); ok {
				addrs = append(addrs, ipn.IP.String())
			}
		}
	}
	for _, p := range []int{c.cfg.AdvertisePort, c.cfg.KvrocksPort} {
		if elect.IsHandoverTarget(lv.HandoverTo, addrs, p) {
			return true
		}
	}
	return false
}

// onHandoverHint handles a vacant lock carrying an active handover hint.
// It returns true when the caller must not run a normal election.
func (c *Controller) onHandoverHint(ctx context.Context, ri *kvrocks.ReplicationInfo, lv *consulstore.LeaderValue) bool {
	if !c.isHandoverTarget(lv) {
		c.log.Info("planned handover in progress; not contending", map[string]any{"to": lv.HandoverTo, "until": lv.HandoverUntil})
		c.setRole(elect.RoleFollower)
		if ri != nil && !c.isFenced(ri) {
			c.fence(ctx, nil)
		}
		return true
	}
	if lv.HandoverOffset <= 0 || ri == nil || ri.LocalOffset < 0 {
		return false // nothing to compare; the normal lag gate decides
	}
	lag := lv.HandoverOffset - ri.LocalOffset
	if lag < 0 {
		lag = 0
	}
	// Insist on a full catch-up for the first half of the window, then
	// accept anything within the handover lag bound.
	bound := c.cfg.HandoverMaxLag
	if until, err := time.Parse(time.RFC3339Nano, lv.HandoverUntil); err == nil && time.Now().Before(until.Add(-c.cfg.HandoverTimeout/2)) {
		bound = 0
	}
	if lag > bound {
		c.log.Info("handover target catching up before acquiring", map[string]any{"lag": lag, "bound": bound})
		return true
	}
	c.mu.Lock()
	c.peerOffset, c.peerSampled, c.peerAt = lv.HandoverOffset, true, time.Now()
	c.mu.Unlock()
	return false
}
