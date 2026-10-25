package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/p1u3o/kvlock/internal/consulstore"
	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/kvrocks"
)

// markRejoinLocked records that an earlier incarnation of this sidecar was
// the last leader. Caller holds c.mu.
func (c *Controller) markRejoinLocked() {
	if !c.rejoin {
		c.rejoin = true
		c.rejoinSince = time.Now()
	}
}

// clearRejoin ends the rejoin state once the local instance replicates
// from another node's leader with the link up.
func (c *Controller) clearRejoin(lv *consulstore.LeaderValue) {
	c.mu.Lock()
	was := c.rejoin
	c.rejoin = false
	c.rejoinSince = time.Time{}
	c.mu.Unlock()
	if was && lv != nil {
		c.event("rejoin", fmt.Sprintf("resynced from new leader %s; eligible for election again", lv.Node))
		c.log.Info("resynced from new leader; eligible for election again", map[string]any{"leader": lv.Node})
	}
}

// releaseStaleSelf handles the lock still held by a session of an earlier
// incarnation of this sidecar (it was killed while leader). The local
// instance is a replica (fenced), so that session leads nothing: give the
// key back with Consul's release op, which unlike session expiry applies
// no lock-delay, then destroy the session. Returns true if it released.
func (c *Controller) releaseStaleSelf(ctx context.Context, ri *kvrocks.ReplicationInfo, holder string) bool {
	if c.cfg.Mode == "observe" || ri == nil || ri.Role != "slave" || holder == "" {
		return false
	}
	c.mu.Lock()
	c.markRejoinLocked()
	c.mu.Unlock()
	if err := c.deps.Lock.Release(ctx, c.cfg.LeaderKey(), holder); err != nil {
		c.log.Warn("could not release lock held by this node's previous session", map[string]any{"error": err.Error()})
		return false
	}
	if err := c.deps.Session.Destroy(ctx, holder); err != nil {
		c.log.Warn("could not destroy this node's previous session", map[string]any{"error": err.Error()})
	}
	c.event("rejoin", "released lock held by this node's previous session (sidecar restarted)")
	c.log.Warn("released lock held by this node's previous session", map[string]any{"session": holder})
	return true
}

// deferToPeer keeps a restarted former leader (unknown lag, no previous
// leader to compare with) from taking the lock while another sidecar of
// the service is healthy. It runs again once it has resynced from the new
// leader, when no healthy peer exists, or after LagGateMaxWait.
func (c *Controller) deferToPeer(ctx context.Context, ri *kvrocks.ReplicationInfo) bool {
	c.mu.RLock()
	rejoin, since := c.rejoin, c.rejoinSince
	c.mu.RUnlock()
	if !rejoin {
		return false
	}
	if c.cfg.LagGateMaxWait > 0 && time.Since(since) >= c.cfg.LagGateMaxWait {
		c.log.Warn("rejoin wait over; running for leader", map[string]any{"waited": time.Since(since).String()})
		return false
	}
	peer, err := c.livePeer(ctx)
	if err != nil {
		c.log.Warn("could not list healthy peers; running for leader", map[string]any{"error": err.Error()})
		return false
	}
	if peer == "" {
		c.observeLog("no healthy peer found; running for leader", nil)
		return false
	}
	c.setRole(elect.RoleFollower)
	c.registerReplica(ctx)
	if ri != nil && !c.isFenced(ri) {
		c.fence(ctx, nil)
	}
	c.observeLog("not running for leader: restarted former leader defers to healthy peer", map[string]any{"peer": peer})
	return true
}

// publishMember holds this node's member key with the current session.
func (c *Controller) publishMember(ctx context.Context, sid string) {
	if _, ok := c.deps.Lock.(consulstore.Presence); !ok {
		return
	}
	c.mu.RLock()
	done := c.memberSID == sid
	c.mu.RUnlock()
	if done {
		return
	}
	ok, err := c.deps.Lock.Acquire(ctx, c.cfg.MembersPrefix()+c.cfg.NodeName, sid, c.memberValue())
	if err != nil || !ok {
		return
	}
	c.mu.Lock()
	c.memberSID = sid
	c.mu.Unlock()
}

// livePeer names another live sidecar: a member key held by a session,
// else a passing catalog instance. Empty when none is known.
func (c *Controller) livePeer(ctx context.Context) (string, error) {
	if pr, ok := c.deps.Lock.(consulstore.Presence); ok {
		ms, err := pr.Members(ctx, c.cfg.MembersPrefix())
		if err != nil {
			return "", err
		}
		for _, v := range ms {
			if m := ParseMember(v).Node; m != "" && m != c.cfg.NodeName {
				return m, nil
			}
		}
	}
	ph, ok := c.deps.Registrar.(consulstore.PeerHealth)
	if !ok {
		return "", nil
	}
	insts, err := ph.HealthyInstances(ctx, c.cfg.ServiceName, "")
	if err != nil {
		return "", err
	}
	for _, in := range insts {
		if in.ID != "" && in.ID != c.serviceID() {
			return in.ID, nil
		}
	}
	return "", nil
}
