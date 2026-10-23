package controller

import (
	"context"
	"time"

	"github.com/p1u3o/kvlock/internal/elect"
)

// hbState is the heartbeat bookkeeping, guarded by Controller.mu.
type hbState struct {
	local     elect.Heartbeat
	haveLocal bool
	localAt   time.Time // when local was read

	peer        elect.Heartbeat
	havePeer    bool
	peerAt      time.Time // when peer was last read successfully
	peerSession string    // lock session the peer sample belongs to

	lag time.Duration // -1 unknown
}

// heartbeatLoop writes the heartbeat while this node leads (active mode
// only) and refreshes the heartbeat lag while it follows.
func (c *Controller) heartbeatLoop(ctx context.Context) {
	if c.cfg.HeartbeatInterval <= 0 {
		return
	}
	t := time.NewTicker(c.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.heartbeatStep(ctx)
	}
}

func (c *Controller) heartbeatStep(ctx context.Context) {
	c.mu.RLock()
	leading := c.role == elect.RoleLeader && c.backendRole == "master" && c.kvUp
	epoch := uint64(0)
	if c.leader != nil {
		epoch = c.leader.Epoch
	}
	c.mu.RUnlock()
	if leading {
		c.writeHeartbeat(ctx, epoch)
		return
	}
	c.readHeartbeats(ctx, nil)
}

// writeHeartbeat stores the leader heartbeat. Never in observe mode:
// observe must not write to the instances it watches.
func (c *Controller) writeHeartbeat(ctx context.Context, epoch uint64) {
	if c.cfg.Mode == "observe" {
		return
	}
	now := time.Now()
	wctx, cancel := context.WithTimeout(ctx, minDur(c.cfg.KvrocksTimeout, time.Second))
	defer cancel()
	if err := c.deps.KV.SetHeartbeat(wctx, c.cfg.HeartbeatKey(), elect.FormatHeartbeat(now, epoch)); err != nil {
		c.reg.Heartbeats.WithLabelValues("error").Inc()
		return
	}
	c.reg.Heartbeats.WithLabelValues("ok").Inc()
	c.mu.Lock()
	c.hb.local, c.hb.haveLocal, c.hb.localAt = elect.Heartbeat{UnixMs: now.UnixMilli(), Epoch: epoch}, true, now
	c.hb.lag = 0
	c.mu.Unlock()
	c.reg.LagSeconds.Set(0)
	c.reg.HeartbeatAge.Set(0)
}

// readHeartbeats reads the heartbeat from the local instance and from the
// leader (or old, when given: the leader that just left). Reads only.
func (c *Controller) readHeartbeats(ctx context.Context, old *electTarget) {
	key := c.cfg.HeartbeatKey()
	rctx, cancel := context.WithTimeout(ctx, minDur(c.cfg.KvrocksTimeout, time.Second))
	defer cancel()

	c.mu.RLock()
	lv := c.leader
	c.mu.RUnlock()
	var target *electTarget
	if old != nil {
		target = old
	} else if t := c.followTarget(lv); t != nil {
		target = &electTarget{addr: t.Addr, port: t.Port, session: t.Session}
	}
	if target != nil {
		c.mu.Lock()
		if old == nil && c.hb.peerSession != target.session {
			c.hb.peerSession = target.session
			c.hb.havePeer = false
			c.hb.peerAt = time.Time{}
		}
		c.mu.Unlock()
		if v, err := c.deps.KV.PeerGetHeartbeat(rctx, target.addr, target.port, key); err == nil {
			if p, ok := elect.ParseHeartbeat(v); ok {
				c.mu.Lock()
				if c.hb.peerSession == target.session && (!c.hb.havePeer || p.UnixMs >= c.hb.peer.UnixMs) {
					c.hb.peer, c.hb.havePeer, c.hb.peerAt = p, true, time.Now()
				}
				c.mu.Unlock()
			}
		}
	}
	// Local after peer: a local copy read later can only look fresher.
	v, err := c.deps.KV.GetHeartbeat(rctx, key)
	now := time.Now()
	c.mu.Lock()
	if err == nil {
		c.hb.local, c.hb.haveLocal = elect.ParseHeartbeat(v)
		c.hb.localAt = now
	}
	lag, ok := elect.HeartbeatLag(c.hb.peer, c.hb.local, c.hb.havePeer, c.hb.haveLocal)
	if !ok {
		lag = -1
	}
	c.hb.lag = lag
	age := -1.0
	if c.hb.haveLocal {
		age = now.Sub(time.UnixMilli(c.hb.local.UnixMs)).Seconds()
	}
	c.mu.Unlock()
	if lag < 0 {
		c.reg.LagSeconds.Set(-1)
	} else {
		c.reg.LagSeconds.Set(lag.Seconds())
	}
	c.reg.HeartbeatAge.Set(age)
}

type electTarget struct {
	addr    string
	port    int
	session string
}

// timeGateInput returns the heartbeat lag in milliseconds and the age of
// the leader's heartbeat sample when it was last seen, or ok=false when no
// heartbeat of the departed leader was ever sampled.
func (c *Controller) timeGateInput(oldSeen time.Time) (lagMs int64, sampleAge time.Duration, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.hb.havePeer || c.hb.lag < 0 {
		return -1, -1, false
	}
	ref := oldSeen
	if ref.IsZero() || c.hb.peerAt.After(ref) {
		ref = c.hb.peerAt
	}
	return c.hb.lag.Milliseconds(), ref.Sub(c.hb.peerAt), true
}
