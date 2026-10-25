package controller

import (
	"context"
	"time"
)

// announcer is implemented by kvrocks clients that can set the replica
// announce address (the real client; optional for fakes).
type announcer interface {
	SetReplicaAnnounce(ctx context.Context, ip string, port int) error
	ReplicaAnnounce(ctx context.Context) (string, int, error)
}

// announceRecheck is how often an applied announce address is verified,
// catching a kvrocks restart faster than one health tick.
const announceRecheck = 30 * time.Second

// ensureAnnounce sets replica-announce-ip/-port on the local kvrocks when
// forwarding is enabled: through the forwarder the master would otherwise
// list this replica under the leader host's own address. Applied before
// REPLICAOF, re-applied after kvrocks was seen down, and re-verified
// periodically.
func (c *Controller) ensureAnnounce(ctx context.Context) {
	if !c.cfg.ForwardEnabled || !c.cfg.ReplicaAnnounce || c.cfg.ReplicaAnnounceIP == "" {
		return
	}
	a, ok := c.deps.KV.(announcer)
	if !ok {
		return
	}
	now := time.Now()
	if c.annOK.Load() {
		if now.Sub(time.Unix(0, c.annAt.Load())) < announceRecheck {
			return
		}
		c.annAt.Store(now.UnixNano())
		if ip, p, err := a.ReplicaAnnounce(ctx); err == nil && ip == c.cfg.ReplicaAnnounceIP && p == c.cfg.ReplicaAnnouncePort {
			return
		}
	}
	if err := a.SetReplicaAnnounce(ctx, c.cfg.ReplicaAnnounceIP, c.cfg.ReplicaAnnouncePort); err != nil {
		c.annOK.Store(false)
		c.log.Error("setting replica-announce-ip/port failed", map[string]any{"error": err.Error()})
		return
	}
	c.annAt.Store(now.UnixNano())
	c.annOK.Store(true)
	c.log.Info("replica announce address set", map[string]any{"ip": c.cfg.ReplicaAnnounceIP, "port": c.cfg.ReplicaAnnouncePort})
}
