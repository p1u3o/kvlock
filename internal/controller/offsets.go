package controller

import (
	"context"
	"strings"
	"time"

	"github.com/p1u3o/kvlock/internal/consulstore"
	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/kvrocks"
)

// offsetFresh is how old a published offset may be and still count.
func (c *Controller) offsetFresh() time.Duration {
	d := 5 * c.cfg.CheckInterval
	if d < 2*time.Second {
		d = 2 * time.Second
	}
	return d
}

// offsetDecision runs while no session holds the leader lock. It
// publishes this node's replication offset under OffsetsPrefix (held by
// this node's session, so it vanishes with the sidecar) and compares it
// with the offsets of every live peer it knows about. Allow means this
// node is the most up-to-date live node: no live peer is ahead and every
// live peer (and the old leader, if it was another node) has published.
func (c *Controller) offsetDecision(ctx context.Context, ri *kvrocks.ReplicationInfo, oldMaster *consulstore.LeaderValue) elect.PromotionDecision {
	deny := func(r string) elect.PromotionDecision { return elect.PromotionDecision{Allow: false, Reason: r} }
	if !c.cfg.OffsetElect || c.cfg.Mode == "observe" {
		return deny("offset election off")
	}
	pr, ok := c.deps.Lock.(consulstore.Presence)
	if !ok {
		return deny("no presence support")
	}
	if ri == nil || ri.LocalOffset < 0 {
		return deny("own offset unknown")
	}
	c.mu.RLock()
	sid := c.sessionID
	c.mu.RUnlock()
	if sid == "" {
		return deny("no session")
	}
	now := time.Now()
	if ok, err := c.deps.Lock.Acquire(ctx, c.cfg.OffsetsPrefix()+c.cfg.NodeName, sid,
		elect.MarshalOffset(c.cfg.NodeName, ri.LocalOffset, now)); err != nil || !ok {
		if err != nil {
			c.log.Warn("could not publish replication offset", map[string]any{"error": err.Error()})
		}
		return deny("could not publish own offset")
	}
	peers, err := c.livePeers(ctx, pr)
	if err != nil {
		return deny("could not list live peers: " + err.Error())
	}
	vals, err := pr.Members(ctx, c.cfg.OffsetsPrefix())
	if err != nil {
		return deny("could not read peer offsets: " + err.Error())
	}
	var entries []elect.OffsetEntry
	for _, v := range vals {
		if e, ok := elect.ParseOffset(v); ok {
			entries = append(entries, e)
		}
	}
	in := elect.OffsetElectInput{Self: c.cfg.NodeName, SelfOffset: ri.LocalOffset, Peers: peers,
		Offsets: entries, Fresh: c.offsetFresh(), Now: now}
	if oldMaster != nil {
		in.OldLeader = oldMaster.Node
	}
	dec := elect.OffsetElect(in)
	res := "deny"
	if dec.Allow {
		res = "allow"
		c.mu.Lock()
		c.offsetBestAt = now
		c.offsetBest = dec.Reason
		c.mu.Unlock()
	}
	c.reg.OffsetElections.WithLabelValues(res).Inc()
	return dec
}

// livePeers lists every other live sidecar it can see: member keys held
// by a session plus passing catalog instances (service ID minus the
// service-name prefix).
func (c *Controller) livePeers(ctx context.Context, pr consulstore.Presence) ([]string, error) {
	seen := map[string]bool{}
	ms, err := pr.Members(ctx, c.cfg.MembersPrefix())
	if err != nil {
		return nil, err
	}
	for _, v := range ms {
		if m := ParseMember(v).Node; m != "" && m != c.cfg.NodeName {
			seen[m] = true
		}
	}
	if ph, ok := c.deps.Registrar.(consulstore.PeerHealth); ok {
		insts, err := ph.HealthyInstances(ctx, c.cfg.ServiceName, "")
		if err != nil {
			return nil, err
		}
		for _, in := range insts {
			if in.ID == "" || in.ID == c.serviceID() {
				continue
			}
			// Service IDs are <service>-<node>. An ID that does not map to
			// a node name just has no offset entry, which denies: safe.
			n := strings.TrimPrefix(in.ID, c.cfg.ServiceName+"-")
			if n != c.cfg.NodeName {
				seen[n] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	return out, nil
}
