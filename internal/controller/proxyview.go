package controller

import (
	"net"
	"strconv"
	"time"

	"github.com/p1u3o/kvlock/internal/elect"
)

// ProxyView is what the leader proxies route on.
type ProxyView struct {
	// Leader is the address to dial for writes, "" when no safe target is
	// known (Reason says why).
	Leader string
	// Key identifies the routing decision; proxied connections are closed
	// whenever it changes (new holder, role change, fencing).
	Key    string
	Reason string
	// Replica is the local instance's address when it is a healthy,
	// caught-up replica of a known leader, else "".
	Replica string
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

// ProxyView computes the current routing decision. It fails closed: no
// leader target unless the lock holder is known and Consul answered
// within two check intervals.
func (c *Controller) ProxyView() ProxyView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	local := net.JoinHostPort(c.cfg.KvrocksAddr, strconv.Itoa(c.cfg.KvrocksPort))
	grace := 2 * c.cfg.CheckInterval
	if grace < time.Second {
		grace = time.Second
	}
	confirmed := !c.lastConsulOK.IsZero() && time.Since(c.lastConsulOK) < grace

	var v ProxyView
	lv := c.leader
	if lv != nil && c.role != elect.RoleLeader && c.kvUp && c.backendRole == "slave" && c.lag >= 0 && c.lag <= c.cfg.MaxLagBytes {
		v.Replica = local
	}
	switch {
	case lv == nil:
		v.Reason = "leader unknown"
	case !confirmed:
		v.Reason = "leadership state not confirmed with Consul"
	case lv.Session != "" && lv.Session == c.sessionID:
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
			v.Reason = "leader address unknown"
			break
		}
		v.Leader = net.JoinHostPort(host, strconv.Itoa(lv.Port))
	}
	if v.Leader != "" {
		v.Key = v.Leader + "|" + lv.Session + "|" + string(c.role) + "|" + strconv.FormatUint(c.fenceGen.Load(), 10)
	}
	return v
}
