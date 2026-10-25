package controller

import (
	"context"
	"encoding/json"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/p1u3o/kvlock/internal/consulstore"
)

// Member is what a node publishes in its member key. Without peer
// exposure the value is the bare node name (the original format); with
// it, a JSON object that also carries the alloc index and address.
type Member struct {
	Node    string `json:"node"`
	Index   int    `json:"index"`
	Addr    string `json:"addr,omitempty"`
	Private string `json:"private,omitempty"`
	Port    int    `json:"port,omitempty"`
}

// ParseMember reads either member value format. Index is -1 for the
// bare-name format.
func ParseMember(v string) Member {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "{") {
		var m Member
		if json.Unmarshal([]byte(v), &m) == nil && m.Node != "" {
			return m
		}
	}
	return Member{Node: v, Index: -1}
}

// memberValue is this node's member key value.
func (c *Controller) memberValue() []byte {
	if !c.cfg.PeerExpose {
		return []byte(c.cfg.NodeName)
	}
	b, _ := json.Marshal(Member{Node: c.cfg.NodeName, Index: c.cfg.AllocIndex,
		Addr: c.cfg.AdvertiseAddr, Private: c.cfg.AdvertiseAddrPrivate, Port: c.cfg.AdvertisePort})
	return b
}

type peerTable struct {
	mu      sync.RWMutex
	byIndex map[int]Member
	byNode  map[string]Member
}

// RefreshPeers re-reads live member keys. With peer exposure on, it is
// called periodically by the peer listener manager.
func (c *Controller) RefreshPeers(ctx context.Context) error {
	pr, ok := c.deps.Lock.(consulstore.Presence)
	if !ok {
		return nil
	}
	vals, err := pr.Members(ctx, c.cfg.MembersPrefix())
	if err != nil {
		return err
	}
	byIndex, byNode := map[int]Member{}, map[string]Member{}
	for _, v := range vals {
		m := ParseMember(v)
		if m.Node == "" {
			continue
		}
		byNode[m.Node] = m
		if m.Index >= 0 && m.Port > 0 {
			if prev, dup := byIndex[m.Index]; dup && prev.Node != m.Node {
				// Two live members claim one index (shouldn't happen with
				// Nomad); expose neither rather than guess.
				byIndex[m.Index] = Member{Index: m.Index}
				continue
			}
			byIndex[m.Index] = m
		}
	}
	c.peers.mu.Lock()
	c.peers.byIndex, c.peers.byNode = byIndex, byNode
	c.peers.mu.Unlock()
	return nil
}

// PeerIndexes lists alloc indexes currently known, sorted.
func (c *Controller) PeerIndexes() []int {
	c.peers.mu.RLock()
	defer c.peers.mu.RUnlock()
	out := make([]int, 0, len(c.peers.byIndex))
	for i := range c.peers.byIndex {
		out = append(out, i)
	}
	sort.Ints(out)
	return out
}

// PeerRoute is the route for the peer address of alloc index i: this
// node's own kvrocks for its own index, else the member's published
// address (private preferred). The key changes when the member does.
func (c *Controller) PeerRoute(i int) (addr, key, reason string) {
	if i == c.cfg.AllocIndex {
		a := net.JoinHostPort(c.cfg.KvrocksAddr, strconv.Itoa(c.cfg.KvrocksPort))
		return a, "self|" + a, ""
	}
	c.peers.mu.RLock()
	m, ok := c.peers.byIndex[i]
	c.peers.mu.RUnlock()
	if !ok || m.Node == "" {
		return "", "", "no live member with this alloc index"
	}
	if m.Node == c.cfg.NodeName {
		return "", "", "alloc index belongs to this node"
	}
	h := m.Addr
	if m.Private != "" {
		h = m.Private
	}
	if h == "" {
		return "", "", "member has no address"
	}
	a := net.JoinHostPort(h, strconv.Itoa(m.Port))
	return a, m.Node + "|" + a, ""
}

// probeAddr is where to sample a peer's replication info: its stable
// peer address when exposed and known, else its real address.
func (c *Controller) probeAddr(t *consulstore.LeaderValue) (string, int) {
	if t == nil {
		return "", 0
	}
	if c.cfg.PeerExpose {
		c.peers.mu.RLock()
		m, ok := c.peers.byNode[t.Node]
		c.peers.mu.RUnlock()
		if ok && m.Index >= 0 {
			if a, err := c.cfg.PeerAddr(m.Index); err == nil {
				if h, ps, err := net.SplitHostPort(a); err == nil {
					p, _ := strconv.Atoi(ps)
					return h, p
				}
			}
		}
	}
	return t.Addr, t.Port
}
