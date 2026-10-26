package elect

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// OffsetEntry is a node's published replication offset, written while no
// leader holds the lock so that leaderless nodes can compare data.
type OffsetEntry struct {
	Node   string `json:"node"`
	Offset int64  `json:"offset"`
	AtMs   int64  `json:"at_ms"`
}

// MarshalOffset encodes an offset entry.
func MarshalOffset(node string, offset int64, at time.Time) []byte {
	b, _ := json.Marshal(OffsetEntry{Node: node, Offset: offset, AtMs: at.UnixMilli()})
	return b
}

// ParseOffset decodes an offset entry; ok is false for anything unusable.
func ParseOffset(v string) (OffsetEntry, bool) {
	var e OffsetEntry
	if json.Unmarshal([]byte(v), &e) != nil || e.Node == "" || e.AtMs <= 0 {
		return OffsetEntry{}, false
	}
	return e, true
}

// OffsetElectInput is what the offset comparison looks at.
type OffsetElectInput struct {
	Self       string
	SelfOffset int64 // < 0 unknown
	// Peers are the other live nodes this node knows about (member keys
	// held by a session, passing catalog entries). Self is ignored.
	Peers []string
	// Offsets are the published entries that are still session-held.
	Offsets []OffsetEntry
	// OldLeader is the node named by the last leader value, "" if none or
	// this node. If it is another node it must be a live peer with a fresh
	// entry: a departed leader may hold writes nobody else has.
	OldLeader string
	Fresh     time.Duration
	Now       time.Time
}

// OffsetElect decides whether this node is the most up-to-date live node
// and may promote although its lag behind the old leader is unknown or
// it is deferring as a restarted former leader. It allows only when every
// known live peer has a fresh published offset and none is ahead; ties go
// to the lowest node name. Every node evaluating the same entries picks
// the same winner, and a node behind any live node never wins.
func OffsetElect(in OffsetElectInput) PromotionDecision {
	if in.SelfOffset < 0 {
		return PromotionDecision{false, "own offset unknown"}
	}
	fresh := map[string]int64{}
	for _, e := range in.Offsets {
		age := in.Now.Sub(time.UnixMilli(e.AtMs))
		if e.Node == in.Self || age > in.Fresh || age < -in.Fresh {
			continue
		}
		fresh[e.Node] = e.Offset
	}
	peers := map[string]bool{}
	for _, p := range in.Peers {
		if p != "" && p != in.Self {
			peers[p] = true
		}
	}
	if in.OldLeader != "" && in.OldLeader != in.Self {
		peers[in.OldLeader] = true
	}
	if len(peers) == 0 {
		return PromotionDecision{false, "no peers to compare with"}
	}
	names := make([]string, 0, len(peers))
	for p := range peers {
		names = append(names, p)
	}
	sort.Strings(names)
	for _, p := range names {
		off, ok := fresh[p]
		if !ok {
			return PromotionDecision{false, fmt.Sprintf("no fresh offset from peer %s", p)}
		}
		if off > in.SelfOffset {
			return PromotionDecision{false, fmt.Sprintf("peer %s is ahead (%d > %d)", p, off, in.SelfOffset)}
		}
		if off == in.SelfOffset && p < in.Self {
			return PromotionDecision{false, fmt.Sprintf("peer %s has the same offset %d and wins the tie", p, off)}
		}
	}
	return PromotionDecision{true, fmt.Sprintf("most up-to-date live node (offset %d, %d peers compared)", in.SelfOffset, len(names))}
}
