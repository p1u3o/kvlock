// Package elect holds the election state machine and promotion-gate logic.
// It is deliberately free of Consul/kvrocks imports so unit tests can drive
// it with fakes.
package elect

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/example/kvrocks-leader/internal/consulstore"
)

// Role is the sidecar's current role.
type Role string

const (
	RoleUnknown  Role = "unknown"
	RoleLeader   Role = "leader"
	RoleFollower Role = "follower"
)

// PromotionDecision explains whether becoming leader is allowed.
type PromotionDecision struct {
	Allow  bool
	Reason string
}

// ShouldPromote gates promotion on replica lag.
//
//   - lagBytes < 0 means "unknown": allow only if the old master has been
//     unreachable longer than holdDown (or there is no old master).
//   - otherwise allow only if lag <= maxLag.
func ShouldPromote(lagBytes int64, maxLag int64, masterLastSeen time.Time, holdDown time.Duration, now time.Time, haveOldMaster bool) PromotionDecision {
	if lagBytes < 0 {
		if !haveOldMaster {
			return PromotionDecision{true, "no previous leader recorded; lag unknown but nothing to compare against"}
		}
		if now.Sub(masterLastSeen) >= holdDown {
			return PromotionDecision{true, "lag unknown; old master unreachable beyond hold-down"}
		}
		return PromotionDecision{false, "lag unknown and old master recently seen; waiting out hold-down"}
	}
	if lagBytes <= maxLag {
		return PromotionDecision{true, "lag within bound"}
	}
	return PromotionDecision{false, "replica too far behind last known master"}
}

// NextEpoch bumps the epoch for a new leader. Empty/invalid old value -> 1.
func NextEpoch(raw []byte) uint64 {
	if len(raw) == 0 {
		return 1
	}
	var lv consulstore.LeaderValue
	if err := json.Unmarshal(raw, &lv); err != nil {
		return 1
	}
	return lv.Epoch + 1
}

// MarshalLeader builds the lock value.
func MarshalLeader(node, addr string, port int, session string, epoch uint64, since time.Time) []byte {
	lv := consulstore.LeaderValue{
		Node: node, Addr: addr, Port: port,
		Session: session, Since: since.UTC().Format(time.RFC3339Nano), Epoch: epoch,
	}
	b, _ := json.Marshal(lv)
	return b
}

// ParseLeader parses the lock value; returns nil when empty.
func ParseLeader(raw []byte) *consulstore.LeaderValue {
	if len(raw) == 0 {
		return nil
	}
	var lv consulstore.LeaderValue
	if err := json.Unmarshal(raw, &lv); err != nil {
		return nil
	}
	return &lv
}

// Backoff computes capped exponential backoff with full jitter.
func Backoff(attempt int, base, max time.Duration, rng *rand.Rand) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := base << attempt
	if d <= 0 || d > max {
		d = max
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(rng.Int63n(int64(d) + 1))
}

// LockState classifies who holds the leader key.
type LockState int

const (
	LockVacant LockState = iota // no session holds the key
	LockMine                    // our session holds it
	LockOther                   // some other session holds it
)

// Classify decides leadership purely from the session holding the key.
// The value's node name is informational only: it outlives releases and
// does not distinguish two incarnations of the sidecar on one node.
func Classify(holder, mySession string) LockState {
	switch {
	case holder == "":
		return LockVacant
	case mySession != "" && holder == mySession:
		return LockMine
	default:
		return LockOther
	}
}

// ConsulOutageExceeded reports whether a leader that last renewed its
// session at lastRenew must step down because Consul may already have
// expired the session (TTL elapsed without a successful renew).
func ConsulOutageExceeded(lastRenew, now time.Time, ttl time.Duration) bool {
	return lastRenew.IsZero() || now.Sub(lastRenew) >= ttl
}
