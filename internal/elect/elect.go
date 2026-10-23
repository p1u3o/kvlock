// Package elect holds the election state machine and promotion-gate logic.
// It is deliberately free of Consul/kvrocks imports so unit tests can drive
// it with fakes.
package elect

import (
	"encoding/json"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/p1u3o/kvlock/internal/consulstore"
	"github.com/p1u3o/kvlock/internal/kvrocks"
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

// LagGateInput is everything the promotion lag gate looks at.
type LagGateInput struct {
	Lag    int64 // local lag behind the leader sample; < 0 unknown
	MaxLag int64
	// SampleAge is how old the leader offset sample was when the leader was
	// last seen (< 0 unknown). A stale sample can understate the lag.
	SampleAge    time.Duration
	MaxSampleAge time.Duration // 0 disables the staleness check
	LastSeen     time.Time     // when the old leader was last seen
	HaveOld      bool
	HoldDown     time.Duration
	// MaxWait bounds how long a known-too-far-behind replica refuses: once
	// the old leader has been gone this long, promote anyway. 0 = never.
	MaxWait time.Duration
	Now     time.Time
}

// LagGate decides whether a replica may promote. It extends ShouldPromote
// with sample staleness (a sample older than MaxSampleAge counts as unknown
// lag, so the hold-down applies) and a bounded wait for a lagging replica.
func LagGate(in LagGateInput) PromotionDecision {
	lag := in.Lag
	stale := false
	// Only a sample that looks good can mislead: a stale sample understates
	// lag, so one already over the bound stays a refusal.
	if lag >= 0 && lag <= in.MaxLag && in.MaxSampleAge > 0 && (in.SampleAge < 0 || in.SampleAge > in.MaxSampleAge) {
		lag, stale = -1, true
	}
	d := ShouldPromote(lag, in.MaxLag, in.LastSeen, in.HoldDown, in.Now, in.HaveOld)
	if stale {
		d.Reason = "leader offset sample stale; " + d.Reason
	}
	if !d.Allow && lag > in.MaxLag && in.MaxWait > 0 && in.HaveOld && !in.LastSeen.IsZero() && in.Now.Sub(in.LastSeen) >= in.MaxWait {
		return PromotionDecision{true, "lag gate max wait exceeded; promoting behind the last known master"}
	}
	return d
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
func MarshalLeader(node, addr, privateAddr string, port int, session string, epoch uint64, since time.Time) []byte {
	lv := consulstore.LeaderValue{
		Node: node, Addr: addr, PrivateAddr: privateAddr, Port: port,
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

// ComputeLag returns how far local is behind the leader, using the leader's
// offset as last sampled. -1 means unknown (no sample, or local unknown).
// A sample is a lower bound on the leader's progress, so the result can
// underestimate lag by whatever the leader wrote after it was taken.
func ComputeLag(leaderOffset int64, haveSample bool, localOffset int64) int64 {
	if !haveSample || leaderOffset < 0 || localOffset < 0 {
		return -1
	}
	if d := leaderOffset - localOffset; d > 0 {
		return d
	}
	return 0
}

// WithinMinLeaderTime reports whether a leader since `since` is still inside
// the minimum leadership window. min <= 0 disables the window.
func WithinMinLeaderTime(since, now time.Time, min time.Duration) bool {
	return min > 0 && !since.IsZero() && now.Sub(since) < min
}

// HealthProblems lists why the sidecar is unhealthy; empty means healthy.
// backendRole is the role the local instance reports ("master"/"slave").
// In observe mode the sidecar never changes the instance's role, so role
// consistency is not checked.
func HealthProblems(role Role, backendRole string, backendUp, consulOK, observe bool) []string {
	var p []string
	if !backendUp {
		p = append(p, "backend unreachable")
	}
	if !consulOK {
		p = append(p, "consul unreachable")
	}
	if observe || !backendUp {
		return p
	}
	switch role {
	case RoleLeader:
		if backendRole != "master" {
			p = append(p, "leader but backend role is "+backendRole)
		}
	case RoleFollower:
		if backendRole != "slave" {
			p = append(p, "follower but backend role is "+backendRole)
		}
	default:
		p = append(p, "role not yet determined")
	}
	return p
}

// HandoverActive reports whether lv carries a planned-handover hint that
// has not yet expired.
func HandoverActive(lv *consulstore.LeaderValue, now time.Time) bool {
	if lv == nil || lv.HandoverTo == "" || lv.HandoverUntil == "" {
		return false
	}
	until, err := time.Parse(time.RFC3339Nano, lv.HandoverUntil)
	return err == nil && now.Before(until)
}

// IsHandoverTarget reports whether "ip:port" target names this node, given
// the addresses it is known by and its port.
func IsHandoverTarget(target string, addrs []string, port int) bool {
	host, p, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	if n, err := strconv.Atoi(p); err != nil || n != port {
		return false
	}
	for _, a := range addrs {
		if a != "" && a == host {
			return true
		}
	}
	return false
}

// HandoverCandidate is a replica that could take over, with its lag.
type HandoverCandidate struct {
	Addr string // ip:port as reported by the master
	Lag  int64
}

// PickHandoverTarget chooses the replica to hand over to from the leader's
// own INFO replication: the least-lagged replica that is online (when the
// server reports a state) and, if want is set, whose ip:port equals want.
// ok is false when there is no usable replica at all; the caller compares
// Lag against its bound.
func PickHandoverTarget(ri *kvrocks.ReplicationInfo, want string) (HandoverCandidate, bool) {
	var best HandoverCandidate
	found := false
	if ri == nil || ri.Role != "master" || ri.MasterOffset < 0 {
		return best, false
	}
	for _, r := range ri.Replicas {
		if r.IP == "" || r.Port <= 0 || (r.State != "" && r.State != "online") {
			continue
		}
		addr := net.JoinHostPort(r.IP, strconv.Itoa(r.Port))
		if want != "" && addr != want {
			continue
		}
		lag := ri.MasterOffset - r.Offset
		if lag < 0 {
			lag = 0
		}
		if !found || lag < best.Lag {
			best, found = HandoverCandidate{Addr: addr, Lag: lag}, true
		}
	}
	return best, found
}
