package elect

import (
	"strconv"
	"strings"
	"time"
)

// Heartbeat is the value the leader writes to its heartbeat key: the
// leader's wall clock in unix milliseconds and its epoch.
type Heartbeat struct {
	UnixMs int64
	Epoch  uint64
}

// FormatHeartbeat encodes a heartbeat as "<unix ms>:<epoch>".
func FormatHeartbeat(t time.Time, epoch uint64) string {
	return strconv.FormatInt(t.UnixMilli(), 10) + ":" + strconv.FormatUint(epoch, 10)
}

// ParseHeartbeat decodes FormatHeartbeat's output.
func ParseHeartbeat(s string) (Heartbeat, bool) {
	ms, ep, ok := strings.Cut(strings.TrimSpace(s), ":")
	if !ok {
		return Heartbeat{}, false
	}
	m, err := strconv.ParseInt(ms, 10, 64)
	if err != nil || m <= 0 {
		return Heartbeat{}, false
	}
	e, err := strconv.ParseUint(ep, 10, 64)
	if err != nil {
		return Heartbeat{}, false
	}
	return Heartbeat{UnixMs: m, Epoch: e}, true
}

// MissingLocalHeartbeat is the lag reported when the leader has a
// heartbeat but the local instance has none: it has not replicated even
// one heartbeat, so it is treated as far behind.
const MissingLocalHeartbeat = time.Hour

// HeartbeatLag is how far the local copy of the heartbeat trails the
// leader's, both stamped by the leader's clock (so clock skew between nodes
// does not matter). ok is false when the leader's heartbeat is unknown;
// the caller then falls back to the offset gate.
func HeartbeatLag(peer, local Heartbeat, havePeer, haveLocal bool) (time.Duration, bool) {
	if !havePeer {
		return -1, false
	}
	if !haveLocal {
		return MissingLocalHeartbeat, true
	}
	d := time.Duration(peer.UnixMs-local.UnixMs) * time.Millisecond
	if d < 0 {
		d = 0
	}
	return d, true
}
