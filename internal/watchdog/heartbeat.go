// Package watchdog fences the local kvrocks when its kvlock sidecar dies.
//
// The sidecar writes a small heartbeat file (normally in the Nomad alloc
// dir, shared by every task in the group) from a dedicated goroutine. A
// separate process, `kvrocks-leader watchdog`, reads it. When the file has
// not been refreshed within --timeout and the local kvrocks is still a
// writable master, the watchdog fences it with REPLICAOF to the unroutable
// fence target, closes client connections and confirms that a write is now
// refused. It never promotes, never touches Consul and never undoes a fence:
// the restarted sidecar decides what the instance should be.
package watchdog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Heartbeat is the content of the heartbeat file.
type Heartbeat struct {
	// UnixMs is when the sidecar wrote the file (sidecar clock; the
	// watchdog runs on the same host).
	UnixMs int64 `json:"unix_ms"`
	// LoopUnixMs is when the sidecar's control loop last finished an
	// iteration (informational; staleness is judged on UnixMs).
	LoopUnixMs int64  `json:"loop_unix_ms,omitempty"`
	PID        int    `json:"pid"`
	Node       string `json:"node"`
	Mode       string `json:"mode"`
	Role       string `json:"role"`
	Epoch      uint64 `json:"epoch"`
}

// Time returns the write time.
func (h Heartbeat) Time() time.Time { return time.UnixMilli(h.UnixMs) }

// WriteHeartbeat writes hb to path atomically (temp file + rename) so a
// reader never sees a torn file. The parent directory is created.
func WriteHeartbeat(path string, hb Heartbeat) error {
	if path == "" {
		return errors.New("empty heartbeat path")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".kvlock-hb-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// ReadHeartbeat reads and parses the heartbeat file. A missing file
// returns an error satisfying errors.Is(err, os.ErrNotExist).
func ReadHeartbeat(path string) (Heartbeat, error) {
	var hb Heartbeat
	b, err := os.ReadFile(path)
	if err != nil {
		return hb, err
	}
	if err := json.Unmarshal(b, &hb); err != nil {
		return hb, fmt.Errorf("parse heartbeat %s: %w", path, err)
	}
	if hb.UnixMs <= 0 {
		return hb, fmt.Errorf("heartbeat %s has no timestamp", path)
	}
	return hb, nil
}
