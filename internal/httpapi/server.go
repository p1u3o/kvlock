package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/example/kvrocks-leader/internal/elect"
	"github.com/example/kvrocks-leader/internal/metrics"
)

// Snapshot is served at /status.
type Snapshot struct {
	Role            elect.Role `json:"role"`
	Leader          string     `json:"leader,omitempty"`
	LeaderAddr      string     `json:"leader_addr,omitempty"`
	Epoch           uint64     `json:"epoch"`
	LagBytes        int64      `json:"lag_bytes"`
	KvrocksUp       bool       `json:"kvrocks_up"`
	Mode            string     `json:"mode"`
	SessionValid    bool       `json:"session_valid"`
	UnhealthyStreak int        `json:"unhealthy_streak"`
}

// Provider supplies the current snapshot + metrics.
type Provider interface {
	Snapshot() Snapshot
	Metrics() *metrics.Registry
}

// Server wires /healthz /status /metrics.
func Handler(p Provider) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		snap := p.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		if !snap.KvrocksUp && snap.Role != elect.RoleLeader {
			// Follower with down kvrocks is still alive as a process; report degraded.
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "role": snap.Role})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.Snapshot())
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(p.Metrics().Render()))
	})
	return mux
}
