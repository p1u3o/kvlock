package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/metrics"
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
	ConsulOK        bool       `json:"consul_ok"`
	BackendRole     string     `json:"backend_role,omitempty"`
	Healthy         bool       `json:"healthy"`
	Problems        []string   `json:"problems,omitempty"`
}

// Provider supplies the current snapshot + metrics.
type Provider interface {
	Snapshot() Snapshot
	Metrics() *metrics.Registry
}

// Server wires /healthz /status /metrics.
//
// With an Admin whose Handover is set it also serves POST /handover.
func Handler(p Provider, admin ...Admin) http.Handler {
	mux := http.NewServeMux()
	if len(admin) > 0 && admin[0].Handover != nil {
		mux.HandleFunc("/handover", admin[0].handleHandover)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		snap := p.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		if !snap.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": snap.Healthy, "role": snap.Role, "problems": snap.Problems})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.Snapshot())
	})
	mux.Handle("/metrics", p.Metrics().Handler())
	return mux
}
