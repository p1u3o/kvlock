package httpapi

import (
	"encoding/json"
	"github.com/p1u3o/kvlock/internal/backup"
	"net/http"

	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/metrics"
)

// Snapshot is served at /status.
type Snapshot struct {
	// Backup is nil when backups are not configured.
	Backup *backup.Status `json:"backup,omitempty"`

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

	// Added in v0.2.0 for the status page.
	Version     string `json:"version,omitempty"`
	Cluster     string `json:"cluster"`
	Node        string `json:"node"`
	NodeAddr    string `json:"node_addr"`
	LeaderSince string `json:"leader_since,omitempty"`
	// LeaderAgeSeconds is how long the current holder has held the lock.
	LeaderAgeSeconds float64 `json:"leader_age_seconds"`
	// LagSeconds is the heartbeat lag (-1 unknown, 0 on the leader).
	LagSeconds float64 `json:"lag_seconds"`
	// HeartbeatAgeSeconds is local clock minus the newest local heartbeat
	// (includes clock skew; -1 unknown).
	HeartbeatAgeSeconds float64 `json:"heartbeat_age_seconds"`
	MaxLagSeconds       float64 `json:"max_lag_seconds"`
	MaxLagSeq           int64   `json:"max_lag_seq"`
	// LagGate is the gate a promotion would use now: "time" or "seq".
	LagGate      string   `json:"lag_gate"`
	MasterHost   string   `json:"master_host,omitempty"`
	MasterPort   int      `json:"master_port,omitempty"`
	MasterLinkUp bool     `json:"master_link_up"`
	Replicas     []string `json:"replicas,omitempty"`
	// LastRenewAgeSeconds is the time since Consul last confirmed our
	// session (-1 never).
	LastRenewAgeSeconds float64        `json:"last_renew_age_seconds"`
	FenceState          string         `json:"fence_state"`
	FencePending        bool           `json:"fence_pending"`
	Connections         map[string]int `json:"connections,omitempty"`
	// Forwarder is nil unless the replication forwarder is enabled.
	Forwarder *Forwarder `json:"forwarder,omitempty"`
	// Peers lists stable peer addresses when peer exposure is enabled.
	Peers  []Peer  `json:"peers,omitempty"`
	Events []Event `json:"events"`
	// AdminEnabled tells the page whether POST /handover exists.
	AdminEnabled bool `json:"admin_enabled"`
}

// Forwarder describes the replication forwarder.
type Forwarder struct {
	Listen    string `json:"listen"`
	Target    string `json:"target"`
	Reason    string `json:"reason,omitempty"`
	Active    int64  `json:"active"`
	Total     uint64 `json:"total"`
	BytesUp   uint64 `json:"bytes_up"`
	BytesDown uint64 `json:"bytes_down"`
	Resets    uint64 `json:"resets"`
	Dropped   uint64 `json:"dropped"`
	Refused   uint64 `json:"refused"`
}

// Event is one entry of the recent-events ring buffer.
type Event struct {
	Time    string `json:"time"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
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
	ui := uiHandler()
	mux.Handle("/ui", ui)
	mux.Handle("/ui/", ui)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		ui.ServeHTTP(w, r)
	})
	return mux
}

// Peer is one stable peer address (peer exposure).
type Peer struct {
	Index  int    `json:"index"`
	Listen string `json:"listen"`
	Target string `json:"target,omitempty"`
	Reason string `json:"reason,omitempty"`
	Active int64  `json:"active"`
}
