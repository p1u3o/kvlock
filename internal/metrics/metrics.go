// Package metrics holds the sidecar's Prometheus metrics.
//
// Since v0.2.0 every sidecar metric carries the kvlock_ prefix
// (kvlock_is_leader, kvlock_elections_total, ...). The Go and process
// collectors keep their standard go_/process_ names.
package metrics

import (
	"math"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Step-down reasons used as the "reason" label of step_downs_total.
const (
	ReasonBackendUnhealthy   = "backend_unhealthy"
	ReasonSessionInvalidated = "session_invalidated"
	ReasonConsulUnreachable  = "consul_unreachable"
	ReasonLockLost           = "lock_lost"
	ReasonLockTaken          = "lock_taken"
	ReasonHandover           = "handover"
	ReasonShutdown           = "shutdown"
)

// Namespace prefixes every sidecar metric name.
const Namespace = "kvlock"

// Registry owns a private Prometheus registry and the sidecar's metrics.
type Registry struct {
	prom *prometheus.Registry

	IsLeader       prometheus.Gauge
	BackendUp      prometheus.Gauge
	ReplicationLag prometheus.Gauge
	// LagSeconds is the heartbeat-based lag behind the leader.
	LagSeconds prometheus.Gauge
	// HeartbeatAge is now minus the newest heartbeat in the local instance.
	HeartbeatAge prometheus.Gauge
	Heartbeats   *prometheus.CounterVec // result=ok|error
	LagRefusals  prometheus.Counter
	// PromotedDespiteLag counts promotions forced by --lag-gate-max-wait;
	// DespiteLagLeader is 1 while this node leads after such a promotion.
	PromotedDespiteLag prometheus.Counter
	DespiteLagLeader   prometheus.Gauge

	Elections      prometheus.Counter
	LeaderAcquired prometheus.Counter
	LeaderChanges  prometheus.Counter
	StepDowns      *prometheus.CounterVec // reason
	Fencing        *prometheus.CounterVec // target=leader|fence, result=ok|error
	ConsulErrors   *prometheus.CounterVec // op
	ClientKills    *prometheus.CounterVec // result=ok|error
	ClientsKilled  prometheus.Counter
	Handovers      *prometheus.CounterVec // result
	ProxyActive    *prometheus.GaugeVec   // listener
	ProxyDropped   *prometheus.CounterVec // listener
	ProxyRefused   *prometheus.CounterVec // listener
	ProxyAccepted  *prometheus.CounterVec // listener
	ProxyResets    *prometheus.CounterVec // listener
	ProxyBytes     *prometheus.CounterVec // listener, direction

	// Backups (all zero while backups are disabled).
	BackupLastSuccess prometheus.Gauge
	BackupLastSize    prometheus.Gauge
	BackupDuration    prometheus.Gauge
	BackupFailures    prometheus.Counter

	// WatchdogWrites counts heartbeat-file writes for the watchdog.
	WatchdogWrites *prometheus.CounterVec // result=ok|error

	connMu sync.Mutex
	conns  map[string]float64

	renewAge atomic.Pointer[func() float64]
	lockHeld atomic.Pointer[func() float64]
}

// New builds a Registry with every metric registered and zero-initialised.
func New() *Registry {
	r := &Registry{prom: prometheus.NewRegistry()}
	r.IsLeader = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "is_leader", Help: "1 while this sidecar holds the leader lock."})
	r.BackendUp = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "kvrocks_up", Help: "1 while the local backend answers PING and INFO."})
	r.ReplicationLag = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "replication_lag_bytes", Help: "Lag behind the leader (kvrocks: sequence numbers, Redis: bytes); -1 unknown, 0 on the leader."})
	r.LagSeconds = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "replication_lag_seconds", Help: "Heartbeat lag behind the leader in seconds (leader heartbeat minus the heartbeat replicated locally); -1 unknown, 0 on the leader."})
	r.HeartbeatAge = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "heartbeat_age_seconds", Help: "Local clock minus the newest heartbeat in the local instance (includes clock skew); -1 unknown."})
	r.Heartbeats = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "heartbeat_writes_total", Help: "Heartbeat writes by the leader, by result."}, []string{"result"})
	r.LagRefusals = prometheus.NewCounter(prometheus.CounterOpts{Namespace: Namespace, Name: "lag_gate_refusals_total", Help: "Promotions refused by the lag gate."})
	r.PromotedDespiteLag = prometheus.NewCounter(prometheus.CounterOpts{Namespace: Namespace, Name: "promoted_despite_lag_total", Help: "Promotions forced by --lag-gate-max-wait while the replica was over --max-lag (writes may have been lost)."})
	r.DespiteLagLeader = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "promoted_despite_lag", Help: "1 while this node is leader after a promotion forced despite lag."})
	r.Elections = prometheus.NewCounter(prometheus.CounterOpts{Namespace: Namespace, Name: "elections_total", Help: "Lock acquisition attempts."})
	r.LeaderAcquired = prometheus.NewCounter(prometheus.CounterOpts{Namespace: Namespace, Name: "leader_acquired_total", Help: "Times this node won the lock (failovers/handovers onto this node)."})
	r.LeaderChanges = prometheus.NewCounter(prometheus.CounterOpts{Namespace: Namespace, Name: "leader_changes_total", Help: "Local role transitions."})
	r.StepDowns = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "step_downs_total", Help: "Times this node stopped being leader, by reason."}, []string{"reason"})
	r.Fencing = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "fencing_events_total", Help: "REPLICAOF fencing attempts, by target kind and result."}, []string{"target", "result"})
	r.ConsulErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "consul_errors_total", Help: "Failed Consul calls, by operation."}, []string{"op"})
	r.ClientKills = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "client_kill_runs_total", Help: "Client-kill runs after demotion, by result."}, []string{"result"})
	r.ClientsKilled = prometheus.NewCounter(prometheus.CounterOpts{Namespace: Namespace, Name: "clients_killed_total", Help: "Client connections closed after demotion."})
	r.Handovers = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "handovers_total", Help: "Planned handover requests, by result."}, []string{"result"})
	r.ProxyActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: Namespace, Name: "proxy_active_connections", Help: "Currently proxied client connections, by listener (write|read)."}, []string{"listener"})
	r.ProxyDropped = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "proxy_dropped_connections_total", Help: "Proxied connections closed because the routing target changed (failover, fencing, lost Consul confirmation)."}, []string{"listener"})
	r.ProxyResets = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "proxy_route_resets_total", Help: "Times an established route was replaced or withdrawn (leader, epoch or fencing change), cutting all connections."}, []string{"listener"})
	r.ProxyAccepted = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "proxy_connections_total", Help: "Connections forwarded to an upstream, by listener (write|read|forward)."}, []string{"listener"})
	r.ProxyBytes = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "proxy_bytes_total", Help: "Bytes forwarded, by listener and direction (up = client to upstream)."}, []string{"listener", "direction"})
	r.ProxyRefused = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "proxy_refused_connections_total", Help: "Connections refused because no safe target was known or the dial failed."}, []string{"listener"})

	r.BackupLastSuccess = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "backup_last_success_timestamp_seconds", Help: "Unix time of this node's last successful backup upload; 0 never."})
	r.BackupLastSize = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "backup_last_size_bytes", Help: "Size of this node's last uploaded backup object (encrypted)."})
	r.BackupDuration = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: Namespace, Name: "backup_duration_seconds", Help: "Duration of this node's last successful backup (BGSAVE + upload)."})
	r.BackupFailures = prometheus.NewCounter(prometheus.CounterOpts{Namespace: Namespace, Name: "backup_failures_total", Help: "Failed backup attempts on this node."})

	r.WatchdogWrites = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: Namespace, Name: "watchdog_heartbeat_writes_total", Help: "Writes of the watchdog heartbeat file, by result."}, []string{"result"})

	renew := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: Namespace, Name: "consul_last_renew_age_seconds", Help: "Seconds since Consul last confirmed our session; +Inf before the first success."},
		func() float64 { return call(&r.renewAge, math.Inf(1)) })
	held := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: Namespace, Name: "lock_held_seconds", Help: "Seconds this node has held the leader lock; 0 when not leader."},
		func() float64 { return call(&r.lockHeld, 0) })

	r.prom.MustRegister(r.IsLeader, r.BackendUp, r.ReplicationLag, r.Elections, r.LeaderAcquired, r.LeaderChanges,
		r.StepDowns, r.Fencing, r.ConsulErrors, r.ClientKills, r.ClientsKilled, r.Handovers, r.ProxyActive, r.ProxyDropped, r.ProxyRefused, r.ProxyAccepted, r.ProxyBytes, r.ProxyResets, renew, held,
		r.LagSeconds, r.HeartbeatAge, r.Heartbeats, r.LagRefusals, r.PromotedDespiteLag, r.DespiteLagLeader,
		r.BackupLastSuccess, r.BackupLastSize, r.BackupDuration, r.BackupFailures, r.WatchdogWrites,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	r.ReplicationLag.Set(-1)
	r.LagSeconds.Set(-1)
	r.HeartbeatAge.Set(-1)
	r.Heartbeats.WithLabelValues("ok")
	r.Heartbeats.WithLabelValues("error")
	r.WatchdogWrites.WithLabelValues("ok")
	r.WatchdogWrites.WithLabelValues("error")
	for _, reason := range []string{ReasonBackendUnhealthy, ReasonSessionInvalidated, ReasonConsulUnreachable, ReasonLockLost, ReasonLockTaken, ReasonHandover, ReasonShutdown} {
		r.StepDowns.WithLabelValues(reason)
	}
	for _, t := range []string{"leader", "fence"} {
		r.Fencing.WithLabelValues(t, "ok")
		r.Fencing.WithLabelValues(t, "error")
	}
	for _, op := range []string{"session", "renew", "lock"} {
		r.ConsulErrors.WithLabelValues(op)
	}
	return r
}

func call(p *atomic.Pointer[func() float64], def float64) float64 {
	if f := p.Load(); f != nil {
		return (*f)()
	}
	return def
}

// SetRenewAgeFunc sets the source of consul_last_renew_age_seconds.
func (r *Registry) SetRenewAgeFunc(f func() float64) { r.renewAge.Store(&f) }

// SetLockHeldFunc sets the source of lock_held_seconds.
func (r *Registry) SetLockHeldFunc(f func() float64) { r.lockHeld.Store(&f) }

// SetBool sets a gauge to 1 or 0.
func SetBool(g prometheus.Gauge, b bool) {
	if b {
		g.Set(1)
	} else {
		g.Set(0)
	}
}

// Handler serves the registry in Prometheus exposition format.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.prom, promhttp.HandlerOpts{})
}

// Gatherer exposes the underlying registry (tests).
func (r *Registry) Gatherer() prometheus.Gatherer { return r.prom }

// AddProxyActive adjusts kvlock_proxy_active_connections for a listener
// and the copy shown on the status page.
func (r *Registry) AddProxyActive(listener string, d float64) {
	r.ProxyActive.WithLabelValues(listener).Add(d)
	r.connMu.Lock()
	if r.conns == nil {
		r.conns = map[string]float64{}
	}
	r.conns[listener] += d
	r.connMu.Unlock()
}

// ProxyActiveCounts returns the active proxied connections per listener.
func (r *Registry) ProxyActiveCounts() map[string]int {
	r.connMu.Lock()
	defer r.connMu.Unlock()
	out := make(map[string]int, len(r.conns))
	for k, v := range r.conns {
		out[k] = int(v)
	}
	return out
}
