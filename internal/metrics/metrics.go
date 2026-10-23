// Package metrics holds the sidecar's Prometheus metrics.
//
// Metric names are kept unprefixed (is_leader, elections_total, ...) so
// existing scrapes and dashboards keep working.
package metrics

import (
	"math"
	"net/http"
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

// Registry owns a private Prometheus registry and the sidecar's metrics.
type Registry struct {
	prom *prometheus.Registry

	IsLeader       prometheus.Gauge
	BackendUp      prometheus.Gauge
	ReplicationLag prometheus.Gauge

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

	renewAge atomic.Pointer[func() float64]
	lockHeld atomic.Pointer[func() float64]
}

// New builds a Registry with every metric registered and zero-initialised.
func New() *Registry {
	r := &Registry{prom: prometheus.NewRegistry()}
	r.IsLeader = prometheus.NewGauge(prometheus.GaugeOpts{Name: "is_leader", Help: "1 while this sidecar holds the leader lock."})
	r.BackendUp = prometheus.NewGauge(prometheus.GaugeOpts{Name: "kvrocks_up", Help: "1 while the local backend answers PING and INFO."})
	r.ReplicationLag = prometheus.NewGauge(prometheus.GaugeOpts{Name: "replication_lag_bytes", Help: "Lag behind the leader (kvrocks: sequence numbers, Redis: bytes); -1 unknown, 0 on the leader."})
	r.Elections = prometheus.NewCounter(prometheus.CounterOpts{Name: "elections_total", Help: "Lock acquisition attempts."})
	r.LeaderAcquired = prometheus.NewCounter(prometheus.CounterOpts{Name: "leader_acquired_total", Help: "Times this node won the lock (failovers/handovers onto this node)."})
	r.LeaderChanges = prometheus.NewCounter(prometheus.CounterOpts{Name: "leader_changes_total", Help: "Local role transitions."})
	r.StepDowns = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "step_downs_total", Help: "Times this node stopped being leader, by reason."}, []string{"reason"})
	r.Fencing = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fencing_events_total", Help: "REPLICAOF fencing attempts, by target kind and result."}, []string{"target", "result"})
	r.ConsulErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "consul_errors_total", Help: "Failed Consul calls, by operation."}, []string{"op"})
	r.ClientKills = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "client_kill_runs_total", Help: "Client-kill runs after demotion, by result."}, []string{"result"})
	r.ClientsKilled = prometheus.NewCounter(prometheus.CounterOpts{Name: "clients_killed_total", Help: "Client connections closed after demotion."})
	r.Handovers = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "handovers_total", Help: "Planned handover requests, by result."}, []string{"result"})
	r.ProxyActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "proxy_active_connections", Help: "Currently proxied client connections, by listener (write|read)."}, []string{"listener"})
	r.ProxyDropped = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "proxy_dropped_connections_total", Help: "Proxied connections closed because the routing target changed (failover, fencing, lost Consul confirmation)."}, []string{"listener"})
	r.ProxyRefused = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "proxy_refused_connections_total", Help: "Connections refused because no safe target was known or the dial failed."}, []string{"listener"})

	renew := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "consul_last_renew_age_seconds", Help: "Seconds since Consul last confirmed our session; +Inf before the first success."},
		func() float64 { return call(&r.renewAge, math.Inf(1)) })
	held := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "lock_held_seconds", Help: "Seconds this node has held the leader lock; 0 when not leader."},
		func() float64 { return call(&r.lockHeld, 0) })

	r.prom.MustRegister(r.IsLeader, r.BackendUp, r.ReplicationLag, r.Elections, r.LeaderAcquired, r.LeaderChanges,
		r.StepDowns, r.Fencing, r.ConsulErrors, r.ClientKills, r.ClientsKilled, r.Handovers, r.ProxyActive, r.ProxyDropped, r.ProxyRefused, renew, held,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	r.ReplicationLag.Set(-1)
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
