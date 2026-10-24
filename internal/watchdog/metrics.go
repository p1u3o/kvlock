package watchdog

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/p1u3o/kvlock/internal/metrics"
)

// Metrics are the watchdog process' own metrics (separate registry and
// port from the sidecar, so they survive the sidecar's death).
type Metrics struct {
	reg           *prometheus.Registry
	HeartbeatAge  prometheus.Gauge
	Stale         prometheus.Gauge
	Checks        *prometheus.CounterVec
	Fences        *prometheus.CounterVec
	WouldFence    prometheus.Counter
	ClientsKilled prometheus.Counter
	LastFence     prometheus.Gauge
}

// NewMetrics registers the watchdog metrics.
func NewMetrics() *Metrics {
	ns := metrics.Namespace
	m := &Metrics{reg: prometheus.NewRegistry()}
	m.HeartbeatAge = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "watchdog", Name: "heartbeat_age_seconds", Help: "Age of the sidecar heartbeat file; -1 missing or unreadable."})
	m.Stale = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "watchdog", Name: "heartbeat_stale", Help: "1 while the sidecar heartbeat is stale (past the startup grace)."})
	m.Checks = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: "watchdog", Name: "checks_total", Help: "Watchdog checks by outcome."}, []string{"outcome"})
	m.Fences = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Subsystem: "watchdog", Name: "fences_total", Help: "Watchdog fencing attempts by result."}, []string{"result"})
	m.WouldFence = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: "watchdog", Name: "observe_would_fence_total", Help: "Checks where observe mode would have fenced."})
	m.ClientsKilled = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: "watchdog", Name: "clients_killed_total", Help: "Client connections closed by the watchdog after fencing."})
	m.LastFence = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: "watchdog", Name: "last_fence_timestamp_seconds", Help: "Unix time of the last successful watchdog fence; 0 never."})
	m.reg.MustRegister(m.HeartbeatAge, m.Stale, m.Checks, m.Fences, m.WouldFence, m.ClientsKilled, m.LastFence)
	m.HeartbeatAge.Set(-1)
	for _, o := range []string{OutcomeHealthy, OutcomeStarting, OutcomeStaleReplica, OutcomeStaleDown, OutcomeObserve, OutcomeRaced, OutcomeFenced, OutcomeFenceFailed} {
		m.Checks.WithLabelValues(o)
	}
	m.Fences.WithLabelValues("ok")
	m.Fences.WithLabelValues("error")
	return m
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Gatherer exposes the registry (tests).
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.reg }
