package watchdog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/logx"
)

// KV is the watchdog's view of the local instance.
type KV interface {
	InfoReplication(ctx context.Context) (*kvrocks.ReplicationInfo, error)
	ReplicaOf(ctx context.Context, host string, port int) error
	KillClients(ctx context.Context) (int64, error)
	// SetReadOnly sets replica-read-only / slave-read-only. Promotion sets
	// it to "no", so a REPLICAOF alone leaves a promoted node writable.
	SetReadOnly(ctx context.Context, readonly bool) error
	// WriteProbe tries a harmless expiring write and reports whether the
	// instance accepted it.
	WriteProbe(ctx context.Context) (bool, error)
}

// Config tunes the watchdog.
type Config struct {
	HeartbeatFile string
	// Timeout: heartbeat older than this is stale. Keep it below the
	// sidecar's --session-ttl so the dead leader is fenced before the lock
	// expires and a replica can be promoted.
	Timeout time.Duration
	// Interval between checks.
	Interval time.Duration
	// StartupGrace: a missing heartbeat file is tolerated this long after
	// the watchdog starts (the sidecar may not have started yet).
	StartupGrace time.Duration
	FenceHost    string
	FencePort    int
	KillClients  bool
	// Mode "observe" never fences (logs what it would do).
	Mode string
	// KvrocksTimeout bounds each command.
	KvrocksTimeout time.Duration
}

// Outcome of one check.
const (
	OutcomeHealthy      = "healthy"       // fresh heartbeat
	OutcomeStarting     = "starting"      // no heartbeat yet, within grace
	OutcomeStaleReplica = "stale_replica" // stale, but kvrocks is not a master: nothing to do
	OutcomeStaleDown    = "stale_backend_down"
	OutcomeObserve      = "observe_would_fence"
	OutcomeRaced        = "raced" // heartbeat became fresh just before fencing
	OutcomeFenced       = "fenced"
	OutcomeFenceFailed  = "fence_failed"
)

// Result describes one check.
type Result struct {
	Outcome string
	Age     time.Duration // -1 when there is no readable heartbeat
	Role    string
	Killed  int64
	Err     error
}

// Watchdog runs the checks.
type Watchdog struct {
	cfg     Config
	kv      KV
	log     *logx.Logger
	m       *Metrics
	now     func() time.Time
	started time.Time

	mu   sync.Mutex
	last Result
}

// New builds a watchdog. log and m may be nil.
func New(cfg Config, kv KV, log *logx.Logger, m *Metrics) *Watchdog {
	if cfg.KvrocksTimeout <= 0 {
		cfg.KvrocksTimeout = 2 * time.Second
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if m == nil {
		m = NewMetrics()
	}
	w := &Watchdog{cfg: cfg, kv: kv, log: log, m: m, now: time.Now}
	w.started = w.now()
	return w
}

// SetClock replaces the clock (tests). It also resets the start time.
func (w *Watchdog) SetClock(now func() time.Time) { w.now = now; w.started = now() }

// Last returns the most recent result.
func (w *Watchdog) Last() Result { w.mu.Lock(); defer w.mu.Unlock(); return w.last }

// Run checks every Interval until ctx ends.
func (w *Watchdog) Run(ctx context.Context) {
	t := time.NewTicker(w.cfg.Interval)
	defer t.Stop()
	for {
		w.Step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *Watchdog) observe() bool { return w.cfg.Mode == "observe" }

// heartbeatAge returns the age of the heartbeat, its content and whether
// it is fresh. Missing/unreadable files count as stale once the startup
// grace has passed.
func (w *Watchdog) heartbeatAge() (time.Duration, Heartbeat, bool, bool, error) {
	hb, err := ReadHeartbeat(w.cfg.HeartbeatFile)
	now := w.now()
	if err != nil {
		inGrace := now.Sub(w.started) < w.cfg.StartupGrace && errors.Is(err, os.ErrNotExist)
		return -1, hb, false, inGrace, err
	}
	age := now.Sub(hb.Time())
	return age, hb, age <= w.cfg.Timeout, false, nil
}

// Step performs one check and, if needed, one fencing attempt.
func (w *Watchdog) Step(ctx context.Context) Result {
	r := w.step(ctx)
	w.mu.Lock()
	w.last = r
	w.mu.Unlock()
	w.m.Checks.WithLabelValues(r.Outcome).Inc()
	if r.Age >= 0 {
		w.m.HeartbeatAge.Set(r.Age.Seconds())
	} else {
		w.m.HeartbeatAge.Set(-1)
	}
	stale := r.Outcome != OutcomeHealthy && r.Outcome != OutcomeStarting && r.Outcome != OutcomeRaced
	if stale {
		w.m.Stale.Set(1)
	} else {
		w.m.Stale.Set(0)
	}
	return r
}

func (w *Watchdog) step(ctx context.Context) Result {
	age, hb, fresh, inGrace, herr := w.heartbeatAge()
	r := Result{Age: age, Err: herr}
	if fresh {
		r.Outcome = OutcomeHealthy
		return r
	}
	if inGrace {
		r.Outcome = OutcomeStarting
		return r
	}
	// The sidecar announced observe mode: it never owns the instance's
	// role, so neither do we.
	observe := w.observe() || hb.Mode == "observe"

	kctx, cancel := context.WithTimeout(ctx, w.cfg.KvrocksTimeout)
	ri, err := w.kv.InfoReplication(kctx)
	cancel()
	if err != nil {
		r.Outcome, r.Err = OutcomeStaleDown, err
		w.warn("heartbeat stale and kvrocks unreachable; nothing to fence", map[string]any{"age": ageStr(age), "error": err.Error()})
		return r
	}
	r.Role = ri.Role
	if ri.Role != "master" {
		r.Outcome = OutcomeStaleReplica
		return r
	}
	fields := map[string]any{"age": ageStr(age), "timeout": w.cfg.Timeout.String(), "host": w.cfg.FenceHost, "port": w.cfg.FencePort}
	if observe {
		r.Outcome = OutcomeObserve
		w.m.WouldFence.Inc()
		w.warn("observe: sidecar heartbeat stale while kvrocks is master; would fence with REPLICAOF", fields)
		return r
	}
	// Re-read immediately before acting: a sidecar that just restarted
	// writes its heartbeat before it can promote anything.
	if _, _, fresh2, _, _ := w.heartbeatAge(); fresh2 {
		r.Outcome = OutcomeRaced
		w.info("heartbeat became fresh before fencing; not fencing", fields)
		return r
	}
	w.error("sidecar heartbeat stale while kvrocks is a writable master; fencing", fields)
	return w.fence(ctx, r)
}

func (w *Watchdog) fence(ctx context.Context, r Result) Result {
	kctx, cancel := context.WithTimeout(ctx, 3*w.cfg.KvrocksTimeout)
	defer cancel()
	fail := func(err error) Result {
		r.Outcome, r.Err = OutcomeFenceFailed, err
		w.m.Fences.WithLabelValues("error").Inc()
		w.error("watchdog fence failed; instance may still accept writes", map[string]any{"error": err.Error()})
		return r
	}
	// Read-only first: a node that was promoted runs with
	// slave-read-only=no, and REPLICAOF alone would leave it accepting
	// writes as a disconnected replica.
	if err := w.kv.SetReadOnly(kctx, true); err != nil {
		w.warn("setting replica read-only failed; relying on the write probe", map[string]any{"error": err.Error()})
	}
	if err := w.kv.ReplicaOf(kctx, w.cfg.FenceHost, w.cfg.FencePort); err != nil {
		return fail(fmt.Errorf("REPLICAOF %s:%d: %w", w.cfg.FenceHost, w.cfg.FencePort, err))
	}
	if w.cfg.KillClients {
		n, err := w.kv.KillClients(kctx)
		r.Killed = n
		if err != nil {
			w.warn("client kill after watchdog fence failed", map[string]any{"error": err.Error()})
		} else {
			w.m.ClientsKilled.Add(float64(n))
		}
	}
	ri, err := w.kv.InfoReplication(kctx)
	if err != nil {
		return fail(fmt.Errorf("confirm role: %w", err))
	}
	r.Role = ri.Role
	if ri.Role != "slave" {
		return fail(fmt.Errorf("role is still %q after REPLICAOF", ri.Role))
	}
	writable, err := w.kv.WriteProbe(kctx)
	if err == nil && writable {
		// Re-assert read-only once (REPLICAOF may reset state) and re-probe.
		_ = w.kv.SetReadOnly(kctx, true)
		writable, err = w.kv.WriteProbe(kctx)
	}
	if err != nil {
		return fail(fmt.Errorf("write probe: %w", err))
	}
	if writable {
		return fail(errors.New("write probe succeeded after fencing"))
	}
	r.Outcome = OutcomeFenced
	w.m.Fences.WithLabelValues("ok").Inc()
	w.m.LastFence.Set(float64(w.now().Unix()))
	w.error("watchdog fenced local kvrocks: replica of fence target, writes refused", map[string]any{"clients_killed": r.Killed})
	return r
}

func ageStr(d time.Duration) string {
	if d < 0 {
		return "missing"
	}
	return d.Round(time.Millisecond).String()
}

func (w *Watchdog) info(msg string, f map[string]any) {
	if w.log != nil {
		w.log.Info(msg, f)
	}
}
func (w *Watchdog) warn(msg string, f map[string]any) {
	if w.log != nil {
		w.log.Warn(msg, f)
	}
}
func (w *Watchdog) error(msg string, f map[string]any) {
	if w.log != nil {
		w.log.Error(msg, f)
	}
}
