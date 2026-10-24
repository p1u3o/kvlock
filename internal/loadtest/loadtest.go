// Package loadtest drives sequence-numbered read/write load at a kvrocks
// cluster and measures what a handover or failover costs: the longest
// write gap, errors, latency, lost acknowledged writes and stale reads.
//
// Correctness model: every key is owned by exactly one worker, so writes
// to a key are strictly ordered. Each value carries a global sequence
// number. A write is "acked" when the server returned OK; afterwards the
// key must never read back an older sequence (stale read) and must still
// hold at least that sequence at the end (otherwise the write was lost).
// Writes that errored are ambiguous: they may or may not have applied.
package loadtest

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// RefusedPort is never a valid target: it is the production port.
const RefusedPort = "6379"

// Config describes one run.
type Config struct {
	Addrs     []string // kvrocks host:port; the writable master is discovered
	Allow     []string // explicit allowlist: "host:port" or ":port"
	Password  string
	Duration  time.Duration
	Rate      int // total ops/s across workers (0 = unthrottled)
	Workers   int
	Keys      int
	ValueMin  int
	ValueMax  int
	Pipeline  int
	ReadRatio float64
	OpTimeout time.Duration
	Prefix    string // key prefix; a unique per-run prefix is added
	Cleanup   bool
	// Events are run at offsets from the start (handover trigger etc).
	Events []Event
	// Observer is polled during the run (status timeline); may be nil.
	Observer func(ctx context.Context) string
	// ObserveEvery is the Observer period.
	ObserveEvery time.Duration
	// Marks are free-text timeline markers (e.g. "failover injected").
	Marks <-chan string
	Logf  func(format string, a ...any)
}

// Event is something to fire mid-run.
type Event struct {
	At   time.Duration
	Name string
	Fn   func(ctx context.Context) error
}

// CheckTargets enforces the safety rules: never port 6379, and every
// target must be explicitly allowlisted.
func CheckTargets(addrs, allow []string) error {
	if len(addrs) == 0 {
		return errors.New("no --addr given")
	}
	if len(allow) == 0 {
		return errors.New("refusing to run: no --allow given; list every target explicitly (e.g. --allow 10.0.0.5:8379)")
	}
	for _, a := range append(append([]string{}, addrs...), allow...) {
		_, p, err := net.SplitHostPort(a)
		if err != nil {
			return fmt.Errorf("bad address %q: %v", a, err)
		}
		if p == RefusedPort {
			return fmt.Errorf("refusing to run against port %s (%s): that is the production port", RefusedPort, a)
		}
	}
	for _, a := range addrs {
		h, p, _ := net.SplitHostPort(a)
		ok := false
		for _, w := range allow {
			wh, wp, _ := net.SplitHostPort(w)
			if wp == p && (wh == "" || wh == h) {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("refusing to run: %s is not in --allow", a)
		}
	}
	return nil
}

// Report is the outcome.
type Report struct {
	RunID         string           `json:"run_id"`
	Duration      time.Duration    `json:"duration_ns"`
	Writes        int64            `json:"writes_ok"`
	WriteErrors   int64            `json:"write_errors"`
	Reads         int64            `json:"reads_ok"`
	ReadErrors    int64            `json:"read_errors"`
	ErrorKinds    map[string]int64 `json:"error_kinds,omitempty"`
	MaxWriteGap   time.Duration    `json:"max_write_gap_ns"`
	GapStart      time.Duration    `json:"gap_start_offset_ns"`
	Unavailable   time.Duration    `json:"write_unavailable_ns"` // sum of gaps > threshold
	GapThreshold  time.Duration    `json:"gap_threshold_ns"`
	LatencyP50    time.Duration    `json:"latency_p50_ns"`
	LatencyP99    time.Duration    `json:"latency_p99_ns"`
	LatencyMax    time.Duration    `json:"latency_max_ns"`
	StaleReads    int64            `json:"stale_reads"`
	LostWrites    int64            `json:"lost_writes"`
	LostExamples  []string         `json:"lost_examples,omitempty"`
	VerifyError   string           `json:"verify_error,omitempty"`
	MasterChanges []string         `json:"master_changes,omitempty"`
	Timeline      []string         `json:"timeline,omitempty"`
}

// OK is true when no acked write was lost and no stale read was seen.
func (r Report) OK() bool { return r.LostWrites == 0 && r.StaleReads == 0 && r.VerifyError == "" }

type runner struct {
	cfg    Config
	prefix string
	start  time.Time

	mu      sync.Mutex
	master  string
	clients map[string]*redis.Client
	changes []string
	tl      []string
	errKind map[string]int64

	writes, writeErr, reads, readErr, stale atomic.Int64
	seq                                     atomic.Int64

	gapMu    sync.Mutex
	lastAck  time.Time
	maxGap   time.Duration
	gapAt    time.Duration
	unavail  time.Duration
	gapLimit time.Duration

	acked []int64 // per key, owned by the key's worker
	lat   [][]time.Duration
}

func (r *runner) logf(f string, a ...any) {
	if r.cfg.Logf != nil {
		r.cfg.Logf(f, a...)
	}
}

func (r *runner) note(s string) {
	r.mu.Lock()
	r.tl = append(r.tl, fmt.Sprintf("+%6.2fs %s", time.Since(r.start).Seconds(), s))
	r.mu.Unlock()
	r.logf("+%.2fs %s", time.Since(r.start).Seconds(), s)
}

func (r *runner) client(addr string) *redis.Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.clients[addr]
	if !ok {
		c = redis.NewClient(&redis.Options{Addr: addr, Password: r.cfg.Password,
			DialTimeout: r.cfg.OpTimeout, ReadTimeout: r.cfg.OpTimeout, WriteTimeout: r.cfg.OpTimeout,
			PoolSize: r.cfg.Workers + 4, MaxRetries: -1})
		r.clients[addr] = c
	}
	return c
}

// findMaster returns the address reporting role:master. With a single
// address (proxy / DNS name) that address is returned as-is.
func (r *runner) findMaster(ctx context.Context) (string, error) {
	if len(r.cfg.Addrs) == 1 {
		return r.cfg.Addrs[0], nil
	}
	var masters []string
	for _, a := range r.cfg.Addrs {
		c, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
		s, err := r.client(a).Info(c, "replication").Result()
		cancel()
		if err == nil && strings.Contains(s, "role:master") {
			masters = append(masters, a)
		}
	}
	switch len(masters) {
	case 0:
		return "", errors.New("no master found")
	case 1:
		return masters[0], nil
	default:
		r.note("WARNING: several masters visible: " + strings.Join(masters, ","))
		return masters[0], nil
	}
}

func (r *runner) currentMaster() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.master
}

func (r *runner) rediscover(ctx context.Context, failed string) {
	r.mu.Lock()
	if r.master != failed {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	m, err := r.findMaster(ctx)
	if err != nil || m == failed && len(r.cfg.Addrs) > 1 {
		return
	}
	r.mu.Lock()
	if r.master == failed && m != failed {
		r.master = m
		r.changes = append(r.changes, fmt.Sprintf("+%.2fs %s -> %s", time.Since(r.start).Seconds(), failed, m))
		r.mu.Unlock()
		r.note("master now " + m)
		return
	}
	r.mu.Unlock()
}

func classify(err error) string {
	s := err.Error()
	switch {
	case strings.HasPrefix(s, "READONLY"):
		return "readonly"
	case strings.Contains(s, "connection refused"):
		return "conn_refused"
	case strings.Contains(s, "timeout") || errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case strings.Contains(s, "EOF") || strings.Contains(s, "reset"):
		return "conn_closed"
	}
	if len(s) > 40 {
		s = s[:40]
	}
	return s
}

func (r *runner) countErr(err error) {
	r.mu.Lock()
	r.errKind[classify(err)]++
	r.mu.Unlock()
}

func (r *runner) ack(now time.Time) {
	r.gapMu.Lock()
	if !r.lastAck.IsZero() {
		g := now.Sub(r.lastAck)
		if g > r.maxGap {
			r.maxGap = g
			r.gapAt = r.lastAck.Sub(r.start)
		}
		if g > r.gapLimit {
			r.unavail += g
		}
	}
	if now.After(r.lastAck) {
		r.lastAck = now
	}
	r.gapMu.Unlock()
}

func (r *runner) key(i int) string { return r.prefix + strconv.Itoa(i) }

// Value layout: "<seq>:<padding>".
func parseSeq(v string) int64 {
	if i := strings.IndexByte(v, ':'); i > 0 {
		v = v[:i]
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

type op struct {
	key   int
	write bool
	seq   int64
	known int64 // acked seq when issued (reads)
}

func (r *runner) worker(ctx context.Context, w int, rng *rand.Rand, pad string) {
	cfg := r.cfg
	var mine []int
	for k := w; k < cfg.Keys; k += cfg.Workers {
		mine = append(mine, k)
	}
	if len(mine) == 0 {
		return
	}
	var tick *time.Ticker
	if cfg.Rate > 0 {
		iv := time.Duration(float64(time.Second) * float64(cfg.Workers*cfg.Pipeline) / float64(cfg.Rate))
		if iv <= 0 {
			iv = time.Microsecond
		}
		tick = time.NewTicker(iv)
		defer tick.Stop()
	}
	ops := make([]op, cfg.Pipeline)
	for {
		if tick != nil {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		} else if ctx.Err() != nil {
			return
		}
		for i := range ops {
			k := mine[rng.Intn(len(mine))]
			ops[i] = op{key: k, write: rng.Float64() >= cfg.ReadRatio, known: r.acked[k]}
			if ops[i].write {
				ops[i].seq = r.seq.Add(1)
			}
		}
		addr := r.currentMaster()
		c, cancel := context.WithTimeout(context.Background(), cfg.OpTimeout)
		pipe := r.client(addr).Pipeline()
		cmds := make([]redis.Cmder, len(ops))
		for i, o := range ops {
			if o.write {
				n := cfg.ValueMin
				if cfg.ValueMax > cfg.ValueMin {
					n += rng.Intn(cfg.ValueMax - cfg.ValueMin + 1)
				}
				v := strconv.FormatInt(o.seq, 10) + ":"
				if n > len(v) {
					v += pad[:n-len(v)]
				}
				cmds[i] = pipe.Set(c, r.key(o.key), v, 0)
			} else {
				cmds[i] = pipe.Get(c, r.key(o.key))
			}
		}
		t0 := time.Now()
		_, _ = pipe.Exec(c)
		cancel()
		now := time.Now()
		r.lat[w] = append(r.lat[w], now.Sub(t0))
		failed := false
		for i, o := range ops {
			err := cmds[i].Err()
			if o.write {
				if err != nil {
					r.writeErr.Add(1)
					r.countErr(err)
					failed = true
					continue
				}
				r.writes.Add(1)
				if o.seq > r.acked[o.key] {
					r.acked[o.key] = o.seq
				}
				r.ack(now)
				continue
			}
			if err != nil && err != redis.Nil {
				r.readErr.Add(1)
				r.countErr(err)
				failed = true
				continue
			}
			r.reads.Add(1)
			got := int64(0)
			if err == nil {
				got = parseSeq(cmds[i].(*redis.StringCmd).Val())
			}
			if got < o.known {
				r.stale.Add(1)
				r.note(fmt.Sprintf("STALE read key %d: got seq %d, acked %d (from %s)", o.key, got, o.known, addr))
			}
		}
		if failed {
			r.rediscover(ctx, addr)
		}
	}
}

func pct(s []time.Duration, p float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	i := int(p * float64(len(s)-1))
	return s[i]
}

// Run executes the load test.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if err := CheckTargets(cfg.Addrs, cfg.Allow); err != nil {
		return Report{}, err
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.Pipeline <= 0 {
		cfg.Pipeline = 1
	}
	if cfg.Keys < cfg.Workers {
		cfg.Keys = cfg.Workers
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = time.Second
	}
	if cfg.ValueMin < 24 {
		cfg.ValueMin = 24
	}
	if cfg.ValueMax < cfg.ValueMin {
		cfg.ValueMax = cfg.ValueMin
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "kvlock:loadtest:"
	}
	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	r := &runner{cfg: cfg, prefix: cfg.Prefix + runID + ":", clients: map[string]*redis.Client{},
		errKind: map[string]int64{}, acked: make([]int64, cfg.Keys), lat: make([][]time.Duration, cfg.Workers)}
	// A gap is "unavailability" when it is far longer than the expected
	// inter-ack spacing.
	r.gapLimit = 250 * time.Millisecond
	if cfg.Rate > 0 {
		if g := 20 * time.Second / time.Duration(cfg.Rate) * time.Duration(cfg.Pipeline); g > r.gapLimit {
			r.gapLimit = g
		}
	}
	defer func() {
		for _, c := range r.clients {
			c.Close()
		}
	}()
	m, err := r.findMaster(ctx)
	if err != nil {
		return Report{}, err
	}
	r.master = m
	r.start = time.Now()
	r.note("start, master " + m + ", run id " + runID)

	runCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	var wg sync.WaitGroup
	pad := strings.Repeat("x", cfg.ValueMax)
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r.worker(runCtx, w, rand.New(rand.NewSource(time.Now().UnixNano()+int64(w))), pad)
		}(w)
	}
	for _, ev := range cfg.Events {
		wg.Add(1)
		go func(ev Event) {
			defer wg.Done()
			select {
			case <-runCtx.Done():
				return
			case <-time.After(ev.At):
			}
			r.note("event " + ev.Name + " fired")
			if err := ev.Fn(runCtx); err != nil {
				r.note("event " + ev.Name + " failed: " + err.Error())
			} else {
				r.note("event " + ev.Name + " done")
			}
		}(ev)
	}
	if cfg.Marks != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-runCtx.Done():
					return
				case m := <-cfg.Marks:
					r.note("MARK " + m)
				}
			}
		}()
	}
	if cfg.Observer != nil {
		every := cfg.ObserveEvery
		if every <= 0 {
			every = 500 * time.Millisecond
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := ""
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				s := cfg.Observer(runCtx)
				if s != "" && s != last && runCtx.Err() == nil {
					r.note("cluster: " + s)
					last = s
				}
				select {
				case <-runCtx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(r.start)
	// The tail after the last ack counts as a gap too (cluster never came back).
	r.ack(r.start.Add(elapsed))

	rep := Report{RunID: runID, Duration: elapsed, Writes: r.writes.Load(), WriteErrors: r.writeErr.Load(),
		Reads: r.reads.Load(), ReadErrors: r.readErr.Load(), ErrorKinds: r.errKind, MaxWriteGap: r.maxGap,
		GapStart: r.gapAt, Unavailable: r.unavail, GapThreshold: r.gapLimit, StaleReads: r.stale.Load()}
	var all []time.Duration
	for _, l := range r.lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	rep.LatencyP50, rep.LatencyP99 = pct(all, .50), pct(all, .99)
	if len(all) > 0 {
		rep.LatencyMax = all[len(all)-1]
	}
	r.verify(ctx, &rep)
	if cfg.Cleanup {
		r.cleanup(ctx)
	}
	r.mu.Lock()
	rep.MasterChanges, rep.Timeline = r.changes, r.tl
	r.mu.Unlock()
	return rep, nil
}

// verify reads every key back from the (current) master.
func (r *runner) verify(ctx context.Context, rep *Report) {
	var addr string
	var err error
	for i := 0; i < 20; i++ {
		if addr, err = r.findMaster(ctx); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		rep.VerifyError = "verify: " + err.Error()
		return
	}
	r.note("verifying against " + addr)
	c := r.client(addr)
	const batch = 500
	for lo := 0; lo < r.cfg.Keys; lo += batch {
		hi := lo + batch
		if hi > r.cfg.Keys {
			hi = r.cfg.Keys
		}
		keys := make([]string, 0, hi-lo)
		for k := lo; k < hi; k++ {
			keys = append(keys, r.key(k))
		}
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		vals, err := c.MGet(cctx, keys...).Result()
		cancel()
		if err != nil {
			rep.VerifyError = "verify: " + err.Error()
			return
		}
		for i, v := range vals {
			k := lo + i
			got := int64(0)
			if s, ok := v.(string); ok {
				got = parseSeq(s)
			}
			if got < r.acked[k] {
				rep.LostWrites++
				if len(rep.LostExamples) < 10 {
					rep.LostExamples = append(rep.LostExamples, fmt.Sprintf("key %d: have seq %d, acked %d", k, got, r.acked[k]))
				}
			}
		}
	}
}

func (r *runner) cleanup(ctx context.Context) {
	addr, err := r.findMaster(ctx)
	if err != nil {
		return
	}
	c := r.client(addr)
	for lo := 0; lo < r.cfg.Keys; lo += 500 {
		var keys []string
		for k := lo; k < lo+500 && k < r.cfg.Keys; k++ {
			keys = append(keys, r.key(k))
		}
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		c.Del(cctx, keys...)
		cancel()
	}
}

// Format renders the human report.
func Format(rep Report) string {
	var b strings.Builder
	f := func(s string, a ...any) { fmt.Fprintf(&b, s+"\n", a...) }
	ms := func(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d)/1e6) }
	f("kvlock loadtest report (run %s, %.1fs)", rep.RunID, rep.Duration.Seconds())
	f("  writes ok %d, errors %d   reads ok %d, errors %d", rep.Writes, rep.WriteErrors, rep.Reads, rep.ReadErrors)
	if len(rep.ErrorKinds) > 0 {
		var ks []string
		for k, v := range rep.ErrorKinds {
			ks = append(ks, fmt.Sprintf("%s=%d", k, v))
		}
		sort.Strings(ks)
		f("  error kinds: %s", strings.Join(ks, " "))
	}
	f("  longest write gap %s starting at +%.2fs; total unavailability (gaps > %s) %s",
		ms(rep.MaxWriteGap), rep.GapStart.Seconds(), ms(rep.GapThreshold), ms(rep.Unavailable))
	f("  latency per pipeline: p50 %s  p99 %s  max %s", ms(rep.LatencyP50), ms(rep.LatencyP99), ms(rep.LatencyMax))
	f("  lost acknowledged writes: %d   stale reads: %d", rep.LostWrites, rep.StaleReads)
	for _, e := range rep.LostExamples {
		f("    lost: %s", e)
	}
	if rep.VerifyError != "" {
		f("  VERIFY FAILED: %s", rep.VerifyError)
	}
	for _, c := range rep.MasterChanges {
		f("  master change %s", c)
	}
	f("timeline:")
	for _, t := range rep.Timeline {
		f("  %s", t)
	}
	if rep.OK() {
		f("RESULT: PASS (no lost or stale data)")
	} else {
		f("RESULT: FAIL")
	}
	return b.String()
}
