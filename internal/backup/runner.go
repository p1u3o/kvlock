package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/metrics"
)

// Source is the local instance as far as backups are concerned.
type Source interface {
	BGSave(ctx context.Context) error
	InfoPersistence(ctx context.Context) (*kvrocks.Persistence, error)
}

// Coordinator is the Consul KV used to pick a single uploader: a session
// lock plus a plain last-backup record.
type Coordinator interface {
	Acquire(ctx context.Context, key, session string, value []byte) (bool, error)
	Release(ctx context.Context, key, session string) error
	Get(ctx context.Context, key string) (value []byte, session string, modifyIndex uint64, err error)
	Put(ctx context.Context, key string, value []byte) error
}

// NodeState is what the controller knows about this node right now.
type NodeState struct {
	Leader       bool // holds the leader lock
	Follower     bool // another node holds the lock and we follow it
	Healthy      bool
	BackendRole  string // "master" / "slave"
	MasterLinkUp bool
	LagOK        bool // within the promotion lag bound
	// Replicas is how many replicas the local instance lists (leader).
	Replicas int
	// Session is this node's Consul session ("" when none).
	Session string
}

// Config for the Runner.
type Config struct {
	Cluster string
	Node    string
	// Dir is the kvrocks backup-dir as seen by the sidecar.
	Dir    string
	Prefix string // object key prefix ("" or ends in "/")
	Key    []byte // 32-byte client-side encryption key

	Interval time.Duration
	// Keep is how many backups to retain under the prefix; 0 keeps all.
	Keep int
	// AllowLeader lets the leader back up when it has no replica or the
	// replicas have not managed a backup for Interval+LeaderGrace.
	AllowLeader bool
	LeaderGrace time.Duration

	BGSaveTimeout time.Duration
	// Check is how often eligibility is evaluated (default 1m).
	Check time.Duration
	// Poll is the INFO persistence poll period (default 1s).
	Poll time.Duration
	// RetryAfter is the pause after a failed attempt on this node.
	RetryAfter time.Duration
	// Observe disables the runner entirely (observe mode never writes).
	Observe bool
}

// Record is stored at kvrocks/<cluster>/backup/last after each success.
type Record struct {
	Node            string  `json:"node"`
	Key             string  `json:"key"`
	Time            string  `json:"time"`
	Size            int64   `json:"size"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// Status is shown on the status page.
type Status struct {
	Enabled         bool    `json:"enabled"`
	Running         bool    `json:"running"`
	LastSuccess     string  `json:"last_success,omitempty"`
	LastSuccessNode string  `json:"last_success_node,omitempty"`
	LastKey         string  `json:"last_key,omitempty"`
	LastSize        int64   `json:"last_size_bytes"`
	LastDuration    float64 `json:"last_duration_seconds"`
	LastAttempt     string  `json:"last_attempt,omitempty"`
	LastError       string  `json:"last_error,omitempty"`
	Interval        string  `json:"interval"`
}

// Runner takes periodic backups when this node is the right one to.
type Runner struct {
	cfg   Config
	src   Source
	store Store
	coord Coordinator
	reg   *metrics.Registry
	event func(kind, msg string)

	running atomic.Bool
	mu      sync.Mutex
	st      Status
	lastErr time.Time
}

func NewRunner(cfg Config, src Source, store Store, coord Coordinator, reg *metrics.Registry, event func(kind, msg string)) *Runner {
	if cfg.Check <= 0 {
		cfg.Check = time.Minute
	}
	if cfg.Poll <= 0 {
		cfg.Poll = time.Second
	}
	if cfg.BGSaveTimeout <= 0 {
		cfg.BGSaveTimeout = 30 * time.Minute
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = 15 * time.Minute
	}
	if event == nil {
		event = func(string, string) {}
	}
	r := &Runner{cfg: cfg, src: src, store: store, coord: coord, reg: reg, event: event}
	r.st = Status{Enabled: !cfg.Observe, Interval: cfg.Interval.String()}
	return r
}

// SetEventFunc replaces the event hook (the controller sets it).
func (r *Runner) SetEventFunc(f func(kind, msg string)) {
	r.mu.Lock()
	r.event = f
	r.mu.Unlock()
}

func (r *Runner) emit(kind, msg string) {
	r.mu.Lock()
	f := r.event
	r.mu.Unlock()
	f(kind, msg)
}

func (r *Runner) LockKey() string   { return "kvrocks/" + r.cfg.Cluster + "/backup/lock" }
func (r *Runner) RecordKey() string { return "kvrocks/" + r.cfg.Cluster + "/backup/last" }

// Status returns a copy of the current status.
func (r *Runner) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.st
	s.Running = r.running.Load()
	return s
}

// Run evaluates eligibility every Check until ctx ends. It never does
// anything in observe mode.
func (r *Runner) Run(ctx context.Context, state func() NodeState) {
	if r.cfg.Observe {
		return
	}
	t := time.NewTicker(r.cfg.Check)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = r.MaybeBackup(ctx, state())
		}
	}
}

// eligible decides whether this node may back up now, given the last
// recorded success (zero when none).
func (r *Runner) eligible(st NodeState, last time.Time, now time.Time) (bool, string) {
	if st.Session == "" || !st.Healthy {
		return false, "not healthy or no session"
	}
	if st.Follower && !st.Leader {
		if st.BackendRole != "slave" || !st.MasterLinkUp || !st.LagOK {
			return false, "replica not in sync"
		}
		return true, ""
	}
	if st.Leader && r.cfg.AllowLeader && st.BackendRole == "master" {
		if st.Replicas == 0 {
			return true, ""
		}
		if last.IsZero() || now.Sub(last) >= r.cfg.Interval+r.cfg.LeaderGrace {
			return true, ""
		}
		return false, "leader waits for a replica to back up"
	}
	return false, "not a healthy replica"
}

func (r *Runner) readRecord(ctx context.Context) (*Record, time.Time, error) {
	v, _, _, err := r.coord.Get(ctx, r.RecordKey())
	if err != nil || len(v) == 0 {
		return nil, time.Time{}, err
	}
	var rec Record
	if json.Unmarshal(v, &rec) != nil {
		return nil, time.Time{}, nil
	}
	t, _ := time.Parse(time.RFC3339Nano, rec.Time)
	return &rec, t, nil
}

// MaybeBackup runs one backup if this node is eligible, the last success
// is older than Interval, and it wins the backup lock. ran reports
// whether a backup was attempted.
func (r *Runner) MaybeBackup(ctx context.Context, st NodeState) (ran bool, err error) {
	if r.cfg.Observe {
		return false, nil
	}
	now := time.Now()
	r.mu.Lock()
	cooling := !r.lastErr.IsZero() && now.Sub(r.lastErr) < r.cfg.RetryAfter
	r.mu.Unlock()
	if cooling {
		return false, nil
	}
	rec, last, err := r.readRecord(ctx)
	if err != nil {
		return false, err
	}
	r.noteRecord(rec)
	if !last.IsZero() && now.Sub(last) < r.cfg.Interval {
		return false, nil
	}
	if ok, _ := r.eligible(st, last, now); !ok {
		return false, nil
	}
	lv, _ := json.Marshal(map[string]string{"node": r.cfg.Node, "started": now.UTC().Format(time.RFC3339Nano)})
	held, err := r.coord.Acquire(ctx, r.LockKey(), st.Session, lv)
	if err != nil || !held {
		return false, err
	}
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		_ = r.coord.Release(rctx, r.LockKey(), st.Session)
		cancel()
	}()
	// Another node may have finished while we were acquiring.
	if _, last2, err := r.readRecord(ctx); err != nil {
		return false, err
	} else if !last2.IsZero() && time.Since(last2) < r.cfg.Interval {
		return false, nil
	}
	return true, r.backup(ctx)
}

func (r *Runner) noteRecord(rec *Record) {
	if rec == nil {
		return
	}
	r.mu.Lock()
	r.st.LastSuccess, r.st.LastSuccessNode, r.st.LastKey = rec.Time, rec.Node, rec.Key
	r.st.LastSize, r.st.LastDuration = rec.Size, rec.DurationSeconds
	r.mu.Unlock()
}

// ObjectName is the key for a backup started at t.
func ObjectName(prefix, cluster, node string, t time.Time) string {
	return prefix + namePrefix(cluster) + t.UTC().Format("20060102T150405Z") + "-" + node + Suffix
}

// Suffix ends every backup object key.
const Suffix = ".tar.gz.kvlb"

func namePrefix(cluster string) string { return "kvlock-" + cluster + "-" }

func (r *Runner) backup(ctx context.Context) error {
	r.running.Store(true)
	defer r.running.Store(false)
	start := time.Now()
	key := ObjectName(r.cfg.Prefix, r.cfg.Cluster, r.cfg.Node, start)
	r.mu.Lock()
	r.st.LastAttempt = start.UTC().Format(time.RFC3339Nano)
	r.mu.Unlock()
	r.emit("backup", "backup started: "+key)

	size, err := r.snapshotAndUpload(ctx, key)
	if err != nil {
		r.fail(err)
		return err
	}
	dur := time.Since(start)
	rec := Record{Node: r.cfg.Node, Key: key, Time: time.Now().UTC().Format(time.RFC3339Nano), Size: size, DurationSeconds: dur.Seconds()}
	b, _ := json.Marshal(rec)
	if err := r.coord.Put(ctx, r.RecordKey(), b); err != nil {
		// The object is uploaded; only the schedule record is missing.
		r.emit("backup", "backup uploaded but recording it in Consul failed: "+err.Error())
	}
	r.mu.Lock()
	r.lastErr = time.Time{}
	r.st.LastError = ""
	r.mu.Unlock()
	r.noteRecord(&rec)
	if r.reg != nil {
		r.reg.BackupLastSuccess.Set(float64(time.Now().Unix()))
		r.reg.BackupLastSize.Set(float64(size))
		r.reg.BackupDuration.Set(dur.Seconds())
	}
	r.emit("backup", fmt.Sprintf("backup ok: %s (%d bytes, %s)", key, size, dur.Round(time.Millisecond)))
	if err := r.prune(ctx); err != nil {
		r.emit("backup", "retention cleanup failed: "+err.Error())
	}
	return nil
}

func (r *Runner) fail(err error) {
	r.mu.Lock()
	r.lastErr = time.Now()
	r.st.LastError = err.Error()
	r.mu.Unlock()
	if r.reg != nil {
		r.reg.BackupFailures.Inc()
	}
	r.emit("backup", "backup failed: "+err.Error())
}

func (r *Runner) snapshotAndUpload(ctx context.Context, key string) (int64, error) {
	if err := r.bgsave(ctx); err != nil {
		return 0, err
	}
	if fi, err := os.Stat(r.cfg.Dir); err != nil || !fi.IsDir() {
		return 0, fmt.Errorf("backup dir %s not readable: %v", r.cfg.Dir, err)
	}
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		enc, err := NewEncryptWriter(pw, r.cfg.Key)
		if err == nil {
			err = WriteArchive(enc, r.cfg.Dir)
			if cerr := enc.Close(); err == nil {
				err = cerr
			}
		}
		pw.CloseWithError(err)
		done <- err
	}()
	cr := &countReader{r: pr}
	perr := r.store.Put(ctx, key, cr)
	pr.CloseWithError(errors.New("upload stopped"))
	aerr := <-done
	if aerr != nil && perr == nil {
		perr = aerr
	}
	if perr != nil {
		if aerr != nil {
			return 0, fmt.Errorf("archive: %w", aerr)
		}
		return 0, fmt.Errorf("upload: %w", perr)
	}
	return cr.n, nil
}

// bgsave triggers BGSAVE and waits until the instance reports it done.
func (r *Runner) bgsave(ctx context.Context) error {
	before, err := r.src.InfoPersistence(ctx)
	if err != nil {
		return fmt.Errorf("info persistence: %w", err)
	}
	reqSec := time.Now().Unix()
	if err := r.src.BGSave(ctx); err != nil {
		return fmt.Errorf("bgsave: %w", err)
	}
	deadline := time.Now().Add(r.cfg.BGSaveTimeout)
	sawRunning := false
	for {
		p, err := r.src.InfoPersistence(ctx)
		if err != nil {
			return fmt.Errorf("info persistence: %w", err)
		}
		if p.InProgress {
			sawRunning = true
		} else if sawRunning || (p.LastSave >= reqSec && p.LastSave != before.LastSave) {
			if p.LastStatus != "" && p.LastStatus != "ok" {
				return fmt.Errorf("bgsave finished with status %q", p.LastStatus)
			}
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("bgsave did not finish within %s", r.cfg.BGSaveTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.cfg.Poll):
		}
	}
}

// prune deletes the oldest backups of this cluster beyond Keep.
func (r *Runner) prune(ctx context.Context) error {
	if r.cfg.Keep <= 0 {
		return nil
	}
	objs, err := ListBackups(ctx, r.store, r.cfg.Prefix, r.cfg.Cluster)
	if err != nil {
		return err
	}
	for i := 0; i < len(objs)-r.cfg.Keep; i++ {
		if err := r.store.Delete(ctx, objs[i].Key); err != nil {
			return err
		}
		r.emit("backup", "deleted old backup "+objs[i].Key)
	}
	return nil
}

// ListBackups returns this cluster's backups under prefix, oldest first.
func ListBackups(ctx context.Context, s Store, prefix, cluster string) ([]Object, error) {
	all, err := s.List(ctx, prefix+namePrefix(cluster))
	if err != nil {
		return nil, err
	}
	var out []Object
	for _, o := range all {
		rest := strings.TrimPrefix(o.Key, prefix)
		if strings.Contains(rest, "/") || !strings.HasSuffix(o.Key, Suffix) {
			continue
		}
		out = append(out, o)
	}
	return out, nil
}

type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
