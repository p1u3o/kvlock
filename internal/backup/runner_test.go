package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeKvrocks simulates BGSAVE writing a checkpoint into dir.
type fakeKvrocks struct {
	mu       sync.Mutex
	dir      string
	saves    int
	polls    int
	running  int // polls left reporting in-progress
	last     int64
	status   string
	failSave bool
	never    bool // never finishes
}

func (k *fakeKvrocks) BGSave(context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.failSave {
		return errors.New("ERR bgsave in-progress")
	}
	k.saves++
	k.running = 2
	os.RemoveAll(k.dir)
	os.MkdirAll(filepath.Join(k.dir, "sub"), 0o755)
	os.WriteFile(filepath.Join(k.dir, "CURRENT"), []byte("MANIFEST-000001\n"), 0o644)
	big := make([]byte, 7<<20) // forces a multipart upload
	rand.Read(big)
	os.WriteFile(filepath.Join(k.dir, "sub", "000001.sst"), big, 0o644)
	return nil
}

func (k *fakeKvrocks) InfoPersistence(context.Context) (*kvrocks.Persistence, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.polls++
	if k.never {
		return &kvrocks.Persistence{InProgress: true}, nil
	}
	if k.running > 0 {
		k.running--
		if k.running == 0 {
			k.last = time.Now().Unix()
		}
		return &kvrocks.Persistence{InProgress: true, LastSave: k.last, LastStatus: "ok"}, nil
	}
	st := k.status
	if st == "" {
		st = "ok"
	}
	return &kvrocks.Persistence{LastSave: k.last, LastStatus: st}, nil
}

type fakeCoord struct {
	mu   sync.Mutex
	kv   map[string][]byte
	held map[string]string
	puts int
}

func newCoord() *fakeCoord { return &fakeCoord{kv: map[string][]byte{}, held: map[string]string{}} }

func (c *fakeCoord) Acquire(_ context.Context, key, s string, v []byte) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.held[key]; ok && h != s {
		return false, nil
	}
	c.held[key], c.kv[key] = s, v
	return true, nil
}
func (c *fakeCoord) Release(_ context.Context, key, s string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.held[key] == s {
		delete(c.held, key)
	}
	return nil
}
func (c *fakeCoord) Get(_ context.Context, key string) ([]byte, string, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.kv[key], c.held[key], 1, nil
}
func (c *fakeCoord) Put(_ context.Context, key string, v []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.puts++
	c.kv[key] = v
	return nil
}


var replica = NodeState{Follower: true, Healthy: true, BackendRole: "slave", MasterLinkUp: true, LagOK: true, Session: "s-rep"}
var leader = NodeState{Leader: true, Healthy: true, BackendRole: "master", Replicas: 1, Session: "s-lead"}

func setup(t *testing.T, mod func(*Config)) (*Runner, *fakeKvrocks, *fakeS3, *S3Store, *fakeCoord, *metrics.Registry, *[]string) {
	t.Helper()
	src := &fakeKvrocks{dir: filepath.Join(t.TempDir(), "backup")}
	f, srv := newFakeS3(t, "bkt")
	st := f.store(t, srv)
	coord := newCoord()
	reg := metrics.New()
	cfg := Config{Cluster: "c1", Node: "n1", Dir: src.dir, Prefix: "kv/", Key: testKey(), Interval: 6 * time.Hour, Poll: time.Millisecond, BGSaveTimeout: 5 * time.Second, LeaderGrace: time.Hour}
	if mod != nil {
		mod(&cfg)
	}
	var mu sync.Mutex
	events := &[]string{}
	r := NewRunner(cfg, src, st, coord, reg, func(k, m string) { mu.Lock(); *events = append(*events, m); mu.Unlock() })
	return r, src, f, st, coord, reg, events
}

func TestBackupAndRestore(t *testing.T) {
	r, src, f, st, coord, reg, events := setup(t, nil)
	ctx := context.Background()
	ran, err := r.MaybeBackup(ctx, replica)
	if !ran || err != nil {
		t.Fatalf("ran=%v err=%v events=%v", ran, err, *events)
	}
	keys := f.keys()
	if len(keys) != 1 || !strings.HasPrefix(keys[0], "kv/kvlock-c1-") || !strings.HasSuffix(keys[0], "-n1"+Suffix) {
		t.Fatalf("keys %v", keys)
	}
	if f.sse[keys[0]] != "AES256" {
		t.Fatalf("SSE header %q", f.sse[keys[0]])
	}
	// Ciphertext must not contain the plaintext manifest.
	if bytes.Contains(f.objs[keys[0]], []byte("MANIFEST-000001")) {
		t.Fatal("object is not encrypted")
	}
	if src.saves != 1 || coord.held[r.LockKey()] != "" {
		t.Fatalf("saves=%d lock=%q", src.saves, coord.held[r.LockKey()])
	}
	var rec Record
	json.Unmarshal(coord.kv[r.RecordKey()], &rec)
	if rec.Key != keys[0] || rec.Node != "n1" || rec.Size != int64(len(f.objs[keys[0]])) {
		t.Fatalf("record %+v", rec)
	}
	if testutil.ToFloat64(reg.BackupLastSize) != float64(rec.Size) || testutil.ToFloat64(reg.BackupLastSuccess) == 0 {
		t.Fatal("metrics not set")
	}
	if s := r.Status(); s.LastKey != keys[0] || s.LastError != "" {
		t.Fatalf("status %+v", s)
	}

	// Interval not elapsed: no second backup.
	if ran, _ := r.MaybeBackup(ctx, replica); ran || src.saves != 1 {
		t.Fatal("backed up again within interval")
	}

	dst := filepath.Join(t.TempDir(), "restore")
	got, err := Restore(ctx, st, "kv/", "c1", "", r.cfg.Key, dst)
	if err != nil || got != keys[0] {
		t.Fatalf("restore %q %v", got, err)
	}
	for _, p := range []string{"CURRENT", "sub/000001.sst"} {
		a, _ := os.ReadFile(filepath.Join(src.dir, p))
		b, err := os.ReadFile(filepath.Join(dst, p))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("%s differs: %v", p, err)
		}
	}
	if _, err := Restore(ctx, st, "kv/", "c1", "", r.cfg.Key, dst); err == nil {
		t.Fatal("restore into non-empty dir must fail")
	}
	if _, err := Restore(ctx, st, "kv/", "c1", "", bytes.Repeat([]byte{9}, 32), filepath.Join(t.TempDir(), "x")); err == nil {
		t.Fatal("wrong key must fail")
	}
}

func TestEligibility(t *testing.T) {
	r, _, _, _, _, _, _ := setup(t, nil)
	now := time.Now()
	cases := []struct {
		name string
		st   NodeState
		last time.Time
		want bool
	}{
		{"replica", replica, time.Time{}, true},
		{"replica lagging", func() NodeState { s := replica; s.LagOK = false; return s }(), time.Time{}, false},
		{"replica link down", func() NodeState { s := replica; s.MasterLinkUp = false; return s }(), time.Time{}, false},
		{"replica unhealthy", func() NodeState { s := replica; s.Healthy = false; return s }(), time.Time{}, false},
		{"no session", func() NodeState { s := replica; s.Session = ""; return s }(), time.Time{}, false},
		{"leader not allowed", leader, time.Time{}, false},
	}
	for _, c := range cases {
		if got, why := r.eligible(c.st, c.last, now); got != c.want {
			t.Errorf("%s: got %v (%s)", c.name, got, why)
		}
	}
	r.cfg.AllowLeader = true
	if ok, _ := r.eligible(leader, now.Add(-6*time.Hour-30*time.Minute), now); ok {
		t.Error("leader with replica inside grace must wait")
	}
	if ok, _ := r.eligible(leader, now.Add(-8*time.Hour), now); !ok {
		t.Error("leader past grace should back up")
	}
	if ok, _ := r.eligible(leader, time.Time{}, now); !ok {
		t.Error("leader with no record ever should back up")
	}
	solo := leader
	solo.Replicas = 0
	if ok, _ := r.eligible(solo, now.Add(-7*time.Hour), now); !ok {
		t.Error("leader without replicas should back up")
	}
}

func TestSingleUploader(t *testing.T) {
	r, src, f, _, coord, _, _ := setup(t, nil)
	coord.held[r.LockKey()] = "other-session"
	if ran, err := r.MaybeBackup(context.Background(), replica); ran || err != nil || src.saves != 0 || len(f.keys()) != 0 {
		t.Fatalf("ran with lock held elsewhere: %v %v", ran, err)
	}
	// Two runners sharing Consul and the bucket: only one backup.
	delete(coord.held, r.LockKey())
	r2 := NewRunner(r.cfg, src, r.store, coord, nil, nil)
	r2.cfg.Node = "n2"
	var wg sync.WaitGroup
	for _, x := range []*Runner{r, r2} {
		wg.Add(1)
		go func(x *Runner, s string) {
			defer wg.Done()
			st := replica
			st.Session = s
			x.MaybeBackup(context.Background(), st)
		}(x, "s-"+x.cfg.Node)
	}
	wg.Wait()
	if n := len(f.keys()); n != 1 {
		t.Fatalf("%d objects, want 1", n)
	}
}

func TestFailures(t *testing.T) {
	ctx := context.Background()
	r, src, f, _, coord, reg, _ := setup(t, func(c *Config) { c.RetryAfter = time.Hour })
	src.failSave = true
	if _, err := r.MaybeBackup(ctx, replica); err == nil {
		t.Fatal("expected bgsave error")
	}
	if testutil.ToFloat64(reg.BackupFailures) != 1 || r.Status().LastError == "" || coord.puts != 0 {
		t.Fatal("failure not recorded")
	}
	src.failSave = false
	if ran, _ := r.MaybeBackup(ctx, replica); ran {
		t.Fatal("retried inside RetryAfter")
	}

	r, src, _, _, _, _, _ = setup(t, func(c *Config) { c.BGSaveTimeout = 20 * time.Millisecond })
	src.never = true
	if _, err := r.MaybeBackup(ctx, replica); err == nil || !strings.Contains(err.Error(), "did not finish") {
		t.Fatalf("timeout: %v", err)
	}

	r, src, _, _, _, _, _ = setup(t, nil)
	src.status = "err"
	if _, err := r.MaybeBackup(ctx, replica); err == nil {
		t.Fatal("bgsave status err must fail")
	}

	r, _, f, _, coord, _, _ = setup(t, nil)
	f.failPut = true
	if _, err := r.MaybeBackup(ctx, replica); err == nil || coord.puts != 0 {
		t.Fatalf("upload failure: %v puts=%d", err, coord.puts)
	}
	if coord.held[r.LockKey()] != "" {
		t.Fatal("lock not released after failure")
	}
}

func TestRetention(t *testing.T) {
	r, _, f, st, _, _, _ := setup(t, func(c *Config) { c.Keep = 2 })
	ctx := context.Background()
	for i, k := range []string{"kv/kvlock-c1-20200101T000000Z-a" + Suffix, "kv/kvlock-c1-20200102T000000Z-a" + Suffix, "kv/kvlock-c1-20200103T000000Z-a" + Suffix, "kv/kvlock-other-20200101T000000Z-a" + Suffix, "kv/unrelated.txt"} {
		if err := st.Put(ctx, k, strings.NewReader("x")); err != nil {
			t.Fatal(i, err)
		}
	}
	if _, err := r.MaybeBackup(ctx, replica); err != nil {
		t.Fatal(err)
	}
	got := f.keys()
	if len(got) != 4 || got[0] != "kv/kvlock-c1-20200103T000000Z-a"+Suffix || got[2] != "kv/kvlock-other-20200101T000000Z-a"+Suffix {
		t.Fatalf("after prune: %v", got)
	}
}

func TestObserveNeverRuns(t *testing.T) {
	r, src, f, _, coord, _, _ := setup(t, func(c *Config) { c.Observe = true; c.Check = time.Millisecond })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	r.Run(ctx, func() NodeState { return replica })
	if ran, _ := r.MaybeBackup(context.Background(), replica); ran || src.saves != 0 || src.polls != 0 || len(f.keys()) != 0 || coord.puts != 0 || len(coord.held) != 0 {
		t.Fatal("observe mode did something")
	}
}
