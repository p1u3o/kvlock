package controller_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/backup"
	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/kvrocks"
)

// backupProbe is a real backup.Runner over counting fakes.
type backupProbe struct {
	mu                         sync.Mutex
	r                          *backup.Runner
	bgsaves, infos, puts, gets int
	lockAcq, recPuts           int
	rec                        []byte
	dir                        string
}

func newBackupProbe(t *testing.T) *backupProbe {
	p := &backupProbe{dir: t.TempDir()}
	p.r = backup.NewRunner(backup.Config{Cluster: "main", Node: "n", Dir: p.dir, Key: make([]byte, 32), Interval: time.Hour, Check: time.Millisecond, Poll: time.Millisecond, BGSaveTimeout: time.Second},
		(*probeSrc)(p), (*probeStore)(p), (*probeCoord)(p), nil, nil)
	return p
}

func (p *backupProbe) assertNone(t *testing.T) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bgsaves+p.infos+p.puts+p.lockAcq+p.recPuts != 0 {
		t.Errorf("observe ran backups: bgsave=%d info=%d s3puts=%d lock=%d record=%d", p.bgsaves, p.infos, p.puts, p.lockAcq, p.recPuts)
	}
}

type probeSrc backupProbe

func (p *probeSrc) BGSave(context.Context) error {
	p.mu.Lock()
	p.bgsaves++
	p.mu.Unlock()
	return nil
}
func (p *probeSrc) InfoPersistence(context.Context) (*kvrocks.Persistence, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.infos++
	if p.bgsaves == 0 {
		return &kvrocks.Persistence{LastSave: 1, LastStatus: "ok"}, nil
	}
	return &kvrocks.Persistence{LastSave: time.Now().Unix() + 1, LastStatus: "ok"}, nil
}

type probeStore backupProbe

func (p *probeStore) Put(_ context.Context, _ string, r io.Reader) error {
	io.Copy(io.Discard, r)
	p.mu.Lock()
	p.puts++
	p.mu.Unlock()
	return nil
}
func (p *probeStore) Get(context.Context, string) (io.ReadCloser, error) { return nil, io.EOF }
func (p *probeStore) List(context.Context, string) ([]backup.Object, error) {
	return nil, nil
}
func (p *probeStore) Delete(context.Context, string) error { return nil }

type probeCoord backupProbe

func (p *probeCoord) Acquire(context.Context, string, string, []byte) (bool, error) {
	p.mu.Lock()
	p.lockAcq++
	p.mu.Unlock()
	return true, nil
}
func (p *probeCoord) Release(context.Context, string, string) error { return nil }
func (p *probeCoord) Get(context.Context, string) ([]byte, string, uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rec, "", 0, nil
}
func (p *probeCoord) Put(_ context.Context, _ string, v []byte) error {
	p.mu.Lock()
	p.recPuts++
	p.rec = v
	p.mu.Unlock()
	return nil
}

// In active mode a healthy, linked, caught-up replica backs up once.
func TestActiveReplicaBacksUp(t *testing.T) {
	cfg := testConfig("kv02")
	kv := &fakeKV{up: true, role: "slave", masterHost: "10.0.0.9", masterPort: 6666, lag: 0}
	lock := &fakeLock{val: elect.MarshalLeader("kv01", "10.0.0.9", "", 6666, "sess-x", 3, time.Now()), sess: "sess-x"}
	bp := newBackupProbe(t)
	c, _, _, _ := runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: lock, Registrar: &fakeRegistrar{}, KV: kv, Backup: bp.r}, 400*time.Millisecond)
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.bgsaves != 1 || bp.puts != 1 || bp.recPuts != 1 {
		t.Fatalf("role=%v bgsave=%d puts=%d record=%d", c.Snapshot().Role, bp.bgsaves, bp.puts, bp.recPuts)
	}
	if s := c.Snapshot(); s.Backup == nil || !s.Backup.Enabled {
		t.Fatalf("snapshot backup %+v", s.Backup)
	}
}

// The leader never backs up without --backup-allow-leader.
func TestActiveLeaderDoesNotBackUp(t *testing.T) {
	cfg := testConfig("kv01")
	kv := &fakeKV{up: true, role: "master", local: 500}
	bp := newBackupProbe(t)
	c, _, _, _ := runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: &fakeRegistrar{}, KV: kv, Backup: bp.r}, 300*time.Millisecond)
	if c.Snapshot().Role != elect.RoleLeader {
		t.Fatalf("role %v", c.Snapshot().Role)
	}
	bp.assertNone(t)
}
