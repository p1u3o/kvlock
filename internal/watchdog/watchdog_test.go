package watchdog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/kvrocks"
)

type fakeKV struct {
	role     string
	infoErr  error
	replErr  error
	writable bool // probe result after fencing
	// writableUnlessRO models a promoted node (slave-read-only=no).
	writableUnlessRO bool
	ro               bool
	roCalls          int
	replicaOf        []string
	kills            int
	probes           int
	onRepl           func()
}

func (f *fakeKV) InfoReplication(context.Context) (*kvrocks.ReplicationInfo, error) {
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	return &kvrocks.ReplicationInfo{Role: f.role}, nil
}
func (f *fakeKV) ReplicaOf(_ context.Context, h string, p int) error {
	if f.onRepl != nil {
		f.onRepl()
	}
	if f.replErr != nil {
		return f.replErr
	}
	f.replicaOf = append(f.replicaOf, h)
	f.role = "slave"
	return nil
}
func (f *fakeKV) KillClients(context.Context) (int64, error) { f.kills++; return 3, nil }
func (f *fakeKV) WriteProbe(context.Context) (bool, error) {
	f.probes++
	return f.writable || (!f.ro && f.writableUnlessRO), nil
}
func (f *fakeKV) SetReadOnly(_ context.Context, ro bool) error { f.ro = ro; f.roCalls++; return nil }

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func setup(t *testing.T, mode string) (*Watchdog, *fakeKV, *clock, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kvlock", "heartbeat")
	kv := &fakeKV{role: "master"}
	w := New(Config{HeartbeatFile: path, Timeout: 5 * time.Second, Interval: time.Second, StartupGrace: 30 * time.Second,
		FenceHost: "127.0.0.1", FencePort: 1, KillClients: true, Mode: mode}, kv, nil, nil)
	c := &clock{t: time.Unix(1_800_000_000, 0)}
	w.SetClock(c.now)
	return w, kv, c, path
}

func beat(t *testing.T, path string, at time.Time, mode string) {
	t.Helper()
	if err := WriteHeartbeat(path, Heartbeat{UnixMs: at.UnixMilli(), Mode: mode, Role: "leader", PID: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "a", "b", "hb")
	in := Heartbeat{UnixMs: 123456, LoopUnixMs: 123000, PID: 7, Node: "kv01", Mode: "active", Role: "leader", Epoch: 9}
	if err := WriteHeartbeat(p, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadHeartbeat(p)
	if err != nil || out != in {
		t.Fatalf("got %+v %v", out, err)
	}
}

func TestFreshHeartbeatNeverFences(t *testing.T) {
	w, kv, c, path := setup(t, "active")
	c.t = c.t.Add(time.Hour) // well past the grace
	for i := 0; i < 20; i++ {
		beat(t, path, c.t, "active")
		c.t = c.t.Add(4900 * time.Millisecond) // just under the timeout
		if r := w.Step(context.Background()); r.Outcome != OutcomeHealthy {
			t.Fatalf("step %d: %+v", i, r)
		}
	}
	if len(kv.replicaOf) != 0 || kv.kills != 0 || kv.probes != 0 {
		t.Fatalf("touched kvrocks while healthy: %+v", kv)
	}
}

func TestMissingFileWithinGraceIsStarting(t *testing.T) {
	w, kv, c, _ := setup(t, "active")
	c.t = c.t.Add(10 * time.Second)
	if r := w.Step(context.Background()); r.Outcome != OutcomeStarting {
		t.Fatalf("%+v", r)
	}
	c.t = c.t.Add(30 * time.Second)
	if r := w.Step(context.Background()); r.Outcome != OutcomeFenced {
		t.Fatalf("after grace: %+v", r)
	}
	if len(kv.replicaOf) != 1 {
		t.Fatal("expected one fence")
	}
}

func TestStaleMasterIsFencedAndConfirmed(t *testing.T) {
	w, kv, c, path := setup(t, "active")
	beat(t, path, c.t, "active")
	c.t = c.t.Add(6 * time.Second)
	r := w.Step(context.Background())
	if r.Outcome != OutcomeFenced || r.Role != "slave" || r.Killed != 3 {
		t.Fatalf("%+v", r)
	}
	if len(kv.replicaOf) != 1 || kv.replicaOf[0] != "127.0.0.1" || kv.kills != 1 || kv.probes != 1 {
		t.Fatalf("%+v", kv)
	}
	// Idempotent: already a replica now, nothing more happens.
	c.t = c.t.Add(time.Second)
	if r := w.Step(context.Background()); r.Outcome != OutcomeStaleReplica || len(kv.replicaOf) != 1 {
		t.Fatalf("second step %+v", r)
	}
}

func TestStaleReplicaIsLeftAlone(t *testing.T) {
	w, kv, c, path := setup(t, "active")
	kv.role = "slave"
	beat(t, path, c.t, "active")
	c.t = c.t.Add(time.Minute)
	if r := w.Step(context.Background()); r.Outcome != OutcomeStaleReplica {
		t.Fatalf("%+v", r)
	}
	if len(kv.replicaOf) != 0 || kv.kills != 0 {
		t.Fatal("replica touched")
	}
}

func TestObserveModeNeverFences(t *testing.T) {
	for _, tc := range []struct{ wd, hb string }{{"observe", "active"}, {"active", "observe"}, {"observe", "observe"}} {
		w, kv, c, path := setup(t, tc.wd)
		beat(t, path, c.t, tc.hb)
		c.t = c.t.Add(time.Hour)
		r := w.Step(context.Background())
		if r.Outcome != OutcomeObserve {
			t.Fatalf("%+v: %+v", tc, r)
		}
		if len(kv.replicaOf) != 0 || kv.kills != 0 || kv.probes != 0 {
			t.Fatalf("%+v: observe touched kvrocks: %+v", tc, kv)
		}
	}
}

func TestReReadBeforeFencing(t *testing.T) {
	// The heartbeat is refreshed between the first read and the fence:
	// emulate it by refreshing from the InfoReplication call.
	w, kv, c, path := setup(t, "active")
	beat(t, path, c.t, "active")
	c.t = c.t.Add(time.Minute)
	kv2 := &raceKV{fakeKV: kv, refresh: func() { beat(t, path, c.t, "active") }}
	w.kv = kv2
	if r := w.Step(context.Background()); r.Outcome != OutcomeRaced {
		t.Fatalf("%+v", r)
	}
	if len(kv.replicaOf) != 0 {
		t.Fatal("fenced despite fresh heartbeat")
	}
}

type raceKV struct {
	*fakeKV
	refresh func()
}

func (r *raceKV) InfoReplication(ctx context.Context) (*kvrocks.ReplicationInfo, error) {
	r.refresh()
	return r.fakeKV.InfoReplication(ctx)
}

func TestFenceFailures(t *testing.T) {
	w, kv, c, path := setup(t, "active")
	beat(t, path, c.t, "active")
	c.t = c.t.Add(time.Minute)
	kv.replErr = errors.New("boom")
	if r := w.Step(context.Background()); r.Outcome != OutcomeFenceFailed {
		t.Fatalf("%+v", r)
	}
	kv.replErr = nil
	kv.writable = true
	if r := w.Step(context.Background()); r.Outcome != OutcomeFenceFailed || r.Err == nil {
		t.Fatalf("writable after fence must fail: %+v", r)
	}
}

func TestFenceSetsReadOnly(t *testing.T) {
	w, kv, c, path := setup(t, "active")
	beat(t, path, c.t, "active")
	c.t = c.t.Add(time.Minute)
	kv.writableUnlessRO = true
	if r := w.Step(context.Background()); r.Outcome != OutcomeFenced || !kv.ro {
		t.Fatalf("fence must set read-only: %+v ro=%v", r, kv.ro)
	}
}

func TestBackendDown(t *testing.T) {
	w, kv, c, path := setup(t, "active")
	beat(t, path, c.t, "active")
	c.t = c.t.Add(time.Minute)
	kv.infoErr = errors.New("connection refused")
	if r := w.Step(context.Background()); r.Outcome != OutcomeStaleDown {
		t.Fatalf("%+v", r)
	}
}
