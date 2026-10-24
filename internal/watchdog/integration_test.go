package watchdog

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/testkv"
)

// TestWatchdogAgainstRealKvrocks needs a kvrocks binary; skipped otherwise.
func TestWatchdogAgainstRealKvrocks(t *testing.T) {
	srv := testkv.Start(t)
	ctx := context.Background()
	kv := kvrocks.New("127.0.0.1", srv.Port, "", 2*time.Second)
	defer kv.Close()

	// A writer that should be disconnected by the fence.
	writer := srv.Client()
	defer writer.Close()
	if err := writer.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// A promoted node runs with slave-read-only=no (the sidecar sets it
	// on promotion); the fence must still stop writes.
	if err := kv.SetReadOnly(ctx, false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "hb")
	fence := testkv.FreePort(t) // nothing listens here
	w := New(Config{HeartbeatFile: path, Timeout: 2 * time.Second, Interval: 200 * time.Millisecond, StartupGrace: time.Second,
		FenceHost: "127.0.0.1", FencePort: fence, KillClients: true, Mode: "active"}, kv, nil, nil)

	// Healthy sidecar: fresh beats, kvrocks stays a writable master.
	for i := 0; i < 5; i++ {
		_ = WriteHeartbeat(path, Heartbeat{UnixMs: time.Now().UnixMilli(), Mode: "active"})
		if r := w.Step(ctx); r.Outcome != OutcomeHealthy {
			t.Fatalf("healthy step: %+v", r)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ok, err := kv.WriteProbe(ctx); err != nil || !ok {
		t.Fatalf("master should be writable: %v %v", ok, err)
	}

	// Observe mode on the same stale heartbeat: no change.
	_ = WriteHeartbeat(path, Heartbeat{UnixMs: time.Now().Add(-time.Minute).UnixMilli(), Mode: "observe"})
	if r := w.Step(ctx); r.Outcome != OutcomeObserve {
		t.Fatalf("observe: %+v", r)
	}
	if ri, _ := kv.InfoReplication(ctx); ri.Role != "master" {
		t.Fatalf("observe changed role to %s", ri.Role)
	}

	// Sidecar died: stale heartbeat.
	_ = WriteHeartbeat(path, Heartbeat{UnixMs: time.Now().Add(-time.Minute).UnixMilli(), Mode: "active"})
	r := w.Step(ctx)
	if r.Outcome != OutcomeFenced {
		t.Fatalf("expected fenced: %+v", r)
	}
	if r.Killed < 1 {
		t.Fatalf("writer connection not killed: %+v", r)
	}
	err := writer.Set(ctx, "k", "v2", 0).Err()
	if err == nil {
		t.Fatal("write accepted after fence")
	}
	if !kvrocks.IsReadOnlyError(err) {
		// The first command after CLIENT KILL may see the closed socket;
		// go-redis then reconnects and the retry must be READONLY.
		if err2 := writer.Set(ctx, "k", "v2", 0).Err(); !kvrocks.IsReadOnlyError(err2) {
			t.Fatalf("expected READONLY, got %v then %v", err, err2)
		}
	}
	ri, _ := kv.InfoReplication(ctx)
	if ri.Role != "slave" || ri.MasterPort != fence {
		t.Fatalf("role %s master port %d", ri.Role, ri.MasterPort)
	}
}
