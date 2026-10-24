package controller_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/watchdog"
)

func TestSidecarWritesWatchdogHeartbeat(t *testing.T) {
	for _, mode := range []string{"active", "observe"} {
		cfg := testConfig("kv01")
		cfg.Mode = mode
		cfg.WatchdogFile = filepath.Join(t.TempDir(), "kvlock", "heartbeat")
		cfg.WatchdogInterval = 20 * time.Millisecond
		kv := &fakeKV{up: true, role: "master", local: 500}
		reg := &fakeRegistrar{}
		runCtl(t, cfg, controller.Deps{Session: &fakeSession{}, Lock: &fakeLock{}, Registrar: reg, KV: kv}, 200*time.Millisecond)
		hb, err := watchdog.ReadHeartbeat(cfg.WatchdogFile)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if hb.Mode != mode || hb.Node != "kv01" || hb.PID == 0 || time.Since(hb.Time()) > 5*time.Second {
			t.Fatalf("%s: bad heartbeat %+v", mode, hb)
		}
		if mode == "observe" {
			assertNoSideEffects(t, kv, reg)
		}
	}
}
