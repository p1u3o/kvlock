package controller

import (
	"context"
	"os"
	"time"

	"github.com/p1u3o/kvlock/internal/watchdog"
)

// watchdogLoop refreshes the heartbeat file read by `kvrocks-leader
// watchdog`. It runs in its own goroutine so the file tracks process
// liveness: a slow Consul call in the tick loop must not get a healthy
// leader fenced. The tick loop's progress is recorded as loop_unix_ms.
func (c *Controller) watchdogLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.WatchdogInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.writeWatchdogHeartbeat()
		}
	}
}

func (c *Controller) writeWatchdogHeartbeat() {
	c.mu.RLock()
	hb := watchdog.Heartbeat{
		UnixMs:     time.Now().UnixMilli(),
		LoopUnixMs: c.loopAt.Load(),
		PID:        os.Getpid(),
		Node:       c.cfg.NodeName,
		Mode:       c.cfg.Mode,
		Role:       string(c.role),
	}
	if c.leader != nil {
		hb.Epoch = c.leader.Epoch
	}
	c.mu.RUnlock()
	if err := watchdog.WriteHeartbeat(c.cfg.WatchdogFile, hb); err != nil {
		c.reg.WatchdogWrites.WithLabelValues("error").Inc()
		c.log.Warn("watchdog heartbeat write failed", map[string]any{"error": err.Error(), "file": c.cfg.WatchdogFile})
		return
	}
	c.reg.WatchdogWrites.WithLabelValues("ok").Inc()
}
