package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/logx"
	"github.com/p1u3o/kvlock/internal/watchdog"
)

type watchdogOpts struct {
	cfg      watchdog.Config
	addr     string
	port     int
	password string
	httpAddr string
	node     string
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func parseWatchdog(args []string, stderr io.Writer) (*watchdogOpts, error) {
	o := &watchdogOpts{}
	fs := flag.NewFlagSet("watchdog", flag.ContinueOnError)
	fs.SetOutput(stderr)
	c := &o.cfg
	fs.StringVar(&c.HeartbeatFile, "heartbeat-file", os.Getenv("KVLOCK_WATCHDOG_FILE"), "Sidecar heartbeat file (the sidecar's --watchdog-file) ($KVLOCK_WATCHDOG_FILE)")
	fs.DurationVar(&c.Timeout, "timeout", envDur("KVLOCK_WATCHDOG_TIMEOUT", 5*time.Second), "Fence when the heartbeat is older than this; keep below the sidecar's --session-ttl ($KVLOCK_WATCHDOG_TIMEOUT)")
	fs.DurationVar(&c.Interval, "interval", envDur("KVLOCK_WATCHDOG_CHECK_INTERVAL", time.Second), "Check interval ($KVLOCK_WATCHDOG_CHECK_INTERVAL)")
	fs.DurationVar(&c.StartupGrace, "startup-grace", envDur("KVLOCK_WATCHDOG_STARTUP_GRACE", 60*time.Second), "Tolerate a missing heartbeat file this long after start ($KVLOCK_WATCHDOG_STARTUP_GRACE)")
	fs.StringVar(&c.FenceHost, "fence-host", envOr("FENCE_HOST", "127.0.0.1"), "REPLICAOF fence target ($FENCE_HOST)")
	fs.IntVar(&c.FencePort, "fence-port", envInt("FENCE_PORT", 1), "Fence target port; nothing may listen there ($FENCE_PORT)")
	kill := envOr("KILL_CLIENTS_ON_DEMOTE", "true")
	fs.BoolVar(&c.KillClients, "kill-clients", kill == "true" || kill == "1", "CLIENT KILL TYPE normal after fencing ($KILL_CLIENTS_ON_DEMOTE)")
	fs.StringVar(&c.Mode, "mode", envOr("MODE", "active"), "active|observe; observe never fences ($MODE)")
	fs.DurationVar(&c.KvrocksTimeout, "kvrocks-timeout", envDur("KVROCKS_TIMEOUT", 2*time.Second), "Per-command kvrocks timeout ($KVROCKS_TIMEOUT)")
	fs.StringVar(&o.addr, "kvrocks-addr", envOr("KVROCKS_ADDR", "127.0.0.1"), "Local kvrocks host ($KVROCKS_ADDR)")
	fs.IntVar(&o.port, "kvrocks-port", envInt("KVROCKS_PORT", 6666), "Local kvrocks port ($KVROCKS_PORT)")
	pwFile := fs.String("kvrocks-password-file", os.Getenv("KVROCKS_PASSWORD_FILE"), "File containing the kvrocks password ($KVROCKS_PASSWORD_FILE; or set $KVROCKS_PASSWORD)")
	fs.StringVar(&o.httpAddr, "http-addr", envOr("KVLOCK_WATCHDOG_HTTP_ADDR", ""), "Listen address for /metrics and /healthz, e.g. :8081; empty disables ($KVLOCK_WATCHDOG_HTTP_ADDR)")
	fs.StringVar(&o.node, "node", envOr("NODE_NAME", envOr("HOSTNAME", "kv01")), "Node name for logs ($NODE_NAME)")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	o.password = os.Getenv("KVROCKS_PASSWORD")
	if *pwFile != "" {
		b, err := os.ReadFile(*pwFile)
		if err != nil {
			return nil, fmt.Errorf("read kvrocks password file: %w", err)
		}
		o.password = strings.TrimSpace(string(b))
	}
	switch {
	case c.HeartbeatFile == "":
		return nil, errors.New("--heartbeat-file is required")
	case c.Mode != "active" && c.Mode != "observe":
		return nil, fmt.Errorf("invalid --mode %q", c.Mode)
	case c.Timeout <= 0 || c.Interval <= 0:
		return nil, errors.New("--timeout and --interval must be > 0")
	case c.Interval >= c.Timeout:
		return nil, errors.New("--interval must be shorter than --timeout")
	case c.FencePort <= 0 || c.FencePort > 65535:
		return nil, fmt.Errorf("invalid --fence-port %d", c.FencePort)
	case c.FencePort == o.port && (c.FenceHost == o.addr || c.FenceHost == "127.0.0.1" || c.FenceHost == "localhost"):
		return nil, errors.New("--fence-host/--fence-port must not point at the local instance")
	}
	return o, nil
}

// runWatchdog implements `kvrocks-leader watchdog`.
func runWatchdog(args []string, stderr io.Writer) int {
	o, err := parseWatchdog(args, stderr)
	if err != nil {
		fmt.Fprintln(stderr, "watchdog config error:", err)
		return 2
	}
	log := logx.New(o.node, o.cfg.Mode)
	kv := kvrocks.New(o.addr, o.port, o.password, o.cfg.KvrocksTimeout)
	defer kv.Close()
	m := watchdog.NewMetrics()
	w := watchdog.New(o.cfg, kv, log, m)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if o.httpAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", m.Handler())
		mux.HandleFunc("/healthz", func(rw http.ResponseWriter, r *http.Request) {
			// The watchdog itself is healthy while it runs; report the
			// last outcome for humans.
			fmt.Fprintf(rw, "{\"ok\":true,\"last\":%q}\n", w.Last().Outcome)
		})
		srv := &http.Server{Addr: o.httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("watchdog http failed", map[string]any{"error": err.Error()})
			}
		}()
		defer srv.Close()
	}
	log.Info("watchdog started", map[string]any{"file": o.cfg.HeartbeatFile, "timeout": o.cfg.Timeout.String(), "grace": o.cfg.StartupGrace.String(), "kvrocks": fmt.Sprintf("%s:%d", o.addr, o.port)})
	w.Run(ctx)
	return 0
}
