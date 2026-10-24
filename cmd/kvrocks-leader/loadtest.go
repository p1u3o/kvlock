package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/p1u3o/kvlock/internal/clusterstatus"
	"github.com/p1u3o/kvlock/internal/httpapi"
	"github.com/p1u3o/kvlock/internal/loadtest"
)

// runLoadtest implements `kvrocks-leader loadtest`. Exit 0 when no
// acknowledged write was lost and no stale read seen, 1 otherwise, 2 on
// usage or safety refusal.
func runLoadtest(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var addrs, allow, statusAddrs multiFlag
	fs.Var(&addrs, "addr", "kvrocks host:port; repeatable (the master is discovered among them; one address = proxy/DNS)")
	fs.Var(&allow, "allow", "REQUIRED allowlist of targets (host:port or :port); repeatable. Port 6379 is always refused")
	pwFile := fs.String("password-file", "", "file with the kvrocks password (or $KVROCKS_PASSWORD)")
	dur := fs.Duration("duration", 60*time.Second, "test length")
	rate := fs.Int("rate", 1000, "target total ops/s (0 = as fast as possible)")
	workers := fs.Int("workers", 8, "concurrent connections")
	keys := fs.Int("keys", 10000, "keyspace size")
	vmin := fs.Int("value-min", 64, "minimum value size (bytes)")
	vmax := fs.Int("value-max", 1024, "maximum value size (bytes)")
	pipeline := fs.Int("pipeline", 1, "commands per pipeline")
	readRatio := fs.Float64("read-ratio", 0.7, "fraction of reads (0..1)")
	opTimeout := fs.Duration("op-timeout", time.Second, "per-pipeline timeout")
	prefix := fs.String("prefix", "kvlock:loadtest:", "key prefix (a unique run id is appended)")
	keep := fs.Bool("keep-keys", false, "do not delete the test keys afterwards")
	handoverAt := fs.Duration("handover-at", 0, "trigger a graceful handover via the admin API at this offset (0 = never)")
	adminURL := fs.String("admin-url", envOr("KVLOCK_ADMIN_URL", ""), "leader sidecar URL for --handover-at (default: the leader found via --status-addr)")
	handoverTo := fs.String("handover-to", "", "replica ip:port for the handover (default: sidecar picks)")
	tokenFile := addTokenFlags(fs)
	fs.Var(&statusAddrs, "status-addr", "sidecar HTTP address to poll for a leader/epoch timeline; repeatable")
	markFailover := fs.Bool("mark-on-signal", true, "SIGUSR1 adds a 'failover injected' marker to the timeline (for hard-kill tests)")
	asJSON := fs.Bool("json", false, "print the report as JSON")
	if err := rejectTokenFlag(args); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := loadtest.CheckTargets(addrs, allow); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	pw := os.Getenv("KVROCKS_PASSWORD")
	if *pwFile != "" {
		b, err := os.ReadFile(*pwFile)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		pw = strings.TrimSpace(string(b))
	}
	cfg := loadtest.Config{Addrs: addrs, Allow: allow, Password: pw, Duration: *dur, Rate: *rate,
		Workers: *workers, Keys: *keys, ValueMin: *vmin, ValueMax: *vmax, Pipeline: *pipeline,
		ReadRatio: *readRatio, OpTimeout: *opTimeout, Prefix: *prefix, Cleanup: !*keep,
		Logf: func(f string, a ...any) { fmt.Fprintf(stderr, f+"\n", a...) }}
	hc := &http.Client{Timeout: 2 * time.Second}
	if len(statusAddrs) > 0 {
		cfg.Observer = func(ctx context.Context) string {
			rep := clusterstatus.Evaluate(clusterstatus.Fetch(ctx, hc, statusAddrs), clusterstatus.Options{})
			var parts []string
			for _, n := range rep.Nodes {
				if n.Status == nil {
					parts = append(parts, n.URL+"=down")
					continue
				}
				parts = append(parts, fmt.Sprintf("%s=%s/e%d", n.Status.Node, n.Status.Role, n.Status.Epoch))
			}
			return strings.Join(parts, " ")
		}
	}
	if *handoverAt > 0 {
		tok, err := adminToken(*tokenFile)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		if tok == "" {
			fmt.Fprintln(stderr, "--handover-at needs the admin token in $KVLOCK_ADMIN_TOKEN or --token-file")
			return 2
		}
		if *adminURL == "" && len(statusAddrs) == 0 {
			fmt.Fprintln(stderr, "--handover-at needs --admin-url or --status-addr")
			return 2
		}
		cfg.Events = append(cfg.Events, loadtest.Event{At: *handoverAt, Name: "handover", Fn: func(ctx context.Context) error {
			url := *adminURL
			if url == "" {
				for _, n := range clusterstatus.Fetch(ctx, hc, statusAddrs) {
					if n.Status != nil && n.Status.Role == "leader" {
						url = n.URL
					}
				}
				if url == "" {
					return fmt.Errorf("no leader found to hand over from")
				}
			}
			hctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			res, err := httpapi.RequestHandover(hctx, url, tok, httpapi.HandoverRequest{To: *handoverTo})
			if err != nil {
				return err
			}
			cfg.Logf("handover result: stage=%s target=%s new_leader=%s lag=%d", res.Stage, res.Target, res.NewLeader, res.Lag)
			return nil
		}})
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *markFailover {
		ch := make(chan os.Signal, 4)
		marks := make(chan string, 4)
		signal.Notify(ch, syscall.SIGUSR1)
		defer signal.Stop(ch)
		go func() {
			for range ch {
				marks <- "failover injected (SIGUSR1)"
			}
		}()
		cfg.Marks = marks
	}
	rep, err := loadtest.Run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(stderr, "loadtest:", err)
		return 2
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	} else {
		fmt.Fprint(stdout, loadtest.Format(rep))
	}
	if !rep.OK() {
		return 1
	}
	return 0
}
