package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/example/kvrocks-leader/internal/httpapi"
)

// runHandover implements `kvrocks-leader handover`: it asks the sidecar on
// --addr (normally the local leader) to hand leadership to a replica.
func runHandover(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("handover", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", envOr("KVLOCK_ADMIN_URL", "http://127.0.0.1:8080"), "base URL of the leader's sidecar HTTP API ($KVLOCK_ADMIN_URL)")
	to := fs.String("to", "", "replica ip:port to hand over to, as the master reports it (default: least-lagged online replica)")
	maxLag := fs.Int64("max-lag", -1, "maximum replica lag in bytes before committing (default: the sidecar's --handover-max-lag)")
	timeout := fs.Duration("timeout", 0, "how long to wait for the replica to catch up (default: the sidecar's --handover-timeout)")
	token := fs.String("token", os.Getenv("KVLOCK_ADMIN_TOKEN"), "admin token ($KVLOCK_ADMIN_TOKEN)")
	tokenFile := fs.String("token-file", os.Getenv("KVLOCK_ADMIN_TOKEN_FILE"), "file containing the admin token ($KVLOCK_ADMIN_TOKEN_FILE)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	tok := *token
	if tok == "" && *tokenFile != "" {
		b, err := os.ReadFile(*tokenFile)
		if err != nil {
			fmt.Fprintln(stderr, "reading token file:", err)
			return 2
		}
		tok = strings.TrimSpace(string(b))
	}
	req := httpapi.HandoverRequest{To: *to}
	if *maxLag >= 0 {
		req.MaxLag = maxLag
	}
	wait := 2 * time.Minute
	if *timeout > 0 {
		req.Timeout = timeout.String()
		wait = 2**timeout + 30*time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	res, err := httpapi.RequestHandover(ctx, *addr, tok, req)
	out, _ := json.MarshalIndent(res, "", "  ")
	fmt.Fprintln(stdout, string(out))
	if err != nil {
		fmt.Fprintln(stderr, "handover failed:", err)
		if res.StillLeader {
			fmt.Fprintln(stderr, "the current leader kept leadership")
		}
		return 1
	}
	return 0
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
