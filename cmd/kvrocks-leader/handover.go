package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/p1u3o/kvlock/internal/httpapi"
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
	tokenFile := addTokenFlags(fs)
	if err := rejectTokenFlag(args); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	tok, err := adminToken(*tokenFile)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
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

// addTokenFlags registers --token-file and a --token flag that is refused:
// a token on the command line lands in shell history and process lists.
func addTokenFlags(fs *flag.FlagSet) *string {
	fs.Func("token", "removed: pass the admin token via $KVLOCK_ADMIN_TOKEN or --token-file", func(string) error {
		return errTokenFlag
	})
	return fs.String("token-file", os.Getenv("KVLOCK_ADMIN_TOKEN_FILE"), "file containing the admin token ($KVLOCK_ADMIN_TOKEN_FILE; $KVLOCK_ADMIN_TOKEN also works)")
}

var errTokenFlag = errors.New("--token is not accepted (it would land in shell history); use $KVLOCK_ADMIN_TOKEN or --token-file")

// rejectTokenFlag is checked before flag parsing so the flag package never
// echoes the value in its error message.
func rejectTokenFlag(args []string) error {
	for _, a := range args {
		if a == "--" {
			break
		}
		n := strings.TrimLeft(a, "-")
		if a != n && (n == "token" || strings.HasPrefix(n, "token=")) {
			return errTokenFlag
		}
	}
	return nil
}

// adminToken resolves the token from $KVLOCK_ADMIN_TOKEN or a file. The
// token is never printed.
func adminToken(file string) (string, error) {
	if t := os.Getenv("KVLOCK_ADMIN_TOKEN"); t != "" {
		return t, nil
	}
	if file == "" {
		return "", nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("reading token file: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
