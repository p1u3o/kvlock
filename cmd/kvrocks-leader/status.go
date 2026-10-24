package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	consul "github.com/hashicorp/consul/api"

	"github.com/p1u3o/kvlock/internal/clusterstatus"
)

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			*m = append(*m, p)
		}
	}
	return nil
}

// runStatus implements `kvrocks-leader status`. Exit 0 healthy, 1
// unhealthy, 2 usage or discovery error.
func runStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var addrs multiFlag
	fs.Var(&addrs, "addr", "sidecar HTTP address (host:port or URL); repeatable or comma-separated ($KVLOCK_STATUS_ADDRS)")
	useConsul := fs.Bool("consul", false, "discover sidecars from the Consul catalog (uses $CONSUL_HTTP_ADDR / $CONSUL_HTTP_TOKEN)")
	service := fs.String("service", envOr("SERVICE_NAME", "kvrocks"), "Consul service registered by the sidecars ($SERVICE_NAME)")
	httpPort := fs.Int("http-port", 8080, "sidecar HTTP port on discovered nodes")
	asJSON := fs.Bool("json", false, "print the report as JSON")
	maxLag := fs.Duration("max-lag", 0, "lag limit; default each node's --max-lag-time")
	timeout := fs.Duration("timeout", 5*time.Second, "per-request timeout")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if len(addrs) == 0 {
		_ = addrs.Set(envOr("KVLOCK_STATUS_ADDRS", ""))
	}
	if *useConsul {
		found, err := discover(*service, *httpPort)
		if err != nil {
			fmt.Fprintln(stderr, "consul discovery:", err)
			return 2
		}
		addrs = append(addrs, found...)
	}
	if len(addrs) == 0 {
		fmt.Fprintln(stderr, "no nodes: pass --addr (repeatable) or --consul")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout+time.Second)
	defer cancel()
	nodes := clusterstatus.Fetch(ctx, &http.Client{Timeout: *timeout}, dedupe(addrs))
	rep := clusterstatus.Evaluate(nodes, clusterstatus.Options{MaxLagSeconds: maxLag.Seconds()})
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	} else {
		clusterstatus.WriteTable(stdout, rep, time.Now())
	}
	if !rep.Healthy {
		return 1
	}
	return 0
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, a := range in {
		n := clusterstatus.NormalizeURL(a)
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// discover lists every catalog instance of service (healthy or not: a
// broken node is exactly what status should show).
func discover(service string, port int) ([]string, error) {
	c, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		return nil, err
	}
	entries, _, err := c.Catalog().Service(service, "", nil)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		addr := e.ServiceAddress
		if addr == "" {
			addr = e.Address
		}
		out = append(out, "http://"+net.JoinHostPort(addr, strconv.Itoa(port)))
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no instances of %q", service)
	}
	return out, nil
}
