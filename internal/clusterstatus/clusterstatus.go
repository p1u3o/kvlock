// Package clusterstatus gathers /status from several sidecars and judges
// the cluster as a whole (used by `kvrocks-leader status`).
package clusterstatus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/p1u3o/kvlock/internal/httpapi"
)

// Node is one sidecar's answer.
type Node struct {
	URL    string            `json:"url"`
	Error  string            `json:"error,omitempty"`
	Status *httpapi.Snapshot `json:"status,omitempty"`
}

// Report is the cluster verdict.
type Report struct {
	Nodes    []Node   `json:"nodes"`
	Leader   string   `json:"leader,omitempty"`
	Epoch    uint64   `json:"epoch"`
	Healthy  bool     `json:"healthy"`
	Problems []string `json:"problems,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// NormalizeURL turns "host:port" into "http://host:port".
func NormalizeURL(a string) string {
	a = strings.TrimRight(strings.TrimSpace(a), "/")
	if !strings.Contains(a, "://") {
		a = "http://" + a
	}
	return strings.TrimSuffix(a, "/status")
}

// Fetch reads /status from every URL in parallel.
func Fetch(ctx context.Context, client *http.Client, urls []string) []Node {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	out := make([]Node, len(urls))
	var wg sync.WaitGroup
	for i, u := range urls {
		wg.Add(1)
		go func(i int, u string) {
			defer wg.Done()
			u = NormalizeURL(u)
			out[i] = Node{URL: u}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u+"/status", nil)
			if err != nil {
				out[i].Error = err.Error()
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				out[i].Error = err.Error()
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			if resp.StatusCode != http.StatusOK {
				out[i].Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
				return
			}
			var s httpapi.Snapshot
			if err := json.Unmarshal(b, &s); err != nil {
				out[i].Error = "bad status JSON: " + err.Error()
				return
			}
			out[i].Status = &s
		}(i, u)
	}
	wg.Wait()
	return out
}

// Options tune Evaluate.
type Options struct {
	// MaxLagSeconds overrides each node's own max_lag_seconds when > 0.
	MaxLagSeconds float64
}

func name(n Node) string {
	if n.Status != nil && n.Status.Node != "" {
		return n.Status.Node
	}
	return n.URL
}

// Evaluate judges the cluster. Unhealthy means: an unreachable node, no
// leader, more than one leader or writable master (split brain), nodes
// disagreeing on the leader, a replica over the lag limit, or a node that
// reports itself unhealthy.
func Evaluate(nodes []Node, opt Options) Report {
	r := Report{Nodes: nodes}
	var leaders, masters []string
	seen := map[string]bool{}
	for _, n := range nodes {
		if n.Status == nil {
			r.Problems = append(r.Problems, fmt.Sprintf("%s unreachable: %s", n.URL, n.Error))
			continue
		}
		s := n.Status
		if s.Role == "leader" {
			leaders = append(leaders, name(n))
			if s.Epoch > r.Epoch {
				r.Epoch = s.Epoch
			}
		}
		if s.KvrocksUp && s.BackendRole == "master" {
			masters = append(masters, name(n))
		}
		if s.Leader != "" {
			seen[s.Leader] = true
		}
		if !s.Healthy {
			r.Problems = append(r.Problems, fmt.Sprintf("%s unhealthy: %s", name(n), strings.Join(s.Problems, "; ")))
		}
		if s.Role != "leader" && s.Role != "" {
			max := s.MaxLagSeconds
			if opt.MaxLagSeconds > 0 {
				max = opt.MaxLagSeconds
			}
			switch {
			case s.LagSeconds < 0:
				r.Warnings = append(r.Warnings, fmt.Sprintf("%s lag unknown", name(n)))
			case max > 0 && s.LagSeconds > max:
				r.Problems = append(r.Problems, fmt.Sprintf("%s lag %.3fs over limit %.3fs", name(n), s.LagSeconds, max))
			}
		}
		if s.FencePending {
			r.Problems = append(r.Problems, fmt.Sprintf("%s fence pending", name(n)))
		}
	}
	switch {
	case len(leaders) == 0:
		r.Problems = append(r.Problems, "no leader")
	case len(leaders) > 1:
		r.Problems = append(r.Problems, "split brain: several nodes claim leadership: "+strings.Join(leaders, ", "))
	default:
		r.Leader = leaders[0]
	}
	if len(masters) > 1 {
		r.Problems = append(r.Problems, "split brain: several writable masters: "+strings.Join(masters, ", "))
	}
	if len(seen) > 1 {
		var ls []string
		for l := range seen {
			ls = append(ls, l)
		}
		sort.Strings(ls)
		r.Problems = append(r.Problems, "nodes disagree on the leader: "+strings.Join(ls, ", "))
	}
	r.Healthy = len(r.Problems) == 0
	return r
}

func ago(ts string, now time.Time) string {
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return ts
	}
	return now.Sub(t).Round(time.Second).String() + " ago"
}

// WriteTable prints the human table.
func WriteTable(w io.Writer, r Report, now time.Time) {
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tROLE\tEPOCH\tHEALTH\tLAG\tLAST BACKUP\tSESSION")
	for _, n := range r.Nodes {
		if n.Status == nil {
			fmt.Fprintf(tw, "%s\t-\t-\tUNREACHABLE\t-\t-\t-\n", n.URL)
			continue
		}
		s := n.Status
		role := string(s.Role)
		if s.Mode == "observe" {
			role += " (observe)"
		}
		health := "ok"
		if !s.Healthy {
			health = "UNHEALTHY"
		}
		lag := "-"
		switch {
		case s.Role == "leader":
			lag = "0"
		case s.LagSeconds >= 0:
			lag = fmt.Sprintf("%.3fs", s.LagSeconds)
		default:
			lag = "unknown"
		}
		if s.Role != "leader" && s.LagBytes >= 0 {
			lag += fmt.Sprintf(" (%d seq)", s.LagBytes)
		}
		bk := "-"
		if s.Backup != nil && s.Backup.Enabled {
			bk = "never"
			if s.Backup.LastSuccess != "" {
				bk = ago(s.Backup.LastSuccess, now)
				if s.Backup.LastSuccessNode != "" && s.Backup.LastSuccessNode != s.Node {
					bk += " (" + s.Backup.LastSuccessNode + ")"
				}
			}
			if s.Backup.LastError != "" {
				bk += " ERR"
			}
		}
		sess := "invalid"
		if s.SessionValid {
			sess = "valid"
		}
		if s.LastRenewAgeSeconds >= 0 {
			sess += fmt.Sprintf(" (renew %.1fs)", s.LastRenewAgeSeconds)
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\t%s\n", name(n), role, s.Epoch, health, lag, bk, sess)
	}
	tw.Flush()
	writeForwarders(w, r)
	if r.Healthy {
		fmt.Fprintf(w, "\ncluster healthy: leader %s, epoch %d\n", r.Leader, r.Epoch)
	} else {
		fmt.Fprintln(w, "\ncluster UNHEALTHY:")
		for _, p := range r.Problems {
			fmt.Fprintln(w, "  - "+p)
		}
	}
	for _, x := range r.Warnings {
		fmt.Fprintln(w, "  warning: "+x)
	}
}

// writeForwarders lists each node's replication forwarder, if enabled.
func writeForwarders(w io.Writer, r Report) {
	first := true
	for _, n := range r.Nodes {
		if n.Status == nil || n.Status.Forwarder == nil {
			continue
		}
		if first {
			fmt.Fprintln(w, "\nforwarders:")
			first = false
		}
		f := n.Status.Forwarder
		target := f.Target
		if target == "" {
			target = "none (" + f.Reason + ")"
		}
		fmt.Fprintf(w, "  %s %s -> %s  active %d  total %d  resets %d  dropped %d  refused %d  bytes up/down %d/%d\n",
			name(n), f.Listen, target, f.Active, f.Total, f.Resets, f.Dropped, f.Refused, f.BytesUp, f.BytesDown)
	}
	for _, n := range r.Nodes {
		if n.Status == nil || len(n.Status.Peers) == 0 {
			continue
		}
		fmt.Fprintf(w, "\npeer addresses on %s:\n", name(n))
		for _, p := range n.Status.Peers {
			target := p.Target
			if target == "" {
				target = "none (" + p.Reason + ")"
			}
			fmt.Fprintf(w, "  [%d] %s -> %s  active %d\n", p.Index, p.Listen, target, p.Active)
		}
	}
}
