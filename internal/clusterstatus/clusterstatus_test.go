package clusterstatus

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/backup"
	"github.com/p1u3o/kvlock/internal/httpapi"
)

func snap(node, role, backend string, epoch uint64, lag float64) *httpapi.Snapshot {
	return &httpapi.Snapshot{Node: node, Role: httpapiRole(role), BackendRole: backend, Epoch: epoch, LagSeconds: lag,
		MaxLagSeconds: 5, Healthy: true, KvrocksUp: true, SessionValid: true, Leader: "kv01", Mode: "active"}
}

func serve(t *testing.T, s *httpapi.Snapshot) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(s)
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func eval(nodes ...*httpapi.Snapshot) Report {
	var ns []Node
	for _, s := range nodes {
		ns = append(ns, Node{URL: "x", Status: s})
	}
	return Evaluate(ns, Options{})
}

func TestHealthyCluster(t *testing.T) {
	r := eval(snap("kv01", "leader", "master", 7, 0), snap("kv02", "follower", "slave", 7, 0.2))
	if !r.Healthy || r.Leader != "kv01" || r.Epoch != 7 {
		t.Fatalf("%+v", r)
	}
}

func TestNoLeader(t *testing.T) {
	r := eval(snap("kv01", "follower", "slave", 7, 0), snap("kv02", "follower", "slave", 7, 0))
	if r.Healthy || !strings.Contains(strings.Join(r.Problems, ";"), "no leader") {
		t.Fatalf("%+v", r)
	}
}

func TestSplitBrain(t *testing.T) {
	b := snap("kv02", "leader", "master", 8, 0)
	b.Leader = "kv02"
	r := eval(snap("kv01", "leader", "master", 7, 0), b)
	p := strings.Join(r.Problems, ";")
	if r.Healthy || !strings.Contains(p, "several nodes claim leadership") || !strings.Contains(p, "several writable masters") || !strings.Contains(p, "disagree") {
		t.Fatalf("%v", r.Problems)
	}
	// Two writable masters even with one leader is split brain.
	r = eval(snap("kv01", "leader", "master", 7, 0), snap("kv02", "follower", "master", 7, 0))
	if r.Healthy {
		t.Fatal("want unhealthy")
	}
}

func TestLag(t *testing.T) {
	r := eval(snap("kv01", "leader", "master", 7, 0), snap("kv02", "follower", "slave", 7, 9))
	if r.Healthy {
		t.Fatal("lag 9 > 5 should fail")
	}
	r = Evaluate(r.Nodes, Options{MaxLagSeconds: 10})
	if !r.Healthy {
		t.Fatalf("override 10s: %v", r.Problems)
	}
	r = eval(snap("kv01", "leader", "master", 7, 0), snap("kv02", "follower", "slave", 7, -1))
	if !r.Healthy || len(r.Warnings) != 1 {
		t.Fatalf("unknown lag should only warn: %+v", r)
	}
}

func TestFetchAndTable(t *testing.T) {
	a := snap("kv01", "leader", "master", 3, 0)
	a.Backup = &backup.Status{Enabled: true, LastSuccess: time.Now().Add(-time.Hour).Format(time.RFC3339), LastSuccessNode: "kv01"}
	urls := []string{serve(t, a), serve(t, snap("kv02", "follower", "slave", 3, 0.1)), "127.0.0.1:1"}
	nodes := Fetch(context.Background(), &http.Client{Timeout: time.Second}, urls)
	r := Evaluate(nodes, Options{})
	if r.Healthy || !strings.Contains(r.Problems[0], "unreachable") {
		t.Fatalf("unreachable node must fail: %v", r.Problems)
	}
	var b bytes.Buffer
	WriteTable(&b, r, time.Now())
	out := b.String()
	for _, want := range []string{"NODE", "kv01", "leader", "1h0m", "UNREACHABLE", "valid", "UNHEALTHY"} {
		if !strings.Contains(out, want) {
			t.Errorf("table missing %q:\n%s", want, out)
		}
	}
}

func TestNormalizeURL(t *testing.T) {
	for in, want := range map[string]string{"h:1": "http://h:1", "https://h:1/": "https://h:1", "h:1/status": "http://h:1"} {
		if got := NormalizeURL(in); got != want {
			t.Errorf("%s: %s", in, got)
		}
	}
}
