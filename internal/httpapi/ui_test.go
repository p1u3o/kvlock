package httpapi_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/p1u3o/kvlock/internal/elect"
	"github.com/p1u3o/kvlock/internal/httpapi"
	"github.com/p1u3o/kvlock/internal/metrics"
)

type richStub struct{ reg *metrics.Registry }

func (s richStub) Snapshot() httpapi.Snapshot {
	return httpapi.Snapshot{
		Role: elect.RoleFollower, Mode: "active", Cluster: "test", Node: "kv02", NodeAddr: "10.0.0.2:8379",
		Leader: "kv01", LeaderAddr: "10.0.0.1", Epoch: 7, LeaderAgeSeconds: 12.5,
		LagSeconds: 0.25, MaxLagSeconds: 2, LagGate: "time", LagBytes: 3, MaxLagSeq: 1000,
		MasterHost: "10.0.0.1", MasterPort: 8379, MasterLinkUp: true, LastRenewAgeSeconds: 0.4,
		FenceState: "replica (read-only)", Connections: map[string]int{"sentinel": 1},
		Events:  []httpapi.Event{{Time: "2026-10-23T19:00:00Z", Kind: "election", Message: "<script>alert(1)</script>"}},
		Healthy: true, KvrocksUp: true, ConsulOK: true,
	}
}
func (s richStub) Metrics() *metrics.Registry { return s.reg }

func get(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	r, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return r, string(b)
}

func TestStatusFields(t *testing.T) {
	srv := httptest.NewServer(httpapi.Handler(richStub{reg: metrics.New()}))
	defer srv.Close()
	_, body := get(t, srv.URL+"/status")
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{"cluster": "test", "node": "kv02", "role": "follower", "leader": "kv01",
		"lag_seconds": 0.25, "max_lag_seconds": 2.0, "lag_gate": "time", "master_link_up": true,
		"fence_state": "replica (read-only)", "epoch": 7.0, "leader_age_seconds": 12.5} {
		if m[k] != want {
			t.Errorf("%s = %v, want %v", k, m[k], want)
		}
	}
	ev := m["events"].([]any)[0].(map[string]any)
	if ev["message"] != "<script>alert(1)</script>" || ev["kind"] != "election" {
		t.Errorf("event %v", ev)
	}
	if m["connections"].(map[string]any)["sentinel"] != 1.0 {
		t.Errorf("connections %v", m["connections"])
	}
}

func TestUIServed(t *testing.T) {
	srv := httptest.NewServer(httpapi.Handler(richStub{reg: metrics.New()}))
	defer srv.Close()
	for _, p := range []string{"/", "/ui", "/ui/"} {
		r, body := get(t, srv.URL+p)
		if r.StatusCode != 200 || !strings.HasPrefix(r.Header.Get("Content-Type"), "text/html") {
			t.Fatalf("%s: %d %s", p, r.StatusCode, r.Header.Get("Content-Type"))
		}
		if !strings.Contains(r.Header.Get("Content-Security-Policy"), "default-src 'self'") {
			t.Fatalf("%s: CSP %q", p, r.Header.Get("Content-Security-Policy"))
		}
		if !strings.Contains(body, `<script src="/ui/app.js">`) || strings.Contains(body, "http://") || strings.Contains(body, "https://") {
			t.Fatalf("%s: page must be self-contained", p)
		}
	}
	for p, ct := range map[string]string{"/ui/app.js": "javascript", "/ui/style.css": "text/css"} {
		r, body := get(t, srv.URL+p)
		if r.StatusCode != 200 || !strings.Contains(r.Header.Get("Content-Type"), ct) || len(body) < 100 {
			t.Fatalf("%s: %d %s", p, r.StatusCode, r.Header.Get("Content-Type"))
		}
		if strings.Contains(body, "http://") || strings.Contains(body, "https://") {
			t.Fatalf("%s references an external URL", p)
		}
	}
	// Other paths are not the UI.
	if r, _ := get(t, srv.URL+"/nope"); r.StatusCode != 404 {
		t.Fatalf("/nope: %d", r.StatusCode)
	}
	if r, _ := http.Post(srv.URL+"/", "text/plain", nil); r.StatusCode != 405 {
		t.Fatalf("POST /: %d", r.StatusCode)
	}
}

// The page renders /status with textContent only.
func TestUIEscapesOutput(t *testing.T) {
	srv := httptest.NewServer(httpapi.Handler(richStub{reg: metrics.New()}))
	defer srv.Close()
	_, js := get(t, srv.URL+"/ui/app.js")
	for _, bad := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(js, bad) {
			t.Fatalf("app.js uses %s", bad)
		}
	}
	if !strings.Contains(js, `"Authorization": "Bearer "`) {
		t.Fatal("handover must send the token in the Authorization header")
	}
}
