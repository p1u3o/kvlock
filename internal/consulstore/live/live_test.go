package live

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/example/kvrocks-leader/internal/consulstore"
)

type recorded struct {
	method, path, query string
	body                []byte
}

// fakeConsul records requests and answers with canned responses.
type fakeConsul struct {
	mu      sync.Mutex
	reqs    []recorded
	handler func(w http.ResponseWriter, r *http.Request)
}

func newFake(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) (*Client, *fakeConsul) {
	t.Helper()
	f := &fakeConsul{handler: h}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.reqs = append(f.reqs, recorded{r.Method, r.URL.Path, r.URL.RawQuery, b})
		f.mu.Unlock()
		f.handler(w, r)
	}))
	t.Cleanup(srv.Close)
	c, err := New(strings.TrimPrefix(srv.URL, "http://"), "", "", false)
	if err != nil {
		t.Fatal(err)
	}
	return c, f
}

func TestCreateSessionHasNoChecksAndReleaseBehavior(t *testing.T) {
	c, f := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ID":"abc"}`))
	})
	id, err := c.Create(context.Background(), 10*time.Second, time.Second)
	if err != nil || id != "abc" {
		t.Fatalf("id=%q err=%v", id, err)
	}
	req := f.reqs[0]
	if req.method != "PUT" || req.path != "/v1/session/create" {
		t.Fatalf("unexpected request %s %s", req.method, req.path)
	}
	var body map[string]any
	if err := json.Unmarshal(req.body, &body); err != nil {
		t.Fatal(err)
	}
	nc, ok := body["NodeChecks"].([]any)
	if !ok || len(nc) != 0 {
		t.Fatalf("NodeChecks must be an explicit empty list, got %#v", body["NodeChecks"])
	}
	if _, ok := body["Checks"]; ok {
		t.Fatalf("legacy Checks must not be sent (would default to serfHealth): %#v", body)
	}
	if body["Behavior"] != "release" || body["TTL"] != "10s" {
		t.Fatalf("bad session body: %#v", body)
	}
}

func TestRenewNotFoundIsInvalid(t *testing.T) {
	c, _ := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	err := c.Renew(context.Background(), "abc")
	if !errors.Is(err, consulstore.ErrSessionInvalid) {
		t.Fatalf("expected ErrSessionInvalid, got %v", err)
	}
}

func TestRenewTransportErrorIsNotInvalid(t *testing.T) {
	c, _ := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	err := c.Renew(context.Background(), "abc")
	if err == nil || errors.Is(err, consulstore.ErrSessionInvalid) {
		t.Fatalf("expected a plain error, got %v", err)
	}
}

func TestValid(t *testing.T) {
	c, _ := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/gone") {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		_, _ = w.Write([]byte(`[{"ID":"live"}]`))
	})
	if ok, err := c.Valid(context.Background(), "live"); !ok || err != nil {
		t.Fatalf("live: ok=%v err=%v", ok, err)
	}
	if ok, err := c.Valid(context.Background(), "gone"); ok || err != nil {
		t.Fatalf("gone: ok=%v err=%v", ok, err)
	}
}

func TestReleaseUsesAtomicReleaseOp(t *testing.T) {
	c, f := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`true`))
	})
	if err := c.Release(context.Background(), "kvrocks/main/leader", "sess-1"); err != nil {
		t.Fatal(err)
	}
	if len(f.reqs) != 1 {
		t.Fatalf("expected a single request (no get-then-delete), got %d", len(f.reqs))
	}
	r := f.reqs[0]
	if r.method != "PUT" || r.path != "/v1/kv/kvrocks/main/leader" || !strings.Contains(r.query, "release=sess-1") {
		t.Fatalf("unexpected release request: %s %s?%s", r.method, r.path, r.query)
	}
}

func TestPrivateAddressRegistersTwin(t *testing.T) {
	var mu sync.Mutex
	var regs []map[string]any
	var dereg []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/v1/agent/service/register":
			var m map[string]any
			_ = json.NewDecoder(r.Body).Decode(&m)
			regs = append(regs, m)
		case strings.HasPrefix(r.URL.Path, "/v1/agent/service/deregister/"):
			dereg = append(dereg, strings.TrimPrefix(r.URL.Path, "/v1/agent/service/deregister/"))
		}
	}))
	defer srv.Close()
	c, err := New(strings.TrimPrefix(srv.URL, "http://"), "", "", false)
	if err != nil {
		t.Fatal(err)
	}
	c.SetPrivate("kvrocks-private", "100.64.0.1")
	if err := c.RegisterMaster(context.Background(), "kvrocks", "kv01", "203.0.113.1", 6666, []string{"master"}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.Deregister(context.Background(), "kvrocks-kv01"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(regs) != 2 || regs[0]["Address"] != "203.0.113.1" || regs[1]["Address"] != "100.64.0.1" || regs[1]["Name"] != "kvrocks-private" {
		t.Fatalf("unexpected registrations: %v", regs)
	}
	if len(dereg) != 2 {
		t.Fatalf("expected both entries deregistered, got %v", dereg)
	}
}
