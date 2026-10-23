package httpapi_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/p1u3o/kvlock/internal/httpapi"
	"github.com/p1u3o/kvlock/internal/metrics"
)

type stubHandover struct {
	calls int
	req   HandoverRequest
	res   HandoverResult
	err   error
}

func (s *stubHandover) Handover(_ context.Context, req HandoverRequest) (HandoverResult, error) {
	s.calls++
	s.req = req
	return s.res, s.err
}

func post(h http.Handler, remote, auth, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/handover", strings.NewReader(body))
	r.RemoteAddr = remote
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestHandoverLoopbackOnlyWithoutToken(t *testing.T) {
	s := &stubHandover{res: HandoverResult{Stage: "done"}}
	h := Handler(stub{reg: metrics.New()}, Admin{Handover: s})
	if w := post(h, "10.1.2.3:5555", "", ""); w.Code != http.StatusForbidden {
		t.Fatalf("remote without token: %d", w.Code)
	}
	if w := post(h, "127.0.0.1:5555", "", `{"to":"10.0.0.2:6666"}`); w.Code != http.StatusOK {
		t.Fatalf("loopback: %d %s", w.Code, w.Body)
	}
	if w := post(h, "[::1]:5555", "", ""); w.Code != http.StatusOK {
		t.Fatalf("ipv6 loopback: %d", w.Code)
	}
	if s.calls != 2 {
		t.Fatalf("calls=%d", s.calls)
	}
}

func TestHandoverTokenRequiredWhenConfigured(t *testing.T) {
	s := &stubHandover{}
	h := Handler(stub{reg: metrics.New()}, Admin{Token: "tok", Handover: s})
	for _, auth := range []string{"", "Bearer nope", "tok"} {
		if w := post(h, "127.0.0.1:1", auth, ""); w.Code != http.StatusForbidden {
			t.Fatalf("auth %q: %d", auth, w.Code)
		}
	}
	if w := post(h, "10.9.9.9:1", "Bearer tok", ""); w.Code != http.StatusOK {
		t.Fatalf("good token: %d", w.Code)
	}
	if s.calls != 1 {
		t.Fatalf("calls=%d", s.calls)
	}
}

func TestHandoverMethodAndBodyValidation(t *testing.T) {
	s := &stubHandover{}
	h := Handler(stub{reg: metrics.New()}, Admin{Handover: s})
	r := httptest.NewRequest(http.MethodGet, "/handover", nil)
	r.RemoteAddr = "127.0.0.1:1"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %d", w.Code)
	}
	if w := post(h, "127.0.0.1:1", "", "{"); w.Code != http.StatusBadRequest {
		t.Fatalf("bad json: %d", w.Code)
	}
	if w := post(h, "127.0.0.1:1", "", `{"timeout":"soon"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("bad timeout: %d", w.Code)
	}
	if s.calls != 0 {
		t.Fatal("invalid requests must not reach the controller")
	}
}

func TestHandoverStatusCodes(t *testing.T) {
	s := &stubHandover{res: HandoverResult{Stage: "waiting", StillLeader: true}, err: errors.New("lag")}
	h := Handler(stub{reg: metrics.New()}, Admin{Handover: s})
	if w := post(h, "127.0.0.1:1", "", ""); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), `"still_leader":true`) {
		t.Fatalf("abort: %d %s", w.Code, w.Body)
	}
	s.res = HandoverResult{Stage: "committed"}
	if w := post(h, "127.0.0.1:1", "", ""); w.Code != http.StatusAccepted {
		t.Fatalf("committed without successor: %d", w.Code)
	}
}

func TestNoHandoverRouteWithoutAdmin(t *testing.T) {
	h := Handler(stub{reg: metrics.New()})
	if w := post(h, "127.0.0.1:1", "", ""); w.Code != http.StatusNotFound {
		t.Fatalf("got %d", w.Code)
	}
}

func TestRequestHandoverClient(t *testing.T) {
	s := &stubHandover{res: HandoverResult{Stage: "done", NewLeader: "kv02"}}
	srv := httptest.NewServer(Handler(stub{reg: metrics.New()}, Admin{Token: "t", Handover: s}))
	defer srv.Close()
	lag := int64(5)
	res, err := RequestHandover(context.Background(), srv.URL+"/", "t", HandoverRequest{To: "a:1", MaxLag: &lag})
	if err != nil || !res.OK || res.NewLeader != "kv02" || s.req.To != "a:1" || *s.req.MaxLag != 5 {
		t.Fatalf("res=%+v err=%v req=%+v", res, err, s.req)
	}
	if _, err := RequestHandover(context.Background(), srv.URL, "wrong", HandoverRequest{}); err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("expected forbidden, got %v", err)
	}
}
