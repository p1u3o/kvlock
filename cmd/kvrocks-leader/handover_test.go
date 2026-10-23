package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/p1u3o/kvlock/internal/httpapi"
)

func TestHandoverCommandSendsRequest(t *testing.T) {
	var got httpapi.HandoverRequest
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&got)
		_ = json.NewEncoder(w).Encode(httpapi.HandoverResult{OK: true, Stage: "done", NewLeader: "kv02"})
	}))
	defer srv.Close()
	tf := filepath.Join(t.TempDir(), "tok")
	_ = os.WriteFile(tf, []byte("s3cret\n"), 0o600)
	t.Setenv("KVLOCK_ADMIN_TOKEN", "")
	var out, errb bytes.Buffer
	code := runHandover([]string{"--addr", srv.URL, "--to", "10.0.0.2:6666", "--max-lag", "0", "--timeout", "5s", "--token-file", tf}, &out, &errb)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errb.String())
	}
	if got.To != "10.0.0.2:6666" || got.MaxLag == nil || *got.MaxLag != 0 || got.Timeout != "5s" || auth != "Bearer s3cret" {
		t.Fatalf("bad request %+v auth=%q", got, auth)
	}
}

func TestHandoverCommandReportsAbort(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(httpapi.HandoverResult{Stage: "waiting", StillLeader: true, Error: "timed out"})
	}))
	defer srv.Close()
	var out, errb bytes.Buffer
	if code := runHandover([]string{"--addr", srv.URL}, &out, &errb); code != 1 {
		t.Fatalf("exit %d", code)
	}
	if !bytes.Contains(errb.Bytes(), []byte("kept leadership")) {
		t.Fatalf("stderr: %s", errb.String())
	}
}
