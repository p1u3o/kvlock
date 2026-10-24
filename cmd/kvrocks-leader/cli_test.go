package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/p1u3o/kvlock/internal/httpapi"
)

func statusServer(t *testing.T, s httpapi.Snapshot) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(s) }))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestStatusExitCodes(t *testing.T) {
	lead := statusServer(t, httpapi.Snapshot{Node: "a", Role: "leader", BackendRole: "master", KvrocksUp: true, Healthy: true, Leader: "a", Epoch: 2})
	fol := statusServer(t, httpapi.Snapshot{Node: "b", Role: "follower", BackendRole: "slave", KvrocksUp: true, Healthy: true, Leader: "a", Epoch: 2, LagSeconds: 0.1, MaxLagSeconds: 5})
	var out, errb bytes.Buffer
	if c := runStatus([]string{"--addr", lead, "--addr", fol}, &out, &errb); c != 0 {
		t.Fatalf("healthy: exit %d\n%s%s", c, out.String(), errb.String())
	}
	out.Reset()
	if c := runStatus([]string{"--addr", fol, "--json"}, &out, &errb); c != 1 {
		t.Fatalf("no leader: exit %d", c)
	}
	var rep map[string]any
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil || rep["healthy"] != false {
		t.Fatalf("json: %v %s", err, out.String())
	}
	t.Setenv("KVLOCK_STATUS_ADDRS", "")
	if c := runStatus(nil, &out, &errb); c != 2 {
		t.Fatalf("no addrs: exit %d", c)
	}
}

func TestTokenFlagRefused(t *testing.T) {
	var out, errb bytes.Buffer
	for _, a := range [][]string{{"--token", "secret123"}, {"-token=secret123"}} {
		for _, run := range []func([]string, io.Writer, io.Writer) int{runHandover, runLoadtest} {
			out.Reset()
			errb.Reset()
			if c := run(a, &out, &errb); c != 2 {
				t.Fatalf("%v: exit %d", a, c)
			}
			if strings.Contains(errb.String()+out.String(), "secret123") {
				t.Fatalf("%v: token echoed", a)
			}
		}
	}
}

func TestLoadtestRefusesUnsafeTargets(t *testing.T) {
	cases := [][]string{
		{"--addr", "127.0.0.1:8379"},
		{"--addr", "127.0.0.1:6379", "--allow", "127.0.0.1:6379"},
		{"--addr", "10.0.0.1:8379", "--allow", "10.0.0.2:8379"},
	}
	for _, a := range cases {
		var out, errb bytes.Buffer
		if c := runLoadtest(append(a, "--duration", "1s"), &out, &errb); c != 2 {
			t.Errorf("%v: exit %d (%s)", a, c, errb.String())
		}
	}
}
