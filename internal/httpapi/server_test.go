package httpapi_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/example/kvrocks-leader/internal/elect"
	"github.com/example/kvrocks-leader/internal/httpapi"
	"github.com/example/kvrocks-leader/internal/metrics"
)

type stub struct{ reg *metrics.Registry }

func (s stub) Snapshot() httpapi.Snapshot {
	return httpapi.Snapshot{Role: elect.RoleLeader, Leader: "kv01", Epoch: 3, LagBytes: 0, KvrocksUp: true, Mode: "active"}
}
func (s stub) Metrics() *metrics.Registry { return s.reg }

func TestEndpoints(t *testing.T) {
	p := stub{reg: metrics.New()}
	p.reg.SetGauge("is_leader", 1)
	srv := httptest.NewServer(httpapi.Handler(p))
	defer srv.Close()

	for _, path := range []string{"/healthz", "/status", "/metrics"} {
		resp, err := srv.Client().Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s -> %d", path, resp.StatusCode)
		}
	}
	resp, _ := srv.Client().Get(srv.URL + "/status")
	defer resp.Body.Close()
	var snap httpapi.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if snap.Role != elect.RoleLeader || snap.Epoch != 3 {
		t.Fatalf("bad snapshot: %+v", snap)
	}
}
