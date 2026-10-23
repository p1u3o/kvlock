package metrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/p1u3o/kvlock/internal/metrics"
)

func TestExposition(t *testing.T) {
	r := metrics.New()
	r.IsLeader.Set(1)
	r.Elections.Inc()
	r.Elections.Inc()
	r.StepDowns.WithLabelValues(metrics.ReasonHandover).Inc()
	r.SetRenewAgeFunc(func() float64 { return 2.5 })

	srv := httptest.NewServer(r.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	out := string(b)
	for _, want := range []string{
		"kvlock_is_leader 1",
		"kvlock_elections_total 2",
		`kvlock_step_downs_total{reason="handover"} 1`,
		`kvlock_step_downs_total{reason="lock_lost"} 0`,
		"kvlock_consul_last_renew_age_seconds 2.5",
		"kvlock_replication_lag_bytes -1",
		`kvlock_fencing_events_total{result="ok",target="fence"} 0`,
		"kvlock_kvrocks_up 0",
		"kvlock_replication_lag_seconds -1",
		`kvlock_heartbeat_writes_total{result="ok"} 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestRenewAgeDefaultsToInf(t *testing.T) {
	r := metrics.New()
	n, err := testutil.GatherAndCount(r.Gatherer(), "kvlock_consul_last_renew_age_seconds")
	if err != nil || n != 1 {
		t.Fatalf("count=%d err=%v", n, err)
	}
}

func TestNoUnprefixedSidecarMetrics(t *testing.T) {
	mfs, err := metrics.New().Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		n := mf.GetName()
		if !strings.HasPrefix(n, "kvlock_") && !strings.HasPrefix(n, "go_") && !strings.HasPrefix(n, "process_") {
			t.Errorf("metric %q lacks the kvlock_ prefix", n)
		}
	}
}
