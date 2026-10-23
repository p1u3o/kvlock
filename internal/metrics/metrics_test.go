package metrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/example/kvrocks-leader/internal/metrics"
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
		"is_leader 1",
		"elections_total 2",
		`step_downs_total{reason="handover"} 1`,
		`step_downs_total{reason="lock_lost"} 0`,
		"consul_last_renew_age_seconds 2.5",
		"replication_lag_bytes -1",
		`fencing_events_total{result="ok",target="fence"} 0`,
		"kvrocks_up 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestRenewAgeDefaultsToInf(t *testing.T) {
	r := metrics.New()
	n, err := testutil.GatherAndCount(r.Gatherer(), "consul_last_renew_age_seconds")
	if err != nil || n != 1 {
		t.Fatalf("count=%d err=%v", n, err)
	}
}
