package metrics_test

import (
	"strings"
	"testing"

	"github.com/example/kvrocks-leader/internal/metrics"
)

func TestRender(t *testing.T) {
	r := metrics.New()
	r.SetGauge("is_leader", 1)
	r.Inc("elections_total")
	r.Inc("elections_total")
	out := r.Render()
	if !strings.Contains(out, "is_leader 1") {
		t.Fatalf("missing gauge:\n%s", out)
	}
	if !strings.Contains(out, "elections_total 2") {
		t.Fatalf("missing counter:\n%s", out)
	}
}
