package elect_test

import (
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/elect"
)

func TestLagGate(t *testing.T) {
	now := time.Now()
	base := elect.LagGateInput{MaxLag: 100, MaxSampleAge: 3 * time.Second, HaveOld: true, HoldDown: 30 * time.Second, Now: now}
	for _, tc := range []struct {
		name  string
		lag   int64
		age   time.Duration
		gone  time.Duration
		wait  time.Duration
		allow bool
	}{
		{"fresh sample within bound", 10, time.Second, time.Second, 0, true},
		{"fresh sample over bound", 500, time.Second, time.Second, 0, false},
		{"stale sample within bound waits out hold-down", 10, 10 * time.Second, time.Second, 0, false},
		{"stale sample within bound after hold-down", 10, 10 * time.Second, 31 * time.Second, 0, true},
		{"unknown sample age counts as stale", 10, -1, time.Second, 0, false},
		{"stale sample over bound stays refused after hold-down", 500, 10 * time.Second, 31 * time.Second, 0, false},
		{"over bound before max wait", 500, time.Second, 59 * time.Second, time.Minute, false},
		{"over bound after max wait", 500, time.Second, 61 * time.Second, time.Minute, true},
		{"max wait disabled", 500, time.Second, time.Hour, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := base
			in.Lag, in.SampleAge, in.LastSeen, in.MaxWait = tc.lag, tc.age, now.Add(-tc.gone), tc.wait
			if d := elect.LagGate(in); d.Allow != tc.allow {
				t.Fatalf("allow=%v want %v (%s)", d.Allow, tc.allow, d.Reason)
			}
		})
	}
	// Staleness check off keeps the old behaviour.
	in := base
	in.MaxSampleAge, in.Lag, in.SampleAge, in.LastSeen = 0, 10, time.Hour, now
	if !elect.LagGate(in).Allow {
		t.Fatal("staleness disabled must allow a lag within bound")
	}
}

func TestLagGateByteCap(t *testing.T) {
	now := time.Now()
	base := elect.LagGateInput{MaxLag: 100, MaxSampleAge: 0, LastSeen: now.Add(-time.Hour), HaveOld: true, HoldDown: time.Second, Now: now, MaxBytes: 4 << 20}
	// Over the cap: refused even with MaxWait long exceeded.
	in := base
	in.Lag, in.ByteLag, in.MaxWait = 5<<20, 5<<20, time.Second
	if d := elect.LagGate(in); d.Allow {
		t.Fatalf("over byte cap must refuse despite max wait: %s", d.Reason)
	}
	// At the cap with lag within --max-lag: allowed.
	in = base
	in.Lag, in.ByteLag = 50, 4<<20
	in.MaxLag = 5 << 20
	if d := elect.LagGate(in); !d.Allow {
		t.Fatalf("at cap must allow: %s", d.Reason)
	}
	// Under the cap but over --max-lag, MaxWait 0: never forced.
	in = base
	in.Lag, in.ByteLag = 1000, 1000
	if d := elect.LagGate(in); d.Allow {
		t.Fatal("max wait 0 must never force")
	}
	// Same with an opt-in MaxWait: forced, flagged by reason.
	in.MaxWait = time.Minute
	if d := elect.LagGate(in); !d.Allow || d.Reason != elect.ReasonMaxWait {
		t.Fatalf("opt-in max wait: %+v", d)
	}
	// Cap 0 = off.
	in = base
	in.MaxBytes, in.Lag, in.ByteLag = 0, 10, 1<<40
	in.MaxLag = 1 << 41
	if !elect.LagGate(in).Allow {
		t.Fatal("cap 0 must be off")
	}
}
