package elect

import (
	"testing"
	"time"
)

func TestHeartbeatRoundTrip(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_123)
	hb, ok := ParseHeartbeat(FormatHeartbeat(now, 7))
	if !ok || hb.UnixMs != 1_700_000_000_123 || hb.Epoch != 7 {
		t.Fatalf("got %+v %v", hb, ok)
	}
	for _, bad := range []string{"", "x", "12", "a:1", "1:b", "-5:1", "0:1"} {
		if _, ok := ParseHeartbeat(bad); ok {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestHeartbeatLag(t *testing.T) {
	p := Heartbeat{UnixMs: 10_000}
	if _, ok := HeartbeatLag(p, p, false, true); ok {
		t.Fatal("no peer sample must be unknown")
	}
	if d, ok := HeartbeatLag(p, Heartbeat{}, true, false); !ok || d != MissingLocalHeartbeat {
		t.Fatalf("missing local: %v %v", d, ok)
	}
	if d, _ := HeartbeatLag(p, Heartbeat{UnixMs: 8_500}, true, true); d != 1500*time.Millisecond {
		t.Fatalf("lag %v", d)
	}
	if d, _ := HeartbeatLag(p, Heartbeat{UnixMs: 11_000}, true, true); d != 0 {
		t.Fatalf("local ahead must clamp to 0, got %v", d)
	}
}

// The time gate reuses LagGate in milliseconds.
func TestLagGateTimeUnits(t *testing.T) {
	now := time.Now()
	base := LagGateInput{MaxLag: 2000, SampleAge: 0, MaxSampleAge: time.Second, LastSeen: now, HaveOld: true, HoldDown: time.Hour, MaxWait: time.Minute, Now: now}
	in := base
	in.Lag = 1500
	if d := LagGate(in); !d.Allow {
		t.Fatalf("1.5s within 2s must allow: %s", d.Reason)
	}
	in.Lag = 4000
	if d := LagGate(in); d.Allow {
		t.Fatal("4s beyond 2s must refuse")
	}
	in.Now = now.Add(2 * time.Minute)
	if d := LagGate(in); !d.Allow {
		t.Fatal("max wait must eventually allow")
	}
}
