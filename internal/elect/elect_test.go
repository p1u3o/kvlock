package elect_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/example/kvrocks-leader/internal/elect"
)

func TestShouldPromoteWithinLag(t *testing.T) {
	dec := elect.ShouldPromote(100, 1000, time.Now(), 30*time.Second, time.Now(), true)
	if !dec.Allow {
		t.Fatalf("expected allow, got %+v", dec)
	}
}

func TestShouldPromoteRefusesFarBehind(t *testing.T) {
	dec := elect.ShouldPromote(10_000_000, 1000, time.Now(), 30*time.Second, time.Now(), true)
	if dec.Allow {
		t.Fatalf("expected refuse, got %+v", dec)
	}
}

func TestShouldPromoteUnknownLagHoldDown(t *testing.T) {
	now := time.Now()
	// Recently seen -> refuse.
	dec := elect.ShouldPromote(-1, 1000, now.Add(-time.Second), 30*time.Second, now, true)
	if dec.Allow {
		t.Fatalf("expected refuse while master recently seen, got %+v", dec)
	}
	// Beyond hold-down -> allow with loud log (caller logs).
	dec = elect.ShouldPromote(-1, 1000, now.Add(-time.Hour), 30*time.Second, now, true)
	if !dec.Allow {
		t.Fatalf("expected allow after hold-down, got %+v", dec)
	}
	// No old master -> allow.
	dec = elect.ShouldPromote(-1, 1000, time.Time{}, 30*time.Second, now, false)
	if !dec.Allow {
		t.Fatalf("expected allow with no old master, got %+v", dec)
	}
}

func TestNextEpoch(t *testing.T) {
	if e := elect.NextEpoch(nil); e != 1 {
		t.Fatalf("want 1 got %d", e)
	}
	if e := elect.NextEpoch([]byte("garbage")); e != 1 {
		t.Fatalf("want 1 got %d", e)
	}
	v := elect.MarshalLeader("kv01", "10.0.0.1", 6666, "sess", 4, time.Now())
	if e := elect.NextEpoch(v); e != 5 {
		t.Fatalf("want 5 got %d", e)
	}
}

func TestMarshalParseRoundTrip(t *testing.T) {
	v := elect.MarshalLeader("kv01", "10.0.0.1", 6666, "s1", 7, time.Now())
	lv := elect.ParseLeader(v)
	if lv == nil || lv.Node != "kv01" || lv.Epoch != 7 || lv.Port != 6666 {
		t.Fatalf("bad round trip: %+v", lv)
	}
	if !elect.IsSelfLeader(v, "kv01") || elect.IsSelfLeader(v, "kv02") {
		t.Fatal("IsSelfLeader wrong")
	}
}

func TestBackoffBounds(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for a := 0; a < 8; a++ {
		d := elect.Backoff(a, time.Second, 15*time.Second, rng)
		if d < 0 || d > 15*time.Second {
			t.Fatalf("attempt %d out of bounds: %v", a, d)
		}
	}
}
