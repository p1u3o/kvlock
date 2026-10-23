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

func TestClassify(t *testing.T) {
	cases := []struct {
		holder, mine string
		want         elect.LockState
	}{
		{"", "s1", elect.LockVacant},
		{"", "", elect.LockVacant},
		{"s1", "s1", elect.LockMine},
		{"s2", "s1", elect.LockOther},
		{"s2", "", elect.LockOther},
	}
	for _, tc := range cases {
		if got := elect.Classify(tc.holder, tc.mine); got != tc.want {
			t.Errorf("Classify(%q,%q)=%v want %v", tc.holder, tc.mine, got, tc.want)
		}
	}
}

func TestConsulOutageExceeded(t *testing.T) {
	now := time.Now()
	if elect.ConsulOutageExceeded(now.Add(-3*time.Second), now, 10*time.Second) {
		t.Fatal("3s into a 10s TTL must be tolerated")
	}
	if !elect.ConsulOutageExceeded(now.Add(-10*time.Second), now, 10*time.Second) {
		t.Fatal("a full TTL without renew must step down")
	}
	if !elect.ConsulOutageExceeded(time.Time{}, now, 10*time.Second) {
		t.Fatal("never renewed must step down")
	}
}
