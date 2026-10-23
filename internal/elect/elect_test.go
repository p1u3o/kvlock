package elect_test

import (
	"github.com/p1u3o/kvlock/internal/consulstore"
	"github.com/p1u3o/kvlock/internal/kvrocks"
	"math/rand"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/elect"
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
	v := elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "sess", 4, time.Now())
	if e := elect.NextEpoch(v); e != 5 {
		t.Fatalf("want 5 got %d", e)
	}
}

func TestMarshalParseRoundTrip(t *testing.T) {
	v := elect.MarshalLeader("kv01", "10.0.0.1", "", 6666, "s1", 7, time.Now())
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

func TestComputeLag(t *testing.T) {
	if got := elect.ComputeLag(100, true, 40); got != 60 {
		t.Fatalf("got %d", got)
	}
	if got := elect.ComputeLag(100, true, 140); got != 0 {
		t.Fatalf("ahead should clamp to 0, got %d", got)
	}
	if got := elect.ComputeLag(100, false, 40); got != -1 {
		t.Fatalf("no sample should be unknown, got %d", got)
	}
	if got := elect.ComputeLag(100, true, -1); got != -1 {
		t.Fatalf("unknown local should be unknown, got %d", got)
	}
}

func TestWithinMinLeaderTime(t *testing.T) {
	now := time.Now()
	if !elect.WithinMinLeaderTime(now.Add(-time.Second), now, 5*time.Second) {
		t.Fatal("1s into 5s window should be within")
	}
	if elect.WithinMinLeaderTime(now.Add(-6*time.Second), now, 5*time.Second) {
		t.Fatal("past window")
	}
	if elect.WithinMinLeaderTime(now, now, 0) {
		t.Fatal("0 disables")
	}
	if elect.WithinMinLeaderTime(time.Time{}, now, 5*time.Second) {
		t.Fatal("not leader")
	}
}

func TestHealthProblems(t *testing.T) {
	if p := elect.HealthProblems(elect.RoleLeader, "master", true, true, false); len(p) != 0 {
		t.Fatalf("healthy leader: %v", p)
	}
	if p := elect.HealthProblems(elect.RoleFollower, "slave", true, true, false); len(p) != 0 {
		t.Fatalf("healthy follower: %v", p)
	}
	if p := elect.HealthProblems(elect.RoleFollower, "master", true, true, false); len(p) != 1 {
		t.Fatalf("follower on a master must be inconsistent: %v", p)
	}
	if p := elect.HealthProblems(elect.RoleLeader, "slave", true, true, false); len(p) != 1 {
		t.Fatalf("leader on a replica must be inconsistent: %v", p)
	}
	if p := elect.HealthProblems(elect.RoleLeader, "", false, false, false); len(p) != 2 {
		t.Fatalf("expected backend+consul problems: %v", p)
	}
	if p := elect.HealthProblems(elect.RoleFollower, "master", true, true, true); len(p) != 0 {
		t.Fatalf("observe mode skips role check: %v", p)
	}
}

func TestHandoverActive(t *testing.T) {
	now := time.Now()
	lv := &consulstore.LeaderValue{HandoverTo: "10.0.0.2:6666", HandoverUntil: now.Add(time.Second).Format(time.RFC3339Nano)}
	if !elect.HandoverActive(lv, now) {
		t.Fatal("should be active")
	}
	if elect.HandoverActive(lv, now.Add(2*time.Second)) || elect.HandoverActive(nil, now) || elect.HandoverActive(&consulstore.LeaderValue{HandoverTo: "x:1", HandoverUntil: "garbage"}, now) {
		t.Fatal("expired/nil/bad hints are inactive")
	}
}

func TestIsHandoverTarget(t *testing.T) {
	addrs := []string{"", "10.0.0.2", "192.168.1.2"}
	if !elect.IsHandoverTarget("192.168.1.2:6666", addrs, 6666) {
		t.Fatal("should match")
	}
	for _, tgt := range []string{"10.0.0.2:6667", "10.0.0.3:6666", "junk", ":6666"} {
		if elect.IsHandoverTarget(tgt, addrs, 6666) {
			t.Fatalf("%q must not match", tgt)
		}
	}
}

func TestPickHandoverTarget(t *testing.T) {
	ri := &kvrocks.ReplicationInfo{Role: "master", MasterOffset: 1000, Replicas: []kvrocks.ReplicaState{
		{IP: "10.0.0.2", Port: 6666, Offset: 900, State: "online"},
		{IP: "10.0.0.3", Port: 6666, Offset: 990},
		{IP: "10.0.0.4", Port: 6666, Offset: 1000, State: "wait_bgsave"},
	}}
	c, ok := elect.PickHandoverTarget(ri, "")
	if !ok || c.Addr != "10.0.0.3:6666" || c.Lag != 10 {
		t.Fatalf("got %+v %v", c, ok)
	}
	c, ok = elect.PickHandoverTarget(ri, "10.0.0.2:6666")
	if !ok || c.Lag != 100 {
		t.Fatalf("got %+v %v", c, ok)
	}
	if _, ok := elect.PickHandoverTarget(ri, "10.0.0.4:6666"); ok {
		t.Fatal("replica not online must be skipped")
	}
	if _, ok := elect.PickHandoverTarget(&kvrocks.ReplicationInfo{Role: "slave"}, ""); ok {
		t.Fatal("not a master")
	}
}
