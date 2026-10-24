package loadtest

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/testkv"
	"github.com/redis/go-redis/v9"
)

func TestCheckTargets(t *testing.T) {
	ok := [][2][]string{
		{{"127.0.0.1:8379"}, {"127.0.0.1:8379"}},
		{{"10.0.0.1:8379", "10.0.0.2:8379"}, {":8379"}},
	}
	for _, c := range ok {
		if err := CheckTargets(c[0], c[1]); err != nil {
			t.Errorf("%v: %v", c, err)
		}
	}
	bad := [][2][]string{
		{{"127.0.0.1:8379"}, nil},
		{{"127.0.0.1:6379"}, {"127.0.0.1:6379"}},
		{{"127.0.0.1:8379"}, {":6379", ":8379"}},
		{{"127.0.0.1:8379"}, {"10.0.0.1:8379"}},
		{{"127.0.0.1:8379"}, {":8380"}},
		{{"nonsense"}, {":8379"}},
	}
	for _, c := range bad {
		if err := CheckTargets(c[0], c[1]); err == nil {
			t.Errorf("%v: want refusal", c)
		}
	}
}

func cfgFor(addrs ...string) Config {
	var allow []string
	for _, a := range addrs {
		allow = append(allow, a)
	}
	return Config{Addrs: addrs, Allow: allow, Duration: 3 * time.Second, Rate: 2000, Workers: 4, Keys: 2000,
		ValueMin: 32, ValueMax: 256, Pipeline: 4, ReadRatio: 0.5, OpTimeout: 500 * time.Millisecond, Cleanup: true}
}

func TestSteadyLoad(t *testing.T) {
	s := testkv.Start(t)
	rep, err := Run(context.Background(), cfgFor(s.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + Format(rep))
	if !rep.OK() || rep.Writes == 0 || rep.Reads == 0 || rep.WriteErrors != 0 {
		t.Fatalf("%+v", rep)
	}
	n, _ := s.Client().DBSize(context.Background()).Result()
	if n != 0 {
		t.Fatalf("cleanup left %d keys", n)
	}
}

// TestGracefulSwitch moves the master mid-run the way a handover does
// (fence old, let the replica catch up, promote) and expects
// a bounded write gap with nothing lost.
func TestGracefulSwitch(t *testing.T) {
	a, b := testkv.Start(t), testkv.Start(t)
	ctx := context.Background()
	ca, cb := a.Client(), b.Client()
	defer ca.Close()
	defer cb.Close()
	if err := cb.SlaveOf(ctx, "127.0.0.1", strconv.Itoa(a.Port)).Err(); err != nil {
		t.Fatal(err)
	}
	waitRole(t, cb, "slave", true)
	cfg := cfgFor(a.Addr(), b.Addr())
	cfg.Duration = 6 * time.Second
	cfg.Events = []Event{{At: 2 * time.Second, Name: "switch", Fn: func(ctx context.Context) error {
		if err := ca.SlaveOf(ctx, "127.0.0.1", "1").Err(); err != nil { // fence
			return err
		}
		time.Sleep(700 * time.Millisecond)        // replica catches up
		return cb.SlaveOf(ctx, "NO", "ONE").Err() // old master stays fenced
	}}}
	rep, err := Run(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + Format(rep))
	if !rep.OK() || len(rep.MasterChanges) == 0 || rep.MaxWriteGap < 500*time.Millisecond || rep.MaxWriteGap > 3*time.Second {
		t.Fatalf("unexpected: %+v", rep)
	}
}

// TestDetectsLostWrites wipes the data mid-run; the verifier must notice.
func TestDetectsLostWrites(t *testing.T) {
	s := testkv.Start(t)
	c := s.Client()
	defer c.Close()
	cfg := cfgFor(s.Addr())
	cfg.Keys = 20000
	cfg.Events = []Event{{At: 2500 * time.Millisecond, Name: "wipe", Fn: func(ctx context.Context) error { return c.FlushAll(ctx).Err() }}}
	rep, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.LostWrites == 0 || rep.OK() || !strings.Contains(Format(rep), "FAIL") {
		t.Fatalf("lost writes not detected: %+v", rep)
	}
}

func waitRole(t *testing.T, c *redis.Client, role string, linkUp bool) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s, _ := c.Info(context.Background(), "replication").Result()
		if strings.Contains(s, "role:"+role) && (!linkUp || strings.Contains(s, "master_link_status:up")) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("role %s not reached", role)
}
