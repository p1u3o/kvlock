package config_test

import (
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/config"
)

func TestDefaults(t *testing.T) {
	t.Setenv("CONSUL_HTTP_ADDR", "")
	t.Setenv("MODE", "")
	c, err := config.Parse([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if c.SessionTTL != 10*time.Second {
		t.Fatalf("session ttl=%v", c.SessionTTL)
	}
	if c.LockDelay != 1*time.Second {
		t.Fatalf("lock delay=%v", c.LockDelay)
	}
	if c.LeaderKey() != "kvrocks/main/leader" {
		t.Fatalf("key=%q", c.LeaderKey())
	}
}

func TestObserveKeyPrefix(t *testing.T) {
	c, err := config.Parse([]string{"--mode=observe", "--cluster=test"})
	if err != nil {
		t.Fatal(err)
	}
	if c.LeaderKey() != "kvrocks/test/observe/leader" {
		t.Fatalf("key=%q", c.LeaderKey())
	}
}

func TestAdvertiseDefaults(t *testing.T) {
	c, err := config.Parse([]string{"--kvrocks-addr=10.1.2.3", "--kvrocks-port=7777"})
	if err != nil {
		t.Fatal(err)
	}
	if c.AdvertiseAddr != "10.1.2.3" || c.AdvertisePort != 7777 {
		t.Fatalf("advertise=%v:%v", c.AdvertiseAddr, c.AdvertisePort)
	}
}

func TestKVKind(t *testing.T) {
	c, err := config.Parse([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if c.KVKind != "kvrocks" {
		t.Fatalf("default kind=%q", c.KVKind)
	}
	if _, err := config.Parse([]string{"--kv-kind=redis"}); err != nil {
		t.Fatalf("redis kind rejected: %v", err)
	}
	if _, err := config.Parse([]string{"--kv-kind=memcached"}); err == nil {
		t.Fatal("expected error for bogus kind")
	}
}

func TestMaxLagDefaultsFollowBackendUnits(t *testing.T) {
	c, err := config.Parse([]string{})
	if err != nil || c.MaxLagBytes != 1000 {
		t.Fatalf("kvrocks default: %d %v", c.MaxLagBytes, err)
	}
	c, err = config.Parse([]string{"--kv-kind=redis"})
	if err != nil || c.MaxLagBytes != 104857600 {
		t.Fatalf("redis default: %d %v", c.MaxLagBytes, err)
	}
	c, err = config.Parse([]string{"--max-lag-bytes=7"})
	if err != nil || c.MaxLagBytes != 7 || c.HandoverMaxLag != 7 {
		t.Fatalf("old flag: %d %v", c.MaxLagBytes, err)
	}
	c, err = config.Parse([]string{"--max-lag=0"})
	if err != nil || c.MaxLagBytes != 0 {
		t.Fatalf("zero: %d %v", c.MaxLagBytes, err)
	}
	if c.EpochKey() != "kvrocks/main/epoch" {
		t.Fatalf("epoch key %q", c.EpochKey())
	}
}

func TestLagGateDefaults(t *testing.T) {
	c, err := config.Parse([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if c.KVKind != "kvrocks" || c.LagGateMaxWait != 0 || c.LagGateMaxBytes != 1000 {
		t.Fatalf("kvrocks defaults: kind %s max-wait %v max-bytes %d", c.KVKind, c.LagGateMaxWait, c.LagGateMaxBytes)
	}
	r, err := config.Parse([]string{"--kv-kind=redis"})
	if err != nil {
		t.Fatal(err)
	}
	if r.LagGateMaxBytes != 4<<20 {
		t.Fatalf("redis default max-bytes %d", r.LagGateMaxBytes)
	}
	if o, _ := config.Parse([]string{"--lag-gate-max-bytes=0"}); o == nil || o.LagGateMaxBytes != 0 {
		t.Fatal("0 must turn the cap off")
	}
}
