package config_test

import (
	"testing"
	"time"

	"github.com/example/kvrocks-leader/internal/config"
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
