package config_test

import (
	"strings"
	"testing"

	"github.com/p1u3o/kvlock/internal/config"
)

func TestForwardOffByDefault(t *testing.T) {
	c, err := config.Parse([]string{})
	if err != nil {
		t.Fatal(err)
	}
	if c.ForwardEnabled || c.PeerExpose || c.ForwardAddr != "127.0.1.1:18379" || c.ForwardHold.Seconds() != 5 {
		t.Fatalf("defaults: %+v", c)
	}
}

func TestForwardValidation(t *testing.T) {
	for _, tc := range []struct {
		args []string
		err  string
	}{
		{[]string{"--forward", "--kvrocks-port=18379"}, "kvrocks port"},
		{[]string{"--forward", "--forward-addr=10.0.0.1:18379"}, "loopback"},
		{[]string{"--forward", "--forward-hold=-1s"}, "hold"},
		{[]string{"--peer-expose"}, "alloc index"},
		{[]string{"--peer-expose", "--alloc-index=1", "--peer-base=10.0.0.0"}, "loopback"},
		{[]string{"--peer-expose", "--alloc-index=1", "--peer-port=6666", "--kvrocks-port=6666"}, "peer-port"},
	} {
		_, err := config.Parse(tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%v: want error containing %q, got %v", tc.args, tc.err, err)
		}
	}
	if _, err := config.Parse([]string{"--forward", "--peer-expose", "--alloc-index=2"}); err != nil {
		t.Fatalf("valid forward config: %v", err)
	}
}

func TestPeerAddrFromIndex(t *testing.T) {
	c, err := config.Parse([]string{"--peer-expose", "--alloc-index=0"})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range map[int]string{0: "127.0.2.0:18379", 2: "127.0.2.2:18379"} {
		if got, _ := c.PeerAddr(i); got != want {
			t.Errorf("PeerAddr(%d) = %q want %q", i, got, want)
		}
	}
}

func TestAdvertiseFromNomadLabel(t *testing.T) {
	t.Setenv("NOMAD_HOST_ADDR_kvrocks", "10.9.8.7:24567")
	c, err := config.Parse([]string{"--advertise-nomad-label=kvrocks"})
	if err != nil {
		t.Fatal(err)
	}
	if c.AdvertiseAddr != "10.9.8.7" || c.AdvertisePort != 24567 {
		t.Fatalf("advertise %s:%d", c.AdvertiseAddr, c.AdvertisePort)
	}
	if _, err := config.Parse([]string{"--advertise-nomad-label=missing"}); err == nil {
		t.Fatal("unset label must fail")
	}
}

func TestReplicaAnnounceDefaults(t *testing.T) {
	t.Setenv("NOMAD_HOST_ADDR_kvrocks", "10.9.8.7:24567")
	t.Setenv("NOMAD_HOST_IP_kvrocks", "10.9.8.7")
	t.Setenv("NOMAD_HOST_PORT_kvrocks", "24567")
	c, err := config.Parse([]string{"--advertise-nomad-label=kvrocks", "--forward"})
	if err != nil {
		t.Fatal(err)
	}
	if !c.ReplicaAnnounce || c.ReplicaAnnounceIP != "10.9.8.7" || c.ReplicaAnnouncePort != 24567 {
		t.Fatalf("announce %v %s:%d", c.ReplicaAnnounce, c.ReplicaAnnounceIP, c.ReplicaAnnouncePort)
	}
	c, err = config.Parse([]string{"--advertise-nomad-label=kvrocks", "--forward", "--replica-announce-ip=10.1.1.1", "--replica-announce-port=1234"})
	if err != nil || c.ReplicaAnnounceIP != "10.1.1.1" || c.ReplicaAnnouncePort != 1234 {
		t.Fatalf("override: %v %+v", err, c)
	}
	c, err = config.Parse([]string{"--advertise-nomad-label=kvrocks", "--forward", "--replica-announce=false"})
	if err != nil || c.ReplicaAnnounce {
		t.Fatalf("off: %v", err)
	}
	if _, err := config.Parse([]string{"--forward", "--replica-announce-ip=kv02.example"}); err == nil {
		t.Fatal("hostname announce must fail")
	}
}
