package kvrocks_test

import (
	"context"
	"testing"
	"time"

	"github.com/example/kvrocks-leader/internal/kvrocks"
)

const masterInfo = `# Replication
role:master
connected_slaves:1
master_repl_offset:12345
`

const slaveInfo = `# Replication
role:slave
master_host:10.0.0.1
master_port:6666
master_link_status:up
master_repl_offset:12345
slave_repl_offset:12000
`

func TestParseMaster(t *testing.T) {
	ri := kvrocks.ParseInfoReplication(masterInfo)
	if ri.Role != "master" {
		t.Fatalf("role=%q", ri.Role)
	}
	if ri.MasterOffset != 12345 {
		t.Fatalf("offset=%d", ri.MasterOffset)
	}
}

func TestParseSlave(t *testing.T) {
	ri := kvrocks.ParseInfoReplication(slaveInfo)
	if ri.Role != "slave" {
		t.Fatalf("role=%q", ri.Role)
	}
	if ri.MasterHost != "10.0.0.1" || ri.MasterPort != 6666 {
		t.Fatalf("master=%v:%v", ri.MasterHost, ri.MasterPort)
	}
	if !ri.MasterLinkUp {
		t.Fatal("expected link up")
	}
	// master 12345 - local 12000 = 345 lag (when replica reports both).
	if ri.LagBytes != 345 {
		t.Fatalf("lag=%d", ri.LagBytes)
	}
}

func TestParseUnknownLag(t *testing.T) {
	ri := kvrocks.ParseInfoReplication("# Replication\nrole:slave\nmaster_host:10.0.0.1\n")
	if ri.LagBytes != -1 {
		t.Fatalf("expected unknown lag, got %d", ri.LagBytes)
	}
}

func TestEnsureRsIDPsyncRedisNoop(t *testing.T) {
	// Must not touch the network for redis (no such config there).
	c := kvrocks.New("127.0.0.1", 1, "", time.Second, "redis")
	defer c.Close()
	if err := c.EnsureRsIDPsync(context.Background()); err != nil {
		t.Fatalf("redis kind should skip use-rsid-psync, got %v", err)
	}
}
