package kvrocks_test

import (
	"context"
	"testing"
	"time"

	"github.com/example/kvrocks-leader/internal/kvrocks"
)

// Captured shape of kvrocks 2.11 INFO replication (Server::GetReplicationInfo).
const kvrocksMaster = "# Replication\r\n" +
	"role:master\r\n" +
	"connected_slaves:1\r\n" +
	"slave0:ip=10.0.0.2,port=6666,offset=12000,lag=345\r\n" +
	"master_repl_offset:12345\r\n"

const kvrocksReplica = "# Replication\r\n" +
	"role:slave\r\n" +
	"master_host:10.0.0.1\r\n" +
	"master_port:6666\r\n" +
	"master_link_status:up\r\n" +
	"master_sync_unrecoverable_error:no\r\n" +
	"master_sync_in_progress:0\r\n" +
	"master_last_io_seconds_ago:0\r\n" +
	"slave_repl_offset:12000\r\n" +
	"slave_priority:100\r\n" +
	"connected_slaves:0\r\n" +
	"master_repl_offset:12000\r\n"

const redisReplica = `# Replication
role:slave
master_host:10.0.0.1
master_port:6379
master_link_status:down
slave_read_repl_offset:5000
slave_repl_offset:5000
slave_priority:100
connected_slaves:0
master_repl_offset:5000
`

const redisMaster = `# Replication
role:master
connected_slaves:1
slave0:ip=10.0.0.2,port=6379,state=online,offset=4900,lag=1
master_failover_state:no-failover
master_repl_offset:5000
`

func TestParseKvrocksMaster(t *testing.T) {
	ri := kvrocks.ParseInfoReplication(kvrocksMaster)
	if ri.Role != "master" || ri.MasterOffset != 12345 || ri.LocalOffset != 12345 || ri.LagBytes != 0 {
		t.Fatalf("bad master parse: %+v", ri)
	}
	if len(ri.Replicas) != 1 || ri.Replicas[0].IP != "10.0.0.2" || ri.Replicas[0].Port != 6666 || ri.Replicas[0].Offset != 12000 {
		t.Fatalf("bad slave0 parse: %+v", ri.Replicas)
	}
}

func TestParseKvrocksReplica(t *testing.T) {
	ri := kvrocks.ParseInfoReplication(kvrocksReplica)
	if ri.Role != "slave" || ri.MasterHost != "10.0.0.1" || ri.MasterPort != 6666 || !ri.MasterLinkUp {
		t.Fatalf("bad replica parse: %+v", ri)
	}
	if ri.LocalOffset != 12000 {
		t.Fatalf("local offset=%d", ri.LocalOffset)
	}
	// master_repl_offset on a kvrocks replica is its own sequence number,
	// so lag must NOT be derived from it (it would always read 0).
	if ri.LagBytes != -1 || ri.MasterOffset != -1 {
		t.Fatalf("replica lag must be unknown from local INFO, got lag=%d master=%d", ri.LagBytes, ri.MasterOffset)
	}
}

func TestParseRedis(t *testing.T) {
	r := kvrocks.ParseInfoReplication(redisReplica)
	if r.Role != "slave" || r.LocalOffset != 5000 || r.MasterLinkUp || r.LagBytes != -1 {
		t.Fatalf("bad redis replica: %+v", r)
	}
	m := kvrocks.ParseInfoReplication(redisMaster)
	if m.MasterOffset != 5000 || len(m.Replicas) != 1 || m.Replicas[0].Offset != 4900 {
		t.Fatalf("bad redis master: %+v", m)
	}
}

func TestParseUnknown(t *testing.T) {
	ri := kvrocks.ParseInfoReplication("# Replication\nrole:slave\nmaster_host:10.0.0.1\n")
	if ri.LagBytes != -1 || ri.LocalOffset != -1 {
		t.Fatalf("expected unknown, got %+v", ri)
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
