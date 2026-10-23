package kvrocks

import "testing"

func TestParseInfoPersistence(t *testing.T) {
	p := ParseInfoPersistence("# Persistence\r\nloading:0\r\nbgsave_in_progress:1\r\nlast_bgsave_time:1700000000\r\nlast_bgsave_status:ok\r\nlast_bgsave_time_sec:2\r\n")
	if !p.InProgress || p.LastSave != 1700000000 || p.LastStatus != "ok" {
		t.Fatalf("kvrocks: %+v", p)
	}
	p = ParseInfoPersistence("rdb_bgsave_in_progress:0\nrdb_last_save_time:42\nrdb_last_bgsave_status:err\n")
	if p.InProgress || p.LastSave != 42 || p.LastStatus != "err" {
		t.Fatalf("redis: %+v", p)
	}
}
