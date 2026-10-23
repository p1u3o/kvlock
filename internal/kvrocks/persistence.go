package kvrocks

import (
	"bufio"
	"context"
	"strings"
)

// Persistence is the subset of INFO persistence used by backups.
type Persistence struct {
	// InProgress is true while a BGSAVE is running.
	InProgress bool
	// LastSave is the unix time the last BGSAVE started (kvrocks) or
	// finished (Redis); kvrocks reports its start time before the first.
	LastSave int64
	// LastStatus is "ok" or "err"; empty when not reported.
	LastStatus string
}

// ParseInfoPersistence understands both the kvrocks field names
// (bgsave_in_progress, last_bgsave_time, last_bgsave_status) and the Redis
// ones (rdb_bgsave_in_progress, rdb_last_save_time, rdb_last_bgsave_status).
func ParseInfoPersistence(s string) *Persistence {
	p := &Persistence{}
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		k, v, ok := strings.Cut(strings.TrimSpace(sc.Text()), ":")
		if !ok {
			continue
		}
		switch k {
		case "bgsave_in_progress", "rdb_bgsave_in_progress":
			p.InProgress = v == "1" || v == "yes" || v == "true"
		case "last_bgsave_time", "rdb_last_save_time":
			if n, ok := parseInt(v); ok {
				p.LastSave = n
			}
		case "last_bgsave_status", "rdb_last_bgsave_status":
			p.LastStatus = v
		}
	}
	return p
}

// BGSave asks the local instance for a background save. On kvrocks this
// writes a RocksDB checkpoint to its backup-dir.
func (c *RedisClient) BGSave(ctx context.Context) error {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	return c.rdb.Do(ctx, "BGSAVE").Err()
}

// InfoPersistence reads INFO persistence from the local instance.
func (c *RedisClient) InfoPersistence(ctx context.Context) (*Persistence, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	s, err := c.rdb.Info(ctx, "persistence").Result()
	if err != nil {
		return nil, err
	}
	return ParseInfoPersistence(s), nil
}
