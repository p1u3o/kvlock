package loadtest

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// A pipeline that cannot reach the server leaves every command's own error
// unset; those commands must count as failed, not as acked writes or empty
// reads.
func TestCmdErrDialFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there now
	c := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	defer c.Close()
	ctx := context.Background()
	pipe := c.Pipeline()
	set := pipe.Set(ctx, "k", "1:", 0)
	get := pipe.Get(ctx, "k")
	_, execErr := pipe.Exec(ctx)
	if execErr == nil {
		t.Fatal("expected a dial error")
	}
	for _, cmd := range []redis.Cmder{set, get} {
		if cmdErr(cmd, execErr) == nil {
			t.Errorf("%s: unreachable server counted as answered", cmd.Name())
		}
	}
}

func TestCmdErrServerReply(t *testing.T) {
	// Exec returns the first command error; a server reply error on one
	// command says nothing bad about the others.
	cmd := redis.NewStatusCmd(context.Background(), "set", "k", "v")
	if err := cmdErr(cmd, redis.Nil); err != nil {
		t.Fatalf("answered command marked failed: %v", err)
	}
	ro := redis.NewStatusCmd(context.Background(), "set", "k", "v")
	ro.SetErr(redis.Nil)
	if cmdErr(ro, nil) != redis.Nil {
		t.Fatal("own error lost")
	}
}
