// Package testkv starts throwaway local kvrocks servers for integration
// tests. Tests skip when no kvrocks binary is found ($KVROCKS_BIN or
// "kvrocks" on PATH). It only binds 127.0.0.1 on free high ports.
package testkv

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Binary returns the kvrocks binary path or "".
func Binary() string {
	if b := os.Getenv("KVROCKS_BIN"); b != "" {
		return b
	}
	if p, err := exec.LookPath("kvrocks"); err == nil {
		return p
	}
	return ""
}

// Server is a running kvrocks.
type Server struct {
	Port int
	Dir  string
	cmd  *exec.Cmd
}

// Addr is "127.0.0.1:port".
func (s *Server) Addr() string { return fmt.Sprintf("127.0.0.1:%d", s.Port) }

// Client opens a go-redis client to the server.
func (s *Server) Client() *redis.Client { return redis.NewClient(&redis.Options{Addr: s.Addr()}) }

// Stop kills the server.
func (s *Server) Stop() {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_, _ = s.cmd.Process.Wait()
		s.cmd = nil
	}
}

// FreePort returns a free TCP port on 127.0.0.1.
func FreePort(t testing.TB) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Start launches kvrocks or skips the test when no binary is available.
func Start(t testing.TB) *Server {
	t.Helper()
	bin := Binary()
	if bin == "" {
		t.Skip("no kvrocks binary (set KVROCKS_BIN or put kvrocks on PATH)")
	}
	dir := t.TempDir()
	port := FreePort(t)
	conf := filepath.Join(dir, "kvrocks.conf")
	body := fmt.Sprintf("bind 127.0.0.1\nport %d\ndir %s\nlog-dir stdout\nworkers 2\n", port, filepath.Join(dir, "data"))
	if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-c", conf)
	logf, _ := os.Create(filepath.Join(dir, "out.log"))
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &Server{Port: port, Dir: dir, cmd: cmd}
	t.Cleanup(s.Stop)
	c := s.Client()
	defer c.Close()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := c.Ping(ctx).Err()
		cancel()
		if err == nil {
			return s
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(filepath.Join(dir, "out.log"))
			t.Fatalf("kvrocks did not start: %v\n%s", err, b)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
