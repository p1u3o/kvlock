package kvrocks_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/kvrocks"
)

// configServer is a minimal RESP server that keeps CONFIG SET values.
type configServer struct {
	mu  sync.Mutex
	cfg map[string]string
	ln  net.Listener
}

func newConfigServer(t *testing.T) *configServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &configServer{cfg: map[string]string{}, ln: ln}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	return s
}

func readCmd(r *bufio.Reader) ([]string, error) {
	l, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, _ := strconv.Atoi(strings.TrimSpace(l[1:]))
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if _, err := r.ReadString('\n'); err != nil { // $len
			return nil, err
		}
		v, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		out = append(out, strings.TrimRight(v, "\r\n"))
	}
	return out, nil
}

func (s *configServer) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		cmd, err := readCmd(r)
		if err != nil {
			return
		}
		s.mu.Lock()
		switch {
		case len(cmd) == 4 && strings.EqualFold(cmd[0], "config") && strings.EqualFold(cmd[1], "set"):
			s.cfg[cmd[2]] = cmd[3]
			fmt.Fprint(c, "+OK\r\n")
		case len(cmd) == 3 && strings.EqualFold(cmd[0], "config") && strings.EqualFold(cmd[1], "get"):
			v := s.cfg[cmd[2]]
			fmt.Fprintf(c, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(cmd[2]), cmd[2], len(v), v)
		case strings.EqualFold(cmd[0], "hello"):
			fmt.Fprint(c, "-ERR unknown command\r\n")
		default:
			fmt.Fprint(c, "+OK\r\n")
		}
		s.mu.Unlock()
	}
}

func TestReplicaAnnounceSetAndGet(t *testing.T) {
	s := newConfigServer(t)
	a := s.ln.Addr().(*net.TCPAddr)
	c := kvrocks.New("127.0.0.1", a.Port, "", time.Second)
	defer c.Close()
	ctx := context.Background()
	if err := c.SetReplicaAnnounce(ctx, "10.0.0.2", 24567); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	ip, port := s.cfg["replica-announce-ip"], s.cfg["replica-announce-port"]
	s.mu.Unlock()
	if ip != "10.0.0.2" || port != "24567" {
		t.Fatalf("server saw %q %q", ip, port)
	}
	gip, gport, err := c.ReplicaAnnounce(ctx)
	if err != nil || gip != "10.0.0.2" || gport != 24567 {
		t.Fatalf("get: %q %d %v", gip, gport, err)
	}
}
