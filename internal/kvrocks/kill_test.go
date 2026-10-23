package kvrocks_test

import (
	"bufio"
	"context"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/resp"
)

// fakeServer answers just enough of the protocol for go-redis to connect
// and records CLIENT KILL calls.
type fakeServer struct {
	ln       net.Listener
	typeKill bool // whether CLIENT KILL TYPE is supported
	list     string

	mu    sync.Mutex
	kills [][]string
}

func newFakeServer(t *testing.T, typeKill bool, list string) *fakeServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{ln: ln, typeKill: typeKill, list: list}
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

func (s *fakeServer) serve(c net.Conn) {
	defer c.Close()
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			return
		}
		up := make([]string, len(args))
		for i, a := range args {
			up[i] = strings.ToUpper(a)
		}
		var reply any = resp.Simple("OK")
		switch {
		case len(up) > 0 && up[0] == "HELLO":
			reply = resp.Error("ERR unknown command 'HELLO'")
		case len(up) >= 3 && up[0] == "CLIENT" && up[1] == "KILL":
			s.mu.Lock()
			s.kills = append(s.kills, args[2:])
			s.mu.Unlock()
			if up[2] == "TYPE" {
				if s.typeKill {
					reply = 3
				} else {
					reply = resp.Error("ERR syntax error")
				}
			} else {
				reply = resp.Simple("OK")
			}
		case len(up) >= 2 && up[0] == "CLIENT" && up[1] == "LIST":
			reply = s.list
		}
		_ = resp.Write(w, reply)
		_ = w.Flush()
	}
}

func (s *fakeServer) client() *kvrocks.RedisClient {
	a := s.ln.Addr().(*net.TCPAddr)
	return kvrocks.New("127.0.0.1", a.Port, "", time.Second)
}

func TestKillClientsTypeNormal(t *testing.T) {
	s := newFakeServer(t, true, "")
	c := s.client()
	defer c.Close()
	n, err := c.KillClients(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.kills, [][]string{{"TYPE", "normal", "SKIPME", "yes"}}) {
		t.Fatalf("kills = %v", s.kills)
	}
}

// kvrocks 2.11 CLIENT LIST shape (Connection::ToString).
const clientList = "id=5 addr=10.0.0.9:5000 fd=10 name= age=3 idle=0 flags= namespace=__namespace qbuf=0 obuf=0 cmd=get\n" +
	"id=6 addr=10.0.0.2:4000 fd=11 name= age=9 idle=0 flags=S namespace=__namespace qbuf=0 obuf=0 cmd=psync\n" +
	"id=7 addr=127.0.0.1:6000 fd=12 name=kvlock-sidecar age=9 idle=0 flags= namespace=__namespace qbuf=0 obuf=0 cmd=client\n" +
	"id=8 addr=10.0.0.9:5001 fd=13 name=app age=1 idle=0 flags= namespace=__namespace qbuf=0 obuf=0 cmd=set\n"

func TestKillClientsFallbackSparesReplicasAndSelf(t *testing.T) {
	s := newFakeServer(t, false, clientList)
	c := s.client()
	defer c.Close()
	n, err := c.KillClients(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	want := [][]string{{"TYPE", "normal", "SKIPME", "yes"}, {"ID", "5"}, {"ID", "8"}}
	if !reflect.DeepEqual(s.kills, want) {
		t.Fatalf("kills = %v, want %v", s.kills, want)
	}
}

func TestKillableClientIDsRedisPubSub(t *testing.T) {
	list := "id=3 addr=1.1.1.1:1 name= flags=N sub=1 psub=0\nid=4 addr=1.1.1.1:2 name= flags=M sub=0 psub=0\nid=9 addr=1.1.1.1:3 name= flags=N sub=0 psub=0\n"
	if got := kvrocks.KillableClientIDs(list); !reflect.DeepEqual(got, []string{"9"}) {
		t.Fatalf("got %v", got)
	}
}
