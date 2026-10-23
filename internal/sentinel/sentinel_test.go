package sentinel

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeSrc struct {
	mu   sync.Mutex
	m    Master
	ok   bool
	reps []Instance
	err  error
}

func (f *fakeSrc) Master() (Master, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.m, f.ok
}
func (f *fakeSrc) Replicas(context.Context) ([]Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reps, f.err
}
func (f *fakeSrc) set(m Master, ok bool) {
	f.mu.Lock()
	f.m, f.ok = m, ok
	f.mu.Unlock()
}

func start(t *testing.T, src Source) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New("mymaster", src, 10*time.Millisecond, nil)
	go s.Serve(ln)
	t.Cleanup(func() { s.Close() })
	return ln.Addr().String()
}

func TestGoRedisSentinelClient(t *testing.T) {
	src := &fakeSrc{m: Master{IP: "10.0.0.1", Port: 6666, Epoch: 7}, ok: true,
		reps: []Instance{{IP: "10.0.0.1", Port: 6666}, {IP: "10.0.0.2", Port: 6666}}}
	addr := start(t, src)
	sc := redis.NewSentinelClient(&redis.Options{Addr: addr})
	defer sc.Close()
	ctx := context.Background()

	if err := sc.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := sc.GetMasterAddrByName(ctx, "mymaster").Result()
	if err != nil || len(got) != 2 || got[0] != "10.0.0.1" || got[1] != "6666" {
		t.Fatalf("got %v %v", got, err)
	}
	if _, err := sc.GetMasterAddrByName(ctx, "other").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("unknown name should be nil, got %v", err)
	}
	m, err := sc.Master(ctx, "mymaster").Result()
	if err != nil || m["ip"] != "10.0.0.1" || m["flags"] != "master" || m["config-epoch"] != "7" || m["num-slaves"] != "1" {
		t.Fatalf("master: %v %v", m, err)
	}
	reps, err := sc.Replicas(ctx, "mymaster").Result()
	if err != nil || len(reps) != 1 || reps[0]["ip"] != "10.0.0.2" || reps[0]["flags"] != "slave" || reps[0]["master-host"] != "10.0.0.1" {
		t.Fatalf("replicas: %v %v", reps, err)
	}
	if err := sc.Failover(ctx, "mymaster").Err(); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("failover must be refused, got %v", err)
	}
	if _, err := sc.Master(ctx, "nope").Result(); err == nil {
		t.Fatal("unknown master should error")
	}

	src.set(Master{}, false)
	if _, err := sc.GetMasterAddrByName(ctx, "mymaster").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("vacant lock should be nil, got %v", err)
	}
}

func TestFailoverClientDiscoversMaster(t *testing.T) {
	// Point the failover client at a fake "master" that answers ROLE etc.
	mln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer mln.Close()
	go func() {
		for {
			c, err := mln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if strings.HasPrefix(line, "*") {
						// read the command fully, answer +PONG / error generically
						n := 0
						for _, ch := range line[1 : len(line)-2] {
							n = n*10 + int(ch-'0')
						}
						var parts []string
						for i := 0; i < n; i++ {
							r.ReadString('\n')
							p, _ := r.ReadString('\n')
							parts = append(parts, strings.TrimSpace(p))
						}
						switch strings.ToLower(parts[0]) {
						case "ping":
							c.Write([]byte("+PONG\r\n"))
						default:
							c.Write([]byte("-ERR unknown command\r\n"))
						}
					}
				}
			}()
		}
	}()
	host, portS, _ := net.SplitHostPort(mln.Addr().String())
	var port int
	for _, ch := range portS {
		port = port*10 + int(ch-'0')
	}
	addr := start(t, &fakeSrc{m: Master{IP: host, Port: port}, ok: true})
	fc := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "mymaster", SentinelAddrs: []string{addr}, Protocol: 2})
	defer fc.Close()
	if err := fc.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("failover client could not reach the master via the sentinel endpoint: %v", err)
	}
}

func TestSwitchMasterNotification(t *testing.T) {
	src := &fakeSrc{m: Master{IP: "10.0.0.1", Port: 6666}, ok: true}
	addr := start(t, src)
	sc := redis.NewSentinelClient(&redis.Options{Addr: addr})
	defer sc.Close()
	ctx := context.Background()
	ps := sc.Subscribe(ctx, SwitchChannel)
	defer ps.Close()
	if _, err := ps.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	src.set(Master{}, false) // vacant in between
	time.Sleep(30 * time.Millisecond)
	src.set(Master{IP: "10.0.0.2", Port: 6666}, true)
	select {
	case msg := <-ps.Channel():
		if msg.Channel != SwitchChannel || msg.Payload != "mymaster 10.0.0.1 6666 10.0.0.2 6666" {
			t.Fatalf("got %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no +switch-master")
	}
}

func TestRawProtocol(t *testing.T) {
	addr := start(t, &fakeSrc{})
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	exp := func(send, want string) {
		t.Helper()
		c.Write([]byte(send))
		c.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, len(want))
		if _, err := io.ReadFull(r, buf); err != nil || string(buf) != want {
			t.Fatalf("%q: got %q (%v), want %q", send, buf, err, want)
		}
	}
	exp("PING\r\n", "+PONG\r\n")
	exp("SENTINEL get-master-addr-by-name mymaster\r\n", "*-1\r\n")
	exp("SENTINEL masters\r\n", "*0\r\n")
	exp("SENTINEL sentinels mymaster\r\n", "*0\r\n")
	exp("SET a b\r\n", "-ERR unknown command 'SET'\r\n")
	exp("SUBSCRIBE +switch-master\r\n", "*3\r\n$9\r\nsubscribe\r\n$14\r\n+switch-master\r\n:1\r\n")
	exp("PING\r\n", "*2\r\n$4\r\npong\r\n$0\r\n\r\n")
	exp("SENTINEL masters\r\n", "-ERR Can't execute 'sentinel'")
}
