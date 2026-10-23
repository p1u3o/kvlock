// Package sentinel serves a read-only subset of the Redis Sentinel
// protocol so Sentinel-aware clients can discover the current master from
// the Consul lock instead of from real Sentinels.
//
// Supported: PING, SENTINEL get-master-addr-by-name | masters | master |
// replicas | slaves | sentinels | myid, ROLE, INFO, CLIENT SETNAME/SETINFO,
// QUIT, and SUBSCRIBE/UNSUBSCRIBE with +switch-master notifications.
// Everything that would change state (SENTINEL failover, set, monitor,
// remove, reset, ...) is refused: failover is driven by Consul only.
package sentinel

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/p1u3o/kvlock/internal/resp"
)

// Master is the current lock holder as clients should see it.
type Master struct {
	IP    string
	Port  int
	Epoch uint64
	Node  string
}

// Instance is a replica.
type Instance struct {
	Node string
	IP   string
	Port int
}

// Source supplies cluster state. Master returns ok=false while the lock is
// vacant or unknown.
type Source interface {
	Master() (Master, bool)
	Replicas(ctx context.Context) ([]Instance, error)
}

// SwitchChannel is the Sentinel event channel clients subscribe to.
const SwitchChannel = "+switch-master"

// Server is the listener. Create with New, then Serve.
type Server struct {
	name string
	src  Source
	id   string
	poll time.Duration
	logf func(msg string, fields map[string]any)

	mu     sync.Mutex
	ln     net.Listener
	conns  map[*conn]struct{}
	closed bool
	done   chan struct{}
}

// New returns a server answering for master name.
func New(name string, src Source, poll time.Duration, logf func(string, map[string]any)) *Server {
	b := make([]byte, 20)
	_, _ = rand.Read(b)
	if poll <= 0 {
		poll = 500 * time.Millisecond
	}
	if logf == nil {
		logf = func(string, map[string]any) {}
	}
	return &Server{name: name, src: src, id: hex.EncodeToString(b), poll: poll, logf: logf,
		conns: map[*conn]struct{}{}, done: make(chan struct{})}
}

// Serve accepts connections until Close. It also watches the master and
// publishes +switch-master to subscribers when it changes.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	s.ln = ln
	s.mu.Unlock()
	go s.watch()
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		c := &conn{nc: nc, w: bufio.NewWriter(nc), subs: map[string]bool{}}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			nc.Close()
			return nil
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		go s.handle(c)
	}
}

// NumConns returns the number of open client connections.
func (s *Server) NumConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Close stops the listener and drops all connections.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.done)
	for c := range s.conns {
		c.nc.Close()
	}
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

type conn struct {
	nc   net.Conn
	wmu  sync.Mutex
	w    *bufio.Writer
	subs map[string]bool // guarded by wmu
}

func (c *conn) reply(vs ...any) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.replyLocked(vs...)
}

func (c *conn) replyLocked(vs ...any) error {
	for _, v := range vs {
		if err := resp.Write(c.w, v); err != nil {
			return err
		}
	}
	_ = c.nc.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return c.w.Flush()
}

func (s *Server) handle(c *conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		c.nc.Close()
	}()
	r := bufio.NewReader(c.nc)
	for {
		args, err := resp.ReadCommand(r)
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				_ = c.reply(resp.Error("ERR Protocol error: " + err.Error()))
			} else if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logf("sentinel connection error", map[string]any{"error": err.Error()})
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		if quit := s.dispatch(c, args); quit {
			return
		}
	}
}

func (s *Server) dispatch(c *conn, args []string) (quit bool) {
	cmd := strings.ToLower(args[0])
	c.wmu.Lock()
	subscribed := len(c.subs) > 0
	c.wmu.Unlock()
	if subscribed {
		switch cmd {
		case "subscribe", "unsubscribe", "ping", "quit", "reset":
		default:
			_ = c.reply(resp.Error(fmt.Sprintf("ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", cmd)))
			return false
		}
	}
	switch cmd {
	case "ping":
		if subscribed {
			msg := ""
			if len(args) > 1 {
				msg = args[1]
			}
			_ = c.reply([]any{"pong", msg})
		} else if len(args) > 1 {
			_ = c.reply(args[1])
		} else {
			_ = c.reply(resp.Simple("PONG"))
		}
	case "quit":
		_ = c.reply(resp.Simple("OK"))
		return true
	case "sentinel":
		_ = c.reply(s.sentinel(args[1:]))
	case "subscribe":
		if len(args) < 2 {
			_ = c.reply(wrongArgs(cmd))
			break
		}
		c.wmu.Lock()
		for _, ch := range args[1:] {
			c.subs[ch] = true
			_ = resp.Write(c.w, []any{"subscribe", ch, len(c.subs)})
		}
		_ = c.replyLocked()
		c.wmu.Unlock()
	case "unsubscribe":
		c.wmu.Lock()
		chans := args[1:]
		if len(chans) == 0 {
			for ch := range c.subs {
				chans = append(chans, ch)
			}
		}
		if len(chans) == 0 {
			_ = resp.Write(c.w, []any{"unsubscribe", resp.Null{}, 0})
		}
		for _, ch := range chans {
			delete(c.subs, ch)
			_ = resp.Write(c.w, []any{"unsubscribe", ch, len(c.subs)})
		}
		_ = c.replyLocked()
		c.wmu.Unlock()
	case "reset":
		c.wmu.Lock()
		c.subs = map[string]bool{}
		_ = c.replyLocked(resp.Simple("RESET"))
		c.wmu.Unlock()
	case "role":
		_ = c.reply([]any{"sentinel", []string{s.name}})
	case "info":
		_ = c.reply(s.info())
	case "client":
		if len(args) > 1 && (strings.EqualFold(args[1], "setname") || strings.EqualFold(args[1], "setinfo")) {
			_ = c.reply(resp.Simple("OK"))
		} else {
			_ = c.reply(resp.Error("ERR unsupported CLIENT subcommand"))
		}
	case "auth":
		_ = c.reply(resp.Error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?"))
	default:
		_ = c.reply(resp.Error(fmt.Sprintf("ERR unknown command '%s'", args[0])))
	}
	return false
}

func wrongArgs(cmd string) resp.Error {
	return resp.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd))
}

func (s *Server) sentinel(args []string) any {
	if len(args) == 0 {
		return wrongArgs("sentinel")
	}
	sub := strings.ToLower(args[0])
	needName := func() (resp.Error, bool) {
		if len(args) != 2 {
			return wrongArgs("sentinel|" + sub), false
		}
		if args[1] != s.name {
			return resp.Error("ERR No such master with that name"), false
		}
		return "", true
	}
	switch sub {
	case "get-master-addr-by-name":
		if len(args) != 2 {
			return wrongArgs("sentinel|" + sub)
		}
		m, ok := s.src.Master()
		if args[1] != s.name || !ok {
			return resp.NullArray{}
		}
		return []string{m.IP, strconv.Itoa(m.Port)}
	case "masters":
		m, ok := s.src.Master()
		if !ok {
			return []any{}
		}
		return []any{s.masterFields(m)}
	case "master":
		if e, ok := needName(); !ok {
			return e
		}
		m, ok := s.src.Master()
		if !ok {
			return resp.Error("ERR No such master with that name")
		}
		return s.masterFields(m)
	case "replicas", "slaves":
		if e, ok := needName(); !ok {
			return e
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		reps, err := s.src.Replicas(ctx)
		if err != nil {
			return resp.Error("ERR replica lookup failed: " + err.Error())
		}
		m, _ := s.src.Master()
		out := []any{}
		for _, r := range reps {
			if r.IP == m.IP && r.Port == m.Port {
				continue
			}
			out = append(out, replicaFields(r, m))
		}
		return out
	case "sentinels":
		if e, ok := needName(); !ok {
			return e
		}
		return []any{}
	case "myid":
		return s.id
	default:
		return resp.Error(fmt.Sprintf("ERR unsupported SENTINEL subcommand '%s': this endpoint is read-only, failover is managed through Consul", args[0]))
	}
}

func (s *Server) masterFields(m Master) []string {
	return []string{
		"name", s.name,
		"ip", m.IP,
		"port", strconv.Itoa(m.Port),
		"runid", "",
		"flags", "master",
		"link-pending-commands", "0",
		"link-refcount", "1",
		"last-ping-sent", "0",
		"last-ok-ping-reply", "0",
		"last-ping-reply", "0",
		"down-after-milliseconds", "30000",
		"role-reported", "master",
		"config-epoch", strconv.FormatUint(m.Epoch, 10),
		"num-slaves", strconv.Itoa(s.replicaCount(m)),
		"num-other-sentinels", "0",
		"quorum", "1",
		"failover-timeout", "180000",
		"parallel-syncs", "1",
	}
}

func (s *Server) replicaCount(m Master) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reps, err := s.src.Replicas(ctx)
	if err != nil {
		return 0
	}
	n := 0
	for _, r := range reps {
		if r.IP != m.IP || r.Port != m.Port {
			n++
		}
	}
	return n
}

func replicaFields(r Instance, m Master) []string {
	addr := net.JoinHostPort(r.IP, strconv.Itoa(r.Port))
	return []string{
		"name", addr,
		"ip", r.IP,
		"port", strconv.Itoa(r.Port),
		"runid", "",
		"flags", "slave",
		"role-reported", "slave",
		"master-link-down-time", "0",
		"master-link-status", "ok",
		"master-host", m.IP,
		"master-port", strconv.Itoa(m.Port),
		"slave-priority", "100",
	}
}

func (s *Server) info() string {
	status := "ok"
	m, ok := s.src.Master()
	if !ok {
		status = "odown"
	}
	return fmt.Sprintf("# Sentinel\r\nsentinel_masters:1\r\nsentinel_tilt:0\r\nsentinel_running_scripts:0\r\nsentinel_scripts_queue_length:0\r\nmaster0:name=%s,status=%s,address=%s,slaves=%d,sentinels=1\r\n",
		s.name, status, net.JoinHostPort(m.IP, strconv.Itoa(m.Port)), s.replicaCount(m))
}

// watch publishes +switch-master when the known master address changes.
// A vacant lock does not reset the last known master, so A -> (vacant) -> B
// yields a single "A -> B" event.
func (s *Server) watch() {
	t := time.NewTicker(s.poll)
	defer t.Stop()
	last, haveLast := s.src.Master()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		m, ok := s.src.Master()
		if !ok {
			continue
		}
		if haveLast && (m.IP != last.IP || m.Port != last.Port) {
			s.publish(SwitchChannel, fmt.Sprintf("%s %s %d %s %d", s.name, last.IP, last.Port, m.IP, m.Port))
		}
		last, haveLast = m, true
	}
}

func (s *Server) publish(channel, msg string) {
	s.mu.Lock()
	targets := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		targets = append(targets, c)
	}
	s.mu.Unlock()
	n := 0
	for _, c := range targets {
		c.wmu.Lock()
		if c.subs[channel] {
			if err := c.replyLocked([]any{"message", channel, msg}); err == nil {
				n++
			}
		}
		c.wmu.Unlock()
	}
	s.logf("sentinel published "+channel, map[string]any{"message": msg, "subscribers": n})
}
