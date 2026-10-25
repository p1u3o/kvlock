package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/p1u3o/kvlock/internal/logx"
	"github.com/p1u3o/kvlock/internal/metrics"
	"github.com/p1u3o/kvlock/internal/proxy"
)

// peerSource is the controller surface the peer listeners need.
type peerSource interface {
	RefreshPeers(ctx context.Context) error
	PeerIndexes() []int
	PeerRoute(i int) (string, string, string)
}

// peerListeners keeps one fail-closed listener per known alloc index at
// addrFor(index). Listeners stay up once opened; a member that goes away
// makes its route refuse (and drops its connections).
type peerListeners struct {
	src     peerSource
	addrFor func(int) (string, error)
	reg     *metrics.Registry
	log     *logx.Logger

	mu     sync.Mutex
	open   map[int]*proxy.Proxy
	notify map[int]chan struct{}
}

func newPeerListeners(src peerSource, addrFor func(int) (string, error), reg *metrics.Registry, log *logx.Logger) *peerListeners {
	return &peerListeners{src: src, addrFor: addrFor, reg: reg, log: log, open: map[int]*proxy.Proxy{}, notify: map[int]chan struct{}{}}
}

// sync refreshes members and opens listeners for new indexes.
func (pl *peerListeners) sync(ctx context.Context) {
	if err := pl.src.RefreshPeers(ctx); err != nil {
		pl.log.Warn("peer refresh failed", map[string]any{"error": err.Error()})
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	for _, i := range pl.src.PeerIndexes() {
		if _, ok := pl.open[i]; ok {
			continue
		}
		addr, err := pl.addrFor(i)
		if err != nil {
			continue
		}
		idx := i
		ch := make(chan struct{}, 1)
		p, err := startProxyHold(fmt.Sprintf("peer-%d", idx), addr, func() proxy.Decision {
			a, k, r := pl.src.PeerRoute(idx)
			return proxy.Decision{Addr: a, Key: k, Reason: r}
		}, ch, 0, pl.reg, pl.log)
		if err != nil {
			pl.log.Warn("peer listener failed", map[string]any{"addr": addr, "error": err.Error()})
			continue
		}
		pl.open[i], pl.notify[i] = p, ch
	}
	for _, ch := range pl.notify {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (pl *peerListeners) run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		pl.sync(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (pl *peerListeners) Close() error {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	for _, p := range pl.open {
		_ = p.Close()
	}
	return nil
}

// stats lists each open peer listener's state.
func (pl *peerListeners) stats() map[int]proxy.Stats {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	out := map[int]proxy.Stats{}
	for i, p := range pl.open {
		out[i] = p.Stats()
	}
	return out
}
