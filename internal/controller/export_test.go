package controller

import (
	"context"

	"github.com/p1u3o/kvlock/internal/consulstore"
)

// AddEventForTest adds an event (tests only).
func AddEventForTest(c *Controller, kind, msg string) { c.event(kind, msg) }

// ProbeAddrForTest exposes probeAddr (tests only).
func ProbeAddrForTest(c *Controller, node, addr string, port int) (string, int) {
	return c.probeAddr(&consulstore.LeaderValue{Node: node, Addr: addr, Port: port})
}

// LivePeerForTest exposes livePeer (tests only).
func LivePeerForTest(c *Controller, ctx context.Context) (string, error) { return c.livePeer(ctx) }
