package controller

import (
	"time"

	"github.com/p1u3o/kvlock/internal/httpapi"
)

const maxEvents = 50

// event records a notable transition for the status page.
func (c *Controller) event(kind, msg string) {
	now := time.Now()
	c.evMu.Lock()
	defer c.evMu.Unlock()
	if kind == "lag_gate" {
		// Refusals repeat every tick; keep one per reason per 30s.
		if msg == c.lastRefusal && now.Sub(c.lastRefusalAt) < 30*time.Second {
			return
		}
		c.lastRefusal, c.lastRefusalAt = msg, now
	}
	c.events = append(c.events, httpapi.Event{Time: now.UTC().Format(time.RFC3339Nano), Kind: kind, Message: msg})
	if len(c.events) > maxEvents {
		c.events = append([]httpapi.Event(nil), c.events[len(c.events)-maxEvents:]...)
	}
}

// Events returns the recent events, newest first.
func (c *Controller) Events() []httpapi.Event {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	out := make([]httpapi.Event, len(c.events))
	for i, e := range c.events {
		out[len(c.events)-1-i] = e
	}
	return out
}
