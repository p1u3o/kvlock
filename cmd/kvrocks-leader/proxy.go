package main

import (
	"net"
	"time"

	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/logx"
	"github.com/p1u3o/kvlock/internal/metrics"
	"github.com/p1u3o/kvlock/internal/proxy"
)

type proxyViewer interface {
	ProxyView() controller.ProxyView
	Subscribe() <-chan struct{}
}

func writeRoute(v proxyViewer) proxy.Route {
	return func() proxy.Decision {
		pv := v.ProxyView()
		return proxy.Decision{Addr: pv.Leader, Key: pv.Key, Reason: pv.Reason, Refuse: pv.State == controller.RouteRefuse}
	}
}

func readRoute(v proxyViewer, fallbackLeader bool) proxy.Route {
	return func() proxy.Decision {
		pv := v.ProxyView()
		if pv.Replica != "" {
			return proxy.Decision{Addr: pv.Replica, Key: "replica|" + pv.Replica, Reason: "local replica healthy"}
		}
		if fallbackLeader {
			return proxy.Decision{Addr: pv.Leader, Key: pv.Key, Reason: "local replica unusable; " + nonEmpty(pv.Reason, "using leader"), Refuse: pv.State == controller.RouteRefuse}
		}
		return proxy.Decision{Reason: "local replica unusable"}
	}
}

func nonEmpty(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// forwardViewer is the controller surface the replication forwarder uses.
type forwardViewer interface {
	ForwardView() controller.ForwardRoute
	Subscribe() <-chan struct{}
}

// forwardRoute maps the controller's typed decision onto the proxy: only
// a transient state lets the forwarder hold a client.
func forwardRoute(v forwardViewer) proxy.Route {
	return func() proxy.Decision {
		r := v.ForwardView()
		return proxy.Decision{Addr: r.Addr, Key: r.Key, Reason: r.Reason, Refuse: r.Addr == "" && r.State != controller.RouteTransient}
	}
}

func startProxy(name, addr string, route proxy.Route, v proxyViewer, reg *metrics.Registry, log *logx.Logger) (*proxy.Proxy, error) {
	return startProxyHold(name, addr, route, v.Subscribe(), 0, reg, log)
}

func startProxyHold(name, addr string, route proxy.Route, notify <-chan struct{}, hold time.Duration, reg *metrics.Registry, log *logx.Logger) (*proxy.Proxy, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	p := &proxy.Proxy{Name: name, Route: route, Notify: notify, Hold: hold, Logf: log.Info, Counters: proxy.Counters{
		Reset:    func() { reg.ProxyResets.WithLabelValues(name).Inc() },
		Accepted: func() { reg.ProxyAccepted.WithLabelValues(name).Inc() },
		Bytes:    func(dir string, n int) { reg.ProxyBytes.WithLabelValues(name, dir).Add(float64(n)) },
		Active:   func(d float64) { reg.AddProxyActive(name, d) },
		Dropped:  func(n int) { reg.ProxyDropped.WithLabelValues(name).Add(float64(n)) },
		Refused:  func() { reg.ProxyRefused.WithLabelValues(name).Inc() },
	}}
	go func() {
		log.Info("proxy listening", map[string]any{"listener": name, "addr": addr})
		if err := p.Serve(ln); err != nil {
			log.Error("proxy failed", map[string]any{"listener": name, "error": err.Error()})
		}
	}()
	return p, nil
}
