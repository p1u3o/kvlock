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
	return func() (string, string, string) {
		pv := v.ProxyView()
		return pv.Leader, pv.Key, pv.Reason
	}
}

func readRoute(v proxyViewer, fallbackLeader bool) proxy.Route {
	return func() (string, string, string) {
		pv := v.ProxyView()
		if pv.Replica != "" {
			return pv.Replica, "replica|" + pv.Replica, "local replica healthy"
		}
		if fallbackLeader {
			return pv.Leader, pv.Key, "local replica unusable; " + nonEmpty(pv.Reason, "using leader")
		}
		return "", "", "local replica unusable"
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
	ForwardView() (addr, key, reason string)
	Subscribe() <-chan struct{}
}

func forwardRoute(v forwardViewer) proxy.Route { return v.ForwardView }

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
