package main

import (
	"net"

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

func startProxy(name, addr string, route proxy.Route, v proxyViewer, reg *metrics.Registry, log *logx.Logger) (*proxy.Proxy, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	p := &proxy.Proxy{Name: name, Route: route, Notify: v.Subscribe(), Logf: log.Info, Counters: proxy.Counters{
		Active:  func(d float64) { reg.AddProxyActive(name, d) },
		Dropped: func(n int) { reg.ProxyDropped.WithLabelValues(name).Add(float64(n)) },
		Refused: func() { reg.ProxyRefused.WithLabelValues(name).Inc() },
	}}
	go func() {
		log.Info("proxy listening", map[string]any{"listener": name, "addr": addr})
		if err := p.Serve(ln); err != nil {
			log.Error("proxy failed", map[string]any{"listener": name, "error": err.Error()})
		}
	}()
	return p, nil
}
