package main

import (
	"context"
	"github.com/p1u3o/kvlock/internal/backup"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/p1u3o/kvlock/internal/config"
	"github.com/p1u3o/kvlock/internal/consulstore/live"
	"github.com/p1u3o/kvlock/internal/controller"
	"github.com/p1u3o/kvlock/internal/httpapi"
	"github.com/p1u3o/kvlock/internal/kvrocks"
	"github.com/p1u3o/kvlock/internal/logx"
	"github.com/p1u3o/kvlock/internal/metrics"
	"github.com/p1u3o/kvlock/internal/proxy"
	"github.com/p1u3o/kvlock/internal/sentinel"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "handover" {
		os.Exit(runHandover(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "watchdog" {
		os.Exit(runWatchdog(os.Args[2:], os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "status" {
		os.Exit(runStatus(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "loadtest" {
		os.Exit(runLoadtest(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "restore" {
		os.Exit(runRestore(os.Args[2:], os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "backup-decrypt" {
		os.Exit(runDecrypt(os.Args[2:], os.Stdin, os.Stdout, os.Stderr))
	}
	cfg, err := config.Parse(os.Args[1:])
	if err != nil {
		os.Stderr.WriteString("config error: " + err.Error() + "\n")
		os.Exit(2)
	}
	log := logx.New(cfg.NodeName, cfg.Mode)
	reg := metrics.New()

	kv := kvrocks.New(cfg.KvrocksAddr, cfg.KvrocksPort, cfg.KvrocksPassword, cfg.KvrocksTimeout, cfg.KVKind)
	defer kv.Close()

	lc, err := live.New(cfg.ConsulAddr, cfg.ConsulToken, cfg.ConsulTLSCA, cfg.ConsulTLSSkipVerify)
	if err != nil {
		log.Error("consul client failed", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	if cfg.AdvertiseAddrPrivate != "" {
		lc.SetPrivate(cfg.PrivateServiceName, cfg.AdvertiseAddrPrivate)
	}

	deps := controller.Deps{Session: lc, Lock: lc, Registrar: lc, KV: kv}
	if cfg.Backup.Enabled && cfg.Mode != "observe" {
		st, err := newStore(&cfg.Backup)
		if err != nil {
			log.Error("backup store failed", map[string]any{"error": err.Error()})
			os.Exit(1)
		}
		b := cfg.Backup
		deps.Backup = backup.NewRunner(backup.Config{
			Cluster: cfg.Cluster, Node: cfg.NodeName, Dir: b.Dir, Prefix: b.Prefix, Key: b.Key,
			Interval: b.Interval, Keep: b.Keep, AllowLeader: b.AllowLeader, LeaderGrace: b.LeaderGrace,
			BGSaveTimeout: b.BGSaveTimeout,
		}, kv, st, lc, reg, nil)
		log.Info("backups enabled", map[string]any{"bucket": b.Bucket, "prefix": b.Prefix, "interval": b.Interval.String(), "sse": b.SSE})
	} else if cfg.Backup.Enabled {
		log.Warn("backups disabled in observe mode", nil)
	}
	c := controller.New(cfg, deps, log, reg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// HTTP: /healthz /status /metrics /handover. Independent of Nomad checks by design.
	admin := httpapi.Admin{Token: cfg.AdminToken, Handover: c}
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: httpapi.Handler(c, admin), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("http listening", map[string]any{"addr": cfg.HTTPAddr})
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http failed", map[string]any{"error": err.Error()})
		}
	}()

	var sent *sentinel.Server
	if cfg.SentinelAddr != "" {
		ln, err := net.Listen("tcp", cfg.SentinelAddr)
		if err != nil {
			log.Error("sentinel listener failed", map[string]any{"error": err.Error()})
			os.Exit(1)
		}
		sent = sentinel.New(cfg.SentinelName, &sentinelSource{cfg: cfg, ctl: c, cat: lc}, 500*time.Millisecond, log.Info)
		go func() {
			log.Info("sentinel endpoint listening", map[string]any{"addr": cfg.SentinelAddr, "name": cfg.SentinelName})
			if err := sent.Serve(ln); err != nil {
				log.Error("sentinel endpoint failed", map[string]any{"error": err.Error()})
			}
		}()
	}

	var proxies []*proxy.Proxy
	for _, pc := range []struct {
		name, addr string
		route      proxy.Route
	}{
		{"write", cfg.ProxyAddr, writeRoute(c)},
		{"read", cfg.ProxyReadAddr, readRoute(c, cfg.ProxyReadFallback == "leader")},
	} {
		if pc.addr == "" {
			continue
		}
		p, err := startProxy(pc.name, pc.addr, pc.route, c, reg, log)
		if err != nil {
			log.Error("proxy listener failed", map[string]any{"listener": pc.name, "error": err.Error()})
			os.Exit(1)
		}
		proxies = append(proxies, p)
	}

	// SIGTERM: deregister, fence the local instance, then release the lock.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Info("received shutdown signal", nil)
		sctx, scancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer scancel()
		_ = srv.Shutdown(sctx)
		for _, p := range proxies {
			_ = p.Close()
		}
		if sent != nil {
			_ = sent.Close()
		}
		cancel()
	}()

	c.SetInfo(version, true, func() map[string]int {
		m := reg.ProxyActiveCounts()
		if sent != nil {
			m["sentinel"] = sent.NumConns()
		}
		return m
	})
	c.Run(ctx)
}
