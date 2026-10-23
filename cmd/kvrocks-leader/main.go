package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/kvrocks-leader/internal/config"
	"github.com/example/kvrocks-leader/internal/consulstore/live"
	"github.com/example/kvrocks-leader/internal/controller"
	"github.com/example/kvrocks-leader/internal/httpapi"
	"github.com/example/kvrocks-leader/internal/kvrocks"
	"github.com/example/kvrocks-leader/internal/logx"
	"github.com/example/kvrocks-leader/internal/metrics"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "handover" {
		os.Exit(runHandover(os.Args[2:], os.Stdout, os.Stderr))
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

	c := controller.New(cfg, controller.Deps{
		Session: lc, Lock: lc, Registrar: lc, KV: kv,
	}, log, reg)

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

	// SIGTERM: fast handover (deregister + release).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Info("received shutdown signal", nil)
		sctx, scancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer scancel()
		_ = srv.Shutdown(sctx)
		cancel()
	}()

	c.Run(ctx)
}
