package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mhamillwork/unicast-to-multicast-relay/internal/config"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/health"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/metrics"
	"github.com/mhamillwork/unicast-to-multicast-relay/internal/relay"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// version is set with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "mcast-relay: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("mcast-relay", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	defaultConfig := os.Getenv("MCAST_CONFIG")
	if defaultConfig == "" {
		defaultConfig = "/etc/mcast-relay/config.yaml"
	}
	configPath := fs.String("config", defaultConfig, "path to the YAML configuration")
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *showVersion {
		fmt.Println(version)
		return nil
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	var level slog.LevelVar
	logger, err := newLogger(&level, cfg.LogLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	logger.Info("starting", "version", version, "egress_interface", cfg.EgressInterface, "metrics_listen", cfg.MetricsListen)

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewGoCollector())
	met := metrics.New(reg, version)
	mgr := relay.NewManager(logger, met)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Apply(ctx, cfg); err != nil {
		return err
	}

	srv, err := health.Listen(cfg.MetricsListen, reg, mgr)
	if err != nil {
		shut(mgr)
		return err
	}
	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(); err != nil {
			serveErr <- err
		}
	}()
	logger.Info("health endpoints", "listen", srv.Addr(), "paths", []string{"/healthz", "/readyz", "/metrics"})

	sig := make(chan os.Signal, 4)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sig)

	for {
		select {
		case err := <-serveErr:
			cancel()
			shut(mgr)
			return fmt.Errorf("http server: %w", err)
		case s := <-sig:
			switch s {
			case syscall.SIGHUP:
				logger.Info("reload requested")
				next, err := config.Load(*configPath)
				if err != nil {
					logger.Error("reload rejected", "err", err)
					continue
				}
				if err := setLevel(&level, next.LogLevel); err != nil {
					logger.Error("reload rejected", "err", err)
					continue
				}
				if err := mgr.Apply(ctx, next); err != nil {
					logger.Error("reload failed", "err", err)
					continue
				}
				if next.MetricsListen != cfg.MetricsListen {
					if err := swapServer(srv, next.MetricsListen, reg, mgr, serveErr, logger); err != nil {
						logger.Error("metrics listen change failed", "err", err)
					}
				}
				cfg = next
				logger.Info("configuration reloaded")
			default:
				logger.Info("shutting down", "signal", s.String())
				cancel()
				shutCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
				mgr.Shutdown(shutCtx)
				_ = srv.Shutdown(shutCtx)
				done()
				return nil
			}
		}
	}
}

func swapServer(current *health.Server, addr string, reg prometheus.Gatherer, ready health.Checker, serveErr chan error, logger *slog.Logger) error {
	next, err := health.Listen(addr, reg, ready)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = current.Shutdown(ctx)
	*current = *next
	go func() {
		if err := current.Serve(); err != nil {
			serveErr <- err
		}
	}()
	logger.Info("metrics listener moved", "listen", current.Addr())
	return nil
}

func shut(mgr *relay.Manager) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	mgr.Shutdown(ctx)
}

func newLogger(level *slog.LevelVar, name string) (*slog.Logger, error) {
	if err := setLevel(level, name); err != nil {
		return nil, err
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})), nil
}

func setLevel(level *slog.LevelVar, name string) error {
	switch name {
	case "debug":
		level.Set(slog.LevelDebug)
	case "info":
		level.Set(slog.LevelInfo)
	case "warn":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		return fmt.Errorf("invalid log level %q", name)
	}
	return nil
}
