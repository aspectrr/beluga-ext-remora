// remora is the Beluga remote daemon.
// It runs on remote hosts, executes whitelisted read-only commands,
// and syncs directory contents back to beluga via gRPC.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/collinpfeifer/beluga-ext-remora/internal/client"
	"github.com/collinpfeifer/beluga-ext-remora/internal/config"
	"github.com/collinpfeifer/beluga-ext-remora/internal/executor"
)

func main() {
	configPath := flag.String("config", "/etc/beluga-remora/config.yaml", "path to config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	if cfg.LogLevel == "debug" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
		slog.SetDefault(logger)
	}

	hostname, _ := os.Hostname()

	slog.Info("remora starting",
		"beluga_address", cfg.Beluga.Address,
		"allowed_dirs", cfg.AllowedDirectories,
		"allowed_cmds", cfg.AllowedCommands,
		"hostname", hostname,
	)

	exec := executor.NewExecutor(cfg.AllowedDirectories, cfg.CommandTimeout)

	remoraClient := client.NewClient(client.ClientConfig{
		BelugaAddress:     cfg.Beluga.Address,
		HostID:            hostname,
		Hostname:          hostname,
		AllowedDirs:       cfg.AllowedDirectories,
		ReconnectInterval: cfg.Beluga.ReconnectInterval,
		TLSCert:           cfg.Beluga.TLS.Cert,
		TLSKey:            cfg.Beluga.TLS.Key,
		TLSCA:             cfg.Beluga.TLS.CA,
	}, exec, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := remoraClient.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("gRPC client exited with error", "error", err)
		}
	}()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if remoraClient.IsConnected() {
					if err := remoraClient.SendHeartbeat(); err != nil {
						slog.Warn("heartbeat failed", "error", err)
					}
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("received shutdown signal", "signal", sig)
	case <-ctx.Done():
		slog.Info("context cancelled")
	}

	slog.Info("remora shutting down gracefully")
	if err := remoraClient.Close(); err != nil {
		slog.Error("error closing client", "error", err)
	}
}
