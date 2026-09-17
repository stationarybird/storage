package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributedcache/internal/cache"
	"distributedcache/internal/httpapi"
)

func main() {
	config := loadConfig()
	store, err := cache.OpenDurableStore(config.WALPath, config.Capacity)
	if err != nil {
		slog.Error("open durable store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	server := &http.Server{
		Addr:              config.ListenAddress,
		Handler:           httpapi.NewServer(store),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		slog.Info("cache node listening", "address", config.ListenAddress)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErrors <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stop)
	select {
	case <-stop:
	case err := <-serverErrors:
		slog.Error("server stopped unexpectedly", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
		_ = server.Close()
	}
}
