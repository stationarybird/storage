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
	store := cache.NewMemoryStore(config.Capacity)

	server := &http.Server{
		Addr:              config.ListenAddress,
		Handler:           httpapi.NewServer(store),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("cache node listening", "address", config.ListenAddress)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("graceful shutdown failed", "error", err)
	}
}
