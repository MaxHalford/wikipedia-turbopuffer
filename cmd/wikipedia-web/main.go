package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MaxHalford/wikipedia-turbopuffer/web"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	client, err := web.NewSearchClient(
		os.Getenv("TURBOPUFFER_API_KEY"),
		getenv("TURBOPUFFER_REGION", "gcp-us-central1"),
		os.Getenv("TURBOPUFFER_BASE_URL"),
		getenv("TURBOPUFFER_NAMESPACE", "wikipedia-en-live"),
	)
	if err != nil {
		logger.Error("configure search", "error", err)
		os.Exit(1)
	}
	server, err := web.NewServer(client, logger)
	if err != nil {
		logger.Error("configure server", "error", err)
		os.Exit(1)
	}

	httpServer := &http.Server{
		Addr:              ":" + getenv("PORT", "8080"),
		Handler:           server.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		logger.Info("listening", "address", httpServer.Addr)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve", "error", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()
	shutdown(httpServer)
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func shutdown(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
