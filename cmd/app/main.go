package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/config"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/app"
	"github.com/rautaruukkipalich/prettyslog"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

func main() {
	cfg := config.MustLoadConfig()

	log := MustRunLogger(cfg.Env)

	log.Info("logger initialized")

	application := app.New(log, cfg)

	go application.Run()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	<-stop

	// stop server
	application.Stop()
	log.Info("application stopped")
}

func MustRunLogger(env string) *slog.Logger {
	var log *slog.Logger
	switch env {
	case envLocal:
		log = prettyslog.NewPrettyLogger(" ")
	case envDev:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envProd:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	default:
		panic("unknown env: " + env)
	}
	return log
}