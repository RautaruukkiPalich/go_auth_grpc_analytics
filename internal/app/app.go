package app

import (
	"log/slog"

	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/app/kafka"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/app/clickhouse"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/config"
)

type App struct {
	Clickhouse clickhouse.IClickhouse
	Kafka kafka.Consumer
	log *slog.Logger
}

func New(log *slog.Logger, cfg *config.Config) *App{
	kafka := kafka.New(log, &cfg.Kafka)
	clickhouse := clickhouse.New(log)

	return &App{
		Clickhouse: clickhouse,
		Kafka: kafka,
		log: log,
	}
}

func (a *App) Run() {
	const op = "app.app.Run"
	log := a.log.With(slog.String("op", op))
	log.Info("starting app")

	msgch := a.Kafka.Run()
	a.Clickhouse.Run(msgch)
}

func (a *App) Stop() {
	const op = "app.app.Stop"
	log := a.log.With(slog.String("op", op))
	log.Info("stopping app")

	defer a.Kafka.Stop()
	defer a.Clickhouse.Stop()
}


