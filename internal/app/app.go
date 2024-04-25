package app

import (
	"log/slog"

	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/transport/kafka"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/config"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/models"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/storage/db"
)


type Consumer interface {
	Run() chan models.Payload
	Stop()
}

type App struct {
	DB db.DB
	Kafka Consumer
	log *slog.Logger
}

func New(log *slog.Logger, cfg *config.Config) *App{
	// kafka := kafka.New(log, &cfg.Kafka)
	kafka := kafka.New(log, &cfg.Kafka)
	db := db.New(log, &cfg.ClickHouse)
	log.Info("config: ", cfg)
	
	return &App{
		DB: db,
		Kafka: kafka,
		log: log,
	}
}

func (a *App) Run() {
	const op = "app.app.Run"
	log := a.log.With(slog.String("op", op))
	log.Info("starting app")

	msgch := a.Kafka.Run()

	a.DB.Run(msgch)
}

func (a *App) Stop() {
	const op = "app.app.Stop"
	log := a.log.With(slog.String("op", op))
	log.Info("stopping app")

	defer a.Kafka.Stop()
	defer a.DB.Stop()
}


