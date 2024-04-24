package clickhouse

import (
	"fmt"
	"log/slog"

	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/app/kafka"
	// "github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/config"
	// "github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/lib/slerr"
)

const (
	EmptyString = ""
)

var (
	ErrEmptyEmail = fmt.Errorf("empty email address")
	ErrEmptyMessage = fmt.Errorf("empty message")
)

type Clickhouse struct {
	done chan struct{}
	log    *slog.Logger
}

type IClickhouse interface {
	Run (msgch chan kafka.Payload)
	Stop ()
}

func New(log *slog.Logger) *Clickhouse {


	done := make(chan struct{})

	return &Clickhouse{
		done: done,
		log: log,
	}
}

func (c *Clickhouse) Run(msgch chan kafka.Payload) {
	const op = "app.clickhouse.app.Run"
	log := c.log.With(slog.String("op", op))
	log.Info("start clickhouse")
	
	for {
		select {
			case data, ok :=<- msgch:
				if !ok {
					c.log.Error("channel closed")
				}
				if err := c.addMessage(&data); err != nil {
					c.log.Error("add message")
				}
			case <- c.done:
				return
		}
	}
}

func (c *Clickhouse) Stop() {
	const op = "app.clickhouse.app.Stop"
	log := c.log.With(slog.String("op", op))
	log.Info("stop clickhouse")

	defer close(c.done)
}

func (c *Clickhouse) addMessage(msg *kafka.Payload) error {
	const op = "app.clickhouse.app.addMessage"
	log := c.log.With(slog.String("op", op))
	log.Info("add message")
	fmt.Println(msg)
	return nil 
}
