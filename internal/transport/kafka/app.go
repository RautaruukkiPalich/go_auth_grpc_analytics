package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/config"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/models"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/lib/slerr"
	"github.com/segmentio/kafka-go"
)



type Broker struct {
	broker *kafka.Reader
	log    *slog.Logger
	done   chan struct{}
	ticker *time.Ticker
}

const (
	timeout = time.Millisecond * 1000
)

func New(log *slog.Logger, cfg *config.KafkaConfig) *Broker {
	const op = "transport.kafka.app.New"
	log.With(slog.String("op", op)).Info("start kafka consumer")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)},
		GroupID:   cfg.ConsumerGroup,
		Topic:     cfg.Topic,
		MaxBytes:  10e6, // 10MB
	})

	ticker := time.NewTicker(timeout)
	done := make(chan struct{})

	return &Broker{
		broker: r,
		log:    log,
		done:   done,
		ticker: ticker,
	}
}

func (b *Broker) Run() chan models.Payload {
	const op = "transport.kafka.app.Run"
	log := b.log.With(slog.String("op", op))

	log.Info("run kafka consumer")

	msgch := make(chan models.Payload)

	go func() {
		for {
			select {
			case <- b.done:
				return
			case <- b.ticker.C:
				m, err := b.broker.ReadMessage(context.Background())
				if err != nil {
					log.Error("error read message", slerr.Err(err))
				}
				var data models.Message
				err = json.Unmarshal(m.Value, &data)
				if err != nil {
					log.Error("error unmarshalling", op, slerr.Err(err))
				}
				msgch <- data.Payload
			}
		}
	}()

	return msgch
}

func (b *Broker) Stop() {
	const op = "transport.kafka.app.Stop"
	log := b.log.With(slog.String("op", op))
	log.Info("stop kafka client")

	close(b.done)
	defer b.ticker.Stop()
	defer b.broker.Close()
}