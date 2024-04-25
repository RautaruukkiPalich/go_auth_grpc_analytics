package db

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/config"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/lib/slerr"
	"github.com/rautaruukkipalich/go_auth_grpc_analytics/internal/models"
)

type DB interface {
	Run(chan models.Payload)
	Stop()
}

type ClickHouse struct {
	log  *slog.Logger
	done chan struct{}
	conn driver.Conn
}

func New(log *slog.Logger, cfg *config.ClickHouseConfig) *ClickHouse {
	const op = "storage.db.clickhouse.New"
	log.With(slog.String("op", op)).Info("start clickhouse")

	done := make(chan struct{})
	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)}, //"127.0.0.1:8123"
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": cfg.MaxExecutionTime,
		},
		DialTimeout: cfg.DialTimeout,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Protocol: clickhouse.HTTP,
	})

	if err != nil {
		log.Error("Error connecting to clickhouse", err)
		panic(err)
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
            log.Error("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
        }
		panic(err)
	}

	err = createDatabase(conn, log)
	if err != nil {
		log.Error("error create db", err)
	}

	return &ClickHouse{
		log:  log,
		done: done,
		conn: conn,
	}
}

func createDatabase(conn driver.Conn, log *slog.Logger) error {
	const op = "storage.db.clickhouse.CreateDatabase"
	log = log.With(slog.String("op", op))
	log.Info("create db")

	ctx := context.Background()

	// conn.Exec(ctx, "DROP TABLE IF EXIST example")

	err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS stat")
	if err != nil {
		log.Error("Create db", err)
	}
	
	err = conn.Exec(
		ctx,
		`CREATE TABLE IF NOT EXISTS stat.example (
				email String,
				time Datetime
			) engine=Memory
		`)
	
	if err != nil {
		log.Error("Error create table", err)
	}
	return err
}

func (h *ClickHouse) Run(msgch chan models.Payload) {
	const op = "storage.db.clickhouse.Run"
	log := h.log.With(slog.String("op", op))
	log.Info("start clickhouse")
		
	for {
		select {
			case data, ok :=<- msgch:
				if !ok {
					log.Error("channel closed")
				}
				if err := h.Push(&data); err != nil {
					log.Error("error add message", slerr.Err(err))
				}
			case <- h.done:
				return
		}
	}
}


func (h *ClickHouse) Stop() {
	const op = "storage.db.clickhouse.Stop"
	log := h.log.With(slog.String("op", op))
	log.Info("stop clickhouse")

	defer h.conn.Close()
	defer close(h.done)
}

func (h *ClickHouse) Push(msg *models.Payload) error {
	const op = "storage.db.clickhouse.Push"
	log := h.log.With(slog.String("op", op))
	log.Info("add message")

	ctx := context.Background()

	err := h.conn.Exec(
		ctx,
		`INSERT INTO stat.example VALUES ($1, now());`,
		msg.Email,
	)

	if err != nil {
		log.Error("err inserting message", err)
		return err
	}

	// rows, err := h.conn.Query(ctx, "SELECT email, time FROM stat.example")
	
	// if err != nil {
	// 	log.Error("err get rows", err)
	// 	return err
	// }
	// defer rows.Close()

	// for rows.Next() {
	// 	var (
	// 		col1 string
	// 		col2 time.Time
	// 	)
	// 	if err := rows.Scan(&col1, &col2); err != nil {
	// 		log.Error("err scan row", err)
	// 		return err
	// 	}
	// 	fmt.Printf("row: col1=%s, col2=%s\n", col1, col2)
	// }
	
	// fmt.Println(msg)
	
	return nil
}