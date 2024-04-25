package config

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env        string           `yaml:"env"`
	Kafka      KafkaConfig      `yaml:"kafka" env_required:"true"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse" env_required:"true"`
}

type KafkaConfig struct {
	Host          string `yaml:"host"`
	Port          string `yaml:"port"`
	ConsumerGroup string `yaml:"consumerGroup"`
	Topic         string `yaml:"topic"`
}

type ClickHouseConfig struct {
	Host             string        `yaml:"host"`
	Port             string        `yaml:"port"`
	Database         string        `yaml:"database"`
	Username         string        `yaml:"username"`
	Password         string        `yaml:"password"`
	MaxExecutionTime int           `yaml:"max_execution_time"`
	DialTimeout      time.Duration `yaml:"dial_timeout"`
}

func MustLoadConfig() *Config {
	path := fetchConfigPath()

	if path == "" {
		panic("config path is empty")
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		panic("config file does not exist: " + path)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		panic(fmt.Sprintf("can not parse config, %v", err))
	}

	return &cfg
}

func fetchConfigPath() string {
	var res string

	flag.StringVar(&res, "config", "", "path to config file")
	flag.Parse()

	return res
}
