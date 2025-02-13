// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package conn

import (
	client "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Configuration struct {
	Address  []string `mapstructure:"address"`
	Database string   `mapstructure:"database"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
}

func DefaultConfig() Configuration {
	return Configuration{
		Address:  []string{"127.0.0.1:9000"},
		Database: "default",
		Username: "default",
		Password: "default",
	}
}

func NewConn(config Configuration) (driver.Conn, error) {
	option := client.Options{
		Addr: config.Address,
		Auth: client.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
	}

	conn, err := client.Open(&option)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
