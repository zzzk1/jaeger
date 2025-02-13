// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package wrapper

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	v2Client "github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/client"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/internal"
)

// PoolWrapper is a wrapper around clickhouse-go used by read path, ch-go used by write path.
type PoolWrapper struct {
	Pool *chpool.Pool
}

// Do calls this function to internal pool.
func (c PoolWrapper) Do(ctx context.Context, query string, td ...ptrace.Traces) error {
	q := ch.Query{Body: query}
	if td != nil {
		q.Input = internal.Input(td[0])
	}
	return c.WrapDo(ctx, q)
}

func (c PoolWrapper) Close() error {
	if c.Pool != nil {
		c.Pool.Close()
	}
	return nil
}

func WarpPool(pool *chpool.Pool) PoolWrapper {
	return PoolWrapper{Pool: pool}
}

func (c PoolWrapper) WrapDo(ctx context.Context, query ch.Query) error {
	return c.Pool.Do(ctx, query)
}

// ConnWrapper is a wrapper around clickhouse-go used by read path, ch-go used by write path.
type ConnWrapper struct {
	Conn v2Client.Conn
}

func WarpConn(conn v2Client.Conn) ConnWrapper {
	return ConnWrapper{Conn: conn}
}

// Query calls this function to internal connection.
func (c ConnWrapper) Query(ctx context.Context, query string, args string) (client.Rows, error) {
	return c.WrapQuery(ctx, query, args)
}

// QueryRow calls this function to internal connection.
func (c ConnWrapper) QueryRow(ctx context.Context, query string, args string) client.Row {
	return c.WrapQueryRow(ctx, query, args)
}

func (c ConnWrapper) Exec(ctx context.Context, query string) error {
	return c.WrapExec(ctx, query)
}

// Close closes connection and pool.
func (c ConnWrapper) Close() error {
	if c.Conn != nil {
		err := c.Conn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c ConnWrapper) WrapQuery(ctx context.Context, query string, args string) (client.Rows, error) {
	return c.Conn.Query(ctx, query, args)
}

func (c ConnWrapper) WrapQueryRow(ctx context.Context, query string, args string) client.Row {
	return c.Conn.QueryRow(ctx, query, args)
}

func (c ConnWrapper) WrapExec(ctx context.Context, query string) error {
	return c.Conn.Exec(ctx, query)
}
