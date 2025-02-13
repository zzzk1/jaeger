// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type Pool interface {
	Do(ctx context.Context, query string, td ...ptrace.Traces) error
	Close() error
}

type Conn interface {
	QueryRow(ctx context.Context, query string, arg string) Row
	Query(ctx context.Context, query string, arg string) (Rows, error)
	Exec(ctx context.Context, query string) error
	Close() error
}

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	ScanStruct(dest any) error
	Err() error
}

type Row interface {
	Err() error
	Scan(dest ...any) error
	ScanStruct(dest any) error
}
