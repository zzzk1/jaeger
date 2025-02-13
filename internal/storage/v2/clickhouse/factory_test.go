// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package clickhouse

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/client"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/client/mocks"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/config"
	"github.com/jaegertracing/jaeger/pkg/testutils"
)

type mockPoolBuilder struct {
	error error
}

func (m *mockPoolBuilder) NewPool(*config.Configuration, *zap.Logger) (client.Pool, error) {
	if m.error == nil {
		c := &mocks.Pool{}
		c.On("Do", context.Background(), mock.Anything, mock.Anything).Return(nil)
		c.On("Close").Return(nil)
		return c, nil
	}
	return nil, m.error
}

type mockConnBuilder struct {
	err error
}

func (m *mockConnBuilder) NewConn(*config.Configuration) (client.Conn, error) {
	if m.err == nil {
		c := &mocks.Conn{}
		c.On("QueryRow", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		c.On("Close").Return(nil)
		return c, nil
	}
	return nil, m.err
}

func TestTraceFactory(t *testing.T) {
	var err error
	cfg := config.Configuration{}
	f := newFactory()

	poolBuilder := &mockPoolBuilder{}

	f.chPool, err = poolBuilder.NewPool(&cfg, zap.NewNop())
	require.NoError(t, err)

	connBuilder := &mockConnBuilder{}
	f.connection, err = connBuilder.NewConn(&cfg)
	require.NoError(t, err)

	_, err = f.CreateTraceWriter()
	require.NoError(t, err)
	_, err = f.CreateTracReader()
	require.NoError(t, err)

	require.NoError(t, f.connection.Close())
}

func TestMain(m *testing.M) {
	testutils.VerifyGoLeaks(m)
}
