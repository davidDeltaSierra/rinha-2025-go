package database

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync"
)

var (
	pool                *pgxpool.Pool
	once                sync.Once
	ConnectionPoolError error
)

func GetConnectionPool() (*pgxpool.Pool, error) {
	if ConnectionPoolError != nil {
		once = sync.Once{}
		ConnectionPoolError = nil
	}
	once.Do(func() {
		var config *pgxpool.Config
		config, ConnectionPoolError = pgxpool.ParseConfig("postgres://postgres:root@postgres:5432/rinha?pool_min_conns=16&pool_max_conns=16")
		if ConnectionPoolError != nil {
			return
		}
		pool, ConnectionPoolError = pgxpool.NewWithConfig(context.Background(), config)
	})
	if ConnectionPoolError != nil {
		ConnectionPoolError = errors.New(ConnectionPoolError.Error())
		return nil, ConnectionPoolError
	}
	ConnectionPoolError = pool.Ping(context.Background())
	if ConnectionPoolError != nil {
		ConnectionPoolError = errors.New(ConnectionPoolError.Error())
		return nil, ConnectionPoolError
	}
	return pool, nil
}
