// Package storage will provide persistence for topology and node-health
// data. The target backend is Postgres + TimescaleDB, on a dedicated
// instance (not shared infra).
//
// TODO(netmap): implement in the collector/storage/API dispatch. This is
// currently a scaffold-only stub with no real DB driver wired in.
package storage

import (
	"context"
	"errors"
)

// ErrNotImplemented is returned by stub Store methods that have not yet
// been implemented.
var ErrNotImplemented = errors.New("storage: not implemented")

// Store is the persistence interface for topology and node-health data.
type Store interface {
	// Ping checks connectivity to the underlying storage backend.
	Ping(ctx context.Context) error

	// Close releases any resources held by the Store.
	Close() error
}

// stubStore is a placeholder Store implementation that does not talk to any
// real database yet.
type stubStore struct{}

// New returns a placeholder Store.
//
// TODO(netmap): implement in the collector/storage/API dispatch. This will
// wire up a real Postgres/TimescaleDB driver.
func New() Store {
	return &stubStore{}
}

// Ping is a no-op stub; it always returns nil.
func (s *stubStore) Ping(ctx context.Context) error {
	return nil
}

// Close is a no-op stub; it always returns nil.
func (s *stubStore) Close() error {
	return nil
}
