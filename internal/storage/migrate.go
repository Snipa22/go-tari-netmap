package storage

import (
	"context"
	"embed"
	"fmt"
	"log"
	"path"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationsFS embeds the numbered .sql migration files applied at
// startup. Files whose name contains "_optional" are allowed to fail (see
// 0002_timescale_hypertable_optional.sql for why) — the runner logs the
// failure and continues instead of aborting the whole migration run.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationsDir = "migrations"

// migrate applies all pending migrations embedded in migrationsFS, tracking
// applied versions in a schema_migrations table so re-running it on every
// startup is idempotent.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("storage: create schema_migrations: %w", err)
	}

	entries, err := migrationsFS.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("storage: read migrations dir: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		var applied bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, name,
		).Scan(&applied); err != nil {
			return fmt.Errorf("storage: check migration %s: %w", name, err)
		}
		if applied {
			continue
		}

		contents, err := migrationsFS.ReadFile(path.Join(migrationsDir, name))
		if err != nil {
			return fmt.Errorf("storage: read migration %s: %w", name, err)
		}

		optional := strings.Contains(name, "_optional")

		if _, err := pool.Exec(ctx, string(contents)); err != nil {
			if optional {
				log.Printf("storage: optional migration %s failed, skipping (will retry next startup): %v", name, err)
				continue
			}
			return fmt.Errorf("storage: apply migration %s: %w", name, err)
		}

		if _, err := pool.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, name,
		); err != nil {
			return fmt.Errorf("storage: record migration %s: %w", name, err)
		}
	}

	return nil
}
