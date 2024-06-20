package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// Tables represent batch of tables.
type Tables struct {
	db     *sql.DB
	tables []Table
}

// Truncate remove data from all presented tables.
func (ts Tables) Truncate(ctx context.Context) error {
	names := make([]string, len(ts.tables))

	for _, table := range ts.tables {
		names = append(names, table.name)
	}

	if _, err := ts.db.ExecContext(
		ctx,
		fmt.Sprintf("TRUNCATE TABLE %s;", strings.Join(names, ",")),
	); err != nil {
		return fmt.Errorf("error truncating tables: %w", err)
	}

	return nil
}

// Prepare data for each presented table.
func (ts Tables) Prepare(ctx context.Context) error {
	for _, table := range ts.tables {
		table.conn = ts.db

		if err := table.Prepare(ctx); err != nil {
			return fmt.Errorf("error preparing table: %w", err)
		}
	}

	return nil
}
