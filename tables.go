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

// NewTables create new Tables instance.
func NewTables(db *sql.DB, tables []Table) *Tables {
	return &Tables{db: db, tables: tables}
}

// Truncate remove Data from all presented tables.
func (ts Tables) Truncate(ctx context.Context) error {
	names := make([]string, len(ts.tables))

	for _, table := range ts.tables {
		names = append(names, table.Name)
	}

	if _, err := ts.db.ExecContext(
		ctx,
		fmt.Sprintf("TRUNCATE TABLE %s;", strings.Join(names, ",")),
	); err != nil {
		return fmt.Errorf("error truncating tables: %w", err)
	}

	return nil
}

// Prepare Data for each presented table.
func (ts Tables) Prepare(ctx context.Context) error {
	for _, table := range ts.tables {
		table.conn = ts.db

		if err := table.Prepare(ctx); err != nil {
			return fmt.Errorf("error preparing table: %w", err)
		}
	}

	return nil
}
