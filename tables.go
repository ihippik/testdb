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

// Teardown remove Data from all presented tables.
func (ts *Tables) Teardown(ctx context.Context) error {
	names := make([]string, 0, len(ts.tables))

	for _, table := range ts.tables {
		names = append(names, table.Name)
	}

	query := fmt.Sprintf(truncateTemplate, strings.Join(names, ","))

	if _, err := ts.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("error truncating tables: %w", err)
	}

	return nil
}

// Setup Data for each presented table.
func (ts *Tables) Setup(ctx context.Context) error {
	for _, table := range ts.tables {
		table.conn = ts.db

		if err := table.Setup(ctx); err != nil {
			return fmt.Errorf("error preparing table: %w", err)
		}
	}

	return nil
}

// Cleanup remove Data from all presented tables by specific primary key.
func (ts *Tables) Cleanup(ctx context.Context, keys ...string) error {
	for _, table := range ts.tables {
		if err := table.Cleanup(ctx); err != nil {
			return fmt.Errorf("error cleanup table `%s`: %w", table.Name, err)
		}
	}

	return nil
}
