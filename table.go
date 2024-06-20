package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// Table represent database table data.
type Table struct {
	conn    *sql.DB
	name    string
	columns []string
	data    [][]any
}

const insertTemplate = "INSERT INTO %s (%s) VALUES(%s)"

// Prepare insert data to a table with prepared columns and data.
func (t *Table) Prepare(ctx context.Context) error {
	if len(t.columns) != len(t.data) {
		return fmt.Errorf("columns and data must have the same length")
	}

	query := fmt.Sprintf(
		insertTemplate,
		t.name,
		strings.Join(t.columns, ", "),
		placeholders(len(t.columns)),
	)

	for i, data := range t.data {
		if _, err := t.conn.ExecContext(ctx, query, data...); err != nil {
			return fmt.Errorf("exec query for data[%d]: %w", i, err)
		}
	}

	return nil
}

// Truncate remove all data from specific table.
func (t *Table) Truncate(ctx context.Context) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s;", t.name)

	if _, err := t.conn.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("exec query for %s: %w", t.name, err)
	}

	return nil
}

func placeholders(size int) string {
	var arr = make([]string, 0, size)

	for i := 0; i < size; i++ {
		arr = append(arr, fmt.Sprintf("$%d", i+1))
	}

	return strings.Join(arr, ", ")
}
