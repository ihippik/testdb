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
	Name    string
	Columns []string
	Data    [][]any
}

// NewTable create new Table instance.
func NewTable(conn *sql.DB, name string, columns []string, data [][]any) *Table {
	return &Table{conn: conn, Name: name, Columns: columns, Data: data}
}

const (
	insertTemplate   = "INSERT INTO %s (%s) VALUES(%s);"
	truncateTemplate = "TRUNCATE %s;"
)

// Setup insert Data to a table with prepared Columns and Data.
func (t *Table) Setup(ctx context.Context) error {
	query := fmt.Sprintf(
		insertTemplate,
		t.Name,
		strings.Join(t.Columns, ", "),
		placeholders(len(t.Columns)),
	)

	for i, data := range t.Data {
		if len(t.Columns) != len(data) {
			return fmt.Errorf("columns and data[%d] must have the same length", i)
		}

		if _, err := t.conn.ExecContext(ctx, query, data...); err != nil {
			return fmt.Errorf("exec query for data[%d]: %w", i, err)
		}
	}

	return nil
}

// Teardown remove all data from specific table.
func (t *Table) Teardown(ctx context.Context) error {
	query := fmt.Sprintf(truncateTemplate, t.Name)

	if _, err := t.conn.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("exec query for %s: %w", t.Name, err)
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
