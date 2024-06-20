package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// Table represent database table Data.
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

const insertTemplate = "INSERT INTO %s (%s) VALUES(%s)"

// Prepare insert Data to a table with prepared Columns and Data.
func (t *Table) Prepare(ctx context.Context) error {
	if len(t.Columns) != len(t.Data) {
		return fmt.Errorf("Columns and Data must have the same length")
	}

	query := fmt.Sprintf(
		insertTemplate,
		t.Name,
		strings.Join(t.Columns, ", "),
		placeholders(len(t.Columns)),
	)

	for i, data := range t.Data {
		if _, err := t.conn.ExecContext(ctx, query, data...); err != nil {
			return fmt.Errorf("exec query for Data[%d]: %w", i, err)
		}
	}

	return nil
}

// Truncate remove all Data from specific table.
func (t *Table) Truncate(ctx context.Context) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s;", t.Name)

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
