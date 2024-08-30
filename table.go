package testdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// Table represent database table data.
type Table struct {
	conn    *sql.DB
	Name    string
	Keys    []string
	Columns []string
	Data    [][]any
}

// NewTable create new Table instance.
func NewTable(conn *sql.DB, name string, columns []string, data [][]any) *Table {
	return &Table{conn: conn, Name: name, Columns: columns, Data: data}
}

const (
	insertTemplate   = "INSERT INTO %s (%s) VALUES(%s);"
	truncateTemplate = "TRUNCATE %s CASCADE;"
	deleteTemplate   = "DELETE FROM %s WHERE %s;"
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

// Cleanup remove data from specific table by primary key.
func (t *Table) Cleanup(ctx context.Context) error {
	if len(t.Keys) == 0 {
		return errors.New("table must have at least one key")
	}

	query := t.prepareCleanupQuery(t.Keys...)

	args, err := t.prepareCleanupArgs(t.Keys...)
	if err != nil {
		return fmt.Errorf("prepare cleanup args for %s: %w", t.Name, err)
	}

	if _, err := t.conn.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("exec query for %s: %w", t.Name, err)
	}

	return nil
}

func (t *Table) prepareCleanupArgs(keys ...string) ([]any, error) {
	args := make([]any, 0, len(keys))
	indexes := make(map[string]int, len(t.Columns))

	for i, key := range t.Columns {
		indexes[key] = i
	}

	for _, data := range t.Data {
		for _, key := range keys {
			idx, ok := indexes[key]
			if !ok {
				return nil, fmt.Errorf("column '%s' not found in table %s", key, t.Name)
			}

			args = append(args, data[idx])
		}
	}

	return args, nil
}

func (t *Table) prepareCleanupQuery(keys ...string) string {
	var result []string

	var i int

	for range t.Data {
		var pairs []string

		for _, key := range keys {
			i += 1
			pairs = append(pairs, fmt.Sprintf("%s=$%d", key, i))
		}

		subWhere := strings.Join(pairs, " AND ")

		if len(t.Data) > 1 && len(pairs) > 1 {
			subWhere = fmt.Sprintf("(%s)", subWhere)
		}

		result = append(result, subWhere)
	}

	return fmt.Sprintf(deleteTemplate, t.Name, strings.Join(result, " OR "))
}

func placeholders(size int) string {
	var arr = make([]string, 0, size)

	for i := 0; i < size; i++ {
		arr = append(arr, fmt.Sprintf("$%d", i+1))
	}

	return strings.Join(arr, ", ")
}
