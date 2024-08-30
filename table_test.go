package testdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTable_prepareCleanupQuery(t1 *testing.T) {
	type fields struct {
		Name    string
		Columns []string
		Data    [][]any
	}

	tests := []struct {
		name   string
		args   []string
		fields fields
		want   string
	}{
		{
			name: "id primary key",
			args: []string{"id"},
			fields: fields{
				Name: "users",
				Columns: []string{
					"id",
					"name",
					"email",
				},
				Data: [][]any{
					{
						"myid",
						"Elon",
						"elon@test.ts",
					},
				},
			},
			want: "DELETE FROM users WHERE id=$1;",
		},
		{
			name: "id primary key: two rows",
			args: []string{"id"},
			fields: fields{
				Name: "users",
				Columns: []string{
					"id",
					"name",
					"email",
				},
				Data: [][]any{
					{
						"myid",
						"Elon",
						"elon@test.ts",
					},
					{
						"myid2",
						"Donald",
						"donald@test.ts",
					},
				},
			},
			want: "DELETE FROM users WHERE id=$1 OR id=$2;",
		},
		{
			name: "composite primary key",
			args: []string{"name", "email"},
			fields: fields{
				Name: "users",
				Columns: []string{
					"id",
					"name",
					"email",
				},
				Data: [][]any{
					{
						"myid",
						"Elon",
						"elon@test.ts",
					},
				},
			},
			want: "DELETE FROM users WHERE name=$1 AND email=$2;",
		},
		{
			name: "composite primary key: two rows",
			args: []string{"name", "email"},
			fields: fields{
				Name: "users",
				Columns: []string{
					"id",
					"name",
					"email",
				},
				Data: [][]any{
					{
						"myid",
						"Elon",
						"elon@test.ts",
					},
					{
						"myid2",
						"Donald",
						"donald@test.ts",
					},
				},
			},
			want: "DELETE FROM users WHERE (name=$1 AND email=$2) OR (name=$3 AND email=$4);",
		},
	}

	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Table{
				Name:    tt.fields.Name,
				Columns: tt.fields.Columns,
				Data:    tt.fields.Data,
			}

			if got := t.prepareCleanupQuery(tt.args...); got != tt.want {
				t1.Errorf("prepareCleanupQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTable_prepareCleanupArgs(t1 *testing.T) {
	type fields struct {
		Name    string
		Columns []string
		Data    [][]any
	}

	type args struct {
		keys []string
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []any
		wantErr error
	}{
		{
			name: "one row",
			fields: fields{
				Name:    "users",
				Columns: []string{"id", "name", "email"},
				Data: [][]any{
					{
						"myid",
						"Elon",
						"elon@test.ts",
					},
				},
			},
			args: args{
				keys: []string{"name", "email"},
			},
			want:    []any{"Elon", "elon@test.ts"},
			wantErr: nil,
		},
		{
			name: "two rows",
			fields: fields{
				Name:    "users",
				Columns: []string{"id", "name", "email"},
				Data: [][]any{
					{
						"myid",
						"Elon",
						"elon@test.ts",
					},
					{
						"myid2",
						"Donald",
						"donald@test.ts",
					},
				},
			},
			args: args{
				keys: []string{"name", "email"},
			},
			want:    []any{"Elon", "elon@test.ts", "Donald", "donald@test.ts"},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &Table{
				Name:    tt.fields.Name,
				Columns: tt.fields.Columns,
				Data:    tt.fields.Data,
			}

			got, err := t.prepareCleanupArgs(tt.args.keys...)
			if err != nil && assert.Error(t1, tt.wantErr, err.Error()) {
				assert.EqualError(t1, err, tt.wantErr.Error())
			}

			assert.Equal(t1, tt.want, got)
		})
	}
}
