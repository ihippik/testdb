# Test DB

A library that helps to perform integration testing with a database.

### Setup

Helps prepare your database for testing, namely filling your database with data.

You can manage data in the database with both batches of tables and single tables.

### Teardown

We can clear the data of all tables declared in the first **Setup** block.

### Example

```go
	tables := testdb.NewTables(conn, []testdb.Table{
		{
			Name: "test",
			Columns: []string{
				"id",
				"name",
				"created_at",
			},
			Data: [][]any{
				{
					uuid.MustParse("00000000-0000-0000-0000-000000000000"),
					"Elon Musk",
					time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
	})
	
	tables.Setup(ctx) // INSERT
	tables.Teardown(ctx) // TRUNCATE
```
