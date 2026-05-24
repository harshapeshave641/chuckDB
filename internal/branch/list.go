package branch

import (
	"database/sql"
	"fmt"
	"time"
)

type BranchSummary struct {
	Name       string
	SchemaName string
	Status     string
	CreatedAt  time.Time
	DeltaSizes map[string]int // table → row count in delta
}

// List returns all active branches with their status and delta table sizes.
func List(db *sql.DB) ([]BranchSummary, error) {
	rows, err := db.Query(`
		SELECT id, name, schema_name, status, created_at
		FROM chuck_meta.branches
		WHERE dropped_at IS NULL
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query branches: %w", err)
	}
	defer rows.Close()

	var summaries []BranchSummary
	for rows.Next() {
		var id int64
		var bs BranchSummary
		err := rows.Scan(&id, &bs.Name, &bs.SchemaName, &bs.Status, &bs.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan branch summary: %w", err)
		}

		bs.DeltaSizes = make(map[string]int)

		// Get all table names registered for this branch
		tRows, err := db.Query(`
			SELECT t.table_name
			FROM chuck_meta.branch_tables bt
			JOIN chuck_meta.tracked_tables t ON bt.table_id = t.id
			WHERE bt.branch_id = $1
		`, id)
		if err != nil {
			return nil, fmt.Errorf("failed to query branch tables: %w", err)
		}
		
		var tableNames []string
		for tRows.Next() {
			var name string
			if err := tRows.Scan(&name); err == nil {
				tableNames = append(tableNames, name)
			}
		}
		tRows.Close()

		// Get sizes of delta tables
		for _, tableName := range tableNames {
			var count int
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta", bs.SchemaName, tableName)
			err = db.QueryRow(query).Scan(&count)
			if err != nil {
				count = 0
			}
			bs.DeltaSizes[tableName] = count
		}

		summaries = append(summaries, bs)
	}

	return summaries, nil
}
