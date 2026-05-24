package merge

import (
	"database/sql"
	"fmt"
	"time"
)

type Conflict struct {
	Table     string
	RowID     interface{}
	BranchVal interface{}
	BaseVal   interface{}
}

// DetectConflicts finds rows modified in both the branch and base since the branch was created.
// A conflict = same PK modified in branch delta AND in base table after branch.created_at timestamp.
func DetectConflicts(tx *sql.Tx, branchSchema, branchName string) ([]Conflict, error) {
	var createdAt time.Time
	err := tx.QueryRow("SELECT created_at FROM chuck_meta.branches WHERE name = $1 AND dropped_at IS NULL", branchName).Scan(&createdAt)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch branch created_at: %w", err)
	}

	rows, err := tx.Query(`
		SELECT t.table_schema, t.table_name
		FROM chuck_meta.branch_tables bt
		JOIN chuck_meta.tracked_tables t ON bt.table_id = t.id
		JOIN chuck_meta.branches b ON bt.branch_id = b.id
		WHERE b.name = $1
	`, branchName)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch branch tables: %w", err)
	}
	defer rows.Close()

	type TableRef struct {
		Schema string
		Name   string
	}
	var tables []TableRef
	for rows.Next() {
		var t TableRef
		if err := rows.Scan(&t.Schema, &t.Name); err == nil {
			tables = append(tables, t)
		}
	}
	rows.Close()

	var conflicts []Conflict
	for _, t := range tables {
		var hasUpdatedAt bool
		err = tx.QueryRow(`
			SELECT EXISTS (
				SELECT 1 
				FROM information_schema.columns 
				WHERE table_schema = $1 AND table_name = $2 AND column_name = 'updated_at'
			)
		`, t.Schema, t.Name).Scan(&hasUpdatedAt)
		if err != nil || !hasUpdatedAt {
			continue
		}

		query := fmt.Sprintf(`
			SELECT d.id
			FROM %s.%s_delta d
			JOIN %s.%s b ON d.id = b.id
			WHERE b.updated_at > $1
		`, branchSchema, t.Name, t.Schema, t.Name)

		cRows, err := tx.Query(query, createdAt)
		if err != nil {
			continue
		}
		defer cRows.Close()

		for cRows.Next() {
			var id int64
			if err := cRows.Scan(&id); err == nil {
				conflicts = append(conflicts, Conflict{
					Table: t.Name,
					RowID: id,
				})
			}
		}
		cRows.Close()
	}

	return conflicts, nil
}
