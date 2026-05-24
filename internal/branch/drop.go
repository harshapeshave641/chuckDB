package branch

import (
	"database/sql"
	"fmt"
)

// Drop removes a branch schema and all associated metadata.
func Drop(db *sql.DB, branchName string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var id int64
	var schemaName string
	err = tx.QueryRow("SELECT id, schema_name FROM chuck_meta.branches WHERE name = $1 AND dropped_at IS NULL", branchName).Scan(&id, &schemaName)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("branch %q not found", branchName)
		}
		return fmt.Errorf("failed to query branch metadata: %w", err)
	}

	// Drop schema with CASCADE to clean up views, triggers, delta tables
	_, err = tx.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
	if err != nil {
		return fmt.Errorf("failed to drop branch schema %s: %w", schemaName, err)
	}

	// Update metadata status to 'dropped' and record dropped_at time, renaming to free name/schema keys
	_, err = tx.Exec("UPDATE chuck_meta.branches SET name = name || '_dropped_' || id, schema_name = schema_name || '_dropped_' || id, status = 'dropped', dropped_at = now() WHERE id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to update branch metadata: %w", err)
	}

	// Clean up active_branch singleton if this branch was active
	_, err = tx.Exec("DELETE FROM chuck_meta.active_branch WHERE branch_id = $1", id)
	if err != nil {
		return fmt.Errorf("failed to cleanup active branch metadata: %w", err)
	}

	return tx.Commit()
}
