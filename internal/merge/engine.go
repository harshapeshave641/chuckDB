package merge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Merge executes the full merge pipeline inside a SERIALIZABLE transaction.
func Merge(db *sql.DB, branchName string, dryRun bool) (mergeErr error) {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	startTime := time.Now()
	var branchID int64
	var fkViolationsJSON []byte
	var conflictSummaryJSON []byte

	// Record any failure outside the transaction block so it persists after rollback
	defer func() {
		if mergeErr != nil && branchID != 0 {
			_, _ = db.Exec(`
				INSERT INTO chuck_meta.merge_history (branch_id, success, fk_violations, conflict_summary, error_message, duration_ms)
				VALUES ($1, false, $2, $3, $4, $5)
			`, branchID, fkViolationsJSON, conflictSummaryJSON, mergeErr.Error(), time.Since(startTime).Milliseconds())
		}
	}()

	// 1. Fetch branch metadata
	var schemaName string
	var status string
	err = tx.QueryRow(`
		SELECT id, schema_name, status 
		FROM chuck_meta.branches 
		WHERE name = $1 AND dropped_at IS NULL
	`, branchName).Scan(&branchID, &schemaName, &status)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("branch %q not found", branchName)
		}
		return fmt.Errorf("failed to query branch metadata: %w", err)
	}

	if status == "merged" {
		return fmt.Errorf("branch %q is already merged", branchName)
	}

	if status == "locked" {
		// Check for stale lock (> 30 mins)
		var lockedAt time.Time
		err = tx.QueryRow(`
			SELECT locked_at 
			FROM chuck_meta.branches 
			WHERE id = $1
		`, branchID).Scan(&lockedAt)
		if err == nil && time.Since(lockedAt) > 30*time.Minute {
			// Break stale lock
			_, err = tx.Exec(`
				UPDATE chuck_meta.branches 
				SET status = 'active', locked_by = NULL, locked_at = NULL 
				WHERE id = $1
			`, branchID)
			if err != nil {
				return fmt.Errorf("failed to break stale lock: %w", err)
			}
			status = "active"
		} else {
			return fmt.Errorf("branch %q is locked (merge in progress)", branchName)
		}
	}

	// Lock the branch
	_, err = tx.Exec(`
		UPDATE chuck_meta.branches 
		SET status = 'locked', locked_by = 'merge-process', locked_at = now() 
		WHERE id = $1
	`, branchID)
	if err != nil {
		return fmt.Errorf("failed to lock branch: %w", err)
	}

	// 2. Fetch suspended FKs and tracked tables
	type TableMeta struct {
		Name        string
		PrimaryKeys []string
	}
	rows, err := tx.Query(`
		SELECT t.table_name, array_to_json(t.primary_keys), bt.suspended_fks
		FROM chuck_meta.branch_tables bt
		JOIN chuck_meta.tracked_tables t ON bt.table_id = t.id
		WHERE bt.branch_id = $1
	`, branchID)
	if err != nil {
		return fmt.Errorf("failed to query branch tables: %w", err)
	}
	defer rows.Close()

	var tables []TableMeta
	var allFKs []FKConstraint
	for rows.Next() {
		var tm TableMeta
		var pksJSON string
		var fksJSON string
		if err := rows.Scan(&tm.Name, &pksJSON, &fksJSON); err != nil {
			return fmt.Errorf("failed to scan table metadata: %w", err)
		}
		_ = json.Unmarshal([]byte(pksJSON), &tm.PrimaryKeys)
		
		var fks []FKConstraint
		_ = json.Unmarshal([]byte(fksJSON), &fks)
		allFKs = append(allFKs, fks...)
		tables = append(tables, tm)
	}
	rows.Close()

	// 3. Validate FKs
	violations, err := ValidateFKs(tx, schemaName, allFKs)
	if err != nil {
		return fmt.Errorf("FK validation failed: %w", err)
	}
	if len(violations) > 0 {
		fkViolationsJSON, _ = json.Marshal(violations)
		return fmt.Errorf("FK violations detected: %s", string(fkViolationsJSON))
	}

	// 4. Conflict Detection
	conflicts, err := DetectConflicts(tx, schemaName, branchName)
	if err != nil {
		return fmt.Errorf("conflict detection failed: %w", err)
	}
	if len(conflicts) > 0 {
		conflictSummaryJSON, _ = json.Marshal(conflicts)
		return fmt.Errorf("conflicts detected: %s", string(conflictSummaryJSON))
	}

	if dryRun {
		// If dry run, we rollback and exit without making modifications
		return nil
	}

	// 5. Remap PKs
	var tableNames []string
	for _, t := range tables {
		tableNames = append(tableNames, t.Name)
	}
	remaps, err := RemapNewRows(tx, schemaName, tableNames)
	if err != nil {
		return fmt.Errorf("PK remapping failed: %w", err)
	}

	// 6. Fix child FK references
	err = FixFKReferences(tx, schemaName, remaps, allFKs)
	if err != nil {
		return fmt.Errorf("fixing FK references failed: %w", err)
	}

	// 7. Sort tables in dependency order (parents before children)
	sortedTables := sortTables(tableNames, allFKs)

	// 8. Replay delta records to base tables
	for _, tblName := range sortedTables {
		// Find matching TableMeta to get primary keys
		var pks []string
		for _, t := range tables {
			if t.Name == tblName {
				pks = t.PrimaryKeys
				break
			}
		}
		if len(pks) == 0 {
			pks = []string{"id"}
		}

		// Query all column names of the base table
		cRows, err := tx.Query(`
			SELECT column_name 
			FROM information_schema.columns 
			WHERE table_schema = 'public' AND table_name = $1
		`, tblName)
		if err != nil {
			return fmt.Errorf("failed to fetch columns for base table %s: %w", tblName, err)
		}
		
		var cols []string
		for cRows.Next() {
			var col string
			if err := cRows.Scan(&col); err == nil {
				cols = append(cols, col)
			}
		}
		cRows.Close()

		if len(cols) == 0 {
			continue
		}

		colsList := strings.Join(cols, ", ")
		
		// Build sets for ON CONFLICT DO UPDATE
		var sets []string
		pkMap := make(map[string]bool)
		for _, pk := range pks {
			pkMap[pk] = true
		}
		for _, col := range cols {
			if !pkMap[col] {
				sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}
		}
		
		pksCSV := strings.Join(pks, ", ")
		upsertQuery := ""
		if len(sets) > 0 {
			upsertQuery = fmt.Sprintf(`
				INSERT INTO public.%s (%s)
				SELECT %s FROM %s.%s_delta
				WHERE __deleted = false
				ON CONFLICT (%s) DO UPDATE SET %s
			`, tblName, colsList, colsList, schemaName, tblName, pksCSV, strings.Join(sets, ", "))
		} else {
			// PK-only table
			upsertQuery = fmt.Sprintf(`
				INSERT INTO public.%s (%s)
				SELECT %s FROM %s.%s_delta
				WHERE __deleted = false
				ON CONFLICT (%s) DO NOTHING
			`, tblName, colsList, colsList, schemaName, tblName, pksCSV)
		}

		_, err = tx.Exec(upsertQuery)
		if err != nil {
			return fmt.Errorf("replay upsert failed on table %s: %w", tblName, err)
		}

		// Replay deletes using USING join syntax
		var deleteJoin []string
		for _, pk := range pks {
			deleteJoin = append(deleteJoin, fmt.Sprintf("b.%s = d.%s", pk, pk))
		}
		deleteQuery := fmt.Sprintf(`
			DELETE FROM public.%s b
			USING %s.%s_delta d
			WHERE d.__deleted = true AND %s
		`, tblName, schemaName, tblName, strings.Join(deleteJoin, " AND "))

		_, err = tx.Exec(deleteQuery)
		if err != nil {
			return fmt.Errorf("replay delete failed on table %s: %w", tblName, err)
		}
	}

	// 9. Update metadata
	remapsJSON, _ := json.Marshal(remaps)
	_, err = tx.Exec(`
		INSERT INTO chuck_meta.merge_history (branch_id, success, pk_remaps, duration_ms)
		VALUES ($1, true, $2, $3)
	`, branchID, remapsJSON, time.Since(startTime).Milliseconds())
	if err != nil {
		return fmt.Errorf("failed to record merge history: %w", err)
	}

	_, err = tx.Exec("UPDATE chuck_meta.branches SET status = 'merged', merged_at = now(), locked_by = NULL, locked_at = NULL WHERE id = $1", branchID)
	if err != nil {
		return fmt.Errorf("failed to update branch metadata: %w", err)
	}

	// Clear active branch singleton if it was this branch
	_, err = tx.Exec("DELETE FROM chuck_meta.active_branch WHERE branch_id = $1", branchID)
	if err != nil {
		return fmt.Errorf("failed to clear active branch: %w", err)
	}

	// Drop schema with CASCADE
	_, err = tx.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schemaName))
	if err != nil {
		return fmt.Errorf("failed to drop branch schema after merge: %w", err)
	}

	return tx.Commit()
}

func sortTables(tables []string, fks []FKConstraint) []string {
	visited := make(map[string]bool)
	var result []string
	
	var visit func(string)
	visit = func(name string) {
		if visited[name] {
			return
		}
		visited[name] = true
		for _, fk := range fks {
			if fk.SourceTable == name && fk.ReferencedTable != name {
				for _, t := range tables {
					if t == fk.ReferencedTable {
						visit(fk.ReferencedTable)
					}
				}
			}
		}
		result = append(result, name)
	}
	
	for _, t := range tables {
		visit(t)
	}
	return result
}
