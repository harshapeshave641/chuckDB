package branch

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/chuckdb/chuck/internal/delta"
)

var branchNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// Create orchestrates full branch creation for all tracked tables.
func Create(db *sql.DB, branchName string) error {
	if !branchNameRegex.MatchString(branchName) {
		return fmt.Errorf("invalid branch name: must be alphanumeric and underscores only")
	}
	if len(branchName) > 57 {
		return fmt.Errorf("branch name %q too long: maximum is 57 characters", branchName)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if active branch already exists with this name (active/non-dropped)
	var exists bool
	err = tx.QueryRow("SELECT EXISTS(SELECT 1 FROM chuck_meta.branches WHERE name = $1 AND dropped_at IS NULL)", branchName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check branch existence: %w", err)
	}
	if exists {
		return fmt.Errorf("branch %q already exists", branchName)
	}

	// 1. Resolve parent branch and base commit if an active branch exists
	var parentBranchID *int64
	var baseCommitID []byte // UUID can be scanned as slice of bytes/string
	
	var activeBranchID int64
	err = tx.QueryRow("SELECT branch_id FROM chuck_meta.active_branch LIMIT 1").Scan(&activeBranchID)
	if err == nil {
		parentBranchID = &activeBranchID
		// Fetch parent's latest commit as base_commit_id
		var commitUUID string
		err = tx.QueryRow(`
			SELECT id::text 
			FROM chuck_meta.commits 
			WHERE branch_id = $1 
			ORDER BY created_at DESC 
			LIMIT 1
		`, activeBranchID).Scan(&commitUUID)
		if err == nil && commitUUID != "" {
			baseCommitID = []byte(commitUUID)
		}
	} else if err != sql.ErrNoRows {
		return fmt.Errorf("failed to check active branch: %w", err)
	}

	// 2. Insert branch metadata
	schema := "chuck_" + branchName
	var branchID int64
	err = tx.QueryRow(`
		INSERT INTO chuck_meta.branches (name, schema_name, parent_branch_id, base_commit_id, status, sequence_offsets)
		VALUES ($1, $2, $3, NULLIF($4, '')::uuid, 'active', '{}'::jsonb)
		RETURNING id
	`, branchName, schema, parentBranchID, string(baseCommitID)).Scan(&branchID)
	if err != nil {
		return fmt.Errorf("failed to insert branch metadata: %w", err)
	}

	// 3. Create schema
	_, err = tx.Exec(fmt.Sprintf("CREATE SCHEMA %s", schema))
	if err != nil {
		return fmt.Errorf("failed to create branch schema: %w", err)
	}

	// 4. Get all tracked tables
	type TrackedTable struct {
		ID          int64
		TableSchema string
		TableName   string
	}
	rows, err := tx.Query(`
		SELECT id, table_schema, table_name
		FROM chuck_meta.tracked_tables
		ORDER BY id ASC
	`)
	if err != nil {
		return fmt.Errorf("failed to query tracked tables: %w", err)
	}
	defer rows.Close()

	var trackedTables []TrackedTable
	for rows.Next() {
		var t TrackedTable
		if err := rows.Scan(&t.ID, &t.TableSchema, &t.TableName); err != nil {
			return fmt.Errorf("failed to scan tracked table: %w", err)
		}
		trackedTables = append(trackedTables, t)
	}
	rows.Close()

	// 5. Process DDL for each tracked table
	sequenceOffsets := make(map[string]int64)
	for _, t := range trackedTables {
		// Inspect schema (read-only against DB connection)
		cols, err := delta.InspectColumns(db, t.TableSchema, t.TableName)
		if err != nil {
			return fmt.Errorf("failed to inspect columns for %s.%s: %w", t.TableSchema, t.TableName, err)
		}
		
		fks, err := delta.InspectFKs(db, t.TableSchema, t.TableName)
		if err != nil {
			return fmt.Errorf("failed to inspect FKs for %s.%s: %w", t.TableSchema, t.TableName, err)
		}
		
		cascades, err := delta.BuildCascadeGraph(db, t.TableSchema, t.TableName)
		if err != nil {
			return fmt.Errorf("failed to build cascade graph for %s.%s: %w", t.TableSchema, t.TableName, err)
		}
		
		triggers, err := delta.InspectTriggers(db, t.TableSchema, t.TableName)
		if err != nil {
			return fmt.Errorf("failed to inspect triggers for %s.%s: %w", t.TableSchema, t.TableName, err)
		}
		
		var replicableTriggers []delta.BaseTrigger
		for _, trg := range triggers {
			if trg.Replicable {
				replicableTriggers = append(replicableTriggers, trg)
			}
		}

		// Generate DDLs
		deltaTableDDL := delta.GenerateDeltaTableDDL(schema, t.TableName, cols, fks)
		seqDDL := delta.GenerateSequenceDDL(schema, t.TableName, branchID)
		passViewDDL := delta.GeneratePassthroughViewDDL(schema, t.TableSchema, t.TableName, cols)
		triggerDDL := delta.GenerateTriggerDDL(schema, t.TableSchema, t.TableName, cols, cascades, replicableTriggers)

		// Execute DDLs in transaction
		if _, err := tx.Exec(deltaTableDDL); err != nil {
			return fmt.Errorf("failed to execute delta table DDL for %s: %w", t.TableName, err)
		}
		if _, err := tx.Exec(seqDDL); err != nil {
			return fmt.Errorf("failed to execute sequence DDL for %s: %w", t.TableName, err)
		}
		if _, err := tx.Exec(passViewDDL); err != nil {
			return fmt.Errorf("failed to execute passthrough view DDL for %s: %w", t.TableName, err)
		}
		if _, err := tx.Exec(triggerDDL); err != nil {
			return fmt.Errorf("failed to execute trigger DDL for %s: %w", t.TableName, err)
		}

		// Marshal JSON fields for metadata
		fksJSON, _ := json.Marshal(fks)
		cascadesJSON, _ := json.Marshal(cascades)
		triggersJSON, _ := json.Marshal(triggers)

		// Insert into chuck_meta.branch_tables
		_, err = tx.Exec(`
			INSERT INTO chuck_meta.branch_tables 
				(branch_id, table_id, delta_table, view_name, view_tier, suspended_fks, cascade_chains, replicated_triggers, is_dirty)
			VALUES ($1, $2, $3, $4, 'passthrough', $5, $6, $7, false)
		`, branchID, t.ID, t.TableName+"_delta", t.TableName, fksJSON, cascadesJSON, triggersJSON)
		if err != nil {
			return fmt.Errorf("failed to insert branch table metadata: %w", err)
		}

		sequenceOffsets[t.TableName] = 1000000000 * branchID
	}

	// Update sequence offsets
	offsetsJSON, _ := json.Marshal(sequenceOffsets)
	_, err = tx.Exec(`
		UPDATE chuck_meta.branches 
		SET sequence_offsets = $1
		WHERE id = $2
	`, offsetsJSON, branchID)
	if err != nil {
		return fmt.Errorf("failed to update branch sequence offsets: %w", err)
	}

	// 6. Set as active branch if no active branch exists yet
	if parentBranchID == nil {
		_, err = tx.Exec(`
			INSERT INTO chuck_meta.active_branch (singleton, branch_id)
			VALUES (true, $1)
			ON CONFLICT (singleton) DO UPDATE SET branch_id = EXCLUDED.branch_id, switched_at = now()
		`, branchID)
		if err != nil {
			return fmt.Errorf("failed to set active branch: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
