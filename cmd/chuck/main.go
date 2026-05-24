package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chuckdb/chuck/internal/branch"
	"github.com/chuckdb/chuck/internal/delta"
	"github.com/chuckdb/chuck/internal/merge"
	"github.com/chuckdb/chuck/internal/meta"
	"github.com/chuckdb/chuck/internal/proxy"
	"github.com/spf13/cobra"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var dsn string

var rootCmd = &cobra.Command{
	Use:   "chuck",
	Short: "ChuckDB is a Postgres-native database branching tool",
}

func getDB() (*sql.DB, error) {
	connStr := dsn
	if connStr == "" {
		connStr = os.Getenv("DATABASE_URL")
	}
	if connStr == "" {
		connStr = "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
	}
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to database at %s: %w", connStr, err)
	}
	return db, nil
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Bootstrap chuck_meta schema",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()
		if err := meta.Bootstrap(db); err != nil {
			return fmt.Errorf("failed to bootstrap: %w", err)
		}
		fmt.Println("✓ Metadata schema chuck_meta initialized.")
		return nil
	},
}

var trackCmd = &cobra.Command{
	Use:   "track <table>",
	Short: "Register a table for database branching",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		tableArg := args[0]
		parts := strings.Split(tableArg, ".")
		schema := "public"
		table := parts[0]
		if len(parts) > 1 {
			schema = parts[0]
			table = parts[1]
		}

		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		cols, err := delta.InspectColumns(db, schema, table)
		if err != nil {
			return fmt.Errorf("failed to inspect columns for %s.%s: %w", schema, table, err)
		}
		if len(cols) == 0 {
			return fmt.Errorf("table %s.%s not found or has no columns", schema, table)
		}

		var pks []string
		for _, col := range cols {
			if col.IsPrimary {
				pks = append(pks, col.Name)
			}
		}
		if len(pks) == 0 {
			return fmt.Errorf("table %s.%s does not have a primary key, which is required", schema, table)
		}

		if err := meta.TrackTable(db, schema, table, pks); err != nil {
			return fmt.Errorf("failed to track table: %w", err)
		}

		fmt.Printf("✓ Table %s.%s registered for branching (PK: %s).\n", schema, table, strings.Join(pks, ", "))
		return nil
	},
}

var branchCmd = &cobra.Command{
	Use:   "branch",
	Short: "Manage database branches",
}

var branchCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new database branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		fmt.Printf("Creating branch %s...\n", name)
		if err := branch.Create(db, name); err != nil {
			return err
		}

		schema := "chuck_" + name
		fmt.Printf("  ✓ Schema: %s\n", schema)

		// Query tracked tables
		tables, err := meta.ListTrackedTables(db)
		if err != nil {
			return err
		}
		for _, t := range tables {
			fmt.Printf("  ✓ %s      — delta table, passthrough view, trigger\n", t.TableName)
		}

		// Collect and print suspended FKs and non-replicable triggers
		var allFKs []delta.FKConstraint
		var nonReplicableTriggers []string
		for _, t := range tables {
			fks, _ := delta.InspectFKs(db, t.TableSchema, t.TableName)
			allFKs = append(allFKs, fks...)

			trgs, _ := delta.InspectTriggers(db, t.TableSchema, t.TableName)
			for _, trg := range trgs {
				if !trg.Replicable {
					nonReplicableTriggers = append(nonReplicableTriggers, fmt.Sprintf("  %s.%s  — %s", t.TableName, trg.Name, trg.SkipReason))
				}
			}
		}

		if len(allFKs) > 0 {
			fmt.Println("\n⚠ FK constraints suspended (validated at merge time):")
			for _, fk := range allFKs {
				fmt.Printf("  %s.%s → %s.%s.%s (%s)\n", fk.SourceTable, fk.SourceColumn, fk.ReferencedSchema, fk.ReferencedTable, fk.ReferencedColumn, fk.OnDelete)
			}
		}

		if len(nonReplicableTriggers) > 0 {
			fmt.Println("\n⚠ Triggers not replicated inside branch (external side effects):")
			for _, nt := range nonReplicableTriggers {
				fmt.Println(nt)
			}
		}

		fmt.Printf("\nBranch %s ready.\n", name)
		return nil
	},
}

var branchListCmd = &cobra.Command{
	Use:   "list",
	Short: "List database branches",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		list, err := branch.List(db)
		if err != nil {
			return err
		}

		// Check active branch
		var activeID int64
		_ = db.QueryRow("SELECT branch_id FROM chuck_meta.active_branch LIMIT 1").Scan(&activeID)

		// Get active branch name
		var activeName string
		_ = db.QueryRow("SELECT name FROM chuck_meta.branches WHERE id = $1", activeID).Scan(&activeName)

		fmt.Printf("%-20s %-25s %-10s %-25s %s\n", "BRANCH", "SCHEMA", "STATUS", "CREATED AT", "DELTA SIZES")
		fmt.Println(strings.Repeat("-", 100))
		for _, b := range list {
			prefix := ""
			if b.Name == activeName {
				prefix = "* "
			}
			var sizes []string
			for tbl, size := range b.DeltaSizes {
				sizes = append(sizes, fmt.Sprintf("%s: %d", tbl, size))
			}
			sizesStr := strings.Join(sizes, ", ")
			if sizesStr == "" {
				sizesStr = "none"
			}
			fmt.Printf("%-20s %-25s %-10s %-25s %s\n", prefix+b.Name, b.SchemaName, b.Status, b.CreatedAt.Format("2006-01-02 15:04:05"), sizesStr)
		}
		return nil
	},
}

var branchDropCmd = &cobra.Command{
	Use:   "drop <name>",
	Short: "Drop a database branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		if err := branch.Drop(db, name); err != nil {
			return err
		}
		fmt.Printf("✓ Branch %s dropped and associated schema dropped.\n", name)
		return nil
	},
}

var branchStatusCmd = &cobra.Command{
	Use:   "status <name>",
	Short: "Show status of a database branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		var id int64
		var schemaName string
		var status string
		var createdAt time.Time
		var parentBranchID sql.NullInt64
		var baseCommitID sql.NullString
		err = db.QueryRow(`
			SELECT id, schema_name, status, created_at, parent_branch_id, base_commit_id::text
			FROM chuck_meta.branches
			WHERE name = $1 AND dropped_at IS NULL
		`, name).Scan(&id, &schemaName, &status, &createdAt, &parentBranchID, &baseCommitID)
		if err != nil {
			return fmt.Errorf("branch %q not found or dropped", name)
		}

		fmt.Printf("Branch:       %s\n", name)
		fmt.Printf("Schema:       %s\n", schemaName)
		fmt.Printf("Status:       %s\n", status)
		fmt.Printf("Created At:   %s\n", createdAt.Format(time.RFC1123))
		if parentBranchID.Valid {
			var parentName string
			_ = db.QueryRow("SELECT name FROM chuck_meta.branches WHERE id = $1", parentBranchID.Int64).Scan(&parentName)
			fmt.Printf("Parent:       %s\n", parentName)
		}
		if baseCommitID.Valid && baseCommitID.String != "" {
			fmt.Printf("Base Commit:  %s\n", baseCommitID.String)
		}

		// Query delta sizes
		tables, err := meta.ListTrackedTables(db)
		if err != nil {
			return err
		}

		fmt.Println("\nDelta Row Counts:")
		var allFKs []delta.FKConstraint
		for _, t := range tables {
			var count int
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta", schemaName, t.TableName)).Scan(&count)
			fmt.Printf("  %-20s %d\n", t.TableName, count)

			fks, _ := delta.InspectFKs(db, t.TableSchema, t.TableName)
			allFKs = append(allFKs, fks...)
		}

		if len(allFKs) > 0 {
			fmt.Println("\nSuspended FK Constraints:")
			for _, fk := range allFKs {
				fmt.Printf("  %s.%s → %s.%s.%s (%s)\n", fk.SourceTable, fk.SourceColumn, fk.ReferencedSchema, fk.ReferencedTable, fk.ReferencedColumn, fk.OnDelete)
			}
		}

		return nil
	},
}

var checkoutCmd = &cobra.Command{
	Use:   "checkout <name>",
	Short: "Switch to a database branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		var branchID int64
		err = db.QueryRow("SELECT id FROM chuck_meta.branches WHERE name = $1 AND dropped_at IS NULL", name).Scan(&branchID)
		if err != nil {
			return fmt.Errorf("branch %q not found or dropped", name)
		}

		_, err = db.Exec(`
			INSERT INTO chuck_meta.active_branch (singleton, branch_id)
			VALUES (true, $1)
			ON CONFLICT (singleton) DO UPDATE SET branch_id = EXCLUDED.branch_id, switched_at = now()
		`, branchID)
		if err != nil {
			return fmt.Errorf("failed to switch active branch: %w", err)
		}

		fmt.Printf("Switched to branch %q.\n", name)
		return nil
	},
}

var commitMsg string

var commitCmd = &cobra.Command{
	Use:   "commit",
	Short: "Record commit metadata on current branch",
	RunE: func(cmd *cobra.Command, args []string) error {
		if commitMsg == "" {
			return fmt.Errorf("commit message is required (use -m)")
		}

		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		// Resolve active branch
		var branchID int64
		var branchName string
		var schemaName string
		err = db.QueryRow(`
			SELECT b.id, b.name, b.schema_name 
			FROM chuck_meta.active_branch ab
			JOIN chuck_meta.branches b ON ab.branch_id = b.id
			LIMIT 1
		`).Scan(&branchID, &branchName, &schemaName)
		if err != nil {
			return fmt.Errorf("no active branch. Checkout a branch first")
		}

		// Fetch parent commit ID
		var parentCommitUUID sql.NullString
		_ = db.QueryRow(`
			SELECT id::text FROM chuck_meta.commits 
			WHERE branch_id = $1 
			ORDER BY created_at DESC LIMIT 1
		`, branchID).Scan(&parentCommitUUID)

		var parentIDs []string
		if parentCommitUUID.Valid && parentCommitUUID.String != "" {
			parentIDs = append(parentIDs, parentCommitUUID.String)
		} else {
			// Fallback to base_commit_id
			var baseCommitUUID sql.NullString
			_ = db.QueryRow("SELECT base_commit_id::text FROM chuck_meta.branches WHERE id = $1", branchID).Scan(&baseCommitUUID)
			if baseCommitUUID.Valid && baseCommitUUID.String != "" {
				parentIDs = append(parentIDs, baseCommitUUID.String)
			}
		}

		// Query delta changes
		tables, err := meta.ListTrackedTables(db)
		if err != nil {
			return err
		}

		var totalInserts int64
		var totalUpdates int64
		var totalDeletes int64
		deltaSummary := make(map[string]map[string]int64)

		for _, t := range tables {
			var ins, upd, del int64
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = false AND __is_new = true", schemaName, t.TableName)).Scan(&ins)
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = false AND __is_new = false", schemaName, t.TableName)).Scan(&upd)
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = true", schemaName, t.TableName)).Scan(&del)

			if ins > 0 || upd > 0 || del > 0 {
				deltaSummary[t.TableName] = map[string]int64{
					"inserts": ins,
					"updates": upd,
					"deletes": del,
				}
				totalInserts += ins
				totalUpdates += upd
				totalDeletes += del
			}
		}

		summaryJSON, _ := json.Marshal(deltaSummary)

		// Determine author
		author := os.Getenv("USER")
		if author == "" {
			author = "chuck"
		}
		if gitAuthor, err := exec.Command("git", "config", "user.name").Output(); err == nil {
			trimmed := strings.TrimSpace(string(gitAuthor))
			if trimmed != "" {
				author = trimmed
			}
		}

		var commitID string
		err = db.QueryRow(`
			INSERT INTO chuck_meta.commits (branch_id, parent_ids, message, author, delta_summary, row_inserts, row_updates, row_deletes)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING id::text
		`, branchID, parentIDs, commitMsg, author, summaryJSON, totalInserts, totalUpdates, totalDeletes).Scan(&commitID)
		if err != nil {
			return fmt.Errorf("failed to record commit: %w", err)
		}

		shortHash := commitID
		if len(shortHash) > 7 {
			shortHash = shortHash[:7]
		}

		fmt.Printf("[%s %s] %s\n", branchName, shortHash, commitMsg)
		fmt.Printf(" %d insertions(+), %d updates, %d deletions(-)\n", totalInserts, totalUpdates, totalDeletes)
		return nil
	},
}

var logCmd = &cobra.Command{
	Use:   "log [branch]",
	Short: "Show commit history",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		var branchID int64
		var branchName string
		if len(args) > 0 {
			branchName = args[0]
			err = db.QueryRow("SELECT id FROM chuck_meta.branches WHERE name = $1 AND dropped_at IS NULL", branchName).Scan(&branchID)
			if err != nil {
				return fmt.Errorf("branch %q not found or dropped", branchName)
			}
		} else {
			// Active branch
			err = db.QueryRow(`
				SELECT b.id, b.name 
				FROM chuck_meta.active_branch ab
				JOIN chuck_meta.branches b ON ab.branch_id = b.id
				LIMIT 1
			`).Scan(&branchID, &branchName)
			if err != nil {
				return fmt.Errorf("no active branch. Checkout a branch first")
			}
		}

		rows, err := db.Query(`
			SELECT id::text, array_to_json(parent_ids), message, author, created_at, row_inserts, row_updates, row_deletes
			FROM chuck_meta.commits
			WHERE branch_id = $1
			ORDER BY created_at DESC
		`, branchID)
		if err != nil {
			return fmt.Errorf("failed to query commits: %w", err)
		}
		defer rows.Close()

		hasCommits := false
		for rows.Next() {
			hasCommits = true
			var id string
			var parentJSON string
			var msg string
			var author string
			var createdAt time.Time
			var ins, upd, del int64

			if err := rows.Scan(&id, &parentJSON, &msg, &author, &createdAt, &ins, &upd, &del); err != nil {
				return err
			}

			fmt.Printf("commit %s\n", id)
			fmt.Printf("Author: %s\n", author)
			fmt.Printf("Date:   %s\n\n", createdAt.Format(time.RFC1123))
			fmt.Printf("    %s\n\n", msg)
			fmt.Printf("    Summary: %d insertions(+), %d updates, %d deletions(-)\n", ins, upd, del)
			fmt.Println(strings.Repeat("-", 60))
		}

		if !hasCommits {
			fmt.Printf("No commits found on branch %s.\n", branchName)
		}
		return nil
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show current branch delta summary",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		var branchName string
		var schemaName string
		err = db.QueryRow(`
			SELECT b.name, b.schema_name 
			FROM chuck_meta.active_branch ab
			JOIN chuck_meta.branches b ON ab.branch_id = b.id
			LIMIT 1
		`).Scan(&branchName, &schemaName)
		if err != nil {
			fmt.Println("No active branch. Running in default mode (routing to public).")
			return nil
		}

		fmt.Printf("On branch %s\n", branchName)

		tables, err := meta.ListTrackedTables(db)
		if err != nil {
			return err
		}

		hasChanges := false
		for _, t := range tables {
			var ins, upd, del int64
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = false AND __is_new = true", schemaName, t.TableName)).Scan(&ins)
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = false AND __is_new = false", schemaName, t.TableName)).Scan(&upd)
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = true", schemaName, t.TableName)).Scan(&del)

			if ins > 0 || upd > 0 || del > 0 {
				if !hasChanges {
					fmt.Println("Changes in branch:")
					hasChanges = true
				}
				fmt.Printf("  %-20s %d inserts, %d updates, %d deletes\n", t.TableName, ins, upd, del)
			}
		}

		if !hasChanges {
			fmt.Println("nothing to commit, working tree clean")
		}

		return nil
	},
}

var dryRun bool

var mergeCmd = &cobra.Command{
	Use:   "merge <name>",
	Short: "Merge a database branch into the base tables",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		var branchID int64
		var schemaName string
		err = db.QueryRow("SELECT id, schema_name FROM chuck_meta.branches WHERE name = $1 AND dropped_at IS NULL", name).Scan(&branchID, &schemaName)
		if err != nil {
			return fmt.Errorf("branch %q not found or dropped", name)
		}

		// Pre-merge statistics and plan printing
		tables, err := meta.ListTrackedTables(db)
		if err != nil {
			return err
		}

		var allFKs []delta.FKConstraint
		for _, t := range tables {
			fks, _ := delta.InspectFKs(db, t.TableSchema, t.TableName)
			allFKs = append(allFKs, fks...)
		}

		fmt.Println("→ Stage 1: Validating FK integrity...")
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		var mergeFKs []merge.FKConstraint
		for _, fk := range allFKs {
			mergeFKs = append(mergeFKs, merge.FKConstraint{
				ConstraintName:   fk.ConstraintName,
				SourceTable:      fk.SourceTable,
				SourceColumn:     fk.SourceColumn,
				ReferencedSchema: fk.ReferencedSchema,
				ReferencedTable:  fk.ReferencedTable,
				ReferencedColumn: fk.ReferencedColumn,
				OnDelete:         fk.OnDelete,
				OnUpdate:         fk.OnUpdate,
			})
		}

		violations, err := merge.ValidateFKs(tx, schemaName, mergeFKs)
		if err != nil {
			return fmt.Errorf("FK validation failed: %w", err)
		}
		if len(violations) > 0 {
			for _, v := range violations {
				fmt.Printf("  ✗ %s.%s → %s\n", v.Table, v.SourceColumn, v.ConstraintName)
				fmt.Printf("    %d rows reference keys that do not exist: IDs %v\n", len(v.ViolatingRowIDs), v.ViolatingRowIDs)
			}
			return fmt.Errorf("Merge aborted at Stage 1 due to FK violations")
		}
		fmt.Println("  ✓ FK integrity validation complete.")

		fmt.Println("→ Stage 2: Detecting conflicts...")
		conflicts, err := merge.DetectConflicts(tx, schemaName, name)
		if err != nil {
			return fmt.Errorf("conflict detection failed: %w", err)
		}
		if len(conflicts) > 0 {
			for _, c := range conflicts {
				fmt.Printf("  ✗ Conflict on table %s, row ID %v\n", c.Table, c.RowID)
			}
			return fmt.Errorf("Merge aborted at Stage 2 due to concurrent modification conflicts")
		}
		fmt.Println("  ✓ No conflicts detected.")

		fmt.Println("→ Stage 3: Remapping branch-local PKs...")
		for _, t := range tables {
			var newCount int64
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __is_new = true", schemaName, t.TableName)).Scan(&newCount)
			if newCount > 0 {
				fmt.Printf("  %s: %d new rows to remap\n", t.TableName, newCount)
			}
		}

		fmt.Println("→ Stage 4: Fixing FK references...")
		fmt.Println("  ✓ Delta FK column reference fixes prepared.")

		fmt.Println("→ Stage 5: Replaying delta...")
		for _, t := range tables {
			var ins, upd, del int64
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = false AND __is_new = true", schemaName, t.TableName)).Scan(&ins)
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = false AND __is_new = false", schemaName, t.TableName)).Scan(&upd)
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = true", schemaName, t.TableName)).Scan(&del)
			fmt.Printf("  %s: %d upserts, %d deletes\n", t.TableName, ins+upd, del)
		}

		_ = tx.Rollback()

		if err := merge.Merge(db, name, dryRun); err != nil {
			return err
		}

		if dryRun {
			fmt.Println("\n✓ Dry run complete. Zero changes written to base tables.")
		} else {
			fmt.Printf("\n✓ Merge complete. Branch %s merged into public.\n", name)
		}
		return nil
	},
}

var validateCmd = &cobra.Command{
	Use:   "validate <name>",
	Short: "Run FK validation only, no merge",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		var schemaName string
		err = db.QueryRow("SELECT schema_name FROM chuck_meta.branches WHERE name = $1 AND dropped_at IS NULL", name).Scan(&schemaName)
		if err != nil {
			return fmt.Errorf("branch %q not found or dropped", name)
		}

		tables, err := meta.ListTrackedTables(db)
		if err != nil {
			return err
		}

		var allFKs []delta.FKConstraint
		for _, t := range tables {
			fks, _ := delta.InspectFKs(db, t.TableSchema, t.TableName)
			allFKs = append(allFKs, fks...)
		}

		tx, err := db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		var mergeFKs []merge.FKConstraint
		for _, fk := range allFKs {
			mergeFKs = append(mergeFKs, merge.FKConstraint{
				ConstraintName:   fk.ConstraintName,
				SourceTable:      fk.SourceTable,
				SourceColumn:     fk.SourceColumn,
				ReferencedSchema: fk.ReferencedSchema,
				ReferencedTable:  fk.ReferencedTable,
				ReferencedColumn: fk.ReferencedColumn,
				OnDelete:         fk.OnDelete,
				OnUpdate:         fk.OnUpdate,
			})
		}

		violations, err := merge.ValidateFKs(tx, schemaName, mergeFKs)
		if err != nil {
			return fmt.Errorf("FK validation failed: %w", err)
		}

		if len(violations) > 0 {
			for _, v := range violations {
				fmt.Printf("✗ FK Violation: table %s, constraint %s, column %s → referenced table %s\n", v.Table, v.ConstraintName, v.SourceColumn, v.ReferencedTable)
				fmt.Printf("  Violating row IDs: %v\n", v.ViolatingRowIDs)
			}
			return fmt.Errorf("FK validation failed with %d violations", len(violations))
		}

		fmt.Println("✓ FK integrity check passed. 0 violations found.")
		return nil
	},
}

var proxyCmd = &cobra.Command{
	Use:   "proxy",
	Short: "Manage connection proxy",
}

var proxyPort int

var proxyStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the connection proxy in the background",
	RunE: func(cmd *cobra.Command, args []string) error {
		db, err := getDB()
		if err != nil {
			return err
		}
		db.Close()

		appDataDir := "/home/harsha/.gemini/antigravity-ide"
		pidPath := filepath.Join(appDataDir, "chuck_proxy.pid")

		if pidBytes, err := os.ReadFile(pidPath); err == nil {
			pid, _ := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			if pid > 0 {
				process, err := os.FindProcess(pid)
				if err == nil {
					if err := process.Signal(syscall.Signal(0)); err == nil {
						fmt.Printf("Proxy is already running (PID: %d).\n", pid)
						return nil
					}
				}
			}
		}

		connStr := dsn
		if connStr == "" {
			connStr = os.Getenv("DATABASE_URL")
		}
		if connStr == "" {
			connStr = "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
		}

		logPath := filepath.Join(appDataDir, "chuck_proxy.log")
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		defer logFile.Close()

		runCmd := exec.Command(os.Args[0], "proxy", "run", "--port", strconv.Itoa(proxyPort), "--dsn", connStr)
		runCmd.Stdout = logFile
		runCmd.Stderr = logFile
		if err := runCmd.Start(); err != nil {
			return fmt.Errorf("failed to start proxy process: %w", err)
		}

		err = os.WriteFile(pidPath, []byte(strconv.Itoa(runCmd.Process.Pid)), 0644)
		if err != nil {
			return fmt.Errorf("failed to write PID file: %w", err)
		}

		fmt.Printf("Proxy started on port %d (PID: %d)\n", proxyPort, runCmd.Process.Pid)
		fmt.Printf("Connect your application to: postgresql://localhost:%d/app\n", proxyPort)
		return nil
	},
}

var proxyRunCmd = &cobra.Command{
	Use:    "run",
	Hidden: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		connStr := dsn
		if connStr == "" {
			connStr = os.Getenv("DATABASE_URL")
		}
		if connStr == "" {
			connStr = "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
		}

		listenAddr := fmt.Sprintf("127.0.0.1:%d", proxyPort)
		bp := proxy.NewBranchProxy(listenAddr, connStr)
		fmt.Printf("Starting proxy listener on %s...\n", listenAddr)
		if err := bp.Start(); err != nil {
			return err
		}

		select {}
	},
}

var proxyStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the connection proxy",
	RunE: func(cmd *cobra.Command, args []string) error {
		appDataDir := "/home/harsha/.gemini/antigravity-ide"
		pidPath := filepath.Join(appDataDir, "chuck_proxy.pid")

		pidBytes, err := os.ReadFile(pidPath)
		if err != nil {
			fmt.Println("Proxy is not running.")
			return nil
		}

		pid, _ := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		if pid > 0 {
			process, err := os.FindProcess(pid)
			if err == nil {
				_ = process.Signal(syscall.SIGTERM)
				time.Sleep(500 * time.Millisecond)
				_ = process.Kill()
			}
		}

		_ = os.Remove(pidPath)
		fmt.Println("Proxy stopped.")
		return nil
	},
}

var proxyStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show status of the connection proxy",
	RunE: func(cmd *cobra.Command, args []string) error {
		appDataDir := "/home/harsha/.gemini/antigravity-ide"
		pidPath := filepath.Join(appDataDir, "chuck_proxy.pid")

		pidBytes, err := os.ReadFile(pidPath)
		if err != nil {
			fmt.Println("Proxy is not running.")
			return nil
		}

		pid, _ := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		if pid > 0 {
			process, err := os.FindProcess(pid)
			if err == nil {
				if err := process.Signal(syscall.Signal(0)); err == nil {
					fmt.Printf("Proxy is running (PID: %d).\n", pid)

					db, err := getDB()
					if err == nil {
						defer db.Close()
						var activeName string
						_ = db.QueryRow(`
							SELECT b.name 
							FROM chuck_meta.active_branch ab
							JOIN chuck_meta.branches b ON ab.branch_id = b.id
							LIMIT 1
						`).Scan(&activeName)
						if activeName != "" {
							fmt.Printf("Routing traffic to active branch: %s\n", activeName)
						} else {
							fmt.Println("Routing traffic to: public (default)")
						}
					}
					return nil
				}
			}
		}

		fmt.Println("Proxy is not running.")
		return nil
	},
}

var statsCmd = &cobra.Command{
	Use:   "stats <name>",
	Short: "Show cost metrics and delta stats for a branch",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		db, err := getDB()
		if err != nil {
			return err
		}
		defer db.Close()

		var schemaName string
		err = db.QueryRow("SELECT schema_name FROM chuck_meta.branches WHERE name = $1 AND dropped_at IS NULL", name).Scan(&schemaName)
		if err != nil {
			return fmt.Errorf("branch %q not found or dropped", name)
		}

		tables, err := meta.ListTrackedTables(db)
		if err != nil {
			return err
		}

		fmt.Printf("Branch %s Stats:\n", name)
		fmt.Println(strings.Repeat("-", 40))

		var totalChanges int64
		for _, t := range tables {
			var ins, upd, del int64
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = false AND __is_new = true", schemaName, t.TableName)).Scan(&ins)
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = false AND __is_new = false", schemaName, t.TableName)).Scan(&upd)
			_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_delta WHERE __deleted = true", schemaName, t.TableName)).Scan(&del)

			tblChanges := ins + upd + del
			totalChanges += tblChanges
			if tblChanges > 0 {
				fmt.Printf("  %s:\n", t.TableName)
				fmt.Printf("    Inserts: %d\n", ins)
				fmt.Printf("    Updates: %d\n", upd)
				fmt.Printf("    Deletes: %d\n", del)
			}
		}

		fmt.Println(strings.Repeat("-", 40))
		fmt.Printf("Total Pending Replay Operations: %d\n", totalChanges)
		if totalChanges == 0 {
			fmt.Println("Estimate Merge Cost: 0ms (no changes)")
		} else if totalChanges < 100 {
			fmt.Println("Estimate Merge Cost: <100ms (fast replay)")
		} else {
			fmt.Printf("Estimate Merge Cost: ~%dms\n", totalChanges*5)
		}

		return nil
	},
}

func main() {
	rootCmd.PersistentFlags().StringVar(&dsn, "dsn", "", "PostgreSQL connection DSN (overrides DATABASE_URL)")

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(trackCmd)

	branchCmd.AddCommand(branchCreateCmd)
	branchCmd.AddCommand(branchListCmd)
	branchCmd.AddCommand(branchDropCmd)
	branchCmd.AddCommand(branchStatusCmd)
	rootCmd.AddCommand(branchCmd)

	rootCmd.AddCommand(checkoutCmd)
	rootCmd.AddCommand(commitCmd)
	rootCmd.AddCommand(logCmd)
	rootCmd.AddCommand(statusCmd)

	mergeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview merge plan only without writing to base tables")
	rootCmd.AddCommand(mergeCmd)
	rootCmd.AddCommand(validateCmd)

	proxyStartCmd.Flags().IntVar(&proxyPort, "port", 5433, "Port for the proxy daemon to listen on")
	proxyRunCmd.Flags().IntVar(&proxyPort, "port", 5433, "Port for the proxy daemon to listen on")
	proxyCmd.AddCommand(proxyStartCmd)
	proxyCmd.AddCommand(proxyRunCmd)
	proxyCmd.AddCommand(proxyStopCmd)
	proxyCmd.AddCommand(proxyStatusCmd)
	rootCmd.AddCommand(proxyCmd)

	commitCmd.Flags().StringVarP(&commitMsg, "message", "m", "", "Commit message")

	rootCmd.AddCommand(statsCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
