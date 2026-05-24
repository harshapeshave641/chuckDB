package delta

import (
	"database/sql"
	"fmt"
	"strings"
)

type ColumnDef struct {
	Name       string
	DataType   string
	IsNullable bool
	Default    string
	IsPrimary  bool
}

type FKConstraint struct {
	ConstraintName   string
	SourceColumn     string
	ReferencedSchema string
	ReferencedTable  string
	ReferencedColumn string
	OnDelete         string // CASCADE, SET NULL, RESTRICT, NO ACTION, SET DEFAULT
	OnUpdate         string
}

type CascadeNode struct {
	SourceTable  string
	SourceColumn string
	TargetTable  string
	TargetColumn string
	OnDelete     string
	Children     []CascadeNode
}

type BaseTrigger struct {
	Name         string
	Event        string // INSERT, UPDATE, DELETE
	Timing       string // BEFORE, AFTER
	FunctionBody string
	Replicable   bool
	SkipReason   string
}

// InspectColumns returns all columns for a table including PK info.
func InspectColumns(db *sql.DB, schema, table string) ([]ColumnDef, error) {
	query := `
		SELECT 
			c.column_name, 
			c.data_type, 
			c.is_nullable = 'YES' AS is_nullable, 
			COALESCE(c.column_default, '') AS column_default,
			EXISTS (
				SELECT 1 
				FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu 
				  ON tc.constraint_name = kcu.constraint_name 
				 AND tc.table_schema = kcu.table_schema
				WHERE tc.constraint_type = 'PRIMARY KEY'
				  AND tc.table_schema = c.table_schema
				  AND tc.table_name = c.table_name
				  AND kcu.column_name = c.column_name
			) AS is_primary
		FROM information_schema.columns c
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var cols []ColumnDef
	for rows.Next() {
		var col ColumnDef
		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			&col.Default,
			&col.IsPrimary,
		)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// InspectFKs returns all FK constraints declared on a table.
func InspectFKs(db *sql.DB, schema, table string) ([]FKConstraint, error) {
	query := `
		SELECT
			tc.constraint_name,
			kcu.column_name AS source_column,
			ccu.table_schema AS referenced_schema,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column,
			rc.delete_rule AS on_delete,
			rc.update_rule AS on_update
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		JOIN information_schema.referential_constraints rc 
		  ON tc.constraint_name = rc.constraint_name
		  AND tc.table_schema = rc.constraint_schema
		JOIN information_schema.constraint_column_usage ccu 
		  ON rc.unique_constraint_name = ccu.constraint_name
		  AND rc.unique_constraint_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND tc.table_schema = $1
		  AND tc.table_name = $2;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query FKs: %w", err)
	}
	defer rows.Close()

	var fks []FKConstraint
	for rows.Next() {
		var fk FKConstraint
		err := rows.Scan(
			&fk.ConstraintName,
			&fk.SourceColumn,
			&fk.ReferencedSchema,
			&fk.ReferencedTable,
			&fk.ReferencedColumn,
			&fk.OnDelete,
			&fk.OnUpdate,
		)
		if err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// BuildCascadeGraph builds the full recursive FK cascade tree rooted at a table.
// Used to generate cascade tombstone logic in INSTEAD OF DELETE triggers.
func BuildCascadeGraph(db *sql.DB, schema, table string) ([]CascadeNode, error) {
	visited := make(map[string]bool)
	return buildCascadeGraphHelper(db, schema, table, visited)
}

func buildCascadeGraphHelper(db *sql.DB, schema, table string, visited map[string]bool) ([]CascadeNode, error) {
	if visited[table] {
		return nil, nil
	}
	visited[table] = true

	query := `
		SELECT
			tc.table_name AS source_table,
			kcu.column_name AS source_column,
			ccu.column_name AS target_column,
			rc.delete_rule AS on_delete
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		JOIN information_schema.referential_constraints rc 
		  ON tc.constraint_name = rc.constraint_name
		  AND tc.table_schema = rc.constraint_schema
		JOIN information_schema.constraint_column_usage ccu 
		  ON rc.unique_constraint_name = ccu.constraint_name
		  AND rc.unique_constraint_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND ccu.table_schema = $1
		  AND ccu.table_name = $2;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query referencing FKs: %w", err)
	}
	defer rows.Close()

	var nodes []CascadeNode
	for rows.Next() {
		var n CascadeNode
		n.TargetTable = table
		err := rows.Scan(
			&n.SourceTable,
			&n.SourceColumn,
			&n.TargetColumn,
			&n.OnDelete,
		)
		if err != nil {
			return nil, err
		}
		
		// Recursively build children
		children, err := buildCascadeGraphHelper(db, schema, n.SourceTable, visited)
		if err != nil {
			return nil, err
		}
		n.Children = children
		nodes = append(nodes, n)
	}

	delete(visited, table)
	return nodes, rows.Err()
}

// InspectTriggers returns all triggers on a base table and classifies them.
func InspectTriggers(db *sql.DB, schema, table string) ([]BaseTrigger, error) {
	query := `
		SELECT 
			trg.tgname AS trigger_name,
			CASE 
				WHEN (trg.tgtype & 2) = 2 THEN 'BEFORE'
				WHEN (trg.tgtype & 64) = 64 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END AS timing,
			concat_ws(' OR ',
				CASE WHEN (trg.tgtype & 4) = 4 THEN 'INSERT' END,
				CASE WHEN (trg.tgtype & 16) = 16 THEN 'UPDATE' END,
				CASE WHEN (trg.tgtype & 8) = 8 THEN 'DELETE' END
			) AS event,
			p.prosrc AS function_body
		FROM pg_trigger trg
		JOIN pg_class rel ON trg.tgrelid = rel.oid
		JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
		JOIN pg_proc p ON trg.tgfoid = p.oid
		WHERE nsp.nspname = $1 AND rel.relname = $2 AND NOT trg.tgisinternal;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query triggers: %w", err)
	}
	defer rows.Close()

	var triggers []BaseTrigger
	for rows.Next() {
		var t BaseTrigger
		err := rows.Scan(
			&t.Name,
			&t.Timing,
			&t.Event,
			&t.FunctionBody,
		)
		if err != nil {
			return nil, err
		}

		// Classify
		replicable, reason := classifyTrigger(t.FunctionBody)
		t.Replicable = replicable
		t.SkipReason = reason

		triggers = append(triggers, t)
	}
	return triggers, rows.Err()
}

func classifyTrigger(body string) (bool, string) {
	bodyLower := strings.ToLower(body)
	if strings.Contains(bodyLower, "pg_notify") {
		return false, "uses pg_notify"
	}
	if strings.Contains(bodyLower, "pg_net") {
		return false, "references pg_net"
	}
	return true, ""
}
