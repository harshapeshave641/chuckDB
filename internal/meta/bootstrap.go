package meta

import (
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed 001_chuck_metadata.sql
var bootstrapSQL string

type TrackedTable struct {
	ID          int64
	TableOID    uint32
	TableSchema string
	TableName   string
	PrimaryKeys []string
}

// Bootstrap creates the chuck_meta schema and all metadata tables.
// Safe to call on every startup — fully idempotent.
func Bootstrap(db *sql.DB) error {
	_, err := db.Exec(bootstrapSQL)
	return err
}

// TrackTable registers a base table for branching.
// Must be called before any branch that includes this table can be created.
func TrackTable(db *sql.DB, schema, table string, primaryKeys []string) error {
	// Resolve table OID dynamically from PostgreSQL
	var oid uint32
	fullName := fmt.Sprintf(`"%s"."%s"`, schema, table)
	err := db.QueryRow("SELECT $1::regclass::oid", fullName).Scan(&oid)
	if err != nil {
		return fmt.Errorf("failed to resolve table OID for %s: %w", fullName, err)
	}

	// We pass primaryKeys as a Go string slice. The pgx driver naturally
	// maps Go slices to PostgreSQL array types.
	_, err = db.Exec(`
		INSERT INTO chuck_meta.tracked_tables (table_oid, table_schema, table_name, primary_keys)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (table_schema, table_name) DO UPDATE 
		SET table_oid = EXCLUDED.table_oid, primary_keys = EXCLUDED.primary_keys
	`, oid, schema, table, primaryKeys)
	return err
}

// ListTrackedTables returns all tables registered for branching.
func ListTrackedTables(db *sql.DB) ([]TrackedTable, error) {
	rows, err := db.Query(`
		SELECT id, table_oid, table_schema, table_name, array_to_json(primary_keys)
		FROM chuck_meta.tracked_tables
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TrackedTable
	for rows.Next() {
		var t TrackedTable
		var pksJSON string
		if err := rows.Scan(&t.ID, &t.TableOID, &t.TableSchema, &t.TableName, &pksJSON); err != nil {
			return nil, err
		}
		
		var pks []string
		if err := json.Unmarshal([]byte(pksJSON), &pks); err != nil {
			return nil, fmt.Errorf("failed to unmarshal primary_keys: %w", err)
		}
		t.PrimaryKeys = pks
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}
