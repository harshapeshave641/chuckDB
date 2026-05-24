package merge

import (
	"database/sql"
	"fmt"
)

// RemapNewRows allocates production sequence IDs for all __is_new rows in a branch.
// Returns a remap map: branch_local_id → production_id per table.
// Does NOT modify base tables. Only modifies branch delta tables.
func RemapNewRows(tx *sql.Tx, branchSchema string, tables []string) (map[string]map[int64]int64, error) {
	remaps := make(map[string]map[int64]int64)

	for _, tbl := range tables {
		remaps[tbl] = make(map[int64]int64)

		query := fmt.Sprintf("SELECT id FROM %s.%s_delta WHERE __is_new = true ORDER BY id ASC", branchSchema, tbl)
		rows, err := tx.Query(query)
		if err != nil {
			continue
		}
		defer rows.Close()

		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err == nil {
				ids = append(ids, id)
			}
		}
		rows.Close()

		if len(ids) == 0 {
			continue
		}

		var seqName sql.NullString
		_ = tx.QueryRow("SELECT pg_get_serial_sequence($1, 'id')", "public."+tbl).Scan(&seqName)

		actualSeq := "public." + tbl + "_id_seq"
		if seqName.Valid && seqName.String != "" {
			actualSeq = seqName.String
		}

		for _, oldID := range ids {
			var newID int64
			err = tx.QueryRow(fmt.Sprintf("SELECT nextval('%s')", actualSeq)).Scan(&newID)
			if err != nil {
				return nil, fmt.Errorf("failed to allocate production sequence ID from %s: %w", actualSeq, err)
			}

			updateQuery := fmt.Sprintf("UPDATE %s.%s_delta SET id = $1 WHERE id = $2", branchSchema, tbl)
			_, err = tx.Exec(updateQuery, newID, oldID)
			if err != nil {
				return nil, fmt.Errorf("failed to update delta table ID: %w", err)
			}

			remaps[tbl][oldID] = newID
		}
	}

	return remaps, nil
}

// FixFKReferences updates FK columns in delta tables to use remapped production IDs.
func FixFKReferences(tx *sql.Tx, branchSchema string, remaps map[string]map[int64]int64, fks []FKConstraint) error {
	for _, fk := range fks {
		tableRemaps, ok := remaps[fk.ReferencedTable]
		if !ok || len(tableRemaps) == 0 {
			continue
		}

		for oldID, newID := range tableRemaps {
			updateQuery := fmt.Sprintf(`
				UPDATE %s.%s_delta 
				SET %s = $1 
				WHERE %s = $2
			`, branchSchema, fk.SourceTable, fk.SourceColumn, fk.SourceColumn)

			_, err := tx.Exec(updateQuery, newID, oldID)
			if err != nil {
				return fmt.Errorf("failed to fix child FK reference in %s.%s: %w", fk.SourceTable, fk.SourceColumn, err)
			}
		}
	}

	return nil
}
