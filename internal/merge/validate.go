package merge

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

type FKViolation struct {
	Table           string
	ConstraintName  string
	SourceColumn    string
	ReferencedTable string
	ViolatingRowIDs []interface{}
}

// ValidateFKs checks all suspended FK constraints against branch VIEWS.
func ValidateFKs(tx *sql.Tx, branchSchema string, suspendedFKs []FKConstraint) ([]FKViolation, error) {
	var violations []FKViolation

	for _, fk := range suspendedFKs {
		var pks []string
		var pksJSON string
		err := tx.QueryRow(`
			SELECT array_to_json(primary_keys)
			FROM chuck_meta.tracked_tables
			WHERE table_name = $1
		`, fk.SourceTable).Scan(&pksJSON)
		if err == nil && pksJSON != "" {
			_ = json.Unmarshal([]byte(pksJSON), &pks)
		}

		var selectExpr string
		if len(pks) > 0 {
			if len(pks) == 1 {
				selectExpr = fmt.Sprintf("source.%s", pks[0])
			} else {
				var concatParts []string
				for i, pk := range pks {
					if i > 0 {
						concatParts = append(concatParts, "','")
					}
					concatParts = append(concatParts, fmt.Sprintf("'%s:', source.%s", pk, pk))
				}
				var sqlParts string
				for idx, part := range concatParts {
					if idx > 0 {
						sqlParts += ", "
					}
					sqlParts += part
				}
				selectExpr = fmt.Sprintf("CONCAT(%s)", sqlParts)
			}
		} else {
			selectExpr = fmt.Sprintf("source.%s", fk.SourceColumn)
		}

		query := fmt.Sprintf(`
			SELECT %s
			FROM %s.%s_delta source
			WHERE source.__deleted = false
			  AND source.%s IS NOT NULL
			  AND NOT EXISTS (
				  SELECT 1 FROM %s.%s ref
				  WHERE ref.%s = source.%s
			  )
		`, selectExpr, branchSchema, fk.SourceTable, fk.SourceColumn, branchSchema, fk.ReferencedTable, fk.ReferencedColumn, fk.SourceColumn)

		rows, err := tx.Query(query)
		if err != nil {
			return nil, fmt.Errorf("FK validation query failed for %s: %w", fk.ConstraintName, err)
		}
		defer rows.Close()

		var violatingIDs []interface{}
		for rows.Next() {
			var val interface{}
			if err := rows.Scan(&val); err == nil {
				if bytes, ok := val.([]byte); ok {
					val = string(bytes)
				}
				violatingIDs = append(violatingIDs, val)
			}
		}
		rows.Close()

		if len(violatingIDs) > 0 {
			violations = append(violations, FKViolation{
				Table:           fk.SourceTable,
				ConstraintName:  fk.ConstraintName,
				SourceColumn:    fk.SourceColumn,
				ReferencedTable: fk.ReferencedTable,
				ViolatingRowIDs: violatingIDs,
			})
		}
	}

	return violations, nil
}

type FKConstraint struct {
	ConstraintName   string `json:"ConstraintName"`
	SourceTable      string `json:"SourceTable"`
	SourceColumn     string `json:"SourceColumn"`
	ReferencedSchema string `json:"ReferencedSchema"`
	ReferencedTable  string `json:"ReferencedTable"`
	ReferencedColumn string `json:"ReferencedColumn"`
	OnDelete         string `json:"OnDelete"`
	OnUpdate         string `json:"OnUpdate"`
}

func parseFKConstraints(jsonStr string) ([]FKConstraint, error) {
	var fks []FKConstraint
	if err := json.Unmarshal([]byte(jsonStr), &fks); err != nil {
		return nil, err
	}
	return fks, nil
}
