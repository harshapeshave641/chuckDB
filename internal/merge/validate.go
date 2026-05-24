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
		// We query the delta table for violating rows, checking if the referenced key
		// exists in the branch view (which represents base + delta combined state).
		query := fmt.Sprintf(`
			SELECT source.id
			FROM %s.%s_delta source
			WHERE source.__deleted = false
			  AND source.%s IS NOT NULL
			  AND NOT EXISTS (
				  SELECT 1 FROM %s.%s ref
				  WHERE ref.%s = source.%s
			  )
		`, branchSchema, fk.SourceTable, fk.SourceColumn, branchSchema, fk.ReferencedTable, fk.ReferencedColumn, fk.SourceColumn)

		rows, err := tx.Query(query)
		if err != nil {
			return nil, fmt.Errorf("FK validation query failed for %s: %w", fk.ConstraintName, err)
		}
		defer rows.Close()

		var violatingIDs []interface{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err == nil {
				violatingIDs = append(violatingIDs, id)
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
