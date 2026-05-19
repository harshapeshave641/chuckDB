package proxy

import (
	"encoding/json"
	"fmt"
	"bytes"

	"github.com/jackc/pgx/v5/pgproto3"
)

// InterceptRowDescription checks if "after_values" is present in the row description.
// If so, it returns the index of the column, and a new RowDescription with the column stripped.
func InterceptRowDescription(rd *pgproto3.RowDescription) (int, *pgproto3.RowDescription) {
	afterValuesIdx := -1
	for i, f := range rd.Fields {
		if string(f.Name) == "after_values" {
			afterValuesIdx = i
			break
		}
	}

	if afterValuesIdx == -1 {
		return -1, rd
	}

	// Create a new RowDescription without the after_values column
	newFields := make([]pgproto3.FieldDescription, 0, len(rd.Fields)-1)
	newFields = append(newFields, rd.Fields[:afterValuesIdx]...)
	newFields = append(newFields, rd.Fields[afterValuesIdx+1:]...)

	return afterValuesIdx, &pgproto3.RowDescription{Fields: newFields}
}

// InterceptDataRow merges the after_values JSONB into the base row fields if an overlay delta exists.
// Returns a new DataRow with the after_values stripped.
func InterceptDataRow(dr *pgproto3.DataRow, rd *pgproto3.RowDescription, afterValuesIdx int) (*pgproto3.DataRow, error) {
	if afterValuesIdx == -1 || afterValuesIdx >= len(dr.Values) {
		return dr, nil
	}

	afterValuesRaw := dr.Values[afterValuesIdx]
	
	// Create a new DataRow without the after_values column
	newValues := make([][]byte, 0, len(dr.Values)-1)
	newValues = append(newValues, dr.Values[:afterValuesIdx]...)
	newValues = append(newValues, dr.Values[afterValuesIdx+1:]...)
	newDr := &pgproto3.DataRow{Values: newValues}

	if afterValuesRaw == nil || bytes.Equal(afterValuesRaw, []byte("null")) {
		// No overlay delta for this row, return base row as is
		return newDr, nil
	}

	// Parse after_values JSONB
	var afterValues map[string]interface{}
	if err := json.Unmarshal(afterValuesRaw, &afterValues); err != nil {
		return nil, fmt.Errorf("failed to parse after_values JSONB: %w", err)
	}

	// Merge after_values into base row fields
	// Note: The fields in newDr.Values correspond to rd.Fields where after_values is NOT present.
	// But rd.Fields passed here might be the *original* RowDescription or the *new* one. 
	// We need the *new* RowDescription (with after_values stripped) to map indices correctly.
	for i, f := range rd.Fields {
		colName := string(f.Name)
		if val, ok := afterValues[colName]; ok {
			// Convert JSON value to string representation for Postgres wire
			var wireVal []byte
			switch v := val.(type) {
			case nil:
				wireVal = nil
			case string:
				wireVal = []byte(v)
			case float64:
				// JSON numbers are float64, format them correctly
				wireVal = []byte(fmt.Sprintf("%v", v))
			case bool:
				if v {
					wireVal = []byte("t")
				} else {
					wireVal = []byte("f")
				}
			default:
				// For other types like objects/arrays, marshal back to JSON string
				b, _ := json.Marshal(v)
				wireVal = b
			}
			newDr.Values[i] = wireVal
		}
	}

	return newDr, nil
}
