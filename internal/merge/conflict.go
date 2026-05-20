package merge

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	ErrConflictStrict = errors.New("strict merge conflict: base row was modified concurrently")
	ErrConflictGit    = errors.New("git-style merge conflict: overlapping columns modified concurrently")
)

// Strategy defines how merge conflicts are handled
type Strategy string

const (
	StrategyStrict        Strategy = "strict"
	StrategyLastWriteWins Strategy = "last_write_wins"
	StrategyGitStyle      Strategy = "git_style"
)

// ResolveConflict takes the current row state from the base table and compares it against the overlay's before_values.
// It returns the final resolved JSON payload to be written to the base table, or an error if the conflict cannot be resolved.
func ResolveConflict(
	currentBaseRow map[string]interface{},
	beforeValues map[string]interface{},
	afterValues map[string]interface{},
	modifiedCols map[string]interface{},
	strategy Strategy,
) (map[string]interface{}, error) {

	// Find columns that were modified in the base table since the branch diverged
	baseModifiedCols := make(map[string]interface{})
	for k, currentVal := range currentBaseRow {
		beforeVal, ok := beforeValues[k]
		if ok && !reflect.DeepEqual(normalizeNumber(currentVal), normalizeNumber(beforeVal)) {
			baseModifiedCols[k] = currentVal
		}
	}

	if len(baseModifiedCols) > 0 {
		switch strategy {
		case StrategyStrict:
			// Strict mode checks for ANY modification to the base row
			return nil, fmt.Errorf("%w: columns %v changed", ErrConflictStrict, keys(baseModifiedCols))
		
		case StrategyGitStyle:
			// Git-style mode checks for OVERLAPPING modifications.
			// If base modified column X, and branch modified column X -> Conflict.
			// If base modified column X, and branch modified column Y -> Safe to merge.
			overlapping := []string{}
			for k := range baseModifiedCols {
				if _, ok := modifiedCols[k]; ok {
					overlapping = append(overlapping, k)
				}
			}
			if len(overlapping) > 0 {
				return nil, fmt.Errorf("%w: overlapping columns %v", ErrConflictGit, overlapping)
			}
			// Safe! We just return the branch's modified columns. Base modifications are naturally preserved.
			return afterValues, nil

		case StrategyLastWriteWins:
			// In last_write_wins, we just force our after_values over the current base row.
			// Base modifications to columns we DIDN'T touch are preserved because we only UPDATE modifiedCols.
			// We just return the afterValues.
			return afterValues, nil

		default:
			return nil, fmt.Errorf("unknown merge strategy: %s", strategy)
		}
	}

	// No base modifications, clean merge
	return afterValues, nil
}

// normalizeNumber helps deal with JSON unmarshaling where numbers might be float64 or int
func normalizeNumber(v interface{}) interface{} {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return v
	}
}

func keys(m map[string]interface{}) []string {
	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}
	return res
}
