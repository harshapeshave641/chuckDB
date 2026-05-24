package delta

import (
	"database/sql"
	"fmt"
)

// UpgradeToOverlay swaps a passthrough view to an overlay view.
// Uses CREATE OR REPLACE VIEW — no DROP, no cascade.
// Called on the first write to a delta table.
func UpgradeToOverlay(db *sql.DB, branchSchema, baseSchema, table string, cols []ColumnDef) error {
	ddl := GenerateOverlayViewDDL(branchSchema, baseSchema, table, cols)
	_, err := db.Exec(ddl)
	if err != nil {
		return fmt.Errorf("failed to upgrade view to overlay for %s: %w", table, err)
	}
	return nil
}

// DowngradeToPassthrough swaps an overlay view back to a passthrough view.
// Called after merge cleanup when delta is empty.
func DowngradeToPassthrough(db *sql.DB, branchSchema, baseSchema, table string, cols []ColumnDef) error {
	ddl := GeneratePassthroughViewDDL(branchSchema, baseSchema, table, cols)
	_, err := db.Exec(ddl)
	if err != nil {
		return fmt.Errorf("failed to downgrade view to passthrough for %s: %w", table, err)
	}
	return nil
}
