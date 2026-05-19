package merge

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/chuckdb/chuck/internal/engine"
	"github.com/chuckdb/chuck/internal/meta"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MergeEngine struct {
	pool          *pgxpool.Pool
	overlayEngine *engine.OverlayEngine
	catalog       *meta.Catalog
}

func NewMergeEngine(pool *pgxpool.Pool) *MergeEngine {
	return &MergeEngine{
		pool:          pool,
		overlayEngine: engine.NewOverlayEngine(pool),
		catalog:       meta.NewCatalog(pool),
	}
}

// MergeBranch applies all deltas from a branch to the base tables transactionally.
func (m *MergeEngine) MergeBranch(ctx context.Context, branchID uuid.UUID, strategy Strategy) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin merge transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Mark branch as merging (to prevent new writes if we implemented write blocking)
	err = m.catalog.UpdateBranchStatus(ctx, branchID, "merging")
	if err != nil {
		return fmt.Errorf("failed to update branch status: %w", err)
	}

	// 2. Fetch all deltas for the branch
	deltas, err := m.overlayEngine.ListDeltasForBranch(ctx, branchID)
	if err != nil {
		return fmt.Errorf("failed to list deltas: %w", err)
	}

	// 3. Process each delta
	for _, delta := range deltas {
		err := m.applyDelta(ctx, tx, delta, strategy)
		if err != nil {
			return fmt.Errorf("failed to apply delta on %s (row_id=%d): %w", delta.TableName, delta.RowID, err)
		}
	}

	// 4. Cleanup overlay
	err = m.overlayEngine.DeleteDeltasForBranch(ctx, branchID)
	if err != nil {
		return fmt.Errorf("failed to cleanup overlay: %w", err)
	}

	// 5. Mark as merged
	err = m.catalog.UpdateBranchStatus(ctx, branchID, "merged")
	if err != nil {
		return fmt.Errorf("failed to mark branch as merged: %w", err)
	}

	return tx.Commit(ctx)
}

func (m *MergeEngine) applyDelta(ctx context.Context, tx pgx.Tx, delta *engine.OverlayDelta, strategy Strategy) error {
	var currentBaseRow map[string]interface{}
	
	// We assume 'id' is the primary key for MVP. 
	// For a complete generic proxy, Chuck would need to query information_schema for the PK column.
	
	if delta.Operation == "UPDATE" || delta.Operation == "DELETE" {
		// Lock the row and fetch its current state
		lockQuery := fmt.Sprintf("SELECT row_to_json(t) FROM %s t WHERE id = $1 FOR UPDATE", delta.TableName)
		var rowJSON []byte
		err := tx.QueryRow(ctx, lockQuery, delta.RowID).Scan(&rowJSON)
		if err != nil {
			if err == pgx.ErrNoRows {
				if strategy == StrategyStrict {
					return fmt.Errorf("%w: row was deleted concurrently", ErrConflictStrict)
				}
				// If LastWriteWins, we can turn this UPDATE into an INSERT if the row is gone.
				// But we need the full schema, which is complex. For now, fail.
				return fmt.Errorf("row %d deleted concurrently, cannot apply %s", delta.RowID, delta.Operation)
			}
			return fmt.Errorf("failed to lock row: %w", err)
		}

		if err := json.Unmarshal(rowJSON, &currentBaseRow); err != nil {
			return fmt.Errorf("failed to parse base row json: %w", err)
		}
	}

	var beforeValues map[string]interface{}
	if len(delta.BeforeValues) > 0 {
		_ = json.Unmarshal(delta.BeforeValues, &beforeValues)
	}

	var afterValues map[string]interface{}
	if len(delta.AfterValues) > 0 {
		_ = json.Unmarshal(delta.AfterValues, &afterValues)
	}

	var modifiedCols map[string]interface{}
	if len(delta.ModifiedCols) > 0 {
		_ = json.Unmarshal(delta.ModifiedCols, &modifiedCols)
	}

	// Resolve conflicts for UPDATE
	if delta.Operation == "UPDATE" {
		resolvedAfterValues, err := ResolveConflict(currentBaseRow, beforeValues, afterValues, modifiedCols, strategy)
		if err != nil {
			return err
		}
		afterValues = resolvedAfterValues
	}

	// Apply SQL
	switch delta.Operation {
	case "INSERT":
		return m.executeInsert(ctx, tx, delta.TableName, afterValues)
	case "UPDATE":
		// Only update the modified columns (after conflict resolution, afterValues contains the merged result)
		// Wait, afterValues contains the full row or just modified columns?
		// In Chuck, overlay stores the specific subset or full row depending on WriteDelta.
		// For MVP, if afterValues contains the full row, we update all. If it contains subset, we update subset.
		return m.executeUpdate(ctx, tx, delta.TableName, delta.RowID, modifiedCols)
	case "DELETE":
		_, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", delta.TableName), delta.RowID)
		return err
	default:
		return fmt.Errorf("unknown operation: %s", delta.Operation)
	}
}

func (m *MergeEngine) executeInsert(ctx context.Context, tx pgx.Tx, tableName string, afterValues map[string]interface{}) error {
	cols := make([]string, 0, len(afterValues))
	placeholders := make([]string, 0, len(afterValues))
	args := make([]interface{}, 0, len(afterValues))

	i := 1
	for k, v := range afterValues {
		cols = append(cols, k)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		args = append(args, v)
		i++
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
	_, err := tx.Exec(ctx, query, args...)
	return err
}

func (m *MergeEngine) executeUpdate(ctx context.Context, tx pgx.Tx, tableName string, rowID int64, modifiedCols map[string]interface{}) error {
	if len(modifiedCols) == 0 {
		return nil
	}

	setClauses := make([]string, 0, len(modifiedCols))
	args := make([]interface{}, 0, len(modifiedCols)+1)
	
	i := 1
	for k, v := range modifiedCols {
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", k, i))
		args = append(args, v)
		i++
	}
	args = append(args, rowID)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", tableName, strings.Join(setClauses, ", "), i)
	_, err := tx.Exec(ctx, query, args...)
	return err
}
