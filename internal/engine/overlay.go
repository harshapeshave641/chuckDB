package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrDeltaNotFound = errors.New("overlay delta not found")

type OverlayDelta struct {
	BranchID     uuid.UUID
	TableName    string
	RowID        int64
	ShardKey     string
	Operation    string
	ModifiedCols []byte
	BeforeValues []byte
	AfterValues  []byte
	Seq          int64
	CreatedAt    time.Time
}

type OverlayEngine struct {
	pool *pgxpool.Pool
}

func NewOverlayEngine(pool *pgxpool.Pool) *OverlayEngine {
	return &OverlayEngine{pool: pool}
}

// WriteDelta upserts an overlay delta record into _chuck._chuck_overlay.
// If a record already exists for the given branch, table, row, and shard key,
// its original before_values are preserved.
func (e *OverlayEngine) WriteDelta(
	ctx context.Context,
	branchID uuid.UUID,
	tableName string,
	rowID int64,
	shardKey string,
	operation string,
	modifiedCols []byte,
	beforeValues []byte,
	afterValues []byte,
) error {
	query := `
		INSERT INTO _chuck._chuck_overlay (
			branch_id, table_name, row_id, shard_key, operation, modified_cols, before_values, after_values
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (branch_id, table_name, row_id, shard_key)
		DO UPDATE SET
			operation = EXCLUDED.operation,
			modified_cols = EXCLUDED.modified_cols,
			before_values = _chuck._chuck_overlay.before_values, -- preserve original before_values
			after_values = EXCLUDED.after_values
	`

	_, err := e.pool.Exec(ctx, query,
		branchID,
		tableName,
		rowID,
		shardKey,
		operation,
		modifiedCols,
		beforeValues,
		afterValues,
	)
	if err != nil {
		return fmt.Errorf("failed to write overlay delta: %w", err)
	}
	return nil
}

// GetDelta retrieves an overlay delta record for a specific branch, table, row, and shard key.
func (e *OverlayEngine) GetDelta(
	ctx context.Context,
	branchID uuid.UUID,
	tableName string,
	rowID int64,
	shardKey string,
) (*OverlayDelta, error) {
	query := `
		SELECT branch_id, table_name, row_id, shard_key, operation, modified_cols, before_values, after_values, seq, created_at
		FROM _chuck._chuck_overlay
		WHERE branch_id = $1 AND table_name = $2 AND row_id = $3 AND shard_key = $4
	`

	row := e.pool.QueryRow(ctx, query, branchID, tableName, rowID, shardKey)
	var d OverlayDelta
	err := row.Scan(
		&d.BranchID,
		&d.TableName,
		&d.RowID,
		&d.ShardKey,
		&d.Operation,
		&d.ModifiedCols,
		&d.BeforeValues,
		&d.AfterValues,
		&d.Seq,
		&d.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDeltaNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan overlay delta: %w", err)
	}
	return &d, nil
}

// DeleteDeltasForBranch removes all overlay delta records associated with a branch ID.
func (e *OverlayEngine) DeleteDeltasForBranch(ctx context.Context, branchID uuid.UUID) error {
	query := `
		DELETE FROM _chuck._chuck_overlay
		WHERE branch_id = $1
	`

	_, err := e.pool.Exec(ctx, query, branchID)
	if err != nil {
		return fmt.Errorf("failed to delete deltas for branch %s: %w", branchID, err)
	}
	return nil
}

// InjectBranchContext sets the local transaction session variable chuck.branch.
func (e *OverlayEngine) InjectBranchContext(ctx context.Context, tx pgx.Tx, branchName string) error {
	query := fmt.Sprintf("SET LOCAL chuck.branch = '%s';", branchName)
	_, err := tx.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to inject branch context %q: %w", branchName, err)
	}
	return nil
}
