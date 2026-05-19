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

const upsertQuery = `
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
	_, err := e.pool.Exec(ctx, upsertQuery,
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

// BatchWriteDeltas efficiently upserts multiple overlay delta records using pgx.Batch.
func (e *OverlayEngine) BatchWriteDeltas(ctx context.Context, deltas []*OverlayDelta) error {
	if len(deltas) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, d := range deltas {
		batch.Queue(upsertQuery,
			d.BranchID,
			d.TableName,
			d.RowID,
			d.ShardKey,
			d.Operation,
			d.ModifiedCols,
			d.BeforeValues,
			d.AfterValues,
		)
	}

	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin batch transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	br := tx.SendBatch(ctx, batch)

	for i := 0; i < len(deltas); i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return fmt.Errorf("batch exec failed at index %d: %w", i, err)
		}
	}
	
	if err := br.Close(); err != nil {
		return fmt.Errorf("failed to close batch result: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
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
	d, err := scanDelta(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDeltaNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to scan overlay delta: %w", err)
	}
	return d, nil
}

// ListDeltasForBranch retrieves all overlay delta records for a branch, ordered by seq ASC.
// Needed by the merge engine to iterate all deltas when merging.
func (e *OverlayEngine) ListDeltasForBranch(ctx context.Context, branchID uuid.UUID) ([]*OverlayDelta, error) {
	query := `
		SELECT branch_id, table_name, row_id, shard_key, operation, modified_cols, before_values, after_values, seq, created_at
		FROM _chuck._chuck_overlay
		WHERE branch_id = $1
		ORDER BY seq ASC
	`
	return e.listDeltas(ctx, query, branchID)
}

// ListDeltasSince retrieves overlay delta records for a branch with seq > minSeq, ordered by seq ASC.
// Needed for checkpoint-range diffs.
func (e *OverlayEngine) ListDeltasSince(ctx context.Context, branchID uuid.UUID, minSeq int64) ([]*OverlayDelta, error) {
	query := `
		SELECT branch_id, table_name, row_id, shard_key, operation, modified_cols, before_values, after_values, seq, created_at
		FROM _chuck._chuck_overlay
		WHERE branch_id = $1 AND seq > $2
		ORDER BY seq ASC
	`
	return e.listDeltas(ctx, query, branchID, minSeq)
}

// GetDeltasForTable retrieves all overlay delta records for a specific table within a branch, ordered by seq ASC.
// Needed by the query rewriter to build the overlay join.
func (e *OverlayEngine) GetDeltasForTable(ctx context.Context, branchID uuid.UUID, tableName string) ([]*OverlayDelta, error) {
	query := `
		SELECT branch_id, table_name, row_id, shard_key, operation, modified_cols, before_values, after_values, seq, created_at
		FROM _chuck._chuck_overlay
		WHERE branch_id = $1 AND table_name = $2
		ORDER BY seq ASC
	`
	return e.listDeltas(ctx, query, branchID, tableName)
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

func (e *OverlayEngine) listDeltas(ctx context.Context, query string, args ...any) ([]*OverlayDelta, error) {
	rows, err := e.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	var deltas []*OverlayDelta
	for rows.Next() {
		d, err := scanDelta(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		deltas = append(deltas, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	return deltas, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDelta(r rowScanner) (*OverlayDelta, error) {
	var d OverlayDelta
	err := r.Scan(
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
	if err != nil {
		return nil, err
	}
	return &d, nil
}
