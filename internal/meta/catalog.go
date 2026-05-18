package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrBranchNotFound = errors.New("branch not found")
	ErrBranchExists   = errors.New("branch already exists")
)

type Branch struct {
	ID          uuid.UUID
	Name        string
	ParentID    *uuid.UUID
	Status      string
	CreatedAt   time.Time
	TTLSeconds  *int
	MergeStatus *string
}

type Catalog struct {
	pool *pgxpool.Pool
}

func NewCatalog(pool *pgxpool.Pool) *Catalog {
	return &Catalog{pool: pool}
}

func (c *Catalog) CreateBranch(ctx context.Context, name string, parentID *uuid.UUID) (*Branch, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUIDv7: %w", err)
	}

	query := `
		INSERT INTO _chuck._chuck_branches (id, name, parent_id)
		VALUES ($1, $2, $3)
		RETURNING id, name, parent_id, status, created_at, ttl_seconds, merge_status
	`

	row := c.pool.QueryRow(ctx, query, id, name, parentID)
	return scanBranch(row)
}

func (c *Catalog) GetBranch(ctx context.Context, name string) (*Branch, error) {
	query := `
		SELECT id, name, parent_id, status, created_at, ttl_seconds, merge_status
		FROM _chuck._chuck_branches
		WHERE name = $1
	`

	row := c.pool.QueryRow(ctx, query, name)
	b, err := scanBranch(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrBranchNotFound
	}
	return b, err
}

func (c *Catalog) ListBranches(ctx context.Context) ([]*Branch, error) {
	query := `
		SELECT id, name, parent_id, status, created_at, ttl_seconds, merge_status
		FROM _chuck._chuck_branches
		ORDER BY created_at DESC
	`

	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list branches: %w", err)
	}
	defer rows.Close()

	var branches []*Branch
	for rows.Next() {
		b, err := scanBranch(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan branch row: %w", err)
		}
		branches = append(branches, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return branches, nil
}

func (c *Catalog) DropBranch(ctx context.Context, name string) error {
	query := `
		DELETE FROM _chuck._chuck_branches
		WHERE name = $1
	`

	tag, err := c.pool.Exec(ctx, query, name)
	if err != nil {
		return fmt.Errorf("failed to drop branch %q: %w", name, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrBranchNotFound
	}
	return nil
}

func (c *Catalog) UpdateBranchStatus(ctx context.Context, id uuid.UUID, status string) error {
	query := `
		UPDATE _chuck._chuck_branches
		SET status = $2
		WHERE id = $1
	`

	tag, err := c.pool.Exec(ctx, query, id, status)
	if err != nil {
		return fmt.Errorf("failed to update branch status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrBranchNotFound
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanBranch(r rowScanner) (*Branch, error) {
	var b Branch
	err := r.Scan(
		&b.ID,
		&b.Name,
		&b.ParentID,
		&b.Status,
		&b.CreatedAt,
		&b.TTLSeconds,
		&b.MergeStatus,
	)
	if err != nil {
		return nil, err
	}
	return &b, nil
}
