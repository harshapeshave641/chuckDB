package meta_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chuckdb/chuck/internal/meta"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testConnString = "postgres://postgres:postgres@localhost:5432/app"

func TestCatalogLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testConnString)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// 1. Test Bootstrap
	if err := meta.Bootstrap(ctx, pool); err != nil {
		t.Fatalf("Bootstrap failed: %v", err)
	}

	catalog := meta.NewCatalog(pool)

	branchName := "feature/test-lifecycle"
	// Ensure cleanup before test in case of previous run
	_ = catalog.DropBranch(ctx, branchName)

	// 2. Test CreateBranch
	b, err := catalog.CreateBranch(ctx, branchName, nil)
	if err != nil {
		t.Fatalf("CreateBranch failed: %v", err)
	}
	if b.Name != branchName {
		t.Errorf("expected branch name %q, got %q", branchName, b.Name)
	}
	if b.Status != "active" {
		t.Errorf("expected status 'active', got %q", b.Status)
	}

	// 3. Test GetBranch
	got, err := catalog.GetBranch(ctx, branchName)
	if err != nil {
		t.Fatalf("GetBranch failed: %v", err)
	}
	if got.ID != b.ID {
		t.Errorf("expected ID %s, got %s", b.ID, got.ID)
	}

	// 4. Test ListBranches
	branches, err := catalog.ListBranches(ctx)
	if err != nil {
		t.Fatalf("ListBranches failed: %v", err)
	}
	found := false
	for _, item := range branches {
		if item.ID == b.ID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("created branch not found in ListBranches output")
	}

	// 5. Test UpdateBranchStatus
	newStatus := "merging"
	if err := catalog.UpdateBranchStatus(ctx, b.ID, newStatus); err != nil {
		t.Fatalf("UpdateBranchStatus failed: %v", err)
	}
	updated, err := catalog.GetBranch(ctx, branchName)
	if err != nil {
		t.Fatalf("GetBranch after update failed: %v", err)
	}
	if updated.Status != newStatus {
		t.Errorf("expected updated status %q, got %q", newStatus, updated.Status)
	}

	// 6. Test DropBranch
	if err := catalog.DropBranch(ctx, branchName); err != nil {
		t.Fatalf("DropBranch failed: %v", err)
	}

	// 7. Verify Drop
	_, err = catalog.GetBranch(ctx, branchName)
	if !errors.Is(err, meta.ErrBranchNotFound) {
		t.Fatalf("expected ErrBranchNotFound after drop, got %v", err)
	}
}
