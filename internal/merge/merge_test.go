package merge_test

import (
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chuckdb/chuck/internal/branch"
	"github.com/chuckdb/chuck/internal/delta"
	"github.com/chuckdb/chuck/internal/merge"
	"github.com/chuckdb/chuck/internal/meta"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func getTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping database: %v. Make sure a Postgres container is running on localhost:5432.", err)
	}

	return db
}

func setupTestTables(t *testing.T, db *sql.DB) {
	_, _ = db.Exec("DROP TABLE IF EXISTS public.orders CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS public.users CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_test CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_merge_branch CASCADE")
	_, _ = db.Exec("CREATE SCHEMA chuck_test")

	// Sequences for production auto-incrementing IDs
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.users_id_seq CASCADE")
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.orders_id_seq CASCADE")
	_, _ = db.Exec("CREATE SEQUENCE public.users_id_seq START WITH 1")
	_, _ = db.Exec("CREATE SEQUENCE public.orders_id_seq START WITH 1")

	queries := []string{
		`CREATE TABLE public.users (
			id BIGINT PRIMARY KEY DEFAULT nextval('public.users_id_seq'),
			name TEXT NOT NULL,
			email TEXT UNIQUE,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);`,

		`CREATE TABLE public.orders (
			id BIGINT PRIMARY KEY DEFAULT nextval('public.orders_id_seq'),
			user_id BIGINT REFERENCES public.users(id) ON DELETE CASCADE,
			amount NUMERIC,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("failed to setup test tables: %v", err)
		}
	}
}

func TestMergeFKViolation(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})

	err := branch.Create(db, "merge_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Write order pointing to non-existent user
	_, err = db.Exec("INSERT INTO chuck_merge_branch.orders_delta (id, user_id, amount, __is_new) VALUES (1000000001, 99999, 99.99, true)")
	if err != nil {
		t.Fatalf("failed to insert order: %v", err)
	}

	// Merge should fail with FK violations
	err = merge.Merge(db, "merge_branch", false)
	if err == nil {
		t.Errorf("expected merge to fail with FK violations")
	} else if !strings.Contains(err.Error(), "FK violations detected") {
		t.Errorf("expected FK violations error, got %v", err)
	}
}

func TestMergeConflict(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "users", []string{"id"})

	// Insert row in base table first
	_, _ = db.Exec("INSERT INTO public.users (id, name, email, updated_at) VALUES (10, 'Base User', 'base@example.com', now())")

	err := branch.Create(db, "merge_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Update row in branch delta
	_, err = db.Exec("INSERT INTO chuck_merge_branch.users_delta (id, name, email, __is_new, __updated_at) VALUES (10, 'Branch User', 'base@example.com', false, now())")
	if err != nil {
		t.Fatalf("failed to insert delta: %v", err)
	}

	// Sleep to ensure timestamp difference
	time.Sleep(100 * time.Millisecond)

	// Update same row in base table (causing updated_at > branch creation time)
	_, _ = db.Exec("UPDATE public.users SET name = 'Updated Base', updated_at = now() WHERE id = 10")

	// Merge should fail with conflicts
	err = merge.Merge(db, "merge_branch", false)
	if err == nil {
		t.Errorf("expected merge to fail with conflicts")
	} else if !strings.Contains(err.Error(), "conflicts detected") {
		t.Errorf("expected conflicts error, got %v", err)
	}
}

func TestMergeSuccessfulReplayAndRemap(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})

	err := branch.Create(db, "merge_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Insert user and order in branch (with local temp branch IDs)
	tempUserID := int64(1000000001)
	tempOrderID := int64(1000000002)

	_, err = db.Exec("INSERT INTO chuck_merge_branch.users_delta (id, name, email, __is_new) VALUES ($1, $2, $3, true)", tempUserID, "Charlie", "charlie@example.com")
	if err != nil {
		t.Fatalf("failed to insert user delta: %v", err)
	}

	_, err = db.Exec("INSERT INTO chuck_merge_branch.orders_delta (id, user_id, amount, __is_new) VALUES ($1, $2, $3, true)", tempOrderID, tempUserID, 150.50)
	if err != nil {
		t.Fatalf("failed to insert order delta: %v", err)
	}

	// Upgrade views manually to overlay since we bypassed the proxy in test
	colsUser, _ := delta.InspectColumns(db, "public", "users")
	if err := delta.UpgradeToOverlay(db, "chuck_merge_branch", "public", "users", colsUser); err != nil {
		t.Fatalf("failed to upgrade users view: %v", err)
	}
	colsOrder, _ := delta.InspectColumns(db, "public", "orders")
	if err := delta.UpgradeToOverlay(db, "chuck_merge_branch", "public", "orders", colsOrder); err != nil {
		t.Fatalf("failed to upgrade orders view: %v", err)
	}

	// Run successful merge
	err = merge.Merge(db, "merge_branch", false)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify that charlie exists in base table
	var finalUserID int64
	var finalUserName string
	err = db.QueryRow("SELECT id, name FROM public.users WHERE email = $1", "charlie@example.com").Scan(&finalUserID, &finalUserName)
	if err != nil {
		t.Fatalf("failed to fetch replayed user from base table: %v", err)
	}
	if finalUserID >= 1000000000 {
		t.Errorf("expected user ID to be remapped to production sequence, got %d", finalUserID)
	}

	// Verify that order exists in base table and points to the remapped user ID!
	var finalOrderID int64
	var orderUserID int64
	var orderAmount float64
	err = db.QueryRow("SELECT id, user_id, amount FROM public.orders").Scan(&finalOrderID, &orderUserID, &orderAmount)
	if err != nil {
		t.Fatalf("failed to fetch replayed order from base table: %v", err)
	}
	if finalOrderID >= 1000000000 {
		t.Errorf("expected order ID to be remapped to production sequence, got %d", finalOrderID)
	}
	if orderUserID != finalUserID {
		t.Errorf("expected order.user_id to be remapped to %d, got %d", finalUserID, orderUserID)
	}
	if orderAmount != 150.50 {
		t.Errorf("expected amount 150.50, got %f", orderAmount)
	}

	// Verify branch metadata status is 'merged'
	var status string
	err = db.QueryRow("SELECT status FROM chuck_meta.branches WHERE name = 'merge_branch'").Scan(&status)
	if err != nil {
		t.Fatalf("failed to fetch branch status: %v", err)
	}
	if status != "merged" {
		t.Errorf("expected branch status to be 'merged', got %s", status)
	}

	// Verify schema is dropped
	var schemaExists bool
	_ = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)", "chuck_merge_branch").Scan(&schemaExists)
	if schemaExists {
		t.Errorf("expected schema to be dropped after successful merge")
	}
}
