package branch_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/chuckdb/chuck/internal/branch"
	"github.com/chuckdb/chuck/internal/delta"
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
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_test CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_branch_x CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_branch_y CASCADE")
	_, _ = db.Exec("CREATE SCHEMA chuck_test")

	queries := []string{
		`CREATE TABLE chuck_test.users (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE
		);`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("failed to setup test tables: %v", err)
		}
	}
}

func TestBranchLifecycleAndAncestry(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	// 1. Bootstrap and track tables
	if err := meta.Bootstrap(db); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	if err := meta.TrackTable(db, "chuck_test", "users", []string{"id"}); err != nil {
		t.Fatalf("failed to track users: %v", err)
	}

	// 2. Create parent branch (should become active automatically)
	err := branch.Create(db, "branch_x")
	if err != nil {
		t.Fatalf("failed to create branch_x: %v", err)
	}

	// Check branch list
	list, err := branch.List(db)
	if err != nil {
		t.Fatalf("failed to list branches: %v", err)
	}
	if len(list) != 1 || list[0].Name != "branch_x" || list[0].Status != "active" {
		t.Errorf("list output incorrect for branch_x: %+v", list)
	}

	// Verify active branch singleton is set
	var activeID int64
	err = db.QueryRow("SELECT branch_id FROM chuck_meta.active_branch LIMIT 1").Scan(&activeID)
	if err != nil {
		t.Fatalf("failed to query active branch: %v", err)
	}

	// 3. Create child branch (should inherit parent branch ID)
	err = branch.Create(db, "branch_y")
	if err != nil {
		t.Fatalf("failed to create branch_y: %v", err)
	}

	// Verify child branch parent links
	var parentID int64
	err = db.QueryRow("SELECT parent_branch_id FROM chuck_meta.branches WHERE name = 'branch_y'").Scan(&parentID)
	if err != nil {
		t.Fatalf("failed to query child parent link: %v", err)
	}
	if parentID != activeID {
		t.Errorf("expected parent ID %d, got %d", activeID, parentID)
	}

	list, _ = branch.List(db)
	if len(list) != 2 {
		t.Errorf("expected 2 active branches, got %d", len(list))
	}

	// 4. Verify branch DDL works with writes
	_, err = db.Exec("SET search_path TO chuck_branch_x, chuck_test")
	if err != nil {
		t.Fatalf("failed to set search_path: %v", err)
	}

	_, err = db.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")
	if err != nil {
		t.Fatalf("failed to insert user in branch_x: %v", err)
	}

	// Manually upgrade users view in branch_x to overlay
	cols, _ := delta.InspectColumns(db, "chuck_test", "users")
	err = delta.UpgradeToOverlay(db, "chuck_branch_x", "chuck_test", "users", cols)
	if err != nil {
		t.Fatalf("failed to upgrade users view: %v", err)
	}

	// Verify row is read from the view
	var uID int64
	var uName string
	err = db.QueryRow("SELECT id, name FROM users").Scan(&uID, &uName)
	if err != nil {
		t.Fatalf("failed to read from users view: %v", err)
	}
	if uID < 1000000000 {
		t.Errorf("expected branch-local ID, got %d", uID)
	}
	if uName != "Alice" {
		t.Errorf("expected Alice, got %s", uName)
	}

	// Reset path
	_, _ = db.Exec("SET search_path TO public")

	// 5. Drop child branch
	if err := branch.Drop(db, "branch_y"); err != nil {
		t.Fatalf("failed to drop branch_y: %v", err)
	}

	// active branch should still be branch_x
	var currentActive int64
	err = db.QueryRow("SELECT branch_id FROM chuck_meta.active_branch LIMIT 1").Scan(&currentActive)
	if err != nil {
		t.Fatalf("failed to query active branch after child drop: %v", err)
	}
	if currentActive != activeID {
		t.Errorf("active branch changed to %d", currentActive)
	}

	// 6. Drop parent branch
	if err := branch.Drop(db, "branch_x"); err != nil {
		t.Fatalf("failed to drop branch_x: %v", err)
	}

	// active branch singleton should be empty
	var dummy int64
	err = db.QueryRow("SELECT branch_id FROM chuck_meta.active_branch LIMIT 1").Scan(&dummy)
	if err != sql.ErrNoRows {
		t.Errorf("expected active branch singleton to be empty, got %v", err)
	}
}
