package proxy_test

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/chuckdb/chuck/internal/branch"
	"github.com/chuckdb/chuck/internal/meta"
	"github.com/chuckdb/chuck/internal/proxy"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func getTestDSN() string {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
	}
	return dsn
}

func setupTestTables(t *testing.T, db *sql.DB) {
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_test CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_proxy_branch CASCADE")
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

func TestProxyServerSelectAndInsert(t *testing.T) {
	dsn := getTestDSN()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	setupTestTables(t, db)

	// 1. Bootstrap
	if err := meta.Bootstrap(db); err != nil {
		t.Fatalf("Bootstrap failed: %v", err)
	}

	if err := meta.TrackTable(db, "chuck_test", "users", []string{"id"}); err != nil {
		t.Fatalf("TrackTable failed: %v", err)
	}

	// 2. Create Branch
	branchName := "proxy_branch"
	if err := branch.Create(db, branchName); err != nil {
		t.Fatalf("Create branch failed: %v", err)
	}

	// 3. Start Proxy on loopback/localhost (port 5433)
	proxyAddr := "127.0.0.1:5433"
	bp := proxy.NewBranchProxy(proxyAddr, dsn)
	if err := bp.Start(); err != nil {
		t.Fatalf("failed to start proxy: %v", err)
	}
	defer bp.Stop()

	// Wait for proxy port to bind
	time.Sleep(100 * time.Millisecond)

	// 4. Connect to proxy
	proxyDSN := "postgres://postgres:postgres@127.0.0.1:5433/app?sslmode=disable"
	pdb, err := sql.Open("pgx", proxyDSN)
	if err != nil {
		t.Fatalf("failed to connect to proxy: %v", err)
	}
	defer pdb.Close()

	// Attempt write via proxy
	_, err = pdb.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com")
	if err != nil {
		t.Fatalf("failed to insert through proxy: %v", err)
	}

	// Allow notification view upgrade and metadata updates to happen in background
	time.Sleep(300 * time.Millisecond)

	// Query via proxy - should see Bob!
	var uID int64
	var uName, uEmail string
	err = pdb.QueryRow("SELECT id, name, email FROM users").Scan(&uID, &uName, &uEmail)
	if err != nil {
		t.Fatalf("failed to query through proxy: %v", err)
	}

	if uID < 1000000000 {
		t.Errorf("expected branch sequence ID, got %d", uID)
	}
	if uName != "Bob" || uEmail != "bob@example.com" {
		t.Errorf("unexpected query result: %+v %+v %+v", uID, uName, uEmail)
	}

	// Verify base table remains empty
	var baseCount int
	err = db.QueryRow("SELECT COUNT(*) FROM chuck_test.users").Scan(&baseCount)
	if err != nil {
		t.Fatalf("failed to check base table: %v", err)
	}
	if baseCount != 0 {
		t.Errorf("base table is NOT empty (contains %d rows) - isolation failed!", baseCount)
	}

	// Verify branch tables metadata has is_dirty = true
	var isDirty bool
	err = db.QueryRow(`
		SELECT bt.is_dirty 
		FROM chuck_meta.branch_tables bt
		JOIN chuck_meta.branches b ON bt.branch_id = b.id
		WHERE b.name = 'proxy_branch' AND bt.view_name = 'users'
	`).Scan(&isDirty)
	if err != nil {
		t.Fatalf("failed to query is_dirty metadata: %v", err)
	}
	if !isDirty {
		t.Errorf("expected branch table metadata to be dirty")
	}
}
