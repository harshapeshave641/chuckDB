package meta_test

import (
	"database/sql"
	"os"
	"testing"

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

func TestBootstrapAndTrack(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// Clean schema if it exists
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS public.users CASCADE")

	// Create a dummy users table so OID resolution works
	_, err := db.Exec("CREATE TABLE public.users (id INT, tenant_id INT, name TEXT, PRIMARY KEY (id, tenant_id))")
	if err != nil {
		t.Fatalf("failed to create users test table: %v", err)
	}

	// 1. Test Bootstrap
	if err := meta.Bootstrap(db); err != nil {
		t.Fatalf("failed to bootstrap: %v", err)
	}

	// 2. Test Bootstrap Idempotency (call 10 times)
	for i := 0; i < 10; i++ {
		if err := meta.Bootstrap(db); err != nil {
			t.Fatalf("bootstrap is not idempotent: failed on iteration %d: %v", i, err)
		}
	}

	// 3. Test TrackTable with composite primary keys
	pks := []string{"id", "tenant_id"}
	err = meta.TrackTable(db, "public", "users", pks)
	if err != nil {
		t.Fatalf("failed to track table: %v", err)
	}

	// Should be idempotent
	err = meta.TrackTable(db, "public", "users", pks)
	if err != nil {
		t.Fatalf("track table not idempotent: %v", err)
	}

	// 4. Test ListTrackedTables
	tables, err := meta.ListTrackedTables(db)
	if err != nil {
		t.Fatalf("failed to list tracked tables: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("expected 1 tracked table, got %d", len(tables))
	}

	t0 := tables[0]
	if t0.TableSchema != "public" || t0.TableName != "users" {
		t.Errorf("unexpected table metadata: %+v", t0)
	}

	if t0.TableOID == 0 {
		t.Errorf("expected non-zero table OID, got 0")
	}

	if len(t0.PrimaryKeys) != 2 || t0.PrimaryKeys[0] != "id" || t0.PrimaryKeys[1] != "tenant_id" {
		t.Errorf("expected composite keys {id, tenant_id}, got %+v", t0.PrimaryKeys)
	}
}
