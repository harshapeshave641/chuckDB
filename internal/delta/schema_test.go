package delta_test

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/chuckdb/chuck/internal/delta"
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
	_, _ = db.Exec("CREATE SCHEMA chuck_test")

	queries := []string{
		`CREATE TABLE chuck_test.users (
			id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ
		);`,

		`CREATE TABLE chuck_test.orders (
			id BIGINT PRIMARY KEY,
			user_id BIGINT REFERENCES chuck_test.users(id) ON DELETE CASCADE,
			amount NUMERIC
		);`,

		`CREATE TABLE chuck_test.order_items (
			id BIGINT PRIMARY KEY,
			order_id BIGINT REFERENCES chuck_test.orders(id) ON DELETE CASCADE,
			item_name TEXT
		);`,

		// Trigger functions
		`CREATE OR REPLACE FUNCTION chuck_test.set_updated_at() RETURNS trigger AS $$
		BEGIN
			NEW.updated_at := now();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,

		`CREATE OR REPLACE FUNCTION chuck_test.notify_user() RETURNS trigger AS $$
		BEGIN
			PERFORM pg_notify('user_channel', NEW.name);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,

		// Triggers
		`CREATE TRIGGER trg_users_updated_at BEFORE UPDATE ON chuck_test.users FOR EACH ROW EXECUTE FUNCTION chuck_test.set_updated_at();`,
		`CREATE TRIGGER trg_users_notify AFTER INSERT ON chuck_test.users FOR EACH ROW EXECUTE FUNCTION chuck_test.notify_user();`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("failed to setup test tables: %v", err)
		}
	}
}

func TestSchemaInspection(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	// 1. InspectColumns
	cols, err := delta.InspectColumns(db, "chuck_test", "users")
	if err != nil {
		t.Fatalf("InspectColumns failed: %v", err)
	}

	var foundID, foundName bool
	for _, col := range cols {
		if col.Name == "id" {
			foundID = true
			if !col.IsPrimary {
				t.Errorf("expected users.id to be primary key")
			}
		}
		if col.Name == "name" {
			foundName = true
			if col.IsPrimary {
				t.Errorf("expected users.name to NOT be primary key")
			}
		}
	}
	if !foundID || !foundName {
		t.Errorf("could not find all expected columns on users")
	}

	// 2. InspectFKs
	fks, err := delta.InspectFKs(db, "chuck_test", "orders")
	if err != nil {
		t.Fatalf("InspectFKs failed: %v", err)
	}
	if len(fks) != 1 {
		t.Fatalf("expected 1 FK on orders, got %d", len(fks))
	}
	if fks[0].SourceColumn != "user_id" || fks[0].ReferencedTable != "users" || fks[0].OnDelete != "CASCADE" {
		t.Errorf("unexpected FK constraint details: %+v", fks[0])
	}

	// 3. BuildCascadeGraph
	graph, err := delta.BuildCascadeGraph(db, "chuck_test", "users")
	if err != nil {
		t.Fatalf("BuildCascadeGraph failed: %v", err)
	}
	if len(graph) != 1 {
		t.Fatalf("expected 1 direct cascade child of users (orders), got %d", len(graph))
	}
	if graph[0].SourceTable != "orders" || len(graph[0].Children) != 1 || graph[0].Children[0].SourceTable != "order_items" {
		t.Errorf("cascade graph built incorrectly: %+v", graph)
	}

	// 4. InspectTriggers
	triggers, err := delta.InspectTriggers(db, "chuck_test", "users")
	if err != nil {
		t.Fatalf("InspectTriggers failed: %v", err)
	}
	if len(triggers) != 2 {
		t.Fatalf("expected 2 triggers on users, got %d", len(triggers))
	}
	
	for _, trg := range triggers {
		if trg.Name == "trg_users_updated_at" {
			if !trg.Replicable {
				t.Errorf("expected updated_at trigger to be classified as replicable")
			}
		}
		if trg.Name == "trg_users_notify" {
			if trg.Replicable {
				t.Errorf("expected pg_notify trigger to be classified as NOT replicable")
			}
			if !strings.Contains(trg.SkipReason, "pg_notify") {
				t.Errorf("expected SkipReason to contain 'pg_notify', got %q", trg.SkipReason)
			}
		}
	}
}
