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

func TestDDLGenerationAndExecution(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupTestTables(t, db)

	cols, err := delta.InspectColumns(db, "chuck_test", "users")
	if err != nil {
		t.Fatalf("failed to inspect columns: %v", err)
	}

	fks, err := delta.InspectFKs(db, "chuck_test", "users")
	if err != nil {
		t.Fatalf("failed to inspect FKs: %v", err)
	}

	branchSchema := "chuck_branch_test"
	_, _ = db.Exec("DROP SCHEMA IF EXISTS " + branchSchema + " CASCADE")
	_, err = db.Exec("CREATE SCHEMA " + branchSchema)
	if err != nil {
		t.Fatalf("failed to create branch schema: %v", err)
	}

	// 1. Delta Table DDL
	deltaDDL := delta.GenerateDeltaTableDDL(branchSchema, "users", cols, fks)
	if _, err := db.Exec(deltaDDL); err != nil {
		t.Fatalf("executing delta DDL failed: %v\nDDL:\n%s", err, deltaDDL)
	}

	// 2. Sequence DDL
	seqDDL := delta.GenerateSequenceDDL(branchSchema, "users", 1)
	if _, err := db.Exec(seqDDL); err != nil {
		t.Fatalf("executing sequence DDL failed: %v\nDDL:\n%s", err, seqDDL)
	}

	// 3. Passthrough View DDL
	passViewDDL := delta.GeneratePassthroughViewDDL(branchSchema, "chuck_test", "users", cols)
	if _, err := db.Exec(passViewDDL); err != nil {
		t.Fatalf("executing passthrough view DDL failed: %v\nDDL:\n%s", err, passViewDDL)
	}

	// 4. Trigger DDL
	triggers, _ := delta.InspectTriggers(db, "chuck_test", "users")
	var replicable []delta.BaseTrigger
	for _, trg := range triggers {
		if trg.Replicable {
			replicable = append(replicable, trg)
		}
	}
	
	graph, _ := delta.BuildCascadeGraph(db, "chuck_test", "users")

	// Setup orders delta and view for cascade
	ordersCols, _ := delta.InspectColumns(db, "chuck_test", "orders")
	ordersFKs, _ := delta.InspectFKs(db, "chuck_test", "orders")
	_, err = db.Exec(delta.GenerateDeltaTableDDL(branchSchema, "orders", ordersCols, ordersFKs))
	if err != nil {
		t.Fatalf("failed to setup orders delta table: %v", err)
	}
	_, err = db.Exec(delta.GeneratePassthroughViewDDL(branchSchema, "chuck_test", "orders", ordersCols))
	if err != nil {
		t.Fatalf("failed to setup orders view: %v", err)
	}

	triggerDDL := delta.GenerateTriggerDDL(branchSchema, "chuck_test", "users", cols, graph, replicable)
	if _, err := db.Exec(triggerDDL); err != nil {
		t.Fatalf("executing trigger DDL failed: %v\nDDL:\n%s", err, triggerDDL)
	}

	// 5. Test view swap upgrade/downgrade
	err = delta.UpgradeToOverlay(db, branchSchema, "chuck_test", "users", cols)
	if err != nil {
		t.Fatalf("UpgradeToOverlay failed: %v", err)
	}

	err = delta.DowngradeToPassthrough(db, branchSchema, "chuck_test", "users", cols)
	if err != nil {
		t.Fatalf("DowngradeToPassthrough failed: %v", err)
	}
}
