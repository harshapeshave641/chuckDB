package merge_test

import (
	"database/sql"
	"testing"

	"github.com/chuckdb/chuck/internal/branch"
	"github.com/chuckdb/chuck/internal/delta"
	"github.com/chuckdb/chuck/internal/merge"
	"github.com/chuckdb/chuck/internal/meta"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func setupWildTables(t *testing.T, db *sql.DB) {
	_, _ = db.Exec("DROP TABLE IF EXISTS public.employees CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_test CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_wild_branch CASCADE")

	// Sequences
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.emp_id_seq CASCADE")
	_, _ = db.Exec("CREATE SEQUENCE public.emp_id_seq START WITH 1")

	_, err := db.Exec(`CREATE TABLE public.employees (
		id BIGINT PRIMARY KEY DEFAULT nextval('public.emp_id_seq'),
		name TEXT NOT NULL,
		manager_id BIGINT REFERENCES public.employees(id) ON DELETE CASCADE
	);`)
	if err != nil {
		t.Fatalf("failed to create self-referential table: %v", err)
	}
}

// TestWildSelfReferentialCascade verifies that cascade delete propagation behaves correctly
// for recursive self-referential foreign keys in the same table (e.g. Org Chart deletions).
func TestWildSelfReferentialCascade(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupWildTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "employees", []string{"id"})

	err := branch.Create(db, "wild_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Setup hierarchy in branch delta:
	// CEO (1000000001)
	//   ├── VP (1000000002)
	//   │     └── Manager (1000000003)
	//   │           └── Engineer (1000000004)
	ceoID := int64(1000000001)
	vpID := int64(1000000002)
	mgrID := int64(1000000003)
	engID := int64(1000000004)

	_, _ = db.Exec("INSERT INTO chuck_wild_branch.employees_delta (id, name, manager_id, __is_new) VALUES ($1, 'CEO', NULL, true)", ceoID)
	_, _ = db.Exec("INSERT INTO chuck_wild_branch.employees_delta (id, name, manager_id, __is_new) VALUES ($1, 'VP', $2, true)", vpID, ceoID)
	_, _ = db.Exec("INSERT INTO chuck_wild_branch.employees_delta (id, name, manager_id, __is_new) VALUES ($1, 'Manager', $2, true)", mgrID, vpID)
	_, _ = db.Exec("INSERT INTO chuck_wild_branch.employees_delta (id, name, manager_id, __is_new) VALUES ($1, 'Engineer', $2, true)", engID, mgrID)

	// Upgrade views
	cols, _ := delta.InspectColumns(db, "public", "employees")
	_ = delta.UpgradeToOverlay(db, "chuck_wild_branch", "public", "employees", cols)

	// Verify count is 4
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM chuck_wild_branch.employees").Scan(&count)
	if count != 4 {
		t.Fatalf("expected 4 employees on branch, got %d", count)
	}

	// Delete VP on branch (which should cascadingly delete Manager and Engineer, but leave CEO untouched!)
	_, err = db.Exec("DELETE FROM chuck_wild_branch.employees WHERE id = $1", vpID)
	if err != nil {
		t.Fatalf("delete VP failed: %v", err)
	}

	// Verify only CEO is left in the view
	var names []string
	rows, err := db.Query("SELECT name FROM chuck_wild_branch.employees ORDER BY id")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		_ = rows.Scan(&n)
		names = append(names, n)
	}
	rows.Close()

	if len(names) != 1 || names[0] != "CEO" {
		t.Errorf("expected only CEO to remain, got names: %v", names)
	}

	// Merge
	err = merge.Merge(db, "wild_branch", false)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify base table has only CEO
	var finalCount int
	_ = db.QueryRow("SELECT COUNT(*) FROM public.employees").Scan(&finalCount)
	if finalCount != 1 {
		t.Errorf("expected 1 row in base database, got %d", finalCount)
	}

	var finalName string
	_ = db.QueryRow("SELECT name FROM public.employees").Scan(&finalName)
	if finalName != "CEO" {
		t.Errorf("expected CEO in base, got %s", finalName)
	}
}
