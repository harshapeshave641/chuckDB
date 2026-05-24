package merge_test

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/chuckdb/chuck/internal/branch"
	"github.com/chuckdb/chuck/internal/delta"
	"github.com/chuckdb/chuck/internal/merge"
	"github.com/chuckdb/chuck/internal/meta"
	"github.com/chuckdb/chuck/internal/proxy"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func setupEdgeCaseTables(t *testing.T, db *sql.DB) {
	_, _ = db.Exec("DROP TABLE IF EXISTS public.setnull_child CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS public.setnull_parent CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS public.child CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS public.parent CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS public.grandparent CASCADE")
	_, _ = db.Exec("DROP TABLE IF EXISTS public.composite_tbl CASCADE")

	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_test CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_edge_branch CASCADE")
	_, _ = db.Exec("CREATE SCHEMA chuck_test")

	// Sequences
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.gp_id_seq CASCADE")
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.p_id_seq CASCADE")
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.c_id_seq CASCADE")
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.sp_id_seq CASCADE")
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.sc_id_seq CASCADE")

	_, _ = db.Exec("CREATE SEQUENCE public.gp_id_seq START WITH 1")
	_, _ = db.Exec("CREATE SEQUENCE public.p_id_seq START WITH 1")
	_, _ = db.Exec("CREATE SEQUENCE public.c_id_seq START WITH 1")
	_, _ = db.Exec("CREATE SEQUENCE public.sp_id_seq START WITH 1")
	_, _ = db.Exec("CREATE SEQUENCE public.sc_id_seq START WITH 1")

	queries := []string{
		`CREATE TABLE public.composite_tbl (
			org_id INT,
			user_id INT,
			role TEXT,
			PRIMARY KEY (org_id, user_id)
		);`,

		`CREATE TABLE public.grandparent (
			id BIGINT PRIMARY KEY DEFAULT nextval('public.gp_id_seq'),
			name TEXT
		);`,

		`CREATE TABLE public.parent (
			id BIGINT PRIMARY KEY DEFAULT nextval('public.p_id_seq'),
			gp_id BIGINT REFERENCES public.grandparent(id) ON DELETE CASCADE,
			name TEXT
		);`,

		`CREATE TABLE public.child (
			id BIGINT PRIMARY KEY DEFAULT nextval('public.c_id_seq'),
			p_id BIGINT REFERENCES public.parent(id) ON DELETE CASCADE,
			name TEXT
		);`,

		`CREATE TABLE public.setnull_parent (
			id BIGINT PRIMARY KEY DEFAULT nextval('public.sp_id_seq'),
			name TEXT
		);`,

		`CREATE TABLE public.setnull_child (
			id BIGINT PRIMARY KEY DEFAULT nextval('public.sc_id_seq'),
			sp_id BIGINT REFERENCES public.setnull_parent(id) ON DELETE SET NULL,
			name TEXT
		);`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("failed to create edge case tables: %v", err)
		}
	}
}

func TestMergeCompositePK(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupEdgeCaseTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "composite_tbl", []string{"org_id", "user_id"})

	err := branch.Create(db, "edge_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Insert into delta table
	_, err = db.Exec("INSERT INTO chuck_edge_branch.composite_tbl_delta (org_id, user_id, role, __is_new) VALUES (1, 10, 'admin', true)")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Upgrade view to verify read
	cols, _ := delta.InspectColumns(db, "public", "composite_tbl")
	_ = delta.UpgradeToOverlay(db, "chuck_edge_branch", "public", "composite_tbl", cols)

	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM chuck_edge_branch.composite_tbl").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 row in overlay view, got %d", count)
	}

	// Merge
	err = merge.Merge(db, "edge_branch", false)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify row in base table
	var role string
	err = db.QueryRow("SELECT role FROM public.composite_tbl WHERE org_id = 1 AND user_id = 10").Scan(&role)
	if err != nil {
		t.Fatalf("row not found in base: %v", err)
	}
	if role != "admin" {
		t.Errorf("expected admin, got %s", role)
	}
}

func TestMergeMultiLevelCascade(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupEdgeCaseTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "grandparent", []string{"id"})
	_ = meta.TrackTable(db, "public", "parent", []string{"id"})
	_ = meta.TrackTable(db, "public", "child", []string{"id"})

	err := branch.Create(db, "edge_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Insert grandparent, parent, child
	gpID := int64(1000000001)
	pID := int64(1000000002)
	cID := int64(1000000003)

	_, _ = db.Exec("INSERT INTO chuck_edge_branch.grandparent_delta (id, name, __is_new) VALUES ($1, 'GP1', true)", gpID)
	_, _ = db.Exec("INSERT INTO chuck_edge_branch.parent_delta (id, gp_id, name, __is_new) VALUES ($1, $2, 'P1', true)", pID, gpID)
	_, _ = db.Exec("INSERT INTO chuck_edge_branch.child_delta (id, p_id, name, __is_new) VALUES ($1, $2, 'C1', true)", cID, pID)

	// Upgrade views
	colsGP, _ := delta.InspectColumns(db, "public", "grandparent")
	_ = delta.UpgradeToOverlay(db, "chuck_edge_branch", "public", "grandparent", colsGP)
	colsP, _ := delta.InspectColumns(db, "public", "parent")
	_ = delta.UpgradeToOverlay(db, "chuck_edge_branch", "public", "parent", colsP)
	colsC, _ := delta.InspectColumns(db, "public", "child")
	_ = delta.UpgradeToOverlay(db, "chuck_edge_branch", "public", "child", colsC)

	// Verify child exists
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM chuck_edge_branch.child WHERE id = $1", cID).Scan(&count)
	if count != 1 {
		t.Fatalf("expected child to exist on branch")
	}

	// Delete grandparent on branch (which should cascadingly delete parent and child via instead-of triggers!)
	_, err = db.Exec("DELETE FROM chuck_edge_branch.grandparent WHERE id = $1", gpID)
	if err != nil {
		t.Fatalf("delete GP failed: %v", err)
	}

	// Verify parent and child views are now empty (due to cascading tombstoning in delta tables)
	var parentCount int
	_ = db.QueryRow("SELECT COUNT(*) FROM chuck_edge_branch.parent").Scan(&parentCount)
	if parentCount != 0 {
		t.Errorf("expected parent to be cascadingly deleted, got %d rows", parentCount)
	}

	var childCount int
	_ = db.QueryRow("SELECT COUNT(*) FROM chuck_edge_branch.child").Scan(&childCount)
	if childCount != 0 {
		t.Errorf("expected child to be cascadingly deleted, got %d rows", childCount)
	}

	// Merge
	err = merge.Merge(db, "edge_branch", false)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify nothing remains in base tables since everything was deleted on the branch before merge
	var totalGP int
	_ = db.QueryRow("SELECT COUNT(*) FROM public.grandparent").Scan(&totalGP)
	if totalGP != 0 {
		t.Errorf("expected 0 base grandparents, got %d", totalGP)
	}
}

func TestMergeOnDeleteSetNull(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupEdgeCaseTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "setnull_parent", []string{"id"})
	_ = meta.TrackTable(db, "public", "setnull_child", []string{"id"})

	err := branch.Create(db, "edge_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	pID := int64(1000000001)
	cID := int64(1000000002)

	_, _ = db.Exec("INSERT INTO chuck_edge_branch.setnull_parent_delta (id, name, __is_new) VALUES ($1, 'SP1', true)", pID)
	_, _ = db.Exec("INSERT INTO chuck_edge_branch.setnull_child_delta (id, sp_id, name, __is_new) VALUES ($1, $2, 'SC1', true)", cID, pID)

	colsP, _ := delta.InspectColumns(db, "public", "setnull_parent")
	_ = delta.UpgradeToOverlay(db, "chuck_edge_branch", "public", "setnull_parent", colsP)
	colsC, _ := delta.InspectColumns(db, "public", "setnull_child")
	_ = delta.UpgradeToOverlay(db, "chuck_edge_branch", "public", "setnull_child", colsC)

	// Delete parent on branch view
	_, err = db.Exec("DELETE FROM chuck_edge_branch.setnull_parent WHERE id = $1", pID)
	if err != nil {
		t.Fatalf("delete SP failed: %v", err)
	}

	// Verify child's sp_id is set to NULL on branch view
	var childSPID sql.NullInt64
	err = db.QueryRow("SELECT sp_id FROM chuck_edge_branch.setnull_child WHERE id = $1", cID).Scan(&childSPID)
	if err != nil {
		t.Fatalf("failed to query child sp_id: %v", err)
	}
	if childSPID.Valid {
		t.Errorf("expected child sp_id to be NULL, got %d", childSPID.Int64)
	}

	// Merge
	err = merge.Merge(db, "edge_branch", false)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Verify child exists in base and sp_id is NULL
	var baseChildSPID sql.NullInt64
	err = db.QueryRow("SELECT sp_id FROM public.setnull_child").Scan(&baseChildSPID)
	if err != nil {
		t.Fatalf("failed to query base child: %v", err)
	}
	if baseChildSPID.Valid {
		t.Errorf("expected base child sp_id to be NULL after merge, got %d", baseChildSPID.Int64)
	}
}

func TestProxyConnectionSearchPathPersistence(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupEdgeCaseTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "composite_tbl", []string{"org_id", "user_id"})

	err := branch.Create(db, "edge_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Switched context
	_, err = db.Exec(`
		INSERT INTO chuck_meta.active_branch (singleton, branch_id)
		VALUES (true, (SELECT id FROM chuck_meta.branches WHERE name = 'edge_branch'))
		ON CONFLICT (singleton) DO UPDATE SET branch_id = EXCLUDED.branch_id
	`)
	if err != nil {
		t.Fatalf("checkout failed: %v", err)
	}

	// Start proxy
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
	}
	bp := proxy.NewBranchProxy("127.0.0.1:5499", dsn)
	if err := bp.Start(); err != nil {
		t.Fatalf("failed to start proxy: %v", err)
	}
	defer bp.Stop()

	// Connect to proxy via raw TCP or standard driver
	proxyDB, err := sql.Open("pgx", "postgres://postgres:postgres@127.0.0.1:5499/app?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to connect to proxy: %v", err)
	}
	defer proxyDB.Close()

	// Verify search_path is set to the branch schema
	var path string
	err = proxyDB.QueryRow("SHOW search_path").Scan(&path)
	if err != nil {
		t.Fatalf("query failed on proxy: %v", err)
	}

	if !strings.Contains(path, "chuck_edge_branch") {
		t.Errorf("expected search_path to contain chuck_edge_branch, got %q", path)
	}

	// Run multiple commands to verify session-scoped persistence (not transaction-scoped)
	_, _ = proxyDB.Exec("SELECT 1")
	err = proxyDB.QueryRow("SHOW search_path").Scan(&path)
	if err != nil {
		t.Fatalf("second query failed: %v", err)
	}

	if !strings.Contains(path, "chuck_edge_branch") {
		t.Errorf("search_path lost session scope after first command: %q", path)
	}
}

func TestTriggerClassificationSkip(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupEdgeCaseTables(t, db)

	// Create triggers with replicable and non-replicable bodies
	_, _ = db.Exec("DROP TABLE IF EXISTS public.trigger_test CASCADE")
	_, _ = db.Exec(`CREATE TABLE public.trigger_test (
		id INT PRIMARY KEY,
		val TEXT
	)`)

	_, _ = db.Exec(`
		CREATE OR REPLACE FUNCTION public.normal_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			NEW.val := UPPER(NEW.val);
			RETURN NEW;
		END;
		$$;
	`)

	_, _ = db.Exec(`
		CREATE OR REPLACE FUNCTION public.notify_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_notify('some_channel', 'hello');
			RETURN NEW;
		END;
		$$;
	`)

	_, _ = db.Exec("CREATE TRIGGER trg_normal BEFORE INSERT ON public.trigger_test FOR EACH ROW EXECUTE FUNCTION public.normal_trigger_fn()")
	_, _ = db.Exec("CREATE TRIGGER trg_notify BEFORE INSERT ON public.trigger_test FOR EACH ROW EXECUTE FUNCTION public.notify_trigger_fn()")

	triggers, err := delta.InspectTriggers(db, "public", "trigger_test")
	if err != nil {
		t.Fatalf("failed to inspect triggers: %v", err)
	}

	var normalSeen, notifySeen bool
	for _, trg := range triggers {
		if trg.Name == "trg_normal" {
			normalSeen = true
			if !trg.Replicable {
				t.Errorf("expected trg_normal to be replicable")
			}
		}
		if trg.Name == "trg_notify" {
			notifySeen = true
			if trg.Replicable {
				t.Errorf("expected trg_notify to be non-replicable")
			}
			if trg.SkipReason != "uses pg_notify" {
				t.Errorf("expected skip reason uses pg_notify, got %q", trg.SkipReason)
			}
		}
	}

	if !normalSeen || !notifySeen {
		t.Errorf("did not find both expected triggers")
	}
}
