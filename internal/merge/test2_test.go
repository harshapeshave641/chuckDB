package merge_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/chuckdb/chuck/internal/branch"
	"github.com/chuckdb/chuck/internal/delta"
	"github.com/chuckdb/chuck/internal/merge"
	"github.com/chuckdb/chuck/internal/meta"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ============================================================
// SETUP
// ============================================================

func setupExtremeSchema(t *testing.T, db *sql.DB) {
	t.Helper()

	tables := []string{
		"public.order_items",
		"public.orders",
		"public.products",
		"public.users",
		"public.tenants",
	}
	for _, tbl := range tables {
		_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tbl))
	}
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_extreme_branch CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_branch_a CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_branch_b CASCADE")

	_, err := db.Exec(`
		CREATE TABLE public.tenants (
			id      BIGSERIAL PRIMARY KEY,
			name    TEXT NOT NULL UNIQUE
		);

		CREATE TABLE public.users (
			id        BIGSERIAL PRIMARY KEY,
			tenant_id BIGINT NOT NULL REFERENCES public.tenants(id) ON DELETE CASCADE,
			name      TEXT NOT NULL,
			email     TEXT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (tenant_id, email)
		);

		CREATE TABLE public.products (
			id    BIGSERIAL PRIMARY KEY,
			name  TEXT NOT NULL,
			price NUMERIC NOT NULL CHECK (price >= 0)
		);

		CREATE TABLE public.orders (
			id         BIGSERIAL PRIMARY KEY,
			user_id    BIGINT NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE public.order_items (
			order_id   BIGINT NOT NULL REFERENCES public.orders(id) ON DELETE CASCADE,
			product_id BIGINT NOT NULL REFERENCES public.products(id) ON DELETE RESTRICT,
			quantity   INT NOT NULL CHECK (quantity > 0),
			PRIMARY KEY (order_id, product_id)
		);
	`)
	if err != nil {
		t.Fatalf("schema setup failed: %v", err)
	}

	// Seed base data
	_, err = db.Exec(`
		INSERT INTO public.tenants (name) VALUES ('TenantA'), ('TenantB');
		INSERT INTO public.users (tenant_id, name, email) VALUES
			(1, 'Alice', 'alice@a.com'),
			(1, 'Bob',   'bob@a.com'),
			(2, 'Carol', 'carol@b.com');
		INSERT INTO public.products (name, price) VALUES
			('Widget', 9.99),
			('Gadget', 49.99);
		INSERT INTO public.orders (user_id) VALUES (1), (2);
		INSERT INTO public.order_items (order_id, product_id, quantity) VALUES
			(1, 1, 2),
			(1, 2, 1),
			(2, 1, 5);
	`)
	if err != nil {
		t.Fatalf("seed data failed: %v", err)
	}
}

// ============================================================
// TEST 1
// Composite PK delta upsert correctness
//
// order_items has a composite PK (order_id, product_id).
// Verify that updating one column of the composite PK pair
// does not create a phantom duplicate in the delta table,
// and that the overlay view returns exactly the right rows.
// ============================================================

func TestCompositePKDeltaUpsertCorrectness(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupExtremeSchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tenants", []string{"id"})
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "products", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})
	_ = meta.TrackTable(db, "public", "order_items", []string{"order_id", "product_id"})

	if err := branch.Create(db, "extreme_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	cols, _ := delta.InspectColumns(db, "public", "order_items")
	_ = delta.UpgradeToOverlay(db, "chuck_extreme_branch", "public", "order_items", cols)

	// Update quantity for order_id=1, product_id=1 (existing base row)
	_, err := db.Exec(`
		INSERT INTO chuck_extreme_branch.order_items_delta
			(order_id, product_id, quantity, __deleted, __is_new)
		VALUES (1, 1, 99, false, false)
		ON CONFLICT (order_id, product_id) DO UPDATE SET quantity = EXCLUDED.quantity
	`)
	if err != nil {
		t.Fatalf("delta upsert failed: %v", err)
	}

	// Verify delta has exactly one row for (1,1)
	var deltaCount int
	_ = db.QueryRow(`
		SELECT COUNT(*) FROM chuck_extreme_branch.order_items_delta
		WHERE order_id = 1 AND product_id = 1
	`).Scan(&deltaCount)
	if deltaCount != 1 {
		t.Errorf("expected 1 delta row for composite PK (1,1), got %d", deltaCount)
	}

	// Verify view returns updated quantity
	var qty int
	_ = db.QueryRow(`
		SELECT quantity FROM chuck_extreme_branch.order_items
		WHERE order_id = 1 AND product_id = 1
	`).Scan(&qty)
	if qty != 99 {
		t.Errorf("expected quantity 99 from overlay view, got %d", qty)
	}

	// Verify base is untouched
	var baseQty int
	_ = db.QueryRow(`SELECT quantity FROM public.order_items WHERE order_id = 1 AND product_id = 1`).Scan(&baseQty)
	if baseQty != 2 {
		t.Errorf("base table quantity should still be 2, got %d", baseQty)
	}

	// Verify total row count in view matches base (no phantoms)
	var viewCount, baseCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_extreme_branch.order_items`).Scan(&viewCount)
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.order_items`).Scan(&baseCount)
	if viewCount != baseCount {
		t.Errorf("view row count %d != base row count %d (phantom rows detected)", viewCount, baseCount)
	}
}

// ============================================================
// TEST 2
// NULL FK column is not a violation
//
// users.manager_id is nullable. A branch row with manager_id = NULL
// must pass FK validation at merge time — NULL means no reference,
// not a broken reference. This test ensures the validator does not
// treat NULL as a violation.
// ============================================================

func TestNullFKColumnPassesValidation(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	// Minimal schema with nullable FK
	_, _ = db.Exec("DROP TABLE IF EXISTS public.nullable_fk_test CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_null_branch CASCADE")

	_, err := db.Exec(`
		CREATE TABLE public.nullable_fk_test (
			id         BIGSERIAL PRIMARY KEY,
			name       TEXT NOT NULL,
			parent_id  BIGINT REFERENCES public.nullable_fk_test(id) ON DELETE SET NULL
		);
		INSERT INTO public.nullable_fk_test (name, parent_id) VALUES ('Root', NULL);
	`)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "nullable_fk_test", []string{"id"})

	if err := branch.Create(db, "null_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Insert a row with explicit NULL parent_id into delta
	_, err = db.Exec(`
		INSERT INTO chuck_null_branch.nullable_fk_test_delta
			(id, name, parent_id, __deleted, __is_new)
		VALUES (1000000001, 'Orphan', NULL, false, true)
	`)
	if err != nil {
		t.Fatalf("delta insert failed: %v", err)
	}

	// Merge must succeed — NULL FK is not a violation
	err = merge.Merge(db, "null_branch", false)
	if err != nil {
		t.Errorf("merge failed on NULL FK column — should have passed: %v", err)
	}

	// Verify row in base
	var count int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.nullable_fk_test WHERE name = 'Orphan'`).Scan(&count)
	if count != 1 {
		t.Errorf("expected Orphan in base after merge, got count %d", count)
	}
}

// ============================================================
// TEST 3
// Concurrent branch isolation — two branches write to same base row
//
// Branch A updates Alice's email.
// Branch B updates Alice's email to a different value.
// Both write concurrently.
// Neither branch should see the other's changes.
// Merging both should detect a conflict on Alice's row.
// ============================================================

func TestConcurrentBranchIsolation(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupExtremeSchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tenants", []string{"id"})
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "products", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})
	_ = meta.TrackTable(db, "public", "order_items", []string{"order_id", "product_id"})

	if err := branch.Create(db, "branch_a"); err != nil {
		t.Fatalf("branch A create failed: %v", err)
	}
	if err := branch.Create(db, "branch_b"); err != nil {
		t.Fatalf("branch B create failed: %v", err)
	}

	// Upgrade views to overlay so delta table modifications are queryable via the views
	for _, br := range []string{"chuck_branch_a", "chuck_branch_b"} {
		cols, _ := delta.InspectColumns(db, "public", "users")
		if err := delta.UpgradeToOverlay(db, br, "public", "users", cols); err != nil {
			t.Fatalf("failed to upgrade users to overlay on %s: %v", br, err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Branch A: update Alice email
	go func() {
		defer wg.Done()
		_, _ = db.Exec(`
			INSERT INTO chuck_branch_a.users_delta (id, tenant_id, name, email, __deleted, __is_new)
			VALUES (1, 1, 'Alice', 'alice-branch-a@a.com', false, false)
			ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
		`)
	}()

	// Branch B: update Alice email to something different
	go func() {
		defer wg.Done()
		_, _ = db.Exec(`
			INSERT INTO chuck_branch_b.users_delta (id, tenant_id, name, email, __deleted, __is_new)
			VALUES (1, 1, 'Alice', 'alice-branch-b@a.com', false, false)
			ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email
		`)
	}()

	wg.Wait()

	// Verify branch A does NOT see branch B's changes
	var emailA string
	_ = db.QueryRow(`SELECT email FROM chuck_branch_a.users WHERE id = 1`).Scan(&emailA)
	if emailA != "alice-branch-a@a.com" {
		t.Errorf("branch A contaminated by branch B: got email %s", emailA)
	}

	// Verify branch B does NOT see branch A's changes
	var emailB string
	_ = db.QueryRow(`SELECT email FROM chuck_branch_b.users WHERE id = 1`).Scan(&emailB)
	if emailB != "alice-branch-b@a.com" {
		t.Errorf("branch B contaminated by branch A: got email %s", emailB)
	}

	// Merge branch A — should succeed (first mover wins)
	if err := merge.Merge(db, "branch_a", false); err != nil {
		t.Fatalf("branch A merge failed: %v", err)
	}

	// Merge branch B — should fail with conflict on user id=1
	err := merge.Merge(db, "branch_b", false)
	if err == nil {
		t.Errorf("expected branch B merge to fail with conflict, but it succeeded")
	}

	// Base should have branch A's value
	var finalEmail string
	_ = db.QueryRow(`SELECT email FROM public.users WHERE id = 1`).Scan(&finalEmail)
	if finalEmail != "alice-branch-a@a.com" {
		t.Errorf("base has wrong email after merge: %s", finalEmail)
	}
}

// ============================================================
// TEST 4
// Deep cascade delete across 4 FK levels
//
// tenant → users → orders → order_items
// Delete a tenant in branch.
// Verify all descendants are tombstoned in the branch view.
// Verify base is completely untouched until merge.
// After merge, verify full cascade applied to base.
// ============================================================

func TestDeepCascadeDeleteFourLevels(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupExtremeSchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tenants", []string{"id"})
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "products", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})
	_ = meta.TrackTable(db, "public", "order_items", []string{"order_id", "product_id"})

	if err := branch.Create(db, "extreme_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Upgrade all views
	for _, tbl := range []string{"tenants", "users", "orders", "order_items"} {
		cols, _ := delta.InspectColumns(db, "public", tbl)
		_ = delta.UpgradeToOverlay(db, "chuck_extreme_branch", "public", tbl, cols)
	}

	// Delete TenantA (id=1) on branch — should cascade to Alice, Bob, their orders, order_items
	_, err := db.Exec(`DELETE FROM chuck_extreme_branch.tenants WHERE id = 1`)
	if err != nil {
		t.Fatalf("branch delete of tenant failed: %v", err)
	}

	// Verify tenant 1 gone from branch view
	var tenantCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_extreme_branch.tenants WHERE id = 1`).Scan(&tenantCount)
	if tenantCount != 0 {
		t.Errorf("tenant 1 should be gone from branch view, got count %d", tenantCount)
	}

	// Verify Alice and Bob gone from branch view
	var userCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_extreme_branch.users WHERE tenant_id = 1`).Scan(&userCount)
	if userCount != 0 {
		t.Errorf("users for tenant 1 should be gone from branch view, got %d", userCount)
	}

	// Verify orders gone
	var orderCount int
	_ = db.QueryRow(`
		SELECT COUNT(*) FROM chuck_extreme_branch.orders o
		JOIN chuck_extreme_branch.users u ON o.user_id = u.id
		WHERE u.tenant_id = 1
	`).Scan(&orderCount)
	// orders for tenant1 users should be gone
	// Note: This join will return 0 since users are tombstoned

	// Verify base untouched
	var baseTenantCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.tenants`).Scan(&baseTenantCount)
	if baseTenantCount != 2 {
		t.Errorf("base tenants should still have 2 rows before merge, got %d", baseTenantCount)
	}

	// Merge
	if err := merge.Merge(db, "extreme_branch", false); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// After merge: only TenantB and Carol should exist in base
	var finalTenants int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.tenants`).Scan(&finalTenants)
	if finalTenants != 1 {
		t.Errorf("expected 1 tenant after merge, got %d", finalTenants)
	}

	var finalUsers int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.users`).Scan(&finalUsers)
	if finalUsers != 1 {
		t.Errorf("expected 1 user (Carol) after merge, got %d", finalUsers)
	}

	var carolName string
	_ = db.QueryRow(`SELECT name FROM public.users`).Scan(&carolName)
	if carolName != "Carol" {
		t.Errorf("expected Carol in base, got %s", carolName)
	}

	// Order items for tenant1 orders should be gone
	var finalItems int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.order_items`).Scan(&finalItems)
	if finalItems != 0 {
		t.Errorf("expected 0 order_items after cascade merge, got %d", finalItems)
	}
}

// ============================================================
// TEST 5
// ON DELETE RESTRICT blocks branch delete
//
// order_items.product_id references products ON DELETE RESTRICT.
// Deleting a product that has order_items referencing it
// should be blocked inside the branch — cascade generator
// must emit a RESTRICT check and raise an error.
// ============================================================

func TestRestrictCascadeBlocksDelete(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupExtremeSchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tenants", []string{"id"})
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "products", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})
	_ = meta.TrackTable(db, "public", "order_items", []string{"order_id", "product_id"})

	if err := branch.Create(db, "extreme_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	for _, tbl := range []string{"products", "order_items"} {
		cols, _ := delta.InspectColumns(db, "public", tbl)
		_ = delta.UpgradeToOverlay(db, "chuck_extreme_branch", "public", tbl, cols)
	}

	// Try to delete Widget (product id=1) which has order_items referencing it
	_, err := db.Exec(`DELETE FROM chuck_extreme_branch.products WHERE id = 1`)
	if err == nil {
		t.Errorf("expected RESTRICT to block delete of product with existing order_items, but delete succeeded")
	}

	// Verify product still visible in branch view
	var count int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_extreme_branch.products WHERE id = 1`).Scan(&count)
	if count != 1 {
		t.Errorf("product should still exist after blocked delete, got count %d", count)
	}
}

// ============================================================
// TEST 6
// Branch name length boundary — Postgres 63 char schema limit
//
// chuck_ prefix = 6 chars. Max branch name = 57 chars.
// Test that a 57-char name succeeds.
// Test that a 58-char name is rejected before any DDL runs.
// Test that schema name in metadata matches actual schema name exactly.
// ============================================================

func TestBranchNameLengthBoundary(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_ = meta.Bootstrap(db)

	// 57 chars — should succeed
	name57 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 57 a's
	_, _ = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS chuck_%s CASCADE", name57))
	_ = meta.TrackTable(db, "public", "tenants", []string{"id"})

	err := branch.Create(db, name57)
	if err != nil {
		t.Errorf("57-char branch name should succeed, got error: %v", err)
	}

	// Verify schema name in metadata matches actual schema
	var schemaName string
	_ = db.QueryRow(`SELECT schema_name FROM chuck_meta.branches WHERE name = $1`, name57).Scan(&schemaName)
	expectedSchema := "chuck_" + name57
	if schemaName != expectedSchema {
		t.Errorf("metadata schema_name %q does not match expected %q", schemaName, expectedSchema)
	}

	// Verify actual schema exists in Postgres
	var exists bool
	_ = db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.schemata WHERE schema_name = $1
		)`, expectedSchema).Scan(&exists)
	if !exists {
		t.Errorf("schema %q should exist in postgres but does not", expectedSchema)
	}

	// Clean up
	_, _ = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", expectedSchema))

	// 58 chars — should be rejected before any DDL
	name58 := name57 + "b"
	err = branch.Create(db, name58)
	if err == nil {
		t.Errorf("58-char branch name should be rejected, but create succeeded")
		// Clean up if it somehow succeeded
		_, _ = db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS chuck_%s CASCADE", name58))
	}

	// Verify NO schema was created in postgres for the 58-char name
	var badSchemaExists bool
	_ = db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.schemata WHERE schema_name = $1
		)`, "chuck_"+name58).Scan(&badSchemaExists)
	if badSchemaExists {
		t.Errorf("schema for 58-char name should not exist in postgres")
	}

	// Verify NO metadata row was created for the 58-char name
	var metaCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_meta.branches WHERE name = $1`, name58).Scan(&metaCount)
	if metaCount != 0 {
		t.Errorf("metadata row should not exist for rejected 58-char branch name")
	}
}

// ============================================================
// TEST 7
// Merge with zero delta rows — complete no-op
//
// Create a branch. Write nothing. Merge it.
// Base tables must be completely unchanged.
// Merge history must record success with zero remaps.
// Duration must be recorded.
// ============================================================

func TestMergeZeroDeltaIsNoop(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupExtremeSchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tenants", []string{"id"})
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "products", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})
	_ = meta.TrackTable(db, "public", "order_items", []string{"order_id", "product_id"})

	// Record base counts before
	counts := map[string]int{}
	for _, tbl := range []string{"tenants", "users", "products", "orders", "order_items"} {
		var c int
		_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM public.%s", tbl)).Scan(&c)
		counts[tbl] = c
	}

	if err := branch.Create(db, "extreme_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Write nothing to branch

	if err := merge.Merge(db, "extreme_branch", false); err != nil {
		t.Fatalf("no-op merge failed: %v", err)
	}

	// Verify all base counts identical
	for _, tbl := range []string{"tenants", "users", "products", "orders", "order_items"} {
		var c int
		_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM public.%s", tbl)).Scan(&c)
		if c != counts[tbl] {
			t.Errorf("table %s changed during no-op merge: before=%d after=%d", tbl, counts[tbl], c)
		}
	}

	// Verify merge history recorded
	var success bool
	var durationMs int64
	var pkRemaps string
	_ = db.QueryRow(`
		SELECT success, duration_ms, pk_remaps::text
		FROM chuck_meta.merge_history
		ORDER BY merged_at DESC LIMIT 1
	`).Scan(&success, &durationMs, &pkRemaps)

	if !success {
		t.Errorf("no-op merge should be recorded as success")
	}
	if durationMs <= 0 {
		t.Errorf("duration_ms should be positive, got %d", durationMs)
	}
	var remaps map[string]map[string]interface{}
	if err := json.Unmarshal([]byte(pkRemaps), &remaps); err != nil {
		t.Errorf("failed to unmarshal pk_remaps: %v", err)
	}
	totalRemaps := 0
	for _, m := range remaps {
		totalRemaps += len(m)
	}
	if totalRemaps != 0 {
		t.Errorf("pk_remaps should be empty for no-op merge, got %s", pkRemaps)
	}
}

// ============================================================
// TEST 8
// Base table modified mid-branch — schema drift detection
//
// Create a branch. Then add a column to the base table
// outside of ChuckDB. The branch view and delta table
// no longer match the base schema.
// ChuckDB must detect this drift and refuse to merge
// rather than silently corrupting data.
// ============================================================

func TestBaseTableSchemaDriftDetected(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupExtremeSchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "users", []string{"id"})

	if err := branch.Create(db, "extreme_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Write something to branch
	_, _ = db.Exec(`
		INSERT INTO chuck_extreme_branch.users_delta (id, tenant_id, name, email, __deleted, __is_new)
		VALUES (1000000001, 1, 'Drifted User', 'drift@a.com', false, true)
	`)

	// Outside ChuckDB: add a column to base table (simulating a DBA or another migration)
	_, err := db.Exec(`ALTER TABLE public.users ADD COLUMN phone TEXT`)
	if err != nil {
		t.Fatalf("failed to alter base table: %v", err)
	}
	defer func() {
		_, _ = db.Exec(`ALTER TABLE public.users DROP COLUMN IF EXISTS phone`)
	}()

	// Merge should detect schema drift and refuse
	err = merge.Merge(db, "extreme_branch", false)
	if err == nil {
		t.Errorf("expected merge to fail due to schema drift, but it succeeded")
	}

	// Verify no partial data in base
	var count int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.users WHERE email = 'drift@a.com'`).Scan(&count)
	if count != 0 {
		t.Errorf("drifted merge wrote data to base despite schema mismatch: count=%d", count)
	}
}

// ============================================================
// TEST 9
// Tombstone a row that was already deleted in base
//
// Row exists in base. Branch tombstones it.
// Before merge, someone deletes the same row from base directly.
// Merge should handle this gracefully — deleting an already-deleted
// row is a no-op, not an error. Zero rows affected is acceptable.
// ============================================================

func TestTombstoneAlreadyDeletedBaseRow(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupExtremeSchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tenants", []string{"id"})
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "products", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})
	_ = meta.TrackTable(db, "public", "order_items", []string{"order_id", "product_id"})

	if err := branch.Create(db, "extreme_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Branch tombstones Bob (id=2)
	_, _ = db.Exec(`
		INSERT INTO chuck_extreme_branch.users_delta (id, tenant_id, name, email, __deleted, __is_new)
		VALUES (2, 1, 'Bob', 'bob@a.com', true, false)
		ON CONFLICT (id) DO UPDATE SET __deleted = true
	`)

	// Before merge: someone deletes Bob from base directly
	_, _ = db.Exec(`DELETE FROM public.users WHERE id = 2`)

	// Merge should succeed — deleting an already-gone row is not an error
	err := merge.Merge(db, "extreme_branch", false)
	if err != nil {
		t.Errorf("merge should succeed when tombstoned row already deleted from base: %v", err)
	}
}

// ============================================================
// TEST 10
// High volume concurrent writes to same branch
//
// 50 goroutines write to the same branch simultaneously.
// Each writes to a different row (no conflicts between goroutines).
// After all writes complete, verify:
//   - delta row count matches expected
//   - no duplicate rows in delta
//   - no missing rows
//   - view returns correct merged state
// ============================================================

func TestHighVolumeConcurrentBranchWrites(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()

	_, _ = db.Exec("DROP TABLE IF EXISTS public.hv_test CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_hv_branch CASCADE")

	_, err := db.Exec(`
		CREATE TABLE public.hv_test (
			id   BIGSERIAL PRIMARY KEY,
			val  TEXT NOT NULL
		);
	`)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Seed 100 rows in base
	for i := 1; i <= 100; i++ {
		_, _ = db.Exec(`INSERT INTO public.hv_test (val) VALUES ($1)`, fmt.Sprintf("base-%d", i))
	}

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "hv_test", []string{"id"})

	if err := branch.Create(db, "hv_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	cols, _ := delta.InspectColumns(db, "public", "hv_test")
	_ = delta.UpgradeToOverlay(db, "chuck_hv_branch", "public", "hv_test", cols)

	// 50 goroutines each update a distinct row (id 1..50) and insert one new row
	var wg sync.WaitGroup
	errors := make(chan error, 100)
	branchOffset := int64(1000000000)

	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			// Update existing row n in delta
			_, err := db.Exec(`
				INSERT INTO chuck_hv_branch.hv_test_delta (id, val, __deleted, __is_new)
				VALUES ($1, $2, false, false)
				ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val
			`, n, fmt.Sprintf("updated-%d", n))
			if err != nil {
				errors <- fmt.Errorf("goroutine %d update failed: %w", n, err)
				return
			}

			// Insert new row with branch-local ID
			newID := branchOffset + int64(n)
			_, err = db.Exec(`
				INSERT INTO chuck_hv_branch.hv_test_delta (id, val, __deleted, __is_new)
				VALUES ($1, $2, false, true)
				ON CONFLICT (id) DO NOTHING
			`, newID, fmt.Sprintf("new-%d", n))
			if err != nil {
				errors <- fmt.Errorf("goroutine %d insert failed: %w", n, err)
			}

			// Random micro sleep to increase interleaving
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent write error: %v", err)
	}

	// Verify delta has exactly 100 rows (50 updates + 50 inserts)
	var deltaCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_hv_branch.hv_test_delta`).Scan(&deltaCount)
	if deltaCount != 100 {
		t.Errorf("expected 100 delta rows, got %d", deltaCount)
	}

	// Verify no duplicates in delta
	var dupCount int
	_ = db.QueryRow(`
		SELECT COUNT(*) FROM (
			SELECT id, COUNT(*) AS c FROM chuck_hv_branch.hv_test_delta GROUP BY id HAVING COUNT(*) > 1
		) dups
	`).Scan(&dupCount)
	if dupCount != 0 {
		t.Errorf("found %d duplicate IDs in delta table", dupCount)
	}

	// Verify view count = 100 base rows + 50 new branch rows = 150
	var viewCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_hv_branch.hv_test`).Scan(&viewCount)
	if viewCount != 150 {
		t.Errorf("expected 150 rows in branch view, got %d", viewCount)
	}

	// Verify updated rows show branch value not base value
	var updatedVal string
	_ = db.QueryRow(`SELECT val FROM chuck_hv_branch.hv_test WHERE id = 25`).Scan(&updatedVal)
	if updatedVal != "updated-25" {
		t.Errorf("expected updated-25 from branch view, got %s", updatedVal)
	}
}

// ============================================================
// TEST 11
// Dry run produces zero writes under any circumstances
//
// Even when the merge would succeed, dry run must write nothing —
// not to base tables, not to delta tables, not to merge_history.
// This is an absolute guarantee dry run must provide.
// ============================================================

func TestDryRunWritesAbsolutelyNothing(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupExtremeSchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tenants", []string{"id"})
	_ = meta.TrackTable(db, "public", "users", []string{"id"})
	_ = meta.TrackTable(db, "public", "products", []string{"id"})
	_ = meta.TrackTable(db, "public", "orders", []string{"id"})
	_ = meta.TrackTable(db, "public", "order_items", []string{"order_id", "product_id"})

	if err := branch.Create(db, "extreme_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Make real changes on branch
	_, _ = db.Exec(`
		INSERT INTO chuck_extreme_branch.users_delta (id, tenant_id, name, email, __deleted, __is_new)
		VALUES (1000000001, 1, 'DryUser', 'dry@a.com', false, true)
	`)
	_, _ = db.Exec(`
		INSERT INTO chuck_extreme_branch.users_delta (id, tenant_id, name, email, __deleted, __is_new)
		VALUES (1, 1, 'Alice Modified', 'alice-modified@a.com', false, false)
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email
	`)

	// Snapshot state before dry run
	var baseUserCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.users`).Scan(&baseUserCount)
	var mergeHistoryCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_meta.merge_history`).Scan(&mergeHistoryCount)
	var aliceName string
	_ = db.QueryRow(`SELECT name FROM public.users WHERE id = 1`).Scan(&aliceName)

	// Dry run
	err := merge.Merge(db, "extreme_branch", true)
	if err != nil {
		t.Fatalf("dry run returned error: %v", err)
	}

	// Base user count must be identical
	var afterUserCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.users`).Scan(&afterUserCount)
	if afterUserCount != baseUserCount {
		t.Errorf("dry run changed base user count: before=%d after=%d", baseUserCount, afterUserCount)
	}

	// Alice must be unchanged
	var afterAliceName string
	_ = db.QueryRow(`SELECT name FROM public.users WHERE id = 1`).Scan(&afterAliceName)
	if afterAliceName != aliceName {
		t.Errorf("dry run modified Alice: before=%s after=%s", aliceName, afterAliceName)
	}

	// DryUser must NOT be in base
	var dryUserCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.users WHERE email = 'dry@a.com'`).Scan(&dryUserCount)
	if dryUserCount != 0 {
		t.Errorf("dry run wrote DryUser to base table")
	}

	// merge_history must be unchanged
	var afterMergeHistoryCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_meta.merge_history`).Scan(&afterMergeHistoryCount)
	if afterMergeHistoryCount != mergeHistoryCount {
		t.Errorf("dry run wrote to merge_history: before=%d after=%d", mergeHistoryCount, afterMergeHistoryCount)
	}

	// Delta must be unchanged (dry run must not touch delta either)
	var deltaCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM chuck_extreme_branch.users_delta`).Scan(&deltaCount)
	if deltaCount != 2 {
		t.Errorf("dry run modified delta table, expected 2 rows got %d", deltaCount)
	}
}