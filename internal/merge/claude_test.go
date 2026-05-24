package merge_test

import (
	"database/sql"
	"fmt"
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

func setupNastySchema(t *testing.T, db *sql.DB) {
	t.Helper()

	drops := []string{
		"public.audit_log",
		"public.balances",
		"public.accounts",
		"public.tag_map",
		"public.tags",
		"public.posts",
		"public.members",
	}
	for _, tbl := range drops {
		_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tbl))
	}
	for _, fn := range []string{
		"public.set_updated_at",
		"public.write_audit_log",
		"public.notify_external",
	} {
		_, _ = db.Exec(fmt.Sprintf("DROP FUNCTION IF EXISTS %s() CASCADE", fn))
	}
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_nasty_branch CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_snap_branch CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_remerge_branch CASCADE")

	_, err := db.Exec(`
		-- Members with self-referential FK (org chart style)
		CREATE TABLE public.members (
			id         BIGSERIAL PRIMARY KEY,
			name       TEXT NOT NULL,
			parent_id  BIGINT REFERENCES public.members(id) ON DELETE SET NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		-- Posts owned by members
		CREATE TABLE public.posts (
			id         BIGSERIAL PRIMARY KEY,
			member_id  BIGINT NOT NULL REFERENCES public.members(id) ON DELETE CASCADE,
			title      TEXT NOT NULL,
			body       TEXT,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		-- Tags (independent)
		CREATE TABLE public.tags (
			id   BIGSERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE
		);

		-- Junction table: posts <-> tags (composite PK, two FKs)
		CREATE TABLE public.tag_map (
			post_id BIGINT NOT NULL REFERENCES public.posts(id) ON DELETE CASCADE,
			tag_id  BIGINT NOT NULL REFERENCES public.tags(id) ON DELETE CASCADE,
			PRIMARY KEY (post_id, tag_id)
		);

		-- Financial accounts (non-negative balance constraint)
		CREATE TABLE public.accounts (
			id      BIGSERIAL PRIMARY KEY,
			owner   TEXT NOT NULL,
			balance NUMERIC NOT NULL DEFAULT 0 CHECK (balance >= 0)
		);

		-- Ledger entries referencing accounts (RESTRICT delete)
		CREATE TABLE public.balances (
			id         BIGSERIAL PRIMARY KEY,
			account_id BIGINT NOT NULL REFERENCES public.accounts(id) ON DELETE RESTRICT,
			amount     NUMERIC NOT NULL,
			note       TEXT
		);

		-- Audit log (written by trigger)
		CREATE TABLE public.audit_log (
			id         BIGSERIAL PRIMARY KEY,
			table_name TEXT NOT NULL,
			row_id     BIGINT NOT NULL,
			operation  TEXT NOT NULL,
			changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		t.Fatalf("nasty schema setup failed: %v", err)
	}

	// updated_at trigger function
	_, err = db.Exec(`
		CREATE OR REPLACE FUNCTION public.set_updated_at()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			NEW.updated_at := now();
			RETURN NEW;
		END;
		$$;

		CREATE TRIGGER members_updated_at
		BEFORE UPDATE ON public.members
		FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();

		CREATE TRIGGER posts_updated_at
		BEFORE UPDATE ON public.posts
		FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
	`)
	if err != nil {
		t.Fatalf("trigger setup failed: %v", err)
	}

	// Audit trigger function (non-replicable — writes to audit_log)
	_, err = db.Exec(`
		CREATE OR REPLACE FUNCTION public.write_audit_log()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			INSERT INTO public.audit_log (table_name, row_id, operation)
			VALUES (TG_TABLE_NAME, COALESCE(NEW.id, OLD.id), TG_OP);
			RETURN COALESCE(NEW, OLD);
		END;
		$$;

		CREATE TRIGGER members_audit
		AFTER INSERT OR UPDATE OR DELETE ON public.members
		FOR EACH ROW EXECUTE FUNCTION public.write_audit_log();
	`)
	if err != nil {
		t.Fatalf("audit trigger setup failed: %v", err)
	}

	// pg_notify trigger (non-replicable — external side effect)
	_, err = db.Exec(`
		CREATE OR REPLACE FUNCTION public.notify_external()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_notify('member_changes', row_to_json(NEW)::text);
			RETURN NEW;
		END;
		$$;

		CREATE TRIGGER members_notify
		AFTER INSERT ON public.members
		FOR EACH ROW EXECUTE FUNCTION public.notify_external();
	`)
	if err != nil {
		t.Fatalf("notify trigger setup failed: %v", err)
	}

	// Seed base data
	_, err = db.Exec(`
		INSERT INTO public.members (name, parent_id) VALUES
			('Root',   NULL),
			('Child1', 1),
			('Child2', 1),
			('Leaf1',  2);

		INSERT INTO public.posts (member_id, title, body) VALUES
			(1, 'Hello World', 'First post'),
			(2, 'Child Post',  'Second post');

		INSERT INTO public.tags (name) VALUES ('go'), ('postgres'), ('testing');

		INSERT INTO public.tag_map (post_id, tag_id) VALUES
			(1, 1), (1, 2), (2, 3);

		INSERT INTO public.accounts (owner, balance) VALUES
			('Alice', 1000.00),
			('Bob',   500.00);

		INSERT INTO public.balances (account_id, amount, note) VALUES
			(1, 500.00, 'initial deposit'),
			(1, -100.00, 'withdrawal'),
			(2, 500.00, 'initial deposit');
	`)
	if err != nil {
		t.Fatalf("seed data failed: %v", err)
	}
}

// ============================================================
// TEST 1
// CHECK constraint violation during merge
//
// accounts.balance has CHECK (balance >= 0).
// Branch sets balance to -500 (violating CHECK).
// Delta table has no CHECK constraint — write succeeds in branch.
// Merge must fail when replaying into base (CHECK fires on base table).
// Merge must be fully atomic — no partial writes.
// ============================================================

func TestCheckConstraintViolationAtMerge(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "accounts", []string{"id"})
	_ = meta.TrackTable(db, "public", "balances", []string{"id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Set Alice's balance to negative in branch delta (CHECK suspended on delta)
	_, _ = db.Exec(`
		INSERT INTO chuck_nasty_branch.accounts_delta (id, owner, balance, __deleted, __is_new)
		VALUES (1, 'Alice', -500.00, false, false)
		ON CONFLICT (id) DO UPDATE SET balance = EXCLUDED.balance
	`)

	// Also make a valid change so we can verify atomicity
	_, _ = db.Exec(`
		INSERT INTO chuck_nasty_branch.accounts_delta (id, owner, balance, __deleted, __is_new)
		VALUES (2, 'Bob', 999.00, false, false)
		ON CONFLICT (id) DO UPDATE SET balance = EXCLUDED.balance
	`)

	// Merge must fail — CHECK (balance >= 0) fires on base table
	err := merge.Merge(db, "nasty_branch", false)
	if err == nil {
		t.Errorf("expected merge to fail due to CHECK constraint violation, but it succeeded")
	}

	// Atomicity: Bob's balance must NOT have been updated either
	var bobBalance float64
	_ = db.QueryRow(`SELECT balance FROM public.accounts WHERE id = 2`).Scan(&bobBalance)
	if bobBalance != 500.00 {
		t.Errorf("merge was not atomic — Bob's balance changed to %.2f despite merge failure", bobBalance)
	}

	// Alice's balance must be unchanged
	var aliceBalance float64
	_ = db.QueryRow(`SELECT balance FROM public.accounts WHERE id = 1`).Scan(&aliceBalance)
	if aliceBalance != 1000.00 {
		t.Errorf("Alice's balance should be 1000.00 after failed merge, got %.2f", aliceBalance)
	}
}

// ============================================================
// TEST 2
// UNIQUE constraint collision between branch insert and base insert
//
// tags.name has a UNIQUE constraint.
// Branch inserts a new tag named 'rust' (branch-local ID 1000000001).
// Before merge, someone inserts 'rust' directly into base (gets real ID e.g. 4).
// Merge must detect the UNIQUE collision and fail.
// Merge history must record the failure.
// Neither the branch tag nor any other branch changes must appear in base.
// ============================================================

func TestUniqueCollisionBranchVsBaseInsert(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tags", []string{"id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Branch inserts 'rust' and 'python'
	_, _ = db.Exec(`
		INSERT INTO chuck_nasty_branch.tags_delta (id, name, __deleted, __is_new)
		VALUES (1000000001, 'rust', false, true),
		       (1000000002, 'python', false, true)
	`)

	// Before merge: someone inserts 'rust' directly into base
	_, _ = db.Exec(`INSERT INTO public.tags (name) VALUES ('rust')`)

	// Merge must fail — UNIQUE violation on tags.name
	err := merge.Merge(db, "nasty_branch", false)
	if err == nil {
		t.Errorf("expected merge to fail due to UNIQUE collision on tags.name")
	}

	// 'python' must NOT be in base (atomicity)
	var pythonCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.tags WHERE name = 'python'`).Scan(&pythonCount)
	if pythonCount != 0 {
		t.Errorf("merge was not atomic — 'python' was written to base despite UNIQUE failure")
	}

	// Merge history must record the failure
	var success bool
	var errorMsg string
	_ = db.QueryRow(`
		SELECT success, error_message
		FROM chuck_meta.merge_history
		ORDER BY merged_at DESC LIMIT 1
	`).Scan(&success, &errorMsg)

	if success {
		t.Errorf("merge_history should record failure, got success=true")
	}
	if errorMsg == "" {
		t.Errorf("merge_history error_message should not be empty on failure")
	}
}

// ============================================================
// TEST 3
// Replicable trigger fires correctly inside branch
// Non-replicable trigger does NOT fire inside branch
//
// members has three triggers:
//   members_updated_at → replicable (sets updated_at)
//   members_audit      → non-replicable (inserts into audit_log)
//   members_notify     → non-replicable (pg_notify)
//
// Inside branch: UPDATE a member.
// Verify updated_at is stamped (replicable trigger worked).
// Verify audit_log has zero branch-local entries (non-replicable correctly skipped).
// After merge: verify audit_log gets ONE entry (base table trigger fires during replay).
// ============================================================

func TestTriggerReplicationCorrectness(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "members", []string{"id"})
	_ = meta.TrackTable(db, "public", "posts", []string{"id"})
	_ = meta.TrackTable(db, "public", "tags", []string{"id"})
	_ = meta.TrackTable(db, "public", "tag_map", []string{"post_id", "tag_id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Check branch creation warned about non-replicable triggers
	var nonReplicableCount int
	_ = db.QueryRow(`
		SELECT COUNT(*) FROM (
			SELECT jsonb_array_elements(replicated_triggers) AS t
			FROM chuck_meta.branch_tables bt
			JOIN chuck_meta.branches b ON b.id = bt.branch_id
			JOIN chuck_meta.tracked_tables tt ON tt.id = bt.table_id
			WHERE b.name = 'nasty_branch'
			AND tt.table_name = 'members'
		) triggers
		WHERE (t->>'replicable')::boolean = false
	`).Scan(&nonReplicableCount)
	if nonReplicableCount < 2 {
		t.Errorf("expected at least 2 non-replicable triggers (audit + notify) recorded, got %d", nonReplicableCount)
	}

	cols, _ := delta.InspectColumns(db, "public", "members")
	_ = delta.UpgradeToOverlay(db, "chuck_nasty_branch", "public", "members", cols)

	// Record baseline audit_log count
	var baseAuditCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.audit_log`).Scan(&baseAuditCount)

	// Record Root's current updated_at
	var beforeUpdatedAt time.Time
	_ = db.QueryRow(`SELECT updated_at FROM public.members WHERE id = 1`).Scan(&beforeUpdatedAt)

	// UPDATE Root inside branch
	time.Sleep(10 * time.Millisecond) // ensure time difference is detectable
	_, err := db.Exec(`
		UPDATE chuck_nasty_branch.members SET name = 'Root Modified' WHERE id = 1
	`)
	if err != nil {
		t.Fatalf("branch update failed: %v", err)
	}

	// Verify updated_at was stamped in delta (replicable trigger worked)
	var deltaUpdatedAt time.Time
	_ = db.QueryRow(`
		SELECT updated_at FROM chuck_nasty_branch.members_delta WHERE id = 1
	`).Scan(&deltaUpdatedAt)
	if !deltaUpdatedAt.After(beforeUpdatedAt) {
		t.Errorf("updated_at should be newer after branch update — replicable trigger did not fire")
	}

	// Verify audit_log has NO new entries from branch write (non-replicable correctly skipped)
	var afterBranchAuditCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.audit_log`).Scan(&afterBranchAuditCount)
	if afterBranchAuditCount != baseAuditCount {
		t.Errorf("audit_log should not have new entries from branch write — non-replicable trigger fired when it should not have")
	}

	// Merge
	if err := merge.Merge(db, "nasty_branch", false); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// After merge: audit_log MUST have one new entry (base table trigger fires during replay)
	var afterMergeAuditCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.audit_log`).Scan(&afterMergeAuditCount)
	if afterMergeAuditCount != baseAuditCount+1 {
		t.Errorf("expected audit_log to have %d entries after merge, got %d", baseAuditCount+1, afterMergeAuditCount)
	}
}

// ============================================================
// TEST 4
// Merge locked branch is rejected
//
// Simulate a merge already in progress by setting
// status = 'locked' on the branch.
// A second concurrent merge attempt must be rejected immediately
// with a clear error — not deadlock, not hang, not silent success.
// After the first merge completes and unlocks, the branch
// must return to 'active' status.
// ============================================================

func TestLockedBranchRejectsConcurrentMerge(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "members", []string{"id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Manually lock the branch (simulates merge in progress)
	_, err := db.Exec(`
		UPDATE chuck_meta.branches
		SET status = 'locked', locked_by = 'test-process-999', locked_at = now()
		WHERE name = 'nasty_branch'
	`)
	if err != nil {
		t.Fatalf("failed to lock branch: %v", err)
	}

	// Attempt to merge locked branch — must be rejected immediately
	err = merge.Merge(db, "nasty_branch", false)
	if err == nil {
		t.Errorf("merge of locked branch should have been rejected, but succeeded")
	}

	// Verify rejection is fast (not a hang waiting for lock)
	// The test itself would timeout if it hung, but we can also verify
	// the branch is still locked (not corrupted by the rejected attempt)
	var status string
	_ = db.QueryRow(`SELECT status FROM chuck_meta.branches WHERE name = 'nasty_branch'`).Scan(&status)
	if status != "locked" {
		t.Errorf("branch status should still be 'locked' after rejected merge attempt, got %s", status)
	}

	// Unlock the branch (simulates first merge completing)
	_, _ = db.Exec(`
		UPDATE chuck_meta.branches
		SET status = 'active', locked_by = NULL, locked_at = NULL
		WHERE name = 'nasty_branch'
	`)

	// Now merge should succeed
	err = merge.Merge(db, "nasty_branch", false)
	if err != nil {
		t.Errorf("merge after unlock should succeed, got: %v", err)
	}

	// Branch status should be 'merged' after successful merge
	_ = db.QueryRow(`SELECT status FROM chuck_meta.branches WHERE name = 'nasty_branch'`).Scan(&status)
	if status != "merged" {
		t.Errorf("branch status should be 'merged' after successful merge, got %s", status)
	}
}

// ============================================================
// TEST 5
// Insert + immediate delete of same row in same branch
//
// Branch inserts a net-new row (branch-local ID).
// Then immediately tombstones it in the same branch.
// The row never existed in base and was deleted before merge.
// Merge must treat this as a complete no-op for that row —
// do not attempt to insert then delete from base.
// The row must never appear in base at any point.
// ============================================================

func TestInsertThenDeleteSameRowSameBranch(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tags", []string{"id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Insert a net-new tag in branch
	_, _ = db.Exec(`
		INSERT INTO chuck_nasty_branch.tags_delta (id, name, __deleted, __is_new)
		VALUES (1000000001, 'ephemeral', false, true)
	`)

	// Immediately tombstone it in same branch
	_, _ = db.Exec(`
		UPDATE chuck_nasty_branch.tags_delta
		SET __deleted = true
		WHERE id = 1000000001
	`)

	// Verify it's invisible in branch view
	var branchCount int
	_ = db.QueryRow(`
		SELECT COUNT(*) FROM chuck_nasty_branch.tags WHERE id = 1000000001
	`).Scan(&branchCount)
	if branchCount != 0 {
		t.Errorf("insert+delete row should be invisible in branch view, got count %d", branchCount)
	}

	// Record base tag count before merge
	var beforeCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.tags`).Scan(&beforeCount)

	// Merge
	if err := merge.Merge(db, "nasty_branch", false); err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	// Base must have same tag count — ephemeral row must never have appeared
	var afterCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.tags`).Scan(&afterCount)
	if afterCount != beforeCount {
		t.Errorf("base tag count changed after insert+delete no-op merge: before=%d after=%d", beforeCount, afterCount)
	}

	// Specifically verify 'ephemeral' never made it to base
	var ephemeralCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.tags WHERE name = 'ephemeral'`).Scan(&ephemeralCount)
	if ephemeralCount != 0 {
		t.Errorf("'ephemeral' tag should never appear in base, found %d rows", ephemeralCount)
	}
}

// ============================================================
// TEST 6
// Branch drop removes ALL artifacts
//
// Create a branch. Make some changes.
// Drop the branch.
// Verify:
//   - schema is gone from postgres
//   - delta tables are gone
//   - views are gone
//   - triggers are gone
//   - branch_tables rows are soft-deleted or removed
//   - branches row has dropped_at set
//   - base tables are completely untouched
//   - chuck_meta schema itself is intact
// ============================================================

func TestBranchDropCleansUpCompletely(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "members", []string{"id"})
	_ = meta.TrackTable(db, "public", "posts", []string{"id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Make some changes
	_, _ = db.Exec(`
		INSERT INTO chuck_nasty_branch.members_delta (id, name, parent_id, __deleted, __is_new)
		VALUES (1000000001, 'Ghost Member', NULL, false, true)
	`)

	// Drop the branch
	if err := branch.Drop(db, "nasty_branch"); err != nil {
		t.Fatalf("branch drop failed: %v", err)
	}

	// Schema must not exist in postgres
	var schemaExists bool
	_ = db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.schemata
			WHERE schema_name = 'chuck_nasty_branch'
		)
	`).Scan(&schemaExists)
	if schemaExists {
		t.Errorf("schema chuck_nasty_branch should not exist after drop")
	}

	// branches row must have dropped_at set
	var droppedAt *time.Time
	_ = db.QueryRow(`SELECT dropped_at FROM chuck_meta.branches WHERE name LIKE 'nasty_branch%' AND status = 'dropped'`).Scan(&droppedAt)
	if droppedAt == nil {
		t.Errorf("branches.dropped_at should be set after drop, got NULL")
	}

	// Base tables must be completely untouched
	var memberCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.members`).Scan(&memberCount)
	if memberCount != 4 {
		t.Errorf("base members should still have 4 rows after branch drop, got %d", memberCount)
	}

	// chuck_meta schema itself must be intact
	var metaExists bool
	_ = db.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.schemata
			WHERE schema_name = 'chuck_meta'
		)
	`).Scan(&metaExists)
	if !metaExists {
		t.Errorf("chuck_meta schema should still exist after branch drop")
	}
}

// ============================================================
// TEST 7
// Merge a branch that has already been merged
//
// Create branch. Merge it successfully.
// Attempt to merge the same branch again.
// Second merge must be rejected — branch status is 'merged'.
// Base tables must not be double-written.
// ============================================================

func TestDoublemergeRejected(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tags", []string{"id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Insert a new tag in branch
	_, _ = db.Exec(`
		INSERT INTO chuck_nasty_branch.tags_delta (id, name, __deleted, __is_new)
		VALUES (1000000001, 'mergedtag', false, true)
	`)

	// First merge — must succeed
	if err := merge.Merge(db, "nasty_branch", false); err != nil {
		t.Fatalf("first merge failed: %v", err)
	}

	// Verify tag is in base after first merge
	var tagCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.tags WHERE name = 'mergedtag'`).Scan(&tagCount)
	if tagCount != 1 {
		t.Errorf("expected 'mergedtag' in base after first merge, got count %d", tagCount)
	}

	// Second merge — must be rejected
	err := merge.Merge(db, "nasty_branch", false)
	if err == nil {
		t.Errorf("second merge of already-merged branch should be rejected, but succeeded")
	}

	// Base must still have exactly one 'mergedtag' row (not double-inserted)
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.tags WHERE name = 'mergedtag'`).Scan(&tagCount)
	if tagCount != 1 {
		t.Errorf("expected exactly 1 'mergedtag' in base after double merge attempt, got %d", tagCount)
	}
}

// ============================================================
// TEST 8
// Stale lock detection and recovery
//
// Branch is locked with locked_at far in the past (simulating a
// crashed merge process). Chuck must detect the stale lock,
// clear it, and allow a new merge to proceed.
// ============================================================

func TestStaleLockDetectionAndRecovery(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "tags", []string{"id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Set a stale lock — locked 2 hours ago by a dead process
	_, _ = db.Exec(`
		UPDATE chuck_meta.branches
		SET status = 'locked',
		    locked_by = 'dead-process-000',
		    locked_at = now() - interval '2 hours'
		WHERE name = 'nasty_branch'
	`)

	// Merge must detect stale lock (> configured timeout, e.g. 30 min)
	// clear it, proceed with merge, and succeed
	err := merge.Merge(db, "nasty_branch", false)
	if err != nil {
		t.Errorf("merge should recover from stale lock and succeed, got: %v", err)
	}

	// Branch must be in 'merged' status (not 'locked')
	var status string
	_ = db.QueryRow(`SELECT status FROM chuck_meta.branches WHERE name = 'nasty_branch'`).Scan(&status)
	if status != "merged" {
		t.Errorf("branch should be 'merged' after stale lock recovery, got %s", status)
	}

	// locked_by and locked_at must be cleared
	var lockedBy *string
	var lockedAt *time.Time
	_ = db.QueryRow(`SELECT locked_by, locked_at FROM chuck_meta.branches WHERE name = 'nasty_branch'`).Scan(&lockedBy, &lockedAt)
	if lockedBy != nil || lockedAt != nil {
		t.Errorf("locked_by and locked_at should be NULL after successful merge")
	}
}

// ============================================================
// TEST 9
// Cross-branch view contamination under search_path switching
//
// Open two connections. Each sets a different search_path.
// Run concurrent queries on both connections.
// Verify that neither connection ever sees the other branch's data.
// This is the fundamental isolation guarantee of the view model.
// ============================================================

func TestSearchPathIsolationUnderConcurrentConnections(t *testing.T) {
	dsn := "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"

	db1, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("db1 open failed: %v", err)
	}
	defer db1.Close()

	db2, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("db2 open failed: %v", err)
	}
	defer db2.Close()

	setupNastySchema(t, db1)

	_ = meta.Bootstrap(db1)
	_ = meta.TrackTable(db1, "public", "tags", []string{"id"})

	if err := branch.Create(db1, "snap_branch"); err != nil {
		t.Fatalf("snap_branch create failed: %v", err)
	}
	if err := branch.Create(db1, "nasty_branch"); err != nil {
		t.Fatalf("nasty_branch create failed: %v", err)
	}

	// Insert distinct data into each branch delta
	_, _ = db1.Exec(`
		INSERT INTO chuck_snap_branch.tags_delta (id, name, __deleted, __is_new)
		VALUES (1000000001, 'snap-only-tag', false, true)
	`)
	_, _ = db1.Exec(`
		INSERT INTO chuck_nasty_branch.tags_delta (id, name, __deleted, __is_new)
		VALUES (2000000001, 'nasty-only-tag', false, true)
	`)

	// Upgrade both branch views
	cols, _ := delta.InspectColumns(db1, "public", "tags")
	_ = delta.UpgradeToOverlay(db1, "chuck_snap_branch", "public", "tags", cols)
	_ = delta.UpgradeToOverlay(db1, "chuck_nasty_branch", "public", "tags", cols)

	// Run concurrent reads on both connections simultaneously
	var wg sync.WaitGroup
	snapErrors := make(chan error, 50)
	nastyErrors := make(chan error, 50)

	for i := 0; i < 50; i++ {
		wg.Add(2)

		// Connection 1 on snap_branch
		go func() {
			defer wg.Done()
			tx, err := db1.Begin()
			if err != nil {
				snapErrors <- err
				return
			}
			defer tx.Rollback()

			_, _ = tx.Exec("SET LOCAL search_path TO chuck_snap_branch, public")

			// snap-only-tag must be visible
			var snapCount int
			_ = tx.QueryRow(`SELECT COUNT(*) FROM tags WHERE name = 'snap-only-tag'`).Scan(&snapCount)
			if snapCount != 1 {
				snapErrors <- fmt.Errorf("snap_branch cannot see snap-only-tag")
			}

			// nasty-only-tag must NOT be visible
			var nastyCount int
			_ = tx.QueryRow(`SELECT COUNT(*) FROM tags WHERE name = 'nasty-only-tag'`).Scan(&nastyCount)
			if nastyCount != 0 {
				snapErrors <- fmt.Errorf("snap_branch sees nasty-only-tag — contamination detected")
			}
		}()

		// Connection 2 on nasty_branch
		go func() {
			defer wg.Done()
			tx, err := db2.Begin()
			if err != nil {
				nastyErrors <- err
				return
			}
			defer tx.Rollback()

			_, _ = tx.Exec("SET LOCAL search_path TO chuck_nasty_branch, public")

			// nasty-only-tag must be visible
			var nastyCount int
			_ = tx.QueryRow(`SELECT COUNT(*) FROM tags WHERE name = 'nasty-only-tag'`).Scan(&nastyCount)
			if nastyCount != 1 {
				nastyErrors <- fmt.Errorf("nasty_branch cannot see nasty-only-tag")
			}

			// snap-only-tag must NOT be visible
			var snapCount int
			_ = tx.QueryRow(`SELECT COUNT(*) FROM tags WHERE name = 'snap-only-tag'`).Scan(&snapCount)
			if snapCount != 0 {
				nastyErrors <- fmt.Errorf("nasty_branch sees snap-only-tag — contamination detected")
			}
		}()
	}

	wg.Wait()
	close(snapErrors)
	close(nastyErrors)

	for err := range snapErrors {
		t.Errorf("snap_branch isolation failure: %v", err)
	}
	for err := range nastyErrors {
		t.Errorf("nasty_branch isolation failure: %v", err)
	}
}

// ============================================================
// TEST 10
// Merge order dependency — parent table must merge before child
//
// members → posts (CASCADE)
// Branch inserts a new member (branch-local ID) and a post for that member.
// Merge must replay members before posts.
// If posts are replayed first, the FK check on posts.member_id fails
// because the new member isn't in base yet.
// ============================================================

func TestMergeReplayOrderFKDependency(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupNastySchema(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "members", []string{"id"})
	_ = meta.TrackTable(db, "public", "posts", []string{"id"})
	_ = meta.TrackTable(db, "public", "tags", []string{"id"})
	_ = meta.TrackTable(db, "public", "tag_map", []string{"post_id", "tag_id"})

	if err := branch.Create(db, "nasty_branch"); err != nil {
		t.Fatalf("branch create failed: %v", err)
	}

	// Upgrade views to overlay so validation queries can locate the branch-local row IDs
	for _, tbl := range []string{"members", "posts"} {
		cols, _ := delta.InspectColumns(db, "public", tbl)
		_ = delta.UpgradeToOverlay(db, "chuck_nasty_branch", "public", tbl, cols)
	}

	// Branch inserts new member and a post for that member
	newMemberID := int64(1000000001)
	newPostID := int64(1000000001)

	_, _ = db.Exec(`
		INSERT INTO chuck_nasty_branch.members_delta (id, name, parent_id, __deleted, __is_new)
		VALUES ($1, 'New Member', NULL, false, true)
	`, newMemberID)

	_, _ = db.Exec(`
		INSERT INTO chuck_nasty_branch.posts_delta (id, member_id, title, body, __deleted, __is_new)
		VALUES ($1, $2, 'New Post', 'Body text', false, true)
	`, newPostID, newMemberID)

	// Merge must succeed — correct replay order: members first, then posts
	err := merge.Merge(db, "nasty_branch", false)
	if err != nil {
		t.Fatalf("merge failed — likely wrong replay order: %v", err)
	}

	// Verify new member in base
	var memberCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM public.members WHERE name = 'New Member'`).Scan(&memberCount)
	if memberCount != 1 {
		t.Errorf("expected New Member in base after merge, got count %d", memberCount)
	}

	// Verify new post in base with correct member_id (remapped from branch-local)
	var postCount int
	_ = db.QueryRow(`
		SELECT COUNT(*) FROM public.posts p
		JOIN public.members m ON p.member_id = m.id
		WHERE p.title = 'New Post' AND m.name = 'New Member'
	`).Scan(&postCount)
	if postCount != 1 {
		t.Errorf("expected New Post linked to New Member in base after merge, got count %d", postCount)
	}
}