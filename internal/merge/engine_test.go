package merge_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chuckdb/chuck/internal/engine"
	"github.com/chuckdb/chuck/internal/merge"
	"github.com/chuckdb/chuck/internal/meta"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const connStr = "postgres://postgres:postgres@localhost:5432/app"

func setupMergeTest(t *testing.T) (*pgxpool.Pool, *meta.Catalog, *engine.OverlayEngine, *merge.MergeEngine) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	// DDL
	_, err = pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_merge_table (
			id INT PRIMARY KEY,
			name TEXT,
			status TEXT,
			score FLOAT
		);
		TRUNCATE test_merge_table;
		DELETE FROM _chuck._chuck_branches;
	`)
	require.NoError(t, err)

	return pool, meta.NewCatalog(pool), engine.NewOverlayEngine(pool), merge.NewMergeEngine(pool)
}

func toJSON(t *testing.T, v interface{}) []byte {
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestMergeEngine_CleanMerge(t *testing.T) {
	pool, catalog, overlay, mergeEng := setupMergeTest(t)
	defer pool.Close()
	ctx := context.Background()

	// 1. Seed base table
	_, err := pool.Exec(ctx, "INSERT INTO test_merge_table (id, name, status, score) VALUES (1, 'base_record', 'active', 10.5)")
	require.NoError(t, err)

	// 2. Create Branch
	branch, err := catalog.CreateBranch(ctx, "merge-test-clean", nil)
	require.NoError(t, err)

	// 3. Write Deltas to Branch
	err = overlay.WriteDelta(ctx, 
		branch.ID, 
		"test_merge_table", 
		1, 
		"default", 
		"UPDATE", 
		toJSON(t, map[string]interface{}{"status": "inactive"}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "active", "score": 10.5}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "inactive", "score": 10.5}),
	)
	require.NoError(t, err)

	err = overlay.WriteDelta(ctx, 
		branch.ID, 
		"test_merge_table", 
		2, 
		"default", 
		"INSERT", 
		nil, 
		nil, 
		toJSON(t, map[string]interface{}{"id": 2, "name": "new_record", "status": "active", "score": 0.0}),
	)
	require.NoError(t, err)

	// 4. Merge Branch
	err = mergeEng.MergeBranch(ctx, branch.ID, merge.StrategyStrict)
	require.NoError(t, err)

	// 5. Verify Base Table
	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM test_merge_table WHERE id = 1").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "inactive", status, "Row 1 should be updated")

	var name string
	err = pool.QueryRow(ctx, "SELECT name FROM test_merge_table WHERE id = 2").Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, "new_record", name, "Row 2 should be inserted")

	// 6. Verify Branch Status and Overlay Cleanup
	b, err := catalog.GetBranch(ctx, "merge-test-clean")
	require.NoError(t, err)
	assert.Equal(t, "merged", b.Status)

	var count int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM _chuck._chuck_overlay WHERE branch_id = $1", branch.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "Overlay should be cleaned up")
}

func TestMergeEngine_StrictConflict(t *testing.T) {
	pool, catalog, overlay, mergeEng := setupMergeTest(t)
	defer pool.Close()
	ctx := context.Background()

	// 1. Seed base table
	_, err := pool.Exec(ctx, "INSERT INTO test_merge_table (id, name, status, score) VALUES (1, 'base_record', 'active', 10.5)")
	require.NoError(t, err)

	// 2. Create Branch
	branch, err := catalog.CreateBranch(ctx, "merge-test-conflict", nil)
	require.NoError(t, err)

	// 3. Write Delta (assuming original base state)
	err = overlay.WriteDelta(ctx, 
		branch.ID, 
		"test_merge_table", 
		1, 
		"default", 
		"UPDATE", 
		toJSON(t, map[string]interface{}{"status": "inactive"}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "active", "score": 10.5}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "inactive", "score": 10.5}),
	)
	require.NoError(t, err)

	// 4. CONCURRENT MODIFICATION to Base Table
	_, err = pool.Exec(ctx, "UPDATE test_merge_table SET score = 99.9 WHERE id = 1")
	require.NoError(t, err)

	// 5. Attempt Strict Merge (should fail because score changed in base table)
	err = mergeEng.MergeBranch(ctx, branch.ID, merge.StrategyStrict)
	require.Error(t, err)
	assert.ErrorIs(t, err, merge.ErrConflictStrict)

	// 6. Verify Base Table untouched by merge
	var status string
	err = pool.QueryRow(ctx, "SELECT status FROM test_merge_table WHERE id = 1").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, "active", status, "Status should NOT be updated due to conflict abort")
}

func TestMergeEngine_LastWriteWins(t *testing.T) {
	pool, catalog, overlay, mergeEng := setupMergeTest(t)
	defer pool.Close()
	ctx := context.Background()

	// 1. Seed base table
	_, err := pool.Exec(ctx, "INSERT INTO test_merge_table (id, name, status, score) VALUES (1, 'base_record', 'active', 10.5)")
	require.NoError(t, err)

	// 2. Create Branch
	branch, err := catalog.CreateBranch(ctx, "merge-test-lww", nil)
	require.NoError(t, err)

	// 3. Write Delta (assuming original base state)
	err = overlay.WriteDelta(ctx, 
		branch.ID, 
		"test_merge_table", 
		1, 
		"default", 
		"UPDATE", 
		toJSON(t, map[string]interface{}{"status": "inactive"}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "active", "score": 10.5}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "inactive", "score": 10.5}),
	)
	require.NoError(t, err)

	// 4. CONCURRENT MODIFICATION to Base Table
	_, err = pool.Exec(ctx, "UPDATE test_merge_table SET score = 99.9 WHERE id = 1")
	require.NoError(t, err)

	// 5. Attempt LastWriteWins Merge (should succeed and apply status=inactive, keeping score=99.9)
	err = mergeEng.MergeBranch(ctx, branch.ID, merge.StrategyLastWriteWins)
	require.NoError(t, err)

	// 6. Verify Base Table
	var status string
	var score float64
	err = pool.QueryRow(ctx, "SELECT status, score FROM test_merge_table WHERE id = 1").Scan(&status, &score)
	require.NoError(t, err)
	assert.Equal(t, "inactive", status, "Status should be updated by branch")
	assert.Equal(t, 99.9, score, "Score should be preserved from concurrent base update")
}

func TestMergeEngine_GitStyle(t *testing.T) {
	pool, catalog, overlay, mergeEng := setupMergeTest(t)
	defer pool.Close()
	ctx := context.Background()

	// 1. Seed base table
	_, err := pool.Exec(ctx, "INSERT INTO test_merge_table (id, name, status, score) VALUES (1, 'base_record', 'active', 10.5)")
	require.NoError(t, err)

	// 2. Create Branch
	branch, err := catalog.CreateBranch(ctx, "merge-test-git", nil)
	require.NoError(t, err)

	// 3. Write Delta modifying 'status'
	err = overlay.WriteDelta(ctx, 
		branch.ID, 
		"test_merge_table", 
		1, 
		"default", 
		"UPDATE", 
		toJSON(t, map[string]interface{}{"status": "inactive"}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "active", "score": 10.5}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "inactive", "score": 10.5}),
	)
	require.NoError(t, err)

	// 4. CONCURRENT MODIFICATION modifying 'score'
	_, err = pool.Exec(ctx, "UPDATE test_merge_table SET score = 99.9 WHERE id = 1")
	require.NoError(t, err)

	// 5. Attempt Git-Style Merge (should succeed because modified columns do not overlap)
	err = mergeEng.MergeBranch(ctx, branch.ID, merge.StrategyGitStyle)
	require.NoError(t, err)

	// 6. Verify Base Table
	var status string
	var score float64
	err = pool.QueryRow(ctx, "SELECT status, score FROM test_merge_table WHERE id = 1").Scan(&status, &score)
	require.NoError(t, err)
	assert.Equal(t, "inactive", status, "Status should be updated by branch")
	assert.Equal(t, 99.9, score, "Score should be preserved from concurrent base update")
}

func TestMergeEngine_GitStyleConflict(t *testing.T) {
	pool, catalog, overlay, mergeEng := setupMergeTest(t)
	defer pool.Close()
	ctx := context.Background()

	// 1. Seed base table
	_, err := pool.Exec(ctx, "INSERT INTO test_merge_table (id, name, status, score) VALUES (1, 'base_record', 'active', 10.5)")
	require.NoError(t, err)

	// 2. Create Branch
	branch, err := catalog.CreateBranch(ctx, "merge-test-git-conflict", nil)
	require.NoError(t, err)

	// 3. Write Delta modifying 'score'
	err = overlay.WriteDelta(ctx, 
		branch.ID, 
		"test_merge_table", 
		1, 
		"default", 
		"UPDATE", 
		toJSON(t, map[string]interface{}{"score": 50.0}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "active", "score": 10.5}), 
		toJSON(t, map[string]interface{}{"id": 1, "name": "base_record", "status": "active", "score": 50.0}),
	)
	require.NoError(t, err)

	// 4. CONCURRENT MODIFICATION modifying 'score'
	_, err = pool.Exec(ctx, "UPDATE test_merge_table SET score = 99.9 WHERE id = 1")
	require.NoError(t, err)

	// 5. Attempt Git-Style Merge (should fail because both modified 'score')
	err = mergeEng.MergeBranch(ctx, branch.ID, merge.StrategyGitStyle)
	require.Error(t, err)
	assert.ErrorIs(t, err, merge.ErrConflictGit)
}
