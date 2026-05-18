package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/chuckdb/chuck/internal/engine"
	"github.com/chuckdb/chuck/internal/meta"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testConnString = "postgres://postgres:postgres@localhost:5432/app"

func assertJSONEqual(t *testing.T, expected, actual []byte, label string) {
	t.Helper()
	var expMap, actMap map[string]any
	if err := json.Unmarshal(expected, &expMap); err != nil {
		t.Fatalf("failed to unmarshal expected %s: %v", label, err)
	}
	if err := json.Unmarshal(actual, &actMap); err != nil {
		t.Fatalf("failed to unmarshal actual %s: %v", label, err)
	}
	if !reflect.DeepEqual(expMap, actMap) {
		t.Errorf("mismatch %s: expected %v, got %v", label, expMap, actMap)
	}
}

func TestOverlayEngineLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testConnString)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	// 1. Ensure Schema exists
	if err := meta.Bootstrap(ctx, pool); err != nil {
		t.Fatalf("Bootstrap failed: %v", err)
	}

	eng := engine.NewOverlayEngine(pool)
	branchID := uuid.New()
	tableName := "compliance_rules"
	rowID := int64(101)
	shardKey := "tenant_abc"

	// Cleanup any existing deltas for this branch
	_ = eng.DeleteDeltasForBranch(ctx, branchID)

	// 2. Test WriteDelta (Initial Insert)
	op1 := "UPDATE"
	modCols1 := []byte(`{"active": false}`)
	beforeVal1 := []byte(`{"id": 101, "active": true, "name": "rule_1"}`)
	afterVal1 := []byte(`{"id": 101, "active": false, "name": "rule_1"}`)

	err = eng.WriteDelta(ctx, branchID, tableName, rowID, shardKey, op1, modCols1, beforeVal1, afterVal1)
	if err != nil {
		t.Fatalf("WriteDelta initial failed: %v", err)
	}

	// 3. Test GetDelta
	d, err := eng.GetDelta(ctx, branchID, tableName, rowID, shardKey)
	if err != nil {
		t.Fatalf("GetDelta failed: %v", err)
	}
	if d.Operation != op1 {
		t.Errorf("expected operation %q, got %q", op1, d.Operation)
	}
	assertJSONEqual(t, beforeVal1, d.BeforeValues, "initial before_values")
	assertJSONEqual(t, afterVal1, d.AfterValues, "initial after_values")

	// 4. Test WriteDelta (Subsequent Update - preserve before_values)
	op2 := "UPDATE"
	modCols2 := []byte(`{"active": false, "name": "rule_1_mod"}`)
	beforeVal2 := []byte(`{"id": 101, "active": false, "name": "rule_1"}`) // should be ignored / preserved from beforeVal1
	afterVal2 := []byte(`{"id": 101, "active": false, "name": "rule_1_mod"}`)

	err = eng.WriteDelta(ctx, branchID, tableName, rowID, shardKey, op2, modCols2, beforeVal2, afterVal2)
	if err != nil {
		t.Fatalf("WriteDelta subsequent failed: %v", err)
	}

	d2, err := eng.GetDelta(ctx, branchID, tableName, rowID, shardKey)
	if err != nil {
		t.Fatalf("GetDelta after subsequent write failed: %v", err)
	}
	// Verify original before_values are preserved!
	assertJSONEqual(t, beforeVal1, d2.BeforeValues, "preserved before_values")
	// Verify after_values are updated!
	assertJSONEqual(t, afterVal2, d2.AfterValues, "updated after_values")

	// 5. Test BatchWriteDeltas
	batchDeltas := []*engine.OverlayDelta{
		{
			BranchID:     branchID,
			TableName:    tableName,
			RowID:        102,
			ShardKey:     shardKey,
			Operation:    "INSERT",
			ModifiedCols: []byte(`{"id": 102, "active": true}`),
			BeforeValues: []byte(`{"id": 102, "active": true}`),
			AfterValues:  []byte(`{"id": 102, "active": true}`),
		},
		{
			BranchID:     branchID,
			TableName:    "pricing_models",
			RowID:        201,
			ShardKey:     shardKey,
			Operation:    "UPDATE",
			ModifiedCols: []byte(`{"price": 99}`),
			BeforeValues: []byte(`{"id": 201, "price": 50}`),
			AfterValues:  []byte(`{"id": 201, "price": 99}`),
		},
	}
	if err := eng.BatchWriteDeltas(ctx, batchDeltas); err != nil {
		t.Fatalf("BatchWriteDeltas failed: %v", err)
	}

	// 6. Test ListDeltasForBranch
	allDeltas, err := eng.ListDeltasForBranch(ctx, branchID)
	if err != nil {
		t.Fatalf("ListDeltasForBranch failed: %v", err)
	}
	if len(allDeltas) != 3 {
		t.Fatalf("expected 3 deltas for branch, got %d", len(allDeltas))
	}

	// 7. Test ListDeltasSince (using seq of the first delta)
	firstSeq := allDeltas[0].Seq
	recentDeltas, err := eng.ListDeltasSince(ctx, branchID, firstSeq)
	if err != nil {
		t.Fatalf("ListDeltasSince failed: %v", err)
	}
	if len(recentDeltas) != 2 {
		t.Fatalf("expected 2 recent deltas, got %d", len(recentDeltas))
	}

	// 8. Test GetDeltasForTable
	pricingDeltas, err := eng.GetDeltasForTable(ctx, branchID, "pricing_models")
	if err != nil {
		t.Fatalf("GetDeltasForTable failed: %v", err)
	}
	if len(pricingDeltas) != 1 {
		t.Fatalf("expected 1 delta for pricing_models, got %d", len(pricingDeltas))
	}
	if pricingDeltas[0].RowID != 201 {
		t.Errorf("expected rowID 201, got %d", pricingDeltas[0].RowID)
	}

	// 9. Test InjectBranchContext
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	defer tx.Rollback(ctx)

	branchName := "feature/risk-scoring"
	if err := eng.InjectBranchContext(ctx, tx, branchName); err != nil {
		t.Fatalf("InjectBranchContext failed: %v", err)
	}

	// Verify session variable
	var activeBranch string
	err = tx.QueryRow(ctx, "SELECT current_setting('chuck.branch', true);").Scan(&activeBranch)
	if err != nil {
		t.Fatalf("failed to query chuck.branch setting: %v", err)
	}
	if activeBranch != branchName {
		t.Errorf("expected active branch setting %q, got %q", branchName, activeBranch)
	}
	_ = tx.Commit(ctx)

	// 10. Test DeleteDeltasForBranch
	if err := eng.DeleteDeltasForBranch(ctx, branchID); err != nil {
		t.Fatalf("DeleteDeltasForBranch failed: %v", err)
	}

	// 11. Verify Deletion
	_, err = eng.GetDelta(ctx, branchID, tableName, rowID, shardKey)
	if !errors.Is(err, engine.ErrDeltaNotFound) {
		t.Fatalf("expected ErrDeltaNotFound after delete, got %v", err)
	}
}
