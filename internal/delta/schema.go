package delta

import (
	"database/sql"
	"fmt"
	"strings"
)

type ColumnDef struct {
	Name       string
	DataType   string
	IsNullable bool
	Default    string
	IsPrimary  bool
}

type FKConstraint struct {
	ConstraintName   string `json:"ConstraintName"`
	SourceTable      string `json:"SourceTable"`
	SourceColumn     string `json:"SourceColumn"`
	ReferencedSchema string `json:"ReferencedSchema"`
	ReferencedTable  string `json:"ReferencedTable"`
	ReferencedColumn string `json:"ReferencedColumn"`
	OnDelete         string `json:"OnDelete"` // CASCADE, SET NULL, RESTRICT, NO ACTION, SET DEFAULT
	OnUpdate         string `json:"OnUpdate"`
}

type CascadeNode struct {
	SourceTable  string
	SourceColumn string
	TargetTable  string
	TargetColumn string
	OnDelete     string
	Children     []CascadeNode
}

type BaseTrigger struct {
	Name         string `json:"name"`
	Event        string `json:"event"`
	Timing       string `json:"timing"`
	FunctionBody string `json:"function_body"`
	Replicable   bool   `json:"replicable"`
	SkipReason   string `json:"skip_reason"`
}

// InspectColumns returns all columns for a table including PK info.
func InspectColumns(db *sql.DB, schema, table string) ([]ColumnDef, error) {
	query := `
		SELECT 
			c.column_name, 
			c.data_type, 
			c.is_nullable = 'YES' AS is_nullable, 
			COALESCE(c.column_default, '') AS column_default,
			EXISTS (
				SELECT 1 
				FROM information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu 
				  ON tc.constraint_name = kcu.constraint_name 
				 AND tc.table_schema = kcu.table_schema
				WHERE tc.constraint_type = 'PRIMARY KEY'
				  AND tc.table_schema = c.table_schema
				  AND tc.table_name = c.table_name
				  AND kcu.column_name = c.column_name
			) AS is_primary
		FROM information_schema.columns c
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var cols []ColumnDef
	for rows.Next() {
		var col ColumnDef
		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			&col.Default,
			&col.IsPrimary,
		)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// InspectFKs returns all FK constraints declared on a table.
func InspectFKs(db *sql.DB, schema, table string) ([]FKConstraint, error) {
	query := `
		SELECT
			tc.constraint_name,
			tc.table_name AS source_table,
			kcu.column_name AS source_column,
			ccu.table_schema AS referenced_schema,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column,
			rc.delete_rule AS on_delete,
			rc.update_rule AS on_update
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		JOIN information_schema.referential_constraints rc 
		  ON tc.constraint_name = rc.constraint_name
		  AND tc.table_schema = rc.constraint_schema
		JOIN information_schema.constraint_column_usage ccu 
		  ON rc.unique_constraint_name = ccu.constraint_name
		  AND rc.unique_constraint_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND tc.table_schema = $1
		  AND tc.table_name = $2;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query FKs: %w", err)
	}
	defer rows.Close()

	var fks []FKConstraint
	for rows.Next() {
		var fk FKConstraint
		err := rows.Scan(
			&fk.ConstraintName,
			&fk.SourceTable,
			&fk.SourceColumn,
			&fk.ReferencedSchema,
			&fk.ReferencedTable,
			&fk.ReferencedColumn,
			&fk.OnDelete,
			&fk.OnUpdate,
		)
		if err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}
	return fks, rows.Err()
}

// BuildCascadeGraph builds the full recursive FK cascade tree rooted at a table.
// Used to generate cascade tombstone logic in INSTEAD OF DELETE triggers.
func BuildCascadeGraph(db *sql.DB, schema, table string) ([]CascadeNode, error) {
	visited := make(map[string]bool)
	return buildCascadeGraphHelper(db, schema, table, visited)
}

func buildCascadeGraphHelper(db *sql.DB, schema, table string, visited map[string]bool) ([]CascadeNode, error) {
	if visited[table] {
		return nil, nil
	}
	visited[table] = true

	query := `
		SELECT
			tc.table_name AS source_table,
			kcu.column_name AS source_column,
			ccu.column_name AS target_column,
			rc.delete_rule AS on_delete
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		JOIN information_schema.referential_constraints rc 
		  ON tc.constraint_name = rc.constraint_name
		  AND tc.table_schema = rc.constraint_schema
		JOIN information_schema.constraint_column_usage ccu 
		  ON rc.unique_constraint_name = ccu.constraint_name
		  AND rc.unique_constraint_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND ccu.table_schema = $1
		  AND ccu.table_name = $2;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query referencing FKs: %w", err)
	}
	defer rows.Close()

	var nodes []CascadeNode
	for rows.Next() {
		var n CascadeNode
		n.TargetTable = table
		err := rows.Scan(
			&n.SourceTable,
			&n.SourceColumn,
			&n.TargetColumn,
			&n.OnDelete,
		)
		if err != nil {
			return nil, err
		}
		
		// Recursively build children
		children, err := buildCascadeGraphHelper(db, schema, n.SourceTable, visited)
		if err != nil {
			return nil, err
		}
		n.Children = children
		nodes = append(nodes, n)
	}

	delete(visited, table)
	return nodes, rows.Err()
}

// InspectTriggers returns all triggers on a base table and classifies them.
func InspectTriggers(db *sql.DB, schema, table string) ([]BaseTrigger, error) {
	query := `
		SELECT 
			trg.tgname AS trigger_name,
			CASE 
				WHEN (trg.tgtype & 2) = 2 THEN 'BEFORE'
				WHEN (trg.tgtype & 64) = 64 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END AS timing,
			concat_ws(' OR ',
				CASE WHEN (trg.tgtype & 4) = 4 THEN 'INSERT' END,
				CASE WHEN (trg.tgtype & 16) = 16 THEN 'UPDATE' END,
				CASE WHEN (trg.tgtype & 8) = 8 THEN 'DELETE' END
			) AS event,
			p.prosrc AS function_body
		FROM pg_trigger trg
		JOIN pg_class rel ON trg.tgrelid = rel.oid
		JOIN pg_namespace nsp ON rel.relnamespace = nsp.oid
		JOIN pg_proc p ON trg.tgfoid = p.oid
		WHERE nsp.nspname = $1 AND rel.relname = $2 AND NOT trg.tgisinternal;
	`
	rows, err := db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query triggers: %w", err)
	}
	defer rows.Close()

	var triggers []BaseTrigger
	for rows.Next() {
		var t BaseTrigger
		err := rows.Scan(
			&t.Name,
			&t.Timing,
			&t.Event,
			&t.FunctionBody,
		)
		if err != nil {
			return nil, err
		}

		// Classify
		replicable, reason := classifyTrigger(t.FunctionBody)
		t.Replicable = replicable
		t.SkipReason = reason

		triggers = append(triggers, t)
	}
	return triggers, rows.Err()
}

func classifyTrigger(body string) (bool, string) {
	bodyLower := strings.ToLower(body)
	if strings.Contains(bodyLower, "pg_notify") {
		return false, "uses pg_notify"
	}
	if strings.Contains(bodyLower, "pg_net") {
		return false, "references pg_net"
	}
	if strings.Contains(bodyLower, "insert into") {
		return false, "performs insert side-effects"
	}
	if strings.Contains(bodyLower, "update ") {
		return false, "performs update side-effects"
	}
	if strings.Contains(bodyLower, "delete ") {
		return false, "performs delete side-effects"
	}
	return true, ""
}

// GenerateDeltaTableDDL returns CREATE TABLE DDL for a branch delta table.
func GenerateDeltaTableDDL(branchSchema, table string, cols []ColumnDef, fks []FKConstraint) string {
	var sb strings.Builder
	
	if len(fks) > 0 {
		sb.WriteString("-- Suspended FKs:\n")
		for _, fk := range fks {
			sb.WriteString(fmt.Sprintf("--   %s.%s → %s.%s.%s (%s)\n", 
				table, fk.SourceColumn, fk.ReferencedSchema, fk.ReferencedTable, fk.ReferencedColumn, fk.OnDelete))
		}
	}
	
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s_delta (\n", branchSchema, table))
	
	var pkCols []string
	for _, col := range cols {
		defaultStr := ""
		if col.Default != "" {
			defaultStr = " DEFAULT " + col.Default
		}
		sb.WriteString(fmt.Sprintf("    %s %s%s,\n", col.Name, col.DataType, defaultStr))
		if col.IsPrimary {
			pkCols = append(pkCols, col.Name)
		}
	}
	
	sb.WriteString("    __deleted BOOLEAN NOT NULL DEFAULT false,\n")
	sb.WriteString("    __updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),\n")
	sb.WriteString("    __is_new BOOLEAN NOT NULL DEFAULT false,\n")
	sb.WriteString("    __branch_seq_id BIGINT")
	
	if len(pkCols) > 0 {
		sb.WriteString(",\n")
		sb.WriteString(fmt.Sprintf("    PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}
	sb.WriteString("\n);\n")

	// Generate indexes
	if len(pkCols) > 0 {
		sb.WriteString(fmt.Sprintf("CREATE INDEX ON %s.%s_delta (%s);\n", branchSchema, table, strings.Join(pkCols, ", ")))
	}
	sb.WriteString(fmt.Sprintf("CREATE INDEX ON %s.%s_delta (__deleted) WHERE __deleted = false;\n", branchSchema, table))
	sb.WriteString(fmt.Sprintf("CREATE INDEX ON %s.%s_delta (__is_new) WHERE __is_new = true;\n", branchSchema, table))
	sb.WriteString(fmt.Sprintf("CREATE INDEX ON %s.%s_delta (__updated_at);\n", branchSchema, table))
	
	for _, fk := range fks {
		sb.WriteString(fmt.Sprintf("CREATE INDEX ON %s.%s_delta (%s);\n", branchSchema, table, fk.SourceColumn))
	}
	
	return sb.String()
}

// GenerateSequenceDDL returns CREATE SEQUENCE DDL for a branch-local sequence.
func GenerateSequenceDDL(branchSchema, table string, branchID int64) string {
	offset := 1000000000 * branchID
	return fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s.%s_id_seq START WITH %d;\n", branchSchema, table, offset)
}

// GeneratePassthroughViewDDL returns a simple SELECT * FROM base view.
func GeneratePassthroughViewDDL(branchSchema, baseSchema, table string, cols []ColumnDef) string {
	var colNames []string
	for _, col := range cols {
		colNames = append(colNames, col.Name)
	}
	colsList := strings.Join(colNames, ", ")
	ddl := fmt.Sprintf("CREATE OR REPLACE VIEW %s.%s AS\nSELECT %s FROM %s.%s;\n", 
		branchSchema, table, colsList, baseSchema, table)
	
	for _, col := range cols {
		if col.Default != "" {
			ddl += fmt.Sprintf("ALTER VIEW %s.%s ALTER COLUMN %s SET DEFAULT %s;\n", 
				branchSchema, table, col.Name, col.Default)
		}
	}
	return ddl
}

// GenerateOverlayViewDDL returns the UNION ALL merge view.
func GenerateOverlayViewDDL(branchSchema, baseSchema, table string, cols []ColumnDef) string {
	var colNames []string
	var bColNames []string
	var pkCols []string
	var pkFirst string
	for _, col := range cols {
		colNames = append(colNames, col.Name)
		bColNames = append(bColNames, "b."+col.Name)
		if col.IsPrimary {
			pkCols = append(pkCols, col.Name)
			if pkFirst == "" {
				pkFirst = col.Name
			}
		}
	}
	if pkFirst == "" {
		pkFirst = "id"
		pkCols = []string{"id"}
	}
	
	colsList := strings.Join(colNames, ", ")
	bColsList := strings.Join(bColNames, ", ")
	
	var joinClauses []string
	for _, pk := range pkCols {
		joinClauses = append(joinClauses, fmt.Sprintf("b.%s = d.%s", pk, pk))
	}
	joinStr := strings.Join(joinClauses, " AND ")
	
	ddl := fmt.Sprintf(
		"CREATE OR REPLACE VIEW %s.%s AS\n"+
		"SELECT %s\n"+
		"FROM %s.%s b\n"+
		"LEFT JOIN %s.%s_delta d ON %s\n"+
		"WHERE d.%s IS NULL\n"+
		"UNION ALL\n"+
		"SELECT %s\n"+
		"FROM %s.%s_delta\n"+
		"WHERE __deleted = false;\n",
		branchSchema, table, bColsList, baseSchema, table, branchSchema, table, joinStr, pkFirst, colsList, branchSchema, table)

	for _, col := range cols {
		if col.Default != "" {
			ddl += fmt.Sprintf("ALTER VIEW %s.%s ALTER COLUMN %s SET DEFAULT %s;\n", 
				branchSchema, table, col.Name, col.Default)
		}
	}
	return ddl
}

// GenerateTriggerDDL returns the full INSTEAD OF trigger DDL.
func GenerateTriggerDDL(
	branchSchema, baseSchema, table string,
	cols []ColumnDef,
	cascades []CascadeNode,
	replicableTriggers []BaseTrigger,
) string {
	var sb strings.Builder
	
	var firstPK string
	var idPK string
	var colNames []string
	var valPlaceholders []string
	var pkCols []string
	var updateSets []string
	
	for _, col := range cols {
		colNames = append(colNames, col.Name)
		valPlaceholders = append(valPlaceholders, "NEW."+col.Name)
		if col.IsPrimary {
			pkCols = append(pkCols, col.Name)
			if firstPK == "" {
				firstPK = col.Name
			}
			if strings.ToLower(col.Name) == "id" {
				idPK = col.Name
			}
		} else {
			updateSets = append(updateSets, fmt.Sprintf("%s = EXCLUDED.%s", col.Name, col.Name))
		}
	}
	
	if firstPK == "" {
		firstPK = "id"
		pkCols = []string{"id"}
	}
	
	colsList := strings.Join(colNames, ", ")
	valsList := strings.Join(valPlaceholders, ", ")
	pksCSV := strings.Join(pkCols, ", ")
	
	updateSetsList := strings.Join(updateSets, ", ")
	if updateSetsList != "" {
		updateSetsList += ", "
	}
	
	var beforeInsertTriggersStr string
	var beforeUpdateTriggersStr string
	for _, trg := range replicableTriggers {
		if strings.Contains(trg.Timing, "BEFORE") {
			if strings.Contains(strings.ToLower(trg.FunctionBody), "updated_at") {
				beforeUpdateTriggersStr += "        NEW.updated_at := now();\n"
			}
			if strings.Contains(strings.ToLower(trg.FunctionBody), "created_at") {
				beforeInsertTriggersStr += "        NEW.created_at := now();\n"
			}
		}
	}

	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s.%s_trigger_fn()\n", branchSchema, table))
	sb.WriteString("RETURNS trigger LANGUAGE plpgsql AS $$\n")
	sb.WriteString("BEGIN\n")
	
	// INSERT PATH
	sb.WriteString("    IF TG_OP = 'INSERT' THEN\n")
	sb.WriteString(beforeInsertTriggersStr)
	
	if idPK != "" {
		sb.WriteString(fmt.Sprintf("        IF NEW.%s IS NULL OR NOT EXISTS (SELECT 1 FROM %s.%s WHERE %s = NEW.%s) THEN\n", idPK, baseSchema, table, idPK, idPK))
		sb.WriteString(fmt.Sprintf("            NEW.%s := nextval('%s.%s_id_seq');\n", idPK, branchSchema, table))
		sb.WriteString("        END IF;\n")
	}
	
	sb.WriteString(fmt.Sprintf("        INSERT INTO %s.%s_delta\n", branchSchema, table))
	sb.WriteString(fmt.Sprintf("            (%s, __deleted, __updated_at, __is_new, __branch_seq_id)\n", colsList))
	sb.WriteString(fmt.Sprintf("        VALUES (%s, false, now(), true, NEW.%s)\n", valsList, firstPK))
	sb.WriteString(fmt.Sprintf("        ON CONFLICT (%s) DO UPDATE SET\n", pksCSV))
	sb.WriteString(fmt.Sprintf("            %s__deleted = false, __updated_at = now();\n", updateSetsList))
	sb.WriteString(fmt.Sprintf("        PERFORM pg_notify('chuck_write', '%s.%s');\n", branchSchema, table))
	sb.WriteString("        RETURN NEW;\n\n")
	
	// UPDATE PATH
	sb.WriteString("    ELSIF TG_OP = 'UPDATE' THEN\n")
	sb.WriteString(beforeUpdateTriggersStr)
	sb.WriteString(fmt.Sprintf("        INSERT INTO %s.%s_delta\n", branchSchema, table))
	sb.WriteString(fmt.Sprintf("            (%s, __deleted, __updated_at, __is_new, __branch_seq_id)\n", colsList))
	sb.WriteString(fmt.Sprintf("        VALUES (%s, false, now(), false, NULL)\n", valsList))
	sb.WriteString(fmt.Sprintf("        ON CONFLICT (%s) DO UPDATE SET\n", pksCSV))
	sb.WriteString(fmt.Sprintf("            %s__deleted = false, __updated_at = now();\n", updateSetsList))
	sb.WriteString(fmt.Sprintf("        PERFORM pg_notify('chuck_write', '%s.%s');\n", branchSchema, table))
	sb.WriteString("        RETURN NEW;\n\n")
	
	// DELETE PATH
	sb.WriteString("    ELSIF TG_OP = 'DELETE' THEN\n")
	sb.WriteString(fmt.Sprintf("        -- Tombstone %s\n", table))
	
	var oldPkVals []string
	for _, pk := range pkCols {
		oldPkVals = append(oldPkVals, "OLD."+pk)
	}
	oldPkValsCSV := strings.Join(oldPkVals, ", ")
	
	sb.WriteString(fmt.Sprintf("        INSERT INTO %s.%s_delta (%s, __deleted, __updated_at)\n", branchSchema, table, pksCSV))
	sb.WriteString(fmt.Sprintf("        VALUES (%s, true, now())\n", oldPkValsCSV))
	sb.WriteString(fmt.Sprintf("        ON CONFLICT (%s) DO UPDATE SET __deleted = true, __updated_at = now();\n\n", pksCSV))
	
	// CASCADE logic in delete path
	generateCascadeDDL(&sb, branchSchema, cascades, firstPK)
	
	sb.WriteString(fmt.Sprintf("        PERFORM pg_notify('chuck_write', '%s.%s');\n", branchSchema, table))
	sb.WriteString("        RETURN OLD;\n")
	sb.WriteString("    END IF;\n")
	sb.WriteString("END;\n")
	sb.WriteString("$$;\n\n")
	
	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE TRIGGER %s_instead_of\n", table))
	sb.WriteString(fmt.Sprintf("INSTEAD OF INSERT OR UPDATE OR DELETE ON %s.%s\n", branchSchema, table))
	sb.WriteString(fmt.Sprintf("FOR EACH ROW EXECUTE FUNCTION %s.%s_trigger_fn();\n", branchSchema, table))
	
	return sb.String()
}

func generateCascadeDDL(sb *strings.Builder, branchSchema string, cascades []CascadeNode, parentPK string) {
	for _, node := range cascades {
		rule := strings.ToUpper(node.OnDelete)
		if rule == "CASCADE" {
			sb.WriteString(fmt.Sprintf("        -- CASCADE: %s.%s ON DELETE CASCADE\n", node.SourceTable, node.SourceColumn))
			sb.WriteString(fmt.Sprintf("        DELETE FROM %s.%s WHERE %s = OLD.%s;\n\n", branchSchema, node.SourceTable, node.SourceColumn, parentPK))
		} else if rule == "SET NULL" {
			sb.WriteString(fmt.Sprintf("        -- CASCADE: %s.%s ON DELETE SET NULL\n", node.SourceTable, node.SourceColumn))
			sb.WriteString(fmt.Sprintf("        UPDATE %s.%s SET %s = NULL WHERE %s = OLD.%s;\n\n", branchSchema, node.SourceTable, node.SourceColumn, node.SourceColumn, parentPK))
		} else if rule == "RESTRICT" || rule == "NO ACTION" {
			sb.WriteString(fmt.Sprintf("        -- RESTRICT: check if %s has rows referencing this\n", node.SourceTable))
			sb.WriteString(fmt.Sprintf("        IF EXISTS (SELECT 1 FROM %s.%s WHERE %s = OLD.%s) THEN\n", branchSchema, node.SourceTable, node.SourceColumn, parentPK))
			sb.WriteString(fmt.Sprintf("            RAISE EXCEPTION 'update or delete on table \"%s\" violates foreign key constraint on table \"%s\"' USING ERRCODE = '23503', DETAIL = 'Key is still referenced from table \"%s\".';\n", node.TargetTable, node.SourceTable, node.SourceTable))
			sb.WriteString("        END IF;\n\n")
		}
	}
}
