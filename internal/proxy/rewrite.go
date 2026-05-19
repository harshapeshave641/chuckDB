package proxy

import (
	"fmt"
	"strings"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)

type QueryRewriter struct {
	branchedTables map[string]bool
}

func NewQueryRewriter(branchedTables []string) *QueryRewriter {
	bt := make(map[string]bool)
	for _, t := range branchedTables {
		bt[t] = true
	}
	return &QueryRewriter{branchedTables: bt}
}

// RewriteSelect parses the query, replaces branched tables with a RangeSubselect that joins the overlay, and deparses it.
func (r *QueryRewriter) RewriteSelect(query string) (string, error) {
	tree, err := pgquery.Parse(query)
	if err != nil {
		return "", fmt.Errorf("failed to parse query: %w", err)
	}

	for _, stmt := range tree.Stmts {
		if selStmt := stmt.Stmt.GetSelectStmt(); selStmt != nil {
			r.rewriteSelectStmt(selStmt)
		}
	}

	deparsed, err := pgquery.Deparse(tree)
	if err != nil {
		return "", fmt.Errorf("failed to deparse rewritten query: %w", err)
	}

	return deparsed, nil
}

func (r *QueryRewriter) rewriteSelectStmt(stmt *pgquery.SelectStmt) {
	if stmt == nil {
		return
	}

	// Rewrite FromClause items
	for i, fromItem := range stmt.FromClause {
		stmt.FromClause[i] = r.rewriteFromNode(fromItem)
	}

	// Recursively rewrite subqueries in TargetList, WhereClause, etc if needed.
	// For MVP, we primarily focus on the top-level FromClause and explicit subqueries.
}

func (r *QueryRewriter) rewriteFromNode(node *pgquery.Node) *pgquery.Node {
	if node == nil {
		return nil
	}

	switch n := node.Node.(type) {
	case *pgquery.Node_RangeVar:
		rv := n.RangeVar
		if !r.branchedTables[rv.Relname] {
			return node
		}

		// It's a branched table, replace with RangeSubselect
		return r.createOverlaySubselect(rv)

	case *pgquery.Node_JoinExpr:
		je := n.JoinExpr
		je.Larg = r.rewriteFromNode(je.Larg)
		je.Rarg = r.rewriteFromNode(je.Rarg)
		return node

	case *pgquery.Node_RangeSubselect:
		rs := n.RangeSubselect
		if subSel := rs.Subquery.GetSelectStmt(); subSel != nil {
			r.rewriteSelectStmt(subSel)
		}
		return node
	}

	return node
}

func (r *QueryRewriter) createOverlaySubselect(rv *pgquery.RangeVar) *pgquery.Node {
	// 1. Build the overlay join query string dynamically
	subqueryStr := fmt.Sprintf(`
		SELECT base.*, overlay.after_values 
		FROM %s base 
		LEFT JOIN _chuck._chuck_overlay overlay 
		  ON overlay.branch_id = current_setting('chuck.branch', true)::uuid 
		 AND overlay.table_name = '%s' 
		 AND overlay.row_id = base.id 
		 AND overlay.shard_key = COALESCE(base.tenant_id::text, 'default') 
		WHERE overlay.operation IS NULL OR overlay.operation != 'DELETE'
	`, rv.Relname, rv.Relname)

	// 2. Parse the subquery
	subTree, err := pgquery.Parse(subqueryStr)
	if err != nil {
		// Fallback to original if we somehow fail to parse our own template
		return &pgquery.Node{Node: &pgquery.Node_RangeVar{RangeVar: rv}}
	}

	subSelect := subTree.Stmts[0].Stmt.GetSelectStmt()

	// 3. Keep original alias, or default to table name
	alias := rv.Alias
	if alias == nil {
		alias = &pgquery.Alias{Aliasname: rv.Relname}
	}

	// 4. Return the RangeSubselect node
	return &pgquery.Node{
		Node: &pgquery.Node_RangeSubselect{
			RangeSubselect: &pgquery.RangeSubselect{
				Subquery: &pgquery.Node{
					Node: &pgquery.Node_SelectStmt{
						SelectStmt: subSelect,
					},
				},
				Alias: alias,
			},
		},
	}
}

// AnalyzeStatement inspects the AST to determine the statement type (SELECT, INSERT, UPDATE, DELETE).
func AnalyzeStatement(query string) (string, error) {
	tree, err := pgquery.Parse(query)
	if err != nil {
		return "", err
	}
	if len(tree.Stmts) == 0 {
		return "", nil
	}

	stmt := tree.Stmts[0].Stmt
	switch {
	case stmt.GetSelectStmt() != nil:
		return "SELECT", nil
	case stmt.GetInsertStmt() != nil:
		return "INSERT", nil
	case stmt.GetUpdateStmt() != nil:
		return "UPDATE", nil
	case stmt.GetDeleteStmt() != nil:
		return "DELETE", nil
	case stmt.GetVariableSetStmt() != nil:
		return "SET", nil
	case stmt.GetTransactionStmt() != nil:
		return "TRANSACTION", nil
	default:
		return "OTHER", nil
	}
}

// IsBranchedTable checks if a parsed AST mutates a branched table.
// Returns the table name if it's branched, or empty string if it's shared/not mutating.
func (r *QueryRewriter) MutatesBranchedTable(query string) (string, error) {
	tree, err := pgquery.Parse(query)
	if err != nil {
		return "", err
	}
	if len(tree.Stmts) == 0 {
		return "", nil
	}

	stmt := tree.Stmts[0].Stmt

	var relname string
	switch {
	case stmt.GetInsertStmt() != nil:
		relname = stmt.GetInsertStmt().Relation.Relname
	case stmt.GetUpdateStmt() != nil:
		relname = stmt.GetUpdateStmt().Relation.Relname
	case stmt.GetDeleteStmt() != nil:
		relname = stmt.GetDeleteStmt().Relation.Relname
	}

	if relname != "" && r.branchedTables[relname] {
		return relname, nil
	}
	return "", nil
}

// GenerateUpdateTargetQuery converts "UPDATE table SET col=val WHERE cond" 
// into "SELECT id, tenant_id, row_to_json(base.*) as before_values FROM table base WHERE cond"
func (r *QueryRewriter) GenerateUpdateTargetQuery(query string) (string, error) {
	tree, err := pgquery.Parse(query)
	if err != nil {
		return "", err
	}
	
	updStmt := tree.Stmts[0].Stmt.GetUpdateStmt()
	if updStmt == nil {
		return "", fmt.Errorf("not an update statement")
	}

	// We create a SELECT statement with the WHERE clause from the UPDATE
	whereStr := ""
	if updStmt.WhereClause != nil {
		// Deparse the where clause using a dummy query
		dummy := &pgquery.SelectStmt{
			TargetList: []*pgquery.Node{{Node: &pgquery.Node_ResTarget{ResTarget: &pgquery.ResTarget{Val: &pgquery.Node{Node: &pgquery.Node_AConst{}}}}}},
			WhereClause: updStmt.WhereClause,
		}
		dummyTree := &pgquery.ParseResult{
			Version: 160001,
			Stmts: []*pgquery.RawStmt{{Stmt: &pgquery.Node{Node: &pgquery.Node_SelectStmt{SelectStmt: dummy}}}},
		}
		deparsedDummy, _ := pgquery.Deparse(dummyTree)
		parts := strings.SplitN(deparsedDummy, "WHERE", 2)
		if len(parts) == 2 {
			whereStr = " WHERE " + parts[1]
		}
	}

	// Also account for the overlay in the target query so we don't update deleted rows, etc.
	// But since this is just identifying target rows, we should query the *overlay merged* state!
	// We can use RewriteSelect on a dummy select!
	dummySel := fmt.Sprintf("SELECT id, tenant_id, row_to_json(base.*) AS base_json FROM %s%s", updStmt.Relation.Relname, whereStr)
	
	return r.RewriteSelect(dummySel)
}
