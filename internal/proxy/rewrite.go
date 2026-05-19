package proxy

import (
	"fmt"

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
	// 1. Build the overlay join query string dynamically utilizing jsonb_populate_record
	// This pushes the JSON merge INTO PostgreSQL, allowing WHERE/ORDER BY to evaluate logically correctly.
	subqueryStr := fmt.Sprintf(`
		SELECT (jsonb_populate_record(base, COALESCE(overlay.after_values, '{}'::jsonb))).*
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
