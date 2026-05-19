package main

import (
	"fmt"
	"log"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)

func main() {
	query := "SELECT c.id FROM compliance_rules c WHERE c.active = true;"
	tree, err := pgquery.Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	// We want to replace RangeVar "compliance_rules" with a RangeSubselect.
	// We'll just do a basic test: can we build a RangeSubselect?
	
	// Create a new query for the subquery
	subqueryStr := "SELECT base.*, overlay.after_values FROM compliance_rules base LEFT JOIN _chuck._chuck_overlay overlay ON overlay.branch_id = current_setting('chuck.branch', true)::uuid AND overlay.table_name = 'compliance_rules' AND overlay.row_id = base.id AND overlay.shard_key = COALESCE(base.tenant_id::text, 'default') WHERE overlay.operation IS NULL OR overlay.operation != 'DELETE'"
	
	subTree, err := pgquery.Parse(subqueryStr)
	if err != nil {
		log.Fatal(err)
	}
	
	subSelect := subTree.Stmts[0].Stmt.GetSelectStmt()
	
	// Find the RangeVar in the main query
	stmt := tree.Stmts[0].Stmt.GetSelectStmt()
	for i, fromItem := range stmt.FromClause {
		if rv := fromItem.GetRangeVar(); rv != nil && rv.Relname == "compliance_rules" {
			
			// Replace RangeVar with RangeSubselect
			alias := rv.Alias
			if alias == nil {
				alias = &pgquery.Alias{Aliasname: rv.Relname}
			}
			
			stmt.FromClause[i] = &pgquery.Node{
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
	}

	deparsed, err := pgquery.Deparse(tree)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Deparsed: %s\n", deparsed)
}
