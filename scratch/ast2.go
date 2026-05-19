package main

import (
	"fmt"
	"log"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)

func main() {
	query := `
SELECT base.*, overlay.after_values
FROM compliance_rules base
LEFT JOIN _chuck._chuck_overlay overlay
  ON overlay.branch_id = current_setting('chuck.branch', true)::uuid
 AND overlay.table_name = 'compliance_rules'
 AND overlay.row_id = base.id
 AND overlay.shard_key = COALESCE(base.tenant_id::text, 'default')
WHERE base.active = true
  AND (overlay.operation IS NULL OR overlay.operation != 'DELETE');
`
	tree, err := pgquery.Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Tree: %+v\n", tree)
}
