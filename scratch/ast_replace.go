package main

import (
	"fmt"
	"log"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)

func main() {
	query := `SELECT c.id FROM compliance_rules c WHERE c.active = true;`
	
	result, err := pgquery.Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Parsed: %+v\n", result)
}
