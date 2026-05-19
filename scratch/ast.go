package main

import (
	"fmt"
	"log"

	pgquery "github.com/pganalyze/pg_query_go/v5"
)

func main() {
	query := "SELECT * FROM compliance_rules WHERE active = true;"
	tree, err := pgquery.Parse(query)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Tree: %+v\n", tree)

	deparsed, err := pgquery.Deparse(tree)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Deparsed: %s\n", deparsed)
}
