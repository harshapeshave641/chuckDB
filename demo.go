package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func runCLI(args ...string) (string, error) {
	cmd := exec.Command("./chuck", args...)
	cmd.Env = append(os.Environ(), "DATABASE_URL=postgres://postgres:postgres@localhost:5432/app?sslmode=disable")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("command %v failed: %w. Output:\n%s", args, err, string(out))
	}
	return string(out), nil
}

func main() {
	dsnBase := "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
	dsnProxy := "postgres://postgres:postgres@localhost:5433/app?sslmode=disable"

	fmt.Println("=== 1. Setting up clean base table ===")
	dbBase, err := sql.Open("pgx", dsnBase)
	if err != nil {
		panic(err)
	}
	defer dbBase.Close()

	_, _ = dbBase.Exec("DROP TABLE IF EXISTS public.products CASCADE")
	_, _ = dbBase.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = dbBase.Exec("DROP SCHEMA IF EXISTS chuck_demo_branch CASCADE")
	_, err = dbBase.Exec(`
		CREATE TABLE public.products (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			price NUMERIC NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Table public.products created successfully.")

	fmt.Println("\n=== 2. Initializing ChuckDB ===")
	out, err := runCLI("init")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)

	fmt.Println("\n=== 3. Tracking public.products ===")
	out, err = runCLI("track", "public.products")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)

	fmt.Println("\n=== 4. Creating branch 'demo_branch' ===")
	_, _ = runCLI("branch", "drop", "demo_branch") // drop if exists
	out, err = runCLI("branch", "create", "demo_branch")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)

	fmt.Println("\n=== 5. Checking out 'demo_branch' ===")
	out, err = runCLI("checkout", "demo_branch")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)

	fmt.Println("\n=== 6. Starting connection proxy on port 5433 ===")
	out, err = runCLI("proxy", "start", "--port", "5433")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)
	time.Sleep(1 * time.Second) // wait for proxy boot

	fmt.Println("\n=== 7. Inserting a product via the proxy ===")
	dbProxy, err := sql.Open("pgx", dsnProxy)
	if err != nil {
		panic(err)
	}
	defer dbProxy.Close()

	_, err = dbProxy.Exec("INSERT INTO products (name, price) VALUES ($1, $2)", "Widget A", 19.99)
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Inserted Widget A through proxy connection.")

	fmt.Println("\n=== 8. Checking branch status ===")
	out, err = runCLI("status")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)

	fmt.Println("\n=== 9. Verifying isolation (checking base database directly on port 5432) ===")
	var countBase int
	err = dbBase.QueryRow("SELECT COUNT(*) FROM products").Scan(&countBase)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Base table products row count: %d (expected 0)\n", countBase)

	fmt.Println("\n=== 10. Verifying virtualized write (checking via proxy on port 5433) ===")
	fmt.Println("Debugging chuck_demo_branch.products_delta content:")
	rowsDebug, dErr := dbBase.Query("SELECT id, name, price, __deleted, __is_new FROM chuck_demo_branch.products_delta")
	if dErr == nil {
		for rowsDebug.Next() {
			var dID int64
			var dName string
			var dPrice float64
			var dDel bool
			var dNew bool
			if errScan := rowsDebug.Scan(&dID, &dName, &dPrice, &dDel, &dNew); errScan == nil {
				fmt.Printf("  Row in delta: id=%d, name=%s, price=%.2f, deleted=%t, is_new=%t\n", dID, dName, dPrice, dDel, dNew)
			}
		}
		rowsDebug.Close()
	} else {
		fmt.Printf("Failed to query delta: %v\n", dErr)
	}
	var sPath string
	_ = dbProxy.QueryRow("SHOW search_path").Scan(&sPath)
	fmt.Printf("Connection search_path: %s\n", sPath)

	var viewDef string
	_ = dbBase.QueryRow("SELECT pg_get_viewdef('chuck_demo_branch.products'::regclass, true)").Scan(&viewDef)
	fmt.Printf("Direct view DDL:\n%s\n", viewDef)

	var vCount int
	errV := dbBase.QueryRow("SELECT COUNT(*) FROM chuck_demo_branch.products").Scan(&vCount)
	if errV != nil {
		fmt.Printf("Direct view count query failed: %v\n", errV)
	} else {
		fmt.Printf("Direct view row count: %d\n", vCount)
	}

	rowsV, _ := dbBase.Query("SELECT id, name, price FROM chuck_demo_branch.products")
	for rowsV.Next() {
		var idV int64
		var nameV string
		var priceV float64
		_ = rowsV.Scan(&idV, &nameV, &priceV)
		fmt.Printf("  Row in view directly: id=%d, name=%s, price=%.2f\n", idV, nameV, priceV)
	}
	rowsV.Close()

	var idProxy int64
	var nameProxy string
	var priceProxy float64
	err = dbProxy.QueryRow("SELECT id, name, price FROM products LIMIT 1").Scan(&idProxy, &nameProxy, &priceProxy)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Proxy view products row: id=%d, name=%s, price=%.2f\n", idProxy, nameProxy, priceProxy)
	if idProxy >= 1000000000 {
		fmt.Println("✓ Verified branch-local ID is virtualized (>= 1,000,000,000)")
	} else {
		fmt.Printf("✗ Expected branch-local ID >= 1000000000, got %d\n", idProxy)
	}

	fmt.Println("\n=== 11. Committing branch changes ===")
	out, err = runCLI("commit", "-m", "Add Widget A in demo branch")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)

	fmt.Println("\n=== 12. Viewing branch commit log ===")
	out, err = runCLI("log")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)

	fmt.Println("\n=== 13. Merging 'demo_branch' into production ===")
	out, err = runCLI("merge", "demo_branch")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)

	fmt.Println("\n=== 14. Verifying production table after merge (checking port 5432) ===")
	var idBase int64
	var nameBase string
	var priceBase float64
	err = dbBase.QueryRow("SELECT id, name, price FROM products LIMIT 1").Scan(&idBase, &nameBase, &priceBase)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Base table products row: id=%d, name=%s, price=%.2f\n", idBase, nameBase, priceBase)
	if idBase < 1000000000 {
		fmt.Println("✓ Verified ID is remapped back to production sequence (< 1,000,000,000)")
	} else {
		fmt.Printf("✗ Expected remapped production ID < 1000000000, got %d\n", idBase)
	}

	fmt.Println("\n=== 15. Stopping connection proxy ===")
	out, err = runCLI("proxy", "stop")
	if err != nil {
		panic(err)
	}
	fmt.Print(out)
}
