#!/bin/bash
set -e

# Make sure DSN is set
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/app?sslmode=disable"

echo "=== 1. Setting up clean base tables ==="
psql "$DATABASE_URL" -c "DROP TABLE IF EXISTS public.products CASCADE;"
psql "$DATABASE_URL" -c "CREATE TABLE public.products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);"

echo -e "\n=== 2. Initializing ChuckDB ==="
./chuck init

echo -e "\n=== 3. Tracking public.products ==="
./chuck track public.products

echo -e "\n=== 4. Creating branch 'demo_branch' ==="
./chuck branch create demo_branch

echo -e "\n=== 5. Checking out 'demo_branch' ==="
./chuck checkout demo_branch

echo -e "\n=== 6. Starting connection proxy on port 5433 ==="
./chuck proxy start --port 5433
sleep 1 # wait for proxy to start

echo -e "\n=== 7. Inserting a product via the proxy ==="
psql "postgres://postgres:postgres@localhost:5433/app?sslmode=disable" -c "INSERT INTO products (name, price) VALUES ('Widget A', 19.99);"

echo -e "\n=== 8. Checking branch status ==="
./chuck status

echo -e "\n=== 9. Verifying isolation (checking base database directly on port 5432) ==="
echo "Querying base table on port 5432 (should be empty):"
psql "$DATABASE_URL" -c "SELECT * FROM products;"

echo -e "\n=== 10. Verifying virtualized write (checking via proxy on port 5433) ==="
echo "Querying branch view on port 5433 (should contain Widget A with a branch-local ID):"
psql "postgres://postgres:postgres@localhost:5433/app?sslmode=disable" -c "SELECT * FROM products;"

echo -e "\n=== 11. Committing branch changes ==="
./chuck commit -m "Add Widget A in demo branch"

echo -e "\n=== 12. Viewing branch commit log ==="
./chuck log

echo -e "\n=== 13. Merging 'demo_branch' into production ==="
./chuck merge demo_branch

echo -e "\n=== 14. Verifying production table after merge (checking port 5432) ==="
echo "Querying base table on port 5432 (should now contain Widget A with production-remapped ID):"
psql "$DATABASE_URL" -c "SELECT * FROM products;"

echo -e "\n=== 15. Stopping connection proxy ==="
./chuck proxy stop
