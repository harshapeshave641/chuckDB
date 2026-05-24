# ChuckDB

ChuckDB is a Postgres-native database branching tool designed for single-active-branch virtualization. It allows you to create instant database branches, perform local writes without copying base tables, and run safe transaction-isolated merges with automatic primary key remapping and foreign key validation.

---

## Architecture Overview

ChuckDB virtualizes database branches by keeping a single active branch context. All writes to the active branch are routed into branch-specific delta tables (`chuck_{branch_name}.{table}_delta`), and reads query combined passthrough or overlay views. A lightweight TCP proxy ensures your application queries are automatically routed with the correct schema `search_path` dynamically.

---

## Installation & Setup

1. **Build the CLI**:
   ```bash
   go build -o chuck ./cmd/chuck
   ```

2. **Run a Local Postgres Instance**:
   Ensure you have a Postgres database running. For example:
   ```bash
   docker run -d --name chuck-postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=app postgres:15
   ```

3. **Configure connection DSN**:
   Export the `DATABASE_URL` environment variable or pass the `--dsn` flag to any command:
   ```bash
   export DATABASE_URL="postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
   ```

---

## Quick Start Guide

### 1. Initialize ChuckDB Metadata
Initialize the metadata schema `chuck_meta` on your Postgres instance:
```bash
./chuck init
```

### 2. Register Tables for Branching
Register the tables you want to track for branching. The table must have a primary key:
```bash
./chuck track public.users
./chuck track public.orders
```

### 3. Create a Branch
Create a new database branch. This sets up the branch schema, delta tables, views, and INSTEAD OF triggers:
```bash
./chuck branch create feature_x
```

### 4. Switch to your Branch (Checkout)
Set the branch as active:
```bash
./chuck checkout feature_x
```

### 5. Start the Connection Proxy
Start the single-daemon connection proxy to route queries automatically based on the checked-out branch:
```bash
./chuck proxy start
```
By default, the proxy listens on port `5433` and routes queries to the active branch.

### 6. Develop & Write Data
Point your application's connection string to the proxy port `5433` instead of the default PostgreSQL port `5432`:
```bash
postgresql://localhost:5433/app
```
Any queries or writes run against this connection will automatically target `feature_x` virtual branch.

### 7. View Delta Status and Commit Changes
Inspect uncommitted changes on the active branch:
```bash
./chuck status
```

Commit changes to record snapshots in the DAG commit log:
```bash
./chuck commit -m "Add test users and orders"
```

Show commit history:
```bash
./chuck log
```

### 8. Validate and Merge
Before merging, you can validate foreign key integrity manually:
```bash
./chuck validate feature_x
```

To preview the merge without making writes (dry-run):
```bash
./chuck merge feature_x --dry-run
```

Execute the full merge pipeline atomically:
```bash
./chuck merge feature_x
```
This validates FKs, detects concurrent conflicts, remaps branch-local IDs to production IDs, resolves FK column references, replays delta modifications, and cleans up the branch.

### 9. Drop the Branch
To discard a branch schema and metadata:
```bash
./chuck branch drop feature_x
```

---

## Known Limitations

Ensure you are aware of the following architectural constraints when developing with ChuckDB:

1. **Write-Time FK Enforcement**:
   Foreign key constraints are not enforced at write time inside virtual branches. Instead, they are suspended and validated at merge time against branch views. Run `./chuck validate <branch>` before merging to ensure reference correctness.

2. **Temporary Sequence IDs**:
   `SERIAL` / `BIGSERIAL` sequence IDs assigned inside a branch are temporary and local. They are remapped to production IDs during the merge process. Avoid hardcoding branch-local IDs in application logic or test assertions.

3. **External Trigger Side Effects**:
   Triggers with external side effects (e.g., `pg_notify`, `pg_net`, webhooks) do not fire inside branches. These are inspected and listed at branch creation time.

4. **ORM Introspection vs Queries**:
   ORM schema introspection should use the direct Postgres port `5432` (not the proxy port). Application queries and writes must use the proxy port `5433` to view and write to branch states. We recommend setting two separate connection strings in your application configuration.

5. **Deep Cascade Performance**:
   Cascade chains deeper than 5 levels with millions of rows per level may result in slow `DELETE` operations inside branches. Run `./chuck validate` before merging for such schemas.
