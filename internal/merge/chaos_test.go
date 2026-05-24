package merge_test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/chuckdb/chuck/internal/branch"
	"github.com/chuckdb/chuck/internal/merge"
	"github.com/chuckdb/chuck/internal/meta"
	"github.com/chuckdb/chuck/internal/proxy"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func setupChaosTables(t *testing.T, db *sql.DB) {
	_, _ = db.Exec("DROP TABLE IF EXISTS public.chaos_users CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_test CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_meta CASCADE")
	_, _ = db.Exec("DROP SCHEMA IF EXISTS chuck_chaos_branch CASCADE")

	// Sequences
	_, _ = db.Exec("DROP SEQUENCE IF EXISTS public.chaos_users_id_seq CASCADE")
	_, _ = db.Exec("CREATE SEQUENCE public.chaos_users_id_seq START WITH 1")

	_, err := db.Exec(`CREATE TABLE public.chaos_users (
		id BIGINT PRIMARY KEY DEFAULT nextval('public.chaos_users_id_seq'),
		name TEXT NOT NULL,
		email TEXT UNIQUE
	);`)
	if err != nil {
		t.Fatalf("failed to create chaos table: %v", err)
	}
}

// TestChaosMergeAtomicity verifies that a merge failure midway (like a unique constraint violation during replay)
// results in a complete rollback of the entire merge, writing zero partial changes to base tables.
func TestChaosMergeAtomicity(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupChaosTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "chaos_users", []string{"id"})

	err := branch.Create(db, "chaos_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Insert multiple users on branch
	_, _ = db.Exec("INSERT INTO chuck_chaos_branch.chaos_users_delta (id, name, email, __is_new) VALUES (1000000001, 'User 1', 'u1@example.com', true)")
	_, _ = db.Exec("INSERT INTO chuck_chaos_branch.chaos_users_delta (id, name, email, __is_new) VALUES (1000000002, 'User 2', 'u2@example.com', true)")

	// Force unique constraint violation on base database by inserting u2@example.com directly into public.chaos_users
	_, err = db.Exec("INSERT INTO public.chaos_users (name, email) VALUES ('Public User', 'u2@example.com')")
	if err != nil {
		t.Fatalf("failed to insert base row: %v", err)
	}

	// Merge should fail due to unique key violation when replaying User 2
	err = merge.Merge(db, "chaos_branch", false)
	if err == nil {
		t.Errorf("expected merge to fail with unique constraint violation")
	}

	// Verify that User 1 was NOT merged (atomicity check: complete rollback!)
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM public.chaos_users WHERE email = 'u1@example.com'").Scan(&count)
	if count != 0 {
		t.Errorf("merge was not atomic! User 1 was committed to base: %d", count)
	}
}

// TestChaosProxyKillResilience validates starting, killing, and immediately restarting the proxy daemon.
func TestChaosProxyKillResilience(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupChaosTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "chaos_users", []string{"id"})

	_ = branch.Create(db, "chaos_branch")

	// Checkout branch
	_, _ = db.Exec(`
		INSERT INTO chuck_meta.active_branch (singleton, branch_id)
		VALUES (true, (SELECT id FROM chuck_meta.branches WHERE name = 'chaos_branch'))
		ON CONFLICT (singleton) DO UPDATE SET branch_id = EXCLUDED.branch_id
	`)

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
	}

	for i := 0; i < 3; i++ {
		bp := proxy.NewBranchProxy("127.0.0.1:5588", dsn)
		err := bp.Start()
		if err != nil {
			t.Fatalf("failed to start proxy on iteration %d: %v", i, err)
		}

		// Connect and query
		pDB, err := sql.Open("pgx", "postgres://postgres:postgres@127.0.0.1:5588/app?sslmode=disable")
		if err == nil {
			var one int
			err = pDB.QueryRow("SELECT 1").Scan(&one)
			pDB.Close()
		}
		if err != nil {
			bp.Stop()
			t.Fatalf("proxy connection failed on iteration %d: %v", i, err)
		}

		// Abruptly kill the proxy
		_ = bp.Stop()
		time.Sleep(100 * time.Millisecond) // wait for port release
	}
}

// TestChaosSQLInjectionCheckout verifies that illegal branch names are rejected.
func TestChaosSQLInjectionCheckout(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupChaosTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "chaos_users", []string{"id"})

	badNames := []string{
		"chaos; DROP TABLE public.chaos_users;",
		"chaos' OR '1'='1",
		"chaos--",
		"../chaos",
		"chaos$branch",
	}

	for _, name := range badNames {
		err := branch.Create(db, name)
		if err == nil {
			t.Errorf("expected branch creation to reject name %q, but it succeeded", name)
		}
	}
}

// TestChaosConcurrentReadViewUpgrade starts multiple concurrent readers querying the branch view
// while a writer triggers an upgrade to overlay view asynchronously.
func TestChaosConcurrentReadViewUpgrade(t *testing.T) {
	db := getTestDB(t)
	defer db.Close()
	setupChaosTables(t, db)

	_ = meta.Bootstrap(db)
	_ = meta.TrackTable(db, "public", "chaos_users", []string{"id"})

	err := branch.Create(db, "chaos_branch")
	if err != nil {
		t.Fatalf("failed to create branch: %v", err)
	}

	// Switch search path
	_, _ = db.Exec("SET search_path TO chuck_chaos_branch, public")

	// Start proxy (to handle notifications and upgrade views)
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/app?sslmode=disable"
	}
	bp := proxy.NewBranchProxy("127.0.0.1:5577", dsn)
	_ = bp.Start()
	defer bp.Stop()

	// Spin up readers
	var wg sync.WaitGroup
	readersCount := 20
	errs := make(chan error, readersCount)

	stopChan := make(chan struct{})

	for i := 0; i < readersCount; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			
			// Open separate connection
			pDB, err := sql.Open("pgx", dsn)
			if err != nil {
				errs <- err
				return
			}
			defer pDB.Close()
			_, _ = pDB.Exec("SET search_path TO chuck_chaos_branch, public")

			for {
				select {
				case <-stopChan:
					return
				default:
				}

				// Query the view
				var count int
				err = pDB.QueryRow("SELECT COUNT(*) FROM chaos_users").Scan(&count)
				if err != nil {
					errs <- fmt.Errorf("reader %d failed: %w", readerID, err)
					return
				}

				// Sleep micro duration
				time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
			}
		}(i)
	}

	// Wait a bit, then perform a write on another connection through the proxy (which triggers dynamic view upgrade)
	time.Sleep(200 * time.Millisecond)
	
	proxyDB, err := sql.Open("pgx", "postgres://postgres:postgres@127.0.0.1:5577/app?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to connect to proxy: %v", err)
	}
	defer proxyDB.Close()

	// Switched active branch context in DB so proxy routes to chaos_branch
	_, _ = db.Exec(`
		INSERT INTO chuck_meta.active_branch (singleton, branch_id)
		VALUES (true, (SELECT id FROM chuck_meta.branches WHERE name = 'chaos_branch'))
		ON CONFLICT (singleton) DO UPDATE SET branch_id = EXCLUDED.branch_id
	`)

	_, err = proxyDB.Exec("INSERT INTO chaos_users (name, email) VALUES ('Alice', 'alice@chaos.com')")
	if err != nil {
		t.Fatalf("proxy write failed: %v", err)
	}

	// Let readers run for another 300ms
	time.Sleep(300 * time.Millisecond)
	close(stopChan)
	wg.Wait()

	// Check if any errors occurred during concurrent reads
	close(errs)
	for err := range errs {
		t.Errorf("reader encountered error: %v", err)
	}
}
