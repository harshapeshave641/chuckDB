package proxy_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chuckdb/chuck/internal/proxy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const upstreamConnStr = "postgres://postgres:postgres@localhost:5432/app"
const proxyConnStr = "postgres://postgres:postgres@localhost:6432/app"

func TestProxyServerSelect(t *testing.T) {
	// 1. Setup upstream database connection to seed data
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	upstream, err := pgx.Connect(ctx, upstreamConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to upstream: %v", err)
	}
	defer upstream.Close(ctx)

	// Seed table and data
	_, err = upstream.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS test_proxy_table (
			id SERIAL PRIMARY KEY,
			name TEXT,
			active BOOLEAN
		);
		TRUNCATE test_proxy_table;
		INSERT INTO test_proxy_table (id, name, active) VALUES (1, 'base_record', true);
	`)
	if err != nil {
		t.Fatalf("Failed to seed upstream data: %v", err)
	}

	// 2. Start proxy server
	pool, err := pgxpool.New(ctx, upstreamConnStr)
	if err != nil {
		t.Fatalf("Failed to create pgxpool: %v", err)
	}
	defer pool.Close()

	branchedTables := []string{"test_proxy_table"}
	srv := proxy.NewServer("localhost:6432", "localhost:5432", pool, branchedTables)
	
	go func() {
		if err := srv.Start(); err != nil {
			fmt.Printf("Server start error: %v\n", err)
		}
	}()

	// Give proxy a moment to listen
	time.Sleep(500 * time.Millisecond)

	// 3. Connect to proxy
	proxyConn, err := pgx.Connect(ctx, proxyConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	defer proxyConn.Close(ctx)

	// 4. Test SELECT through proxy
	var id int
	var name string
	var active bool

	err = proxyConn.QueryRow(ctx, "SELECT id, name, active FROM test_proxy_table WHERE id = 1;").Scan(&id, &name, &active)
	if err != nil {
		t.Fatalf("Query through proxy failed: %v", err)
	}

	if id != 1 || name != "base_record" || !active {
		t.Errorf("Unexpected row from proxy: id=%d name=%s active=%v", id, name, active)
	}
}
