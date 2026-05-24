package proxy

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/chuckdb/chuck/internal/delta"
	"github.com/jackc/pgx/v5"
)

type BranchProxy struct {
	ListenAddr  string // e.g. "127.0.0.1:5433"
	UpstreamDSN string // PostgreSQL DSN

	listener net.Listener
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewBranchProxy(listenAddr, upstreamDSN string) *BranchProxy {
	ctx, cancel := context.WithCancel(context.Background())
	return &BranchProxy{
		ListenAddr:  listenAddr,
		UpstreamDSN: upstreamDSN,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start begins listening for connections and handles them.
func (p *BranchProxy) Start() error {
	var err error
	p.listener, err = net.Listen("tcp", p.ListenAddr)
	if err != nil {
		return err
	}

	// Start pg_notify view-upgrade listener on static 'chuck_write' channel
	go p.listenForUpgrades()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			conn, err := p.listener.Accept()
			if err != nil {
				select {
				case <-p.ctx.Done():
					return
				default:
					log.Printf("Proxy accept error: %v", err)
					continue
				}
			}
			go p.handleConnection(conn)
		}
	}()

	return nil
}

// Stop gracefully stops the proxy.
func (p *BranchProxy) Stop() error {
	p.cancel()
	if p.listener != nil {
		p.listener.Close()
	}
	p.wg.Wait()
	return nil
}

func (p *BranchProxy) listenForUpgrades() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		conn, err := pgx.Connect(p.ctx, p.UpstreamDSN)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		_, err = conn.Exec(p.ctx, "LISTEN chuck_write")
		if err != nil {
			conn.Close(p.ctx)
			time.Sleep(1 * time.Second)
			continue
		}

		for {
			notification, err := conn.WaitForNotification(p.ctx)
			if err != nil {
				conn.Close(p.ctx)
				break // reconnect
			}

			payload := notification.Payload
			parts := strings.Split(payload, ".")
			if len(parts) != 2 {
				continue
			}
			branchSchema := parts[0]
			tbl := parts[1]

			go func(schema, table string) {
				log.Printf("Received notify upgrade for schema=%s, table=%s", schema, table)
				db, err := sql.Open("pgx", p.UpstreamDSN)
				if err != nil {
					log.Printf("Failed to open DB in notification thread: %v", err)
					return
				}
				defer db.Close()

				var baseSchema string
				err = db.QueryRow(`
					SELECT t.table_schema 
					FROM chuck_meta.tracked_tables t
					JOIN chuck_meta.branch_tables bt ON bt.table_id = t.id
					JOIN chuck_meta.branches b ON bt.branch_id = b.id
					WHERE b.schema_name = $1 AND t.table_name = $2
				`, schema, table).Scan(&baseSchema)
				if err != nil {
					log.Printf("Failed to scan base schema for %s.%s: %v", schema, table, err)
					return
				}

				cols, err := delta.InspectColumns(db, baseSchema, table)
				if err != nil {
					log.Printf("Failed to inspect columns for %s.%s: %v", baseSchema, table, err)
					return
				}

				// Upgrade the view to overlay mode
				err = delta.UpgradeToOverlay(db, schema, baseSchema, table, cols)
				if err != nil {
					log.Printf("Failed to upgrade view to overlay: %v", err)
					return
				}

				// Update is_dirty flag & last_modified_at timestamp in metadata
				_, err = db.Exec(`
					UPDATE chuck_meta.branch_tables 
					SET is_dirty = true, last_modified_at = now()
					WHERE branch_id = (SELECT id FROM chuck_meta.branches WHERE schema_name = $1)
					  AND table_id = (SELECT id FROM chuck_meta.tracked_tables WHERE table_schema = $2 AND table_name = $3)
				`, schema, baseSchema, table)
				if err != nil {
					log.Printf("Failed to update branch_tables metadata: %v", err)
					return
				}
				log.Printf("Successfully upgraded %s.%s to overlay view.", schema, table)

			}(branchSchema, tbl)
		}
	}
}

func (p *BranchProxy) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	cfg, err := pgx.ParseConfig(p.UpstreamDSN)
	if err != nil {
		log.Printf("Failed to parse UpstreamDSN: %v", err)
		return
	}
	upstreamAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	upstreamConn, err := net.Dial("tcp", upstreamAddr)
	if err != nil {
		log.Printf("Failed to dial upstream %s: %v", upstreamAddr, err)
		return
	}
	defer upstreamConn.Close()

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(clientConn, lenBuf); err != nil {
		return
	}
	length := int(binary.BigEndian.Uint32(lenBuf))

	if length == 8 {
		sslCodeBuf := make([]byte, 4)
		if _, err := io.ReadFull(clientConn, sslCodeBuf); err != nil {
			return
		}
		sslCode := binary.BigEndian.Uint32(sslCodeBuf)
		if sslCode == 80877103 { // SSLRequest code
			if _, err := clientConn.Write([]byte{'N'}); err != nil {
				return
			}
			if _, err := io.ReadFull(clientConn, lenBuf); err != nil {
				return
			}
			length = int(binary.BigEndian.Uint32(lenBuf))
		}
	}

	startupPayload := make([]byte, length-4)
	if _, err := io.ReadFull(clientConn, startupPayload); err != nil {
		return
	}

	startupMsg := make([]byte, length)
	binary.BigEndian.PutUint32(startupMsg[0:4], uint32(length))
	copy(startupMsg[4:], startupPayload)

	if _, err := upstreamConn.Write(startupMsg); err != nil {
		return
	}

	for {
		msgType, payload, err := readMessage(upstreamConn)
		if err != nil {
			return
		}

		if err := writeMessage(clientConn, msgType, payload); err != nil {
			return
		}

		if msgType == 'Z' {
			break
		}

		if msgType == 'R' && len(payload) >= 4 {
			authType := binary.BigEndian.Uint32(payload[0:4])
			// Auth types requiring client response:
			// 3: CleartextPassword, 5: MD5Password, 10: SASL, 11: SASLContinue
			if authType == 3 || authType == 5 || authType == 10 || authType == 11 {
				cType, cPayload, err := readMessage(clientConn)
				if err != nil {
					return
				}
				if err := writeMessage(upstreamConn, cType, cPayload); err != nil {
					return
				}
			}
		}
	}

	// 1. Resolve active branch schema name dynamically from the DB
	// If active_branch is empty, fallback to 'public'
	var activeSchema string
	db, err := sql.Open("pgx", p.UpstreamDSN)
	if err == nil {
		_ = db.QueryRow(`
			SELECT b.schema_name 
			FROM chuck_meta.active_branch ab
			JOIN chuck_meta.branches b ON ab.branch_id = b.id
			LIMIT 1
		`).Scan(&activeSchema)
		db.Close()
	}
	if activeSchema == "" {
		activeSchema = "public"
	}

	// 2. Inject search_path routing statement session-scoped
	injectQuery := fmt.Sprintf("SET search_path TO %s, public;", activeSchema)
	injectPayload := append([]byte(injectQuery), 0)
	if err := writeMessage(upstreamConn, 'Q', injectPayload); err != nil {
		return
	}

	for {
		msgType, _, err := readMessage(upstreamConn)
		if err != nil {
			return
		}
		if msgType == 'Z' {
			break
		}
	}

	// 3. Pipe client and upstream bidirectionally
	errChan := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstreamConn, clientConn)
		errChan <- err
	}()
	go func() {
		_, err := io.Copy(clientConn, upstreamConn)
		errChan <- err
	}()

	<-errChan
}

func readMessage(r io.Reader) (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	length := int(binary.BigEndian.Uint32(header[1:5])) - 4
	if length < 0 {
		return 0, nil, fmt.Errorf("invalid message length: %d", length+4)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

func writeMessage(w io.Writer, msgType byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)+4))
	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}
