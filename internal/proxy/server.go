package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/chuckdb/chuck/internal/engine"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	addr       string
	upstream   string
	pool       *pgxpool.Pool
	engine     *engine.OverlayEngine
	rewriter   *QueryRewriter
}

func NewServer(addr, upstream string, pool *pgxpool.Pool, branchedTables []string) *Server {
	return &Server{
		addr:     addr,
		upstream: upstream,
		pool:     pool,
		engine:   engine.NewOverlayEngine(pool),
		rewriter: NewQueryRewriter(branchedTables),
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf("Chuck Proxy listening on %s", s.addr)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go s.handleConnection(clientConn)
	}
}

func (s *Server) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	// Connect to upstream
	upstreamConn, err := net.Dial("tcp", s.upstream)
	if err != nil {
		log.Printf("Failed to connect to upstream: %v", err)
		return
	}
	defer upstreamConn.Close()

	clientBackend := pgproto3.NewBackend(clientConn, clientConn)
startupLoop:
	for {
		startupMsg, err := clientBackend.ReceiveStartupMessage()
		if err != nil {
			return
		}

		switch msg := startupMsg.(type) {
		case *pgproto3.SSLRequest:
			clientConn.Write([]byte{'N'})
			continue startupLoop
		case *pgproto3.StartupMessage:
			b, _ := msg.Encode(nil)
			_, err = upstreamConn.Write(b)
			if err != nil {
				return
			}
			break startupLoop
		default:
			return
		}
	}

	// Enter proxy loop
	go s.proxyUpstreamToClient(upstreamConn, clientConn)
	s.proxyClientToUpstream(clientConn, upstreamConn)
}

func (s *Server) proxyClientToUpstream(clientConn, upstreamConn net.Conn) {
	for {
		msgType, payload, err := readMessage(clientConn)
		if err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "closed network connection") {
				log.Printf("Error receiving from client: %v", err)
			}
			return
		}

		if msgType == 'Q' {
			var q pgproto3.Query
			if err := q.Decode(payload); err == nil {
				stmtType, _ := AnalyzeStatement(q.String)
				
				if stmtType == "SELECT" {
					if rewritten, err := s.rewriter.RewriteSelect(q.String); err == nil {
						q.String = rewritten
					}
				} else {
					if targetTable, _ := s.rewriter.MutatesBranchedTable(q.String); targetTable != "" {
						log.Printf("WARNING: Interception of %s on branched table '%s' is not fully implemented in MVP Phase 2.", stmtType, targetTable)
					}
				}
				
				b, _ := q.Encode(nil)
				upstreamConn.Write(b)
				continue
			}
		}

		forwardMessage(upstreamConn, msgType, payload)
	}
}

func (s *Server) proxyUpstreamToClient(upstreamConn, clientConn net.Conn) {
	for {
		msgType, payload, err := readMessage(upstreamConn)
		if err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "closed network connection") {
				log.Printf("Error receiving from upstream: %v", err)
			}
			return
		}

		// Since relational merges are now pushed into SQL via jsonb_populate_record,
		// there is no need to intercept or decode RowDescription / DataRow packets anymore.
		// We just blindly forward all bytes upstream -> client!
		forwardMessage(clientConn, msgType, payload)
	}
}

// readMessage reads a Postgres wire message (1 byte type + 4 byte length + payload)
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
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, err
		}
	}

	return msgType, payload, nil
}

// forwardMessage constructs and writes a Postgres wire message
func forwardMessage(w io.Writer, msgType byte, payload []byte) error {
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
