// A small feed API on SQLite for the $12 server challenge.
//
// Design for 1 vCPU / 2 GB, serving the internet directly (no Nginx):
//   - net/http keep-alive server (goroutine per connection is cheap in Go).
//   - A pool of read-only SQLite connections (WAL => concurrent readers),
//     each with its own cached prepared statements (query-plan cache only).
//   - A single serialized writer connection (WAL allows one writer); writes
//     are rare (~2% posts, ~15% likes), so a mutex is plenty and keeps each
//     response waiting on its own commit (durable 201s, rule 6).
//   - Responses are written as raw bytes in the exact key order the suite
//     expects; no encoding/json on the hot path.
//   - JWT HS256 is verified in full on every request (signature + alg + exp),
//     no remembered verifications (rule 5).
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

var (
	jwtSecret []byte
	startTime = time.Now()
)

// ---------------------------------------------------------------------------
// SQLite access
//
// We use the mattn driver's low-level SQLiteConn directly for both readers and
// the writer, to pin prepared statements per connection and skip database/sql
// pooling overhead on the hot path.
// ---------------------------------------------------------------------------

const (
	// Post projection used by /feed and /posts/:id.
	colSelect = `SELECT p.id, p.body, p.created_at, u.username,
	       (SELECT count(*) FROM likes l WHERE l.post_id = p.id)
	  FROM posts p JOIN users u ON u.id = p.user_id`

	feedSQL   = colSelect + ` ORDER BY p.created_at DESC, p.id DESC LIMIT 20`
	postSQL   = colSelect + ` WHERE p.id = ?`
	existsSQL = `SELECT 1 FROM posts WHERE id = ?`

	insertPostSQL = `INSERT INTO posts (user_id, body) VALUES (?, ?) RETURNING id, created_at`
	insertLikeSQL = `INSERT INTO likes (user_id, post_id) VALUES (?, ?) ON CONFLICT (user_id, post_id) DO NOTHING`
)

// reader bundles a raw connection and its long-lived prepared statements.
type reader struct {
	c    *sqlite3.SQLiteConn
	feed *sqlite3.SQLiteStmt
	byID *sqlite3.SQLiteStmt
}

// writer bundles the single write connection and its prepared statements.
type writer struct {
	c      *sqlite3.SQLiteConn
	insP   *sqlite3.SQLiteStmt
	insL   *sqlite3.SQLiteStmt
	exists *sqlite3.SQLiteStmt
	begin  *sqlite3.SQLiteStmt
	commit *sqlite3.SQLiteStmt
	rbck   *sqlite3.SQLiteStmt
	reqs   chan *writeReq // group-commit queue
}

var (
	readers chan *reader
	wr      *writer
	health  *sqlite3.SQLiteStmt
	healthC *sqlite3.SQLiteConn
	healthM sync.Mutex
)

func openConn(path string, readonly bool) (*sqlite3.SQLiteConn, error) {
	d := &sqlite3.SQLiteDriver{}
	dsn := path
	// Open via the driver's Open to get a *SQLiteConn.
	conn, err := d.Open(dsn)
	if err != nil {
		return nil, err
	}
	sc := conn.(*sqlite3.SQLiteConn)
	pragmas := []string{
		"PRAGMA busy_timeout=5000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=OFF",
		"PRAGMA cache_size=-16000",    // ~16 MB page cache per connection
		"PRAGMA mmap_size=1073741824", // 1 GB: maps the whole ~240 MB DB (+WAL) so reads are memory access, not pread() syscalls. mmap is a shared file mapping, not per-conn RAM.
		"PRAGMA temp_store=MEMORY",
	}
	if readonly {
		pragmas = append(pragmas, "PRAGMA query_only=ON")
	}
	for _, p := range pragmas {
		if _, err := sc.Exec(p, nil); err != nil {
			// journal_mode/synchronous may already be set; ignore benign errors
			_ = err
		}
	}
	// Let the planner use the seed's ANALYZE stats (sqlite_stat1) for this
	// connection. Safe and cheap; it only refreshes/loads statistics.
	if _, err := sc.Exec("PRAGMA optimize", nil); err != nil {
		_ = err
	}
	return sc, nil
}

func prepare(c *sqlite3.SQLiteConn, q string) *sqlite3.SQLiteStmt {
	s, err := c.Prepare(q)
	if err != nil {
		panic("prepare failed for [" + q + "]: " + err.Error())
	}
	return s.(*sqlite3.SQLiteStmt)
}

func initDB(path string, nReaders int) error {
	// Writer connection.
	wc, err := openConn(path, false)
	if err != nil {
		return err
	}
	wr = &writer{
		c:      wc,
		insP:   prepare(wc, insertPostSQL),
		insL:   prepare(wc, insertLikeSQL),
		exists: prepare(wc, existsSQL),
		begin:  prepare(wc, "BEGIN IMMEDIATE"),
		commit: prepare(wc, "COMMIT"),
		rbck:   prepare(wc, "ROLLBACK"),
		reqs:   make(chan *writeReq, 1024),
	}
	go wr.run()

	// Health connection (its own, so it never contends with the pool).
	hc, err := openConn(path, true)
	if err != nil {
		return err
	}
	healthC = hc
	health = prepare(hc, "SELECT 1")

	// Reader pool.
	readers = make(chan *reader, nReaders)
	for i := 0; i < nReaders; i++ {
		rc, err := openConn(path, true)
		if err != nil {
			return err
		}
		readers <- &reader{
			c:    rc,
			feed: prepare(rc, feedSQL),
			byID: prepare(rc, postSQL),
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// JSON output helpers (exact key order, compact)
// ---------------------------------------------------------------------------

// appendPost writes {"id":..,"body":..,"created_at":..,"author":..,"like_count":..}
func appendPost(b []byte, id int64, body, createdAt, author string, likeCount int64) []byte {
	b = append(b, `{"id":`...)
	b = strconv.AppendInt(b, id, 10)
	b = append(b, `,"body":`...)
	b = appendJSONString(b, body)
	b = append(b, `,"created_at":`...)
	b = appendJSONString(b, createdAt)
	b = append(b, `,"author":`...)
	b = appendJSONString(b, author)
	b = append(b, `,"like_count":`...)
	b = strconv.AppendInt(b, likeCount, 10)
	b = append(b, '}')
	return b
}

// appendJSONString encodes a Go string as a JSON string with minimal required
// escaping. The spec says escaping <>&/ and non-ASCII is optional; we escape
// only what JSON requires: ", \, and control chars.
func appendJSONString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			b = append(b, '\\', '"')
		case c == '\\':
			b = append(b, '\\', '\\')
		case c == '\n':
			b = append(b, '\\', 'n')
		case c == '\r':
			b = append(b, '\\', 'r')
		case c == '\t':
			b = append(b, '\\', 't')
		case c < 0x20:
			const hex = "0123456789abcdef"
			b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
		default:
			b = append(b, c)
		}
	}
	b = append(b, '"')
	return b
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	b := make([]byte, 0, 32+len(msg))
	b = append(b, `{"error":`...)
	b = appendJSONString(b, msg)
	b = append(b, '}')
	writeJSON(w, status, b)
}

// ---------------------------------------------------------------------------
// JWT (HS256) — verified in full on every request
// ---------------------------------------------------------------------------

var b64 = base64.RawURLEncoding

// parseAuth extracts and verifies the bearer token.
// Returns (userID, username, httpStatus, errMsg). On success status==0.
func parseAuth(authHeader string) (int64, string, int, string) {
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return 0, "", 401, "missing bearer token"
	}
	tok := authHeader[len("Bearer "):]
	// Split into exactly three dot-separated parts without allocating a slice.
	d1 := strings.IndexByte(tok, '.')
	if d1 < 0 {
		return 0, "", 401, "invalid or expired token"
	}
	d2 := strings.IndexByte(tok[d1+1:], '.')
	if d2 < 0 {
		return 0, "", 401, "invalid or expired token"
	}
	d2 += d1 + 1
	// A fourth dot means malformed.
	if strings.IndexByte(tok[d2+1:], '.') >= 0 {
		return 0, "", 401, "invalid or expired token"
	}
	headPart := tok[:d1]
	payPart := tok[d1+1 : d2]
	sigPart := tok[d2+1:]

	headerB, err := b64.DecodeString(headPart)
	if err != nil {
		return 0, "", 401, "invalid or expired token"
	}
	// alg must be HS256.
	if alg, ok := jwtAlg(headerB); !ok || alg != "HS256" {
		return 0, "", 401, "invalid or expired token"
	}
	// Verify signature over header.payload.
	sig, err := b64.DecodeString(sigPart)
	if err != nil {
		return 0, "", 401, "invalid or expired token"
	}
	mac := hmac.New(sha256.New, jwtSecret)
	mac.Write([]byte(tok[:d2])) // header.payload
	expected := mac.Sum(nil)
	if !hmac.Equal(sig, expected) {
		return 0, "", 401, "invalid or expired token"
	}
	// Decode payload.
	payloadB, err := b64.DecodeString(payPart)
	if err != nil {
		return 0, "", 401, "invalid or expired token"
	}
	exp, expOK, subStr, subIsString, nameStr, nameIsString := payloadFields(payloadB)
	// exp check (missing or non-numeric exp is an invalid token).
	if !expOK || exp <= time.Now().Unix() {
		return 0, "", 401, "invalid or expired token"
	}
	// Payload validity: sub must be a positive-integer STRING, username a string.
	if !subIsString {
		return 0, "", 401, "invalid token payload"
	}
	uid, err := strconv.ParseInt(subStr, 10, 64)
	if err != nil || uid <= 0 {
		return 0, "", 401, "invalid token payload"
	}
	if !nameIsString {
		return 0, "", 401, "invalid token payload"
	}
	return uid, nameStr, 0, ""
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	jwtSecret = []byte(getenv("JWT_SECRET", "twelve-dollar-challenge"))
	path := getenv("SQLITE_PATH", "feed.db")
	host := getenv("HOST", "127.0.0.1")
	port := getenv("PORT", "3000")

	nReaders := 4 // readers; tuned for 1 vCPU, raised in start.sh via env if desired
	if v := os.Getenv("READERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			nReaders = n
		}
	}

	if err := initDB(path, nReaders); err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", router)

	ln, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		panic(err)
	}
	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  75 * time.Second, // outlive a VU's 5-15 s think-time so the keep-alive conn isn't recycled mid-loop
	}
	if err := srv.Serve(ln); err != nil {
		panic(err)
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
