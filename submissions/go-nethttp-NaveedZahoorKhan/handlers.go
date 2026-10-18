package main

import (
	"database/sql/driver"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// postRow holds one projected post row.
type postRow struct {
	id        int64
	body      string
	createdAt string
	author    string
	likeCount int64
}

func asInt(v driver.Value) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}

func asStr(v driver.Value) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	}
	return ""
}

func rowFromDest(dest []driver.Value) postRow {
	return postRow{
		id:        asInt(dest[0]),
		body:      asStr(dest[1]),
		createdAt: asStr(dest[2]),
		author:    asStr(dest[3]),
		likeCount: asInt(dest[4]),
	}
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

func router(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case p == "/health" && r.Method == http.MethodGet:
		handleHealth(w)
	case p == "/feed" && r.Method == http.MethodGet:
		handleFeed(w)
	case p == "/posts" && r.Method == http.MethodPost:
		handleCreatePost(w, r)
	case strings.HasPrefix(p, "/posts/"):
		rest := p[len("/posts/"):]
		if strings.HasSuffix(rest, "/like") {
			idStr := rest[:len(rest)-len("/like")]
			if !strings.Contains(idStr, "/") && r.Method == http.MethodPost {
				handleLike(w, r, idStr)
				return
			}
			writeError(w, 404, "not found")
			return
		}
		if !strings.Contains(rest, "/") && r.Method == http.MethodGet {
			handleGetPost(w, rest)
			return
		}
		writeError(w, 404, "not found")
	default:
		writeError(w, 404, "not found")
	}
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func handleHealth(w http.ResponseWriter) {
	healthM.Lock()
	rows, err := health.Query(nil)
	if err == nil {
		dest := make([]driver.Value, 1)
		err = rows.Next(dest)
		if err == io.EOF {
			err = nil
		}
		rows.Close()
	}
	healthM.Unlock()
	if err != nil {
		b := []byte(`{"status":"degraded","db":"unreachable","error":`)
		b = appendJSONString(b, err.Error())
		b = append(b, '}')
		writeJSON(w, 503, b)
		return
	}
	up := int64(timeSinceStartSeconds())
	b := []byte(`{"status":"ok","db":"ok","uptime_s":`)
	b = strconv.AppendInt(b, up, 10)
	b = append(b, '}')
	writeJSON(w, 200, b)
}

func handleFeed(w http.ResponseWriter) {
	rd := <-readers
	rows, err := rd.feed.Query(nil)
	if err != nil {
		readers <- rd
		writeError(w, 500, "internal server error")
		return
	}
	buf := make([]byte, 0, 8192)
	buf = append(buf, `{"posts":[`...)
	dest := make([]driver.Value, 5)
	first := true
	for {
		e := rows.Next(dest)
		if e == io.EOF {
			break
		}
		if e != nil {
			err = e
			break
		}
		if !first {
			buf = append(buf, ',')
		}
		first = false
		r := rowFromDest(dest)
		buf = appendPost(buf, r.id, r.body, r.createdAt, r.author, r.likeCount)
	}
	rows.Close()
	readers <- rd
	if err != nil {
		writeError(w, 500, "internal server error")
		return
	}
	buf = append(buf, ']', '}')
	writeJSON(w, 200, buf)
}

func handleGetPost(w http.ResponseWriter, idStr string) {
	id, ok := parsePostID(idStr)
	if !ok {
		writeError(w, 400, "invalid post id")
		return
	}
	rd := <-readers
	rows, err := rd.byID.Query([]driver.Value{id})
	var (
		found bool
		row   postRow
	)
	if err == nil {
		dest := make([]driver.Value, 5)
		e := rows.Next(dest)
		if e == nil {
			found = true
			row = rowFromDest(dest)
		} else if e != io.EOF {
			err = e
		}
		rows.Close()
	}
	readers <- rd
	if err != nil {
		writeError(w, 500, "internal server error")
		return
	}
	if !found {
		writeError(w, 404, "post not found")
		return
	}
	buf := make([]byte, 0, 256+len(row.body))
	buf = append(buf, `{"post":`...)
	buf = appendPost(buf, row.id, row.body, row.createdAt, row.author, row.likeCount)
	buf = append(buf, '}')
	writeJSON(w, 200, buf)
}

func handleCreatePost(w http.ResponseWriter, r *http.Request) {
	// Auth first.
	uid, username, status, msg := parseAuth(r.Header.Get("Authorization"))
	if status != 0 {
		writeError(w, status, msg)
		return
	}
	// Read body (<=16KB tested; cap generously).
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	body, perr := parseBodyField(raw)
	switch perr {
	case errMalformed:
		writeError(w, 400, "malformed JSON body")
		return
	case errBodyRequired:
		writeError(w, 400, "body is required")
		return
	case errBodyTooLong:
		writeError(w, 400, "body must be at most 500 characters")
		return
	}

	wr.mu.Lock()
	rows, err := wr.insP.Query([]driver.Value{uid, body})
	var (
		newID     int64
		createdAt string
	)
	if err == nil {
		dest := make([]driver.Value, 2)
		e := rows.Next(dest)
		if e == nil {
			newID = asInt(dest[0])
			createdAt = asStr(dest[1])
		} else {
			err = e
		}
		rows.Close()
	}
	wr.mu.Unlock()
	if err != nil {
		writeError(w, 500, "internal server error")
		return
	}
	buf := make([]byte, 0, 256+len(body))
	buf = append(buf, `{"post":`...)
	buf = appendPost(buf, newID, body, createdAt, username, 0)
	buf = append(buf, '}')
	writeJSON(w, 201, buf)
}

func handleLike(w http.ResponseWriter, r *http.Request, idStr string) {
	// Auth first, then id (per SPEC).
	uid, _, status, msg := parseAuth(r.Header.Get("Authorization"))
	if status != 0 {
		writeError(w, status, msg)
		return
	}
	id, ok := parsePostID(idStr)
	if !ok {
		writeError(w, 400, "invalid post id")
		return
	}

	wr.mu.Lock()
	// Does the post exist?
	exRows, err := wr.exists.Query([]driver.Value{id})
	exists := false
	if err == nil {
		dest := make([]driver.Value, 1)
		if e := exRows.Next(dest); e == nil {
			exists = true
		} else if e != io.EOF {
			err = e
		}
		exRows.Close()
	}
	if err != nil {
		wr.mu.Unlock()
		writeError(w, 500, "internal server error")
		return
	}
	if !exists {
		wr.mu.Unlock()
		writeError(w, 404, "post not found")
		return
	}
	// Insert like; detect whether it was new via changes().
	res, err := wr.insL.Exec([]driver.Value{uid, id})
	alreadyLiked := false
	if err == nil {
		n, _ := res.RowsAffected()
		alreadyLiked = n == 0
	}
	wr.mu.Unlock()
	if err != nil {
		writeError(w, 500, "internal server error")
		return
	}
	buf := make([]byte, 0, 64)
	buf = append(buf, `{"liked":true,"already_liked":`...)
	if alreadyLiked {
		buf = append(buf, "true"...)
	} else {
		buf = append(buf, "false"...)
	}
	buf = append(buf, `,"post_id":`...)
	buf = strconv.AppendInt(buf, id, 10)
	buf = append(buf, '}')
	status = 201
	if alreadyLiked {
		status = 200
	}
	writeJSON(w, status, buf)
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

// parsePostID accepts only a positive base-10 integer with no sign, decimal,
// or leading zeros issues. "0", "-1", "abc", "1.5" are all invalid.
func parsePostID(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
