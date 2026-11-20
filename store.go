package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/id"
)

type Store struct {
	db *dbutil.Database
}

func (s *Store) Init(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS mcp_kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS mcp_messages (
			event_id TEXT PRIMARY KEY,
			room_id TEXT NOT NULL,
			sender TEXT NOT NULL,
			sender_name TEXT NOT NULL DEFAULT '',
			ts INTEGER NOT NULL,
			kind TEXT NOT NULL,
			body TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS mcp_messages_room_ts ON mcp_messages (room_id, ts)`,
		`CREATE VIRTUAL TABLE IF NOT EXISTS mcp_messages_fts USING fts5(body, sender_name, event_id UNINDEXED, tokenize = 'unicode61 remove_diacritics 2')`,
		`CREATE TABLE IF NOT EXISTS mcp_room_activity (
			room_id TEXT PRIMARY KEY,
			ts INTEGER NOT NULL DEFAULT 0,
			preview TEXT NOT NULL DEFAULT '',
			sender TEXT NOT NULL DEFAULT '',
			unread INTEGER NOT NULL DEFAULT 0,
			highlights INTEGER NOT NULL DEFAULT 0
		)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(ctx, q); err != nil {
			return err
		}
	}
	v, _ := s.GetKV(ctx, "schema_version")
	// v2: previews used to include membership events; rebuild activity and re-run backfill once.
	if v < "2" {
		for _, q := range []string{`DELETE FROM mcp_room_activity`, `DELETE FROM mcp_kv WHERE key LIKE 'backfilled:%'`} {
			if _, err := s.db.Exec(ctx, q); err != nil {
				return err
			}
		}
	}
	// v3: the full-text index stores folded text (ł -> l etc.); rebuild it from the messages table.
	if v < "3" {
		if err := s.rebuildFTS(ctx); err != nil {
			return err
		}
	}
	if v < "3" {
		return s.SetKV(ctx, "schema_version", "3")
	}
	return nil
}

func (s *Store) rebuildFTS(ctx context.Context) error {
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if _, err := s.db.Exec(ctx, `DELETE FROM mcp_messages_fts`); err != nil {
			return err
		}
		rows, err := s.db.Query(ctx, `SELECT event_id, sender_name, body FROM mcp_messages WHERE body != ''`)
		if err != nil {
			return err
		}
		type row struct{ id, name, body string }
		var all []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.name, &r.body); err != nil {
				rows.Close()
				return err
			}
			all = append(all, r)
		}
		rows.Close()
		for _, r := range all {
			if _, err := s.db.Exec(ctx, `INSERT INTO mcp_messages_fts (body, sender_name, event_id) VALUES ($1, $2, $3)`, foldText(r.body), foldText(r.name), r.id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) GetKV(ctx context.Context, key string) (string, error) {
	var v string
	err := s.db.QueryRow(ctx, `SELECT value FROM mcp_kv WHERE key = $1`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) SetKV(ctx context.Context, key, value string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO mcp_kv (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

func (s *Store) resetBackfill(ctx context.Context) error {
	_, err := s.db.Exec(ctx, `DELETE FROM mcp_kv WHERE key LIKE 'backfilled:%'`)
	return err
}

func (s *Store) Index(ctx context.Context, m *Message) error {
	if m.EventID == "" {
		return nil
	}
	body := m.Body
	if m.Media != nil && m.Media.Filename != "" && !strings.Contains(body, m.Media.Filename) {
		body = strings.TrimSpace(body + " " + m.Media.Filename)
	}
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		res, err := s.db.Exec(ctx, `INSERT INTO mcp_messages (event_id, room_id, sender, sender_name, ts, kind, body) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (event_id) DO NOTHING`,
			m.EventID, m.RoomID, m.Sender, m.SenderName, m.TS, m.Kind, body)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 || body == "" {
			return nil
		}
		_, err = s.db.Exec(ctx, `INSERT INTO mcp_messages_fts (body, sender_name, event_id) VALUES ($1, $2, $3)`, foldText(body), foldText(m.SenderName), m.EventID)
		return err
	})
}

func (s *Store) UpdateBody(ctx context.Context, eventID, body string) error {
	if eventID == "" {
		return nil
	}
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		res, err := s.db.Exec(ctx, `UPDATE mcp_messages SET body = $2 WHERE event_id = $1`, eventID, body)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil
		}
		_, err = s.db.Exec(ctx, `UPDATE mcp_messages_fts SET body = $2 WHERE event_id = $1`, eventID, foldText(body))
		return err
	})
}

func (s *Store) Delete(ctx context.Context, eventID string) error {
	if eventID == "" {
		return nil
	}
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if _, err := s.db.Exec(ctx, `DELETE FROM mcp_messages WHERE event_id = $1`, eventID); err != nil {
			return err
		}
		_, err := s.db.Exec(ctx, `DELETE FROM mcp_messages_fts WHERE event_id = $1`, eventID)
		return err
	})
}

type SearchHit struct {
	EventID    string `json:"message_id"`
	RoomID     string `json:"room_id"`
	RoomName   string `json:"room_name,omitempty"`
	Sender     string `json:"sender"`
	SenderName string `json:"sender_name,omitempty"`
	FromMe     bool   `json:"from_me,omitempty"`
	Time       string `json:"time"`
	Kind       string `json:"kind"`
	Body       string `json:"body"`
}

// foldReplacer handles letters that Unicode does not decompose, so FTS5's remove_diacritics misses them.
var foldReplacer = strings.NewReplacer("ł", "l", "Ł", "L", "đ", "d", "Đ", "D", "ø", "o", "Ø", "O", "ß", "ss", "æ", "ae", "Æ", "AE", "œ", "oe", "Œ", "OE", "ı", "i")

// foldText is applied to both indexed text and queries.
func foldText(s string) string { return foldReplacer.Replace(s) }

// ftsQuery turns free text into a safe FTS5 query: every word must match (prefix match on the last one).
func ftsQuery(q string) string {
	words := strings.Fields(foldText(q))
	parts := make([]string, 0, len(words))
	for i, w := range words {
		w = strings.ReplaceAll(w, `"`, `""`)
		p := `"` + w + `"`
		if i == len(words)-1 {
			p += "*"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, " ")
}

type SearchQuery struct {
	Text          string
	RoomID        id.RoomID
	Sender        string // Matrix ID or part of the display name
	After, Before int64  // unix ms; 0 = unbounded
	Limit         int
}

func (s *Store) Search(ctx context.Context, sq SearchQuery) ([]SearchHit, error) {
	var q string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if strings.TrimSpace(sq.Text) != "" {
		q = `SELECT m.event_id, m.room_id, m.sender, m.sender_name, m.ts, m.kind, m.body
			FROM mcp_messages_fts f JOIN mcp_messages m ON m.event_id = f.event_id
			WHERE mcp_messages_fts MATCH ` + arg(ftsQuery(sq.Text))
	} else {
		q = `SELECT m.event_id, m.room_id, m.sender, m.sender_name, m.ts, m.kind, m.body FROM mcp_messages m WHERE 1 = 1`
	}
	if sq.RoomID != "" {
		q += ` AND m.room_id = ` + arg(sq.RoomID.String())
	}
	if sq.Sender != "" {
		p := arg(sq.Sender)
		q += ` AND (m.sender = ` + p + ` OR m.sender_name LIKE '%' || ` + p + ` || '%')`
	}
	if sq.After > 0 {
		q += ` AND m.ts >= ` + arg(sq.After)
	}
	if sq.Before > 0 {
		q += ` AND m.ts < ` + arg(sq.Before)
	}
	q += ` ORDER BY m.ts DESC LIMIT ` + arg(sq.Limit)
	rows, err := s.db.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hits []SearchHit
	for rows.Next() {
		var h SearchHit
		var ts int64
		if err := rows.Scan(&h.EventID, &h.RoomID, &h.Sender, &h.SenderName, &ts, &h.Kind, &h.Body); err != nil {
			return nil, err
		}
		h.Time = fmtTS(ts)
		hits = append(hits, h)
	}
	return hits, rows.Err()
}

func (s *Store) IndexedCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM mcp_messages`).Scan(&n)
	return n, err
}

type activity struct {
	TS                 int64
	Preview, Sender    string
	Unread, Highlights int
}

func (s *Store) LoadActivity(ctx context.Context) (map[id.RoomID]*activity, error) {
	rows, err := s.db.Query(ctx, `SELECT room_id, ts, preview, sender, unread, highlights FROM mcp_room_activity`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[id.RoomID]*activity{}
	for rows.Next() {
		var roomID string
		a := &activity{}
		if err := rows.Scan(&roomID, &a.TS, &a.Preview, &a.Sender, &a.Unread, &a.Highlights); err != nil {
			return nil, err
		}
		out[id.RoomID(roomID)] = a
	}
	return out, rows.Err()
}

func (s *Store) SaveActivity(ctx context.Context, roomID id.RoomID, a *activity) {
	_, _ = s.db.Exec(ctx, `INSERT INTO mcp_room_activity (room_id, ts, preview, sender, unread, highlights) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (room_id) DO UPDATE SET ts = excluded.ts, preview = excluded.preview, sender = excluded.sender, unread = excluded.unread, highlights = excluded.highlights`,
		roomID.String(), a.TS, a.Preview, a.Sender, a.Unread, a.Highlights)
}
