package pgsql

// Frozen diagnostic snapshot dialect (OPTIMIZATION_ROUND_9.md §R9-3 /
// sub-plan OPTIMIZATION_ROUND_9_3.md).
//
// M-Range: when KINE_SNAPSHOT_LOAD_DSN is set (non-empty), the pgsql driver
// loads the current-state mirror (kine_current, latest row per name,
// tombstones included) into memory at startup — outside any measured window —
// and serves reads from the snapshot through the ordinary SQLLog →
// logstructured → endpoint composition, so the unary Range response and gRPC
// path are shared end to end while the measured path contains no SQL.
//
// Raw wire-format column bytes are captured from pgx binary rows at load time
// and replayed verbatim at query time, so the decode machinery (scanFast)
// is byte-identical to the native read path, including int4/int8 encoding
// distinctions. The two scalar metadata columns (current_rev, compact_rev)
// are synthesized as int8 wire bytes, matching the subquery output of the
// ordinary list queries.
//
// Fail closed: every mutation entry point (Insert, DeleteRevision,
// SetCompactRevision, Compact, PostCompact, Fill, BeginTx) returns an error
// naming the frozen contract; revision-bounded reads below the current
// revision return a declared unsupported error (history is not loaded — the
// snapshot is never disguised as history). The mode is reachable only through
// the explicit opt-in environment variable, never through normal production
// configuration.

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/sirupsen/logrus"
	"github.com/jackc/pgx/v5"
)

const snapshotEnvDSN = "KINE_SNAPSHOT_LOAD_DSN"

// snapshotRow is one kine_current row: the decoded identity used for
// indexing/filtering plus the raw pgx binary wire bytes for all columns.
type snapshotRow struct {
	name    string
	deleted bool
	// raws holds 8 wire-format values in mirror-column order:
	// id, name, created, deleted, create_revision, prev_revision, lease, value
	raws [][]byte
}

type snapshotDialect struct {
	rows       []snapshotRow // sorted by name, one row per name (latest mirror state)
	currentRev int64
	compactRev int64
	liveBytes  int64
}

// snapshotFrozenErr builds the declared fail-closed error for every mutation
// path.
func snapshotFrozenErr(op string) error {
	return fmt.Errorf("frozen diagnostic snapshot (KINE_SNAPSHOT_LOAD_DSN) rejects %s (fail closed)", op)
}

// snapshotUnsupportedErr builds the declared error for reads the snapshot
// cannot serve (history is not loaded).
func snapshotUnsupportedErr(op string) error {
	return fmt.Errorf("frozen diagnostic snapshot supports current reads only: %s (declared unsupported)", op)
}

// loadSnapshot connects to the given DSN and captures the mirror. Load
// happens at startup, outside any measured window.
func loadSnapshot(ctx context.Context, dsn string, tlsInfo tls.Config) (*snapshotDialect, error) {
	// prepareConfig applies the kine DSN convention (no URL scheme; it
	// prepends postgres:// itself). Accept both forms in the env var.
	dsn = strings.TrimPrefix(dsn, "postgres://")
	connConfig, err := prepareConfig(dsn, tlsInfo)
	if err != nil {
		return nil, fmt.Errorf("snapshot load: prepare config: %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("snapshot load: connect: %w", err)
	}
	defer conn.Close(ctx)

	s := &snapshotDialect{}

	if err := conn.QueryRow(ctx, "SELECT COALESCE(MAX(id),0) FROM kine").Scan(&s.currentRev); err != nil {
		return nil, fmt.Errorf("snapshot load: current revision: %w", err)
	}
	if err := conn.QueryRow(ctx, "SELECT COALESCE(MAX(prev_revision),0) FROM kine WHERE name = 'compact_rev_key'").Scan(&s.compactRev); err != nil {
		return nil, fmt.Errorf("snapshot load: compact revision: %w", err)
	}

	rows, err := conn.Query(ctx, "SELECT id, name, created, deleted, create_revision, prev_revision, lease, value FROM kine_current")
	if err != nil {
		return nil, fmt.Errorf("snapshot load: query mirror: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		raws := rows.RawValues()
		if len(raws) != 8 {
			return nil, fmt.Errorf("snapshot load: expected 8 wire columns, got %d", len(raws))
		}
		// Copy now: pgx reuses its buffers across rows.
		copied := make([][]byte, len(raws))
		for i, r := range raws {
			copied[i] = append(make([]byte, 0, len(r)), r...)
		}
		del, err := decWireInt(copied[3])
		if err != nil {
			return nil, fmt.Errorf("snapshot load: decode deleted flag: %w", err)
		}
		row := snapshotRow{
			name:    string(copied[1]),
			deleted: del != 0,
			raws:    copied,
		}
		if !row.deleted {
			s.liveBytes += int64(len(copied[7]))
		}
		s.rows = append(s.rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("snapshot load: scan mirror: %w", err)
	}
	sort.Slice(s.rows, func(i, j int) bool { return s.rows[i].name < s.rows[j].name })

	logrus.Warnf("FROZEN DIAGNOSTIC SNAPSHOT MODE (%s): %d mirror rows loaded, current_rev=%d, compact_rev=%d, live value bytes=%d; all mutations are rejected (fail closed)",
		snapshotEnvDSN, len(s.rows), s.currentRev, s.compactRev, s.liveBytes)
	return s, nil
}

// encInt8 encodes an int64 as a PostgreSQL int8 binary wire value (8 bytes,
// big-endian two's complement), matching what the real metadata subqueries
// produce.
func encInt8(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// encInt4 encodes an int32 as a PostgreSQL int4 binary wire value.
func encInt4(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

// decWireInt decodes an int4 or int8 binary wire value into an int64.
func decWireInt(b []byte) (int64, error) {
	switch len(b) {
	case 0:
		return 0, nil
	case 4:
		return int64(int32(binary.BigEndian.Uint32(b))), nil
	case 8:
		return int64(binary.BigEndian.Uint64(b)), nil
	default:
		return 0, fmt.Errorf("frozen snapshot: unexpected integer wire length %d", len(b))
	}
}

// snapshotColumns returns the full wire column set for a row: the two scalar
// metadata columns followed by the row's captured raw bytes.
func (s *snapshotDialect) snapshotColumns(row *snapshotRow) [][]byte {
	cols := make([][]byte, 0, 10)
	cols = append(cols, encInt8(s.currentRev), encInt8(s.compactRev))
	return append(cols, row.raws...)
}

// selectRange serves name >= key AND (end == "" ? name == key : name < end),
// mirroring the mirror-table SQL semantics (start key translated upstream by
// SQLLog via TranslateStartKey; end used raw). Tombstones are filtered unless
// includeDeleted.
func (s *snapshotDialect) selectRange(key, end string, limit int64, includeDeleted bool) []snapshotRow {
	i := sort.Search(len(s.rows), func(j int) bool { return s.rows[j].name >= key })
	var out []snapshotRow
	for ; i < len(s.rows); i++ {
		r := s.rows[i]
		if end != "" {
			if r.name >= end {
				break
			}
		} else if r.name != key {
			break
		}
		if !includeDeleted && r.deleted {
			continue
		}
		out = append(out, r)
		if limit > 0 && int64(len(out)) >= limit {
			break
		}
	}
	return out
}

// snapRows is a server.RowSet over a selected slice of snapshot rows. Binary()
// is always true and RawValues() exposes the captured wire bytes, so
// RowsToEvents takes the same scanFast path as the native read arm.
type snapRows struct {
	s    *snapshotDialect
	rows []snapshotRow
	i    int // index of the current row; -1 before first Next
}

func (r *snapRows) Next() bool {
	r.i++
	return r.i < len(r.rows)
}

func (r *snapRows) Scan(dest ...any) error {
	if r.i < 0 || r.i >= len(r.rows) {
		return fmt.Errorf("frozen snapshot: Scan called at invalid row position %d", r.i)
	}
	cols := r.s.snapshotColumns(&r.rows[r.i])
	if len(dest) > len(cols) {
		return fmt.Errorf("frozen snapshot: Scan expects at most %d columns, got %d", len(cols), len(dest))
	}
	for k, d := range dest {
		b := cols[k]
		switch dst := d.(type) {
		case *int64:
			v, err := decWireInt(b)
			if err != nil {
				return err
			}
			*dst = v
		case *string:
			*dst = string(b)
		case *[]byte:
			*dst = append(make([]byte, 0, len(b)), b...)
		default:
			return fmt.Errorf("frozen snapshot: unsupported Scan dest type at column %d", k)
		}
	}
	return nil
}

func (r *snapRows) Close() {}

func (r *snapRows) Err() error { return nil }

// RawValues returns the wire-format column values of the current row, valid
// until the next call to Next (same contract as the pgx native RowSet).
func (r *snapRows) RawValues() [][]byte {
	if r.i < 0 || r.i >= len(r.rows) {
		return nil
	}
	return r.s.snapshotColumns(&r.rows[r.i])
}

func (r *snapRows) Binary() bool { return true }

func (s *snapshotDialect) rowSet(rows []snapshotRow) server.RowSet {
	return &snapRows{s: s, rows: rows, i: -1}
}

func (s *snapshotDialect) ListCurrent(ctx context.Context, key, end string, limit int64, includeDeleted, keysOnly bool) (server.RowSet, error) {
	if includeDeleted && end != "" {
		// Tombstone-inclusive range reads are served from the history log in
		// the ordinary path; the frozen snapshot loads no history.
		return nil, snapshotUnsupportedErr("tombstone-inclusive range read")
	}
	return s.rowSet(s.selectRange(key, end, limit, includeDeleted)), nil
}

func (s *snapshotDialect) List(ctx context.Context, key, end string, limit, revision int64, includeDeleted, keysOnly bool) (server.RowSet, error) {
	if revision != s.currentRev {
		return nil, snapshotUnsupportedErr(fmt.Sprintf("revision-bounded read (revision=%d, current=%d)", revision, s.currentRev))
	}
	return s.ListCurrent(ctx, key, end, limit, includeDeleted, keysOnly)
}

func (s *snapshotDialect) CountCurrent(ctx context.Context, key, end string) (int64, int64, error) {
	if key == "" && end == "" {
		// Matches currentCountAllFmt semantics: live rows only.
		var n int64
		for i := range s.rows {
			if !s.rows[i].deleted {
				n++
			}
		}
		return s.currentRev, n, nil
	}
	return s.currentRev, int64(len(s.selectRange(key, end, 0, false))), nil
}

func (s *snapshotDialect) Count(ctx context.Context, key, end string, revision int64) (int64, int64, int64, error) {
	if revision != s.currentRev {
		return 0, 0, 0, snapshotUnsupportedErr(fmt.Sprintf("revision-bounded count (revision=%d, current=%d)", revision, s.currentRev))
	}
	_, n, err := s.CountCurrent(ctx, key, end)
	return s.currentRev, s.compactRev, n, err
}

func (s *snapshotDialect) CurrentRevision(ctx context.Context) (int64, error) {
	return s.currentRev, nil
}

// After serves the compact_rev_key bootstrap row (exactly one, so
// SQLLog.compactStart performs no write) and empty result sets for watch
// polling (a frozen snapshot produces no new events).
func (s *snapshotDialect) After(ctx context.Context, key, end string, rev, limit int64) (server.RowSet, error) {
	if key != "compact_rev_key" {
		return s.rowSet(nil), nil
	}
	created := int32(0)
	if s.compactRev == 0 {
		created = 1
	}
	meta := snapshotRow{
		name: "compact_rev_key",
		raws: [][]byte{
			encInt8(1),                // id
			[]byte("compact_rev_key"), // name (text wire: raw bytes)
			encInt4(created),          // created
			encInt4(0),                // deleted
			encInt8(0),                // create_revision
			encInt8(s.compactRev),     // prev_revision: drives compact revision extraction
			encInt4(0),                // lease
			{},                        // value (empty bytea)
		},
	}
	return s.rowSet([]snapshotRow{meta}), nil
}

func (s *snapshotDialect) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value []byte) (int64, error) {
	return 0, snapshotFrozenErr("Insert")
}

func (s *snapshotDialect) DeleteRevision(ctx context.Context, revision int64) error {
	return snapshotFrozenErr("DeleteRevision")
}

func (s *snapshotDialect) GetCompactRevision(ctx context.Context) (int64, error) {
	return s.compactRev, nil
}

func (s *snapshotDialect) SetCompactRevision(ctx context.Context, revision int64) error {
	return snapshotFrozenErr("SetCompactRevision")
}

func (s *snapshotDialect) Compact(ctx context.Context, from, to int64) (int64, error) {
	return 0, snapshotFrozenErr("Compact")
}

func (s *snapshotDialect) PostCompact(ctx context.Context) error {
	return snapshotFrozenErr("PostCompact")
}

func (s *snapshotDialect) Fill(ctx context.Context, revision int64) error {
	return snapshotFrozenErr("Fill")
}

func (s *snapshotDialect) IsFill(key string) bool { return false }

func (s *snapshotDialect) BeginTx(ctx context.Context, opts *sql.TxOptions) (server.Transaction, error) {
	return nil, snapshotFrozenErr("BeginTx")
}

func (s *snapshotDialect) GetSize(ctx context.Context) (int64, error) {
	return s.liveBytes, nil
}

func (s *snapshotDialect) FillRetryDelay(ctx context.Context) {}

func (s *snapshotDialect) TranslateStartKey(startKey string) string {
	// Same substitution as the ordinary pgsql dialect: replace trailing null
	// with 0x1a, as postgres does not allow null bytes in UTF8 strings.
	if s, ok := strings.CutSuffix(startKey, "\x00"); ok {
		return s + "\x1a"
	}
	return startKey
}

// snapshotRequested reports whether the frozen diagnostic snapshot mode is
// enabled via the opt-in environment variable.
func snapshotRequested() bool {
	return os.Getenv(snapshotEnvDSN) != ""
}
