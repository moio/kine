package pgsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/k3s-io/kine/pkg/dialer"
	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	"github.com/sirupsen/logrus"
)

const (
	defaultDSN = "postgres://postgres:postgres@localhost/"
)

var (
	schema = []string{
		`CREATE TABLE IF NOT EXISTS kine
 			(
				id BIGSERIAL PRIMARY KEY,
				name text COLLATE "C",
				created INTEGER,
				deleted INTEGER,
				create_revision BIGINT,
				prev_revision BIGINT,
 				lease INTEGER,
 				value bytea,
 				old_value bytea
 			);`,

		`CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name)`,
		`CREATE INDEX IF NOT EXISTS kine_name_id_index ON kine (name,id)`,
		`CREATE INDEX IF NOT EXISTS kine_id_deleted_index ON kine (id,deleted)`,
		`CREATE INDEX IF NOT EXISTS kine_prev_revision_index ON kine (prev_revision)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (name, prev_revision)`,
		`CREATE INDEX IF NOT EXISTS kine_list_query_index on kine(name, id DESC, deleted)`,
	}

	listFmt = `
		SELECT (%s) AS current_rev, (%s) AS compact_rev, %s
		FROM (
			SELECT dkv.id AS list_id
			FROM (%s) AS dkv
			WHERE (dkv.deleted = 0 OR ?)
			ORDER BY dkv.name ASC %s
		) AS mkv
		JOIN kine ON kine.id = mkv.list_id
		ORDER BY kine.name ASC`

	// use a subquery to extract `name, id DESC, deleted` so that the postgres
	// query planner will make use of kine_list_query_index; by default the `MAX(id)`
	// plans as a separate group aggregate which will not use the index.
	distinctNameSQL = `
		SELECT DISTINCT ON (name) name, id, deleted
		FROM kine
		WHERE name >= ? AND name < ? %s
		ORDER BY name ASC, id DESC`

	listSQL    = fmt.Sprintf(listFmt, generic.CurrentRevSQL, generic.CompactRevSQL, generic.Columns, distinctNameSQL, query.LimitToken)
	listValSQL = fmt.Sprintf(listFmt, generic.CurrentRevSQL, generic.CompactRevSQL, generic.WithVal, distinctNameSQL, query.LimitToken)

	// Current-state mirror-table variants (OPTIMIZATION_ROUND_8.md Track B,
	// enabled by KINE_PGSQL_CURRENT_STATE). These serve the same call sites
	// and parameter lists as the history-backed queries above, but read
	// kine_current, which the kine_current_maintain trigger maintains as the
	// transactionally committed latest row per key. Deleted keys are
	// represented by their tombstone row (deleted = 1) so that the
	// deleted = 0 filter reproduces the history query's select-latest-then-
	// test-deletion semantics; requests that ask for tombstones
	// Tombstone-inclusive single-key existence checks (Create/Delete) are
	// served from the mirror itself (see currentGetDelFmt); includeDeleted
	// range reads remain on the history log by the dialect wrapper below.
	currentAllFmt = `
		SELECT (%s) AS current_rev, (%s) AS compact_rev, %s
		FROM kine_current
		WHERE deleted = 0
		ORDER BY name ASC`
	currentGetFmt = `
		SELECT (%s) AS current_rev, (%s) AS compact_rev, %s
		FROM kine_current
		WHERE name = ? AND deleted = 0`
	// Tombstone-inclusive single-key variants: the mirror always holds the
	// latest committed row per key, tombstone or not, so these variants are
	// authoritative for the includeDeleted existence checks in Create/Delete
	// (logstructured.get with revision=0, end="", limit=1). History cannot
	// answer these after compaction: the PostgreSQL CompactSQL deletes every
	// tombstone row in the compact window unconditionally, even when it is a
	// key's latest revision, after which the log has no trace of the key.
	// Serving the check from the mirror preserves the uncompacted semantics
	// (and etcd's): Create sees deleted=1 and proceeds; Delete of an already
	// deleted key reports deleted=false, matching etcd, instead of the
	// post-compaction history answer deleted=true.
	currentGetDelFmt = `
		SELECT (%s) AS current_rev, (%s) AS compact_rev, %s
		FROM kine_current
		WHERE name = ?`
	currentListFmt = `
		SELECT (%s) AS current_rev, (%s) AS compact_rev, %s
		FROM kine_current
		WHERE name >= ? AND name < ? AND deleted = 0
		ORDER BY name ASC %s`
	currentCountFmt = `
		SELECT (%s), COUNT(id)
		FROM kine_current
		WHERE name >= ? AND name < ? AND deleted = 0`
	currentCountAllFmt = `
		SELECT (%s), COUNT(id)
		FROM kine_current
		WHERE deleted = 0`

	// Schema for the current-state mirror. Every committed revision insert
	// on kine upserts the same transaction's row into kine_current, so the
	// mirror becomes visible exactly when the history row does and can never
	// reference an uncommitted revision. The WHERE cur.id < EXCLUDED.id
	// guard makes the mirror monotonic per key: a late-committing lower
	// revision can never overwrite a newer committed one.
	currentStateSchema = []string{
		`CREATE TABLE IF NOT EXISTS kine_current (
			id BIGINT NOT NULL,
			name TEXT COLLATE "C" NOT NULL,
			created INTEGER,
			deleted INTEGER,
			create_revision BIGINT,
			prev_revision BIGINT,
			lease INTEGER,
			value BYTEA,
			CONSTRAINT kine_current_pkey PRIMARY KEY (name)
		);`,
		`CREATE OR REPLACE FUNCTION kine_current_maintain() RETURNS trigger AS $func$
		BEGIN
			INSERT INTO kine_current AS cur (id, name, created, deleted, create_revision, prev_revision, lease, value)
			VALUES (NEW.id, NEW.name, NEW.created, NEW.deleted, NEW.create_revision, NEW.prev_revision, NEW.lease, NEW.value)
			ON CONFLICT (name) DO UPDATE SET
				id = EXCLUDED.id,
				created = EXCLUDED.created,
				deleted = EXCLUDED.deleted,
				create_revision = EXCLUDED.create_revision,
				prev_revision = EXCLUDED.prev_revision,
				lease = EXCLUDED.lease,
				value = EXCLUDED.value
			WHERE cur.id < EXCLUDED.id;
			RETURN NEW;
		END;
		$func$ LANGUAGE plpgsql;`,
		`DROP TRIGGER IF EXISTS kine_current_maintain ON kine;`,
		`CREATE TRIGGER kine_current_maintain AFTER INSERT ON kine
		FOR EACH ROW EXECUTE FUNCTION kine_current_maintain();`,
	}

	schemaMigrations = []string{
		`ALTER TABLE kine ALTER COLUMN id SET DATA TYPE BIGINT, ALTER COLUMN create_revision SET DATA TYPE BIGINT, ALTER COLUMN prev_revision SET DATA TYPE BIGINT; ALTER SEQUENCE kine_id_seq AS BIGINT`,
		// It is important to set the collation to "C" to ensure that LIKE and COMPARISON
		// queries use the index.
		`ALTER TABLE kine ALTER COLUMN name SET DATA TYPE TEXT COLLATE "C" USING name::TEXT COLLATE "C"`,
	}
	createDB = `CREATE DATABASE "%s";`
)

// currentStateDialect serves current-state reads from the kine_current
// mirror table while delegating everything else to the wrapped Generic.
// The mirror queries are built by newCurrentStateDialect; the embedded
// Generic keeps the history-backed query set, which is what the
// includeDeleted fallback below must use.
type currentStateDialect struct {
	*generic.Generic

	currentAllSQL       *query.Named
	currentAllValSQL    *query.Named
	currentGetSQL       *query.Named
	currentGetValSQL    *query.Named
	currentGetDelSQL    *query.Named
	currentGetDelValSQL *query.Named
	currentListSQL      *query.Named
	currentListValSQL   *query.Named
	currentCountSQL     *query.Named
	currentCountAllSQL  *query.Named
}

func newCurrentStateDialect(dialect *generic.Generic) *currentStateDialect {
	return &currentStateDialect{
		Generic: dialect,
		currentAllSQL: query.New(fmt.Sprintf(currentAllFmt,
			generic.CurrentRevSQL, generic.CompactRevSQL, generic.Columns), "$", true, "AllCurrent"),
		currentAllValSQL: query.New(fmt.Sprintf(currentAllFmt,
			generic.CurrentRevSQL, generic.CompactRevSQL, generic.WithVal), "$", true, "AllCurrentVal"),
		currentGetSQL: query.New(fmt.Sprintf(currentGetFmt,
			generic.CurrentRevSQL, generic.CompactRevSQL, generic.Columns), "$", true, "GetSingle"),
		currentGetValSQL: query.New(fmt.Sprintf(currentGetFmt,
			generic.CurrentRevSQL, generic.CompactRevSQL, generic.WithVal), "$", true, "GetSingleVal"),
		currentGetDelSQL: query.New(fmt.Sprintf(currentGetDelFmt,
			generic.CurrentRevSQL, generic.CompactRevSQL, generic.Columns), "$", true, "GetSingleDel"),
		currentGetDelValSQL: query.New(fmt.Sprintf(currentGetDelFmt,
			generic.CurrentRevSQL, generic.CompactRevSQL, generic.WithVal), "$", true, "GetSingleDelVal"),
		currentListSQL: query.New(fmt.Sprintf(currentListFmt,
			generic.CurrentRevSQL, generic.CompactRevSQL, generic.Columns, query.LimitToken), "$", true, "ListCurrent"),
		currentListValSQL: query.New(fmt.Sprintf(currentListFmt,
			generic.CurrentRevSQL, generic.CompactRevSQL, generic.WithVal, query.LimitToken), "$", true, "ListCurrentVal"),
		currentCountSQL: query.New(fmt.Sprintf(currentCountFmt, generic.CurrentRevSQL), "$", true, "CountCurrent"),
		currentCountAllSQL: query.New(fmt.Sprintf(currentCountAllFmt, generic.CurrentRevSQL), "$", true, "CountCurrentAll"),
	}
}

// currentStateReads reports whether current-state reads are served from the
// kine_current mirror table.
func currentStateReads() bool {
	v := os.Getenv("KINE_PGSQL_CURRENT_STATE")
	return v == "true" || v == "1" || v == "TRUE"
}

// currentStateTeardown reports whether the mirror trigger should be removed
// when current-state reads are disabled (OPTIMIZATION_ROUND_9.md D4).
func currentStateTeardown() bool {
	v := os.Getenv("KINE_PGSQL_CURRENT_STATE_TEARDOWN")
	return v == "true" || v == "1" || v == "TRUE"
}

// triggerInstalled reports whether the kine_current_maintain trigger exists.
func triggerInstalled(db *sql.DB) bool {
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'kine_current_maintain' AND NOT tgisinternal`).Scan(&n); err != nil {
		return false
	}
	return n > 0
}

// setupCurrentState creates the mirror schema and verifies that kine_current
// agrees with the history log before any current read may use it
// (OPTIMIZATION_ROUND_8.md B4). It runs at startup, before kine admits
// traffic, under the single-instance deployment scope: no other kine or
// external writer may be active while the rebuild executes.
func setupCurrentState(db *sql.DB) error {
	for _, stmt := range currentStateSchema {
		logrus.Tracef("SETUP EXEC CURRENT STATE: %v", query.Strip(stmt))
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("current-state schema setup: %w", err)
		}
	}

	var (
		maxHist sql.NullInt64
		maxCur  sql.NullInt64
	)
	if err := db.QueryRow(`SELECT (SELECT MAX(id) FROM kine), (SELECT MAX(id) FROM kine_current)`).Scan(&maxHist, &maxCur); err != nil {
		return fmt.Errorf("current-state bootstrap check: %w", err)
	}

	// max(id) equality between log and mirror is the primary invariant:
	// every history insert mirrors itself, including tombstones.
	needsRebuild := false
	if maxHist.Valid && maxHist.Int64 > 0 {
		needsRebuild = !maxCur.Valid || maxCur.Int64 != maxHist.Int64
	} else if maxCur.Valid && maxCur.Int64 > 0 {
		needsRebuild = true // empty log with a non-empty mirror
	}

	if !needsRebuild && maxHist.Valid && maxHist.Int64 > 0 {
		// max(id) equality is necessary but not sufficient: require exactly
		// one live mirror row per distinct live history key (catches
		// intermediate drift from, e.g., a writer that ran without the
		// trigger). Tombstones are compared on the live side only: the
		// PostgreSQL compaction statement deletes every tombstone in its
		// window from the log, even when it is a key's latest revision, so
		// after compaction the mirror legitimately holds tombstones that the
		// log no longer contains (OPTIMIZATION_ROUND_9.md D1). Requiring
		// equal total counts here would classify those retained tombstones
		// as corruption and trigger a destructive log-only rebuild that
		// loses them.
		var curLive, histLive int64
		if err := db.QueryRow(`SELECT
			(SELECT COUNT(*) FROM kine_current WHERE deleted = 0),
			(SELECT COUNT(*) FROM (SELECT DISTINCT ON (name) name, deleted FROM kine ORDER BY name, id DESC) latest WHERE deleted = 0)`).Scan(&curLive, &histLive); err != nil {
			return fmt.Errorf("current-state bootstrap check: %w", err)
		}
		needsRebuild = curLive != histLive
	}

	if !needsRebuild {
		logrus.Infof("kine_current mirror verified against history log (max revision %d)", maxHist.Int64)
		return nil
	}

	logrus.Infof("kine_current mirror is out of sync with the history log (log max %s, mirror max %s); rebuilding before traffic is admitted",
		nullInt64Str(maxHist), nullInt64Str(maxCur))
	xTx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer xTx.Rollback()
	if _, err := xTx.Exec(`DELETE FROM kine_current`); err != nil {
		return err
	}
	if _, err := xTx.Exec(`INSERT INTO kine_current (id, name, created, deleted, create_revision, prev_revision, lease, value)
		SELECT DISTINCT ON (name) id, name, created, deleted, create_revision, prev_revision, lease, value
		FROM kine ORDER BY name ASC, id DESC`); err != nil {
		return err
	}
	var n int64
	if err := xTx.QueryRow(`SELECT COUNT(*) FROM kine_current`).Scan(&n); err != nil {
		return err
	}
	if err := xTx.Commit(); err != nil {
		return err
	}
	logrus.Infof("kine_current rebuilt from history: %d rows", n)
	return nil
}

func nullInt64Str(v sql.NullInt64) string {
	if !v.Valid {
		return "NULL"
	}
	return strconv.FormatInt(v.Int64, 10)
}

func (d *currentStateDialect) CountCurrent(ctx context.Context, key, end string) (int64, int64, error) {
	var (
		rev   sql.NullInt64
		count int64
	)
	// All-key requests normalize to key="", end=""; the bounded count query
	// would return 0 for that range (OPTIMIZATION_ROUND_9.md D2).
	if key == "" && end == "" {
		metrics.CurrentStateReads.WithLabelValues("count_all").Inc()
		if err := d.Generic.QueryRowNamed(ctx, d.currentCountAllSQL).Scan(&rev, &count); err != nil {
			return 0, 0, err
		}
		return rev.Int64, count, nil
	}
	metrics.CurrentStateReads.WithLabelValues("count").Inc()
	if err := d.Generic.QueryRowNamed(ctx, d.currentCountSQL, key, end).Scan(&rev, &count); err != nil {
		return 0, 0, err
	}
	return rev.Int64, count, nil
}

func (d *currentStateDialect) ListCurrent(ctx context.Context, key, end string, limit int64, includeDeleted, keysOnly bool) (server.RowSet, error) {
	// Tombstone-inclusive single-key reads are the internal existence checks
	// in Create/Delete (logstructured.get with revision=0, end="", limit=1).
	// The mirror is authoritative for them: it retains tombstones that the
	// PostgreSQL compaction statement removes from the log even when they are
	// a key's latest revision, so the mirror answer matches uncompacted (and
	// etcd) semantics at every compaction state. Tombstone-inclusive RANGE
	// reads (end != "") have no internal caller at current state; keep them
	// on the history log.
	if includeDeleted {
		if end != "" {
			metrics.CurrentStateReads.WithLabelValues("history_fallback_range").Inc()
			return d.Generic.ListCurrent(ctx, key, end, limit, includeDeleted, keysOnly)
		}
		if keysOnly {
			metrics.CurrentStateReads.WithLabelValues("get_del_keys_only").Inc()
			return d.Generic.QueryNamed(ctx, d.currentGetDelSQL, key)
		}
		metrics.CurrentStateReads.WithLabelValues("get_del_value").Inc()
		return d.Generic.QueryNamed(ctx, d.currentGetDelValSQL, key)
	}

	var (
		variant string
		sqlq    *query.Named
		args    []any
	)
	switch {
	case key == "":
		if keysOnly {
			variant, sqlq = "all_keys_only", d.currentAllSQL
		} else {
			variant, sqlq = "all_value", d.currentAllValSQL
		}
	case end == "":
		if keysOnly {
			variant, sqlq = "get_keys_only", d.currentGetSQL
		} else {
			variant, sqlq = "get_value", d.currentGetValSQL
		}
		args = append(args, key)
	default:
		if keysOnly {
			variant, sqlq = "list_keys_only", d.currentListSQL
		} else {
			variant, sqlq = "list_value", d.currentListValSQL
		}
		sqlq = sqlq.WithLimit(limit)
		args = append(args, key, end)
	}
	metrics.CurrentStateReads.WithLabelValues(variant).Inc()
	return d.Generic.QueryNamed(ctx, sqlq, args...)
}

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	config, err := prepareConfig(cfg.DataSourceName, cfg.BackendTLSConfig)
	if err != nil {
		return false, nil, err
	}

	connector := stdlib.GetConnector(*config)
	if err := createDBIfNotExist(ctx, config, connector); err != nil {
		return false, nil, err
	}

	dialect, err := generic.OpenDB(ctx, wg, "pgx", connector, cfg.ConnectionPoolConfig, "$", true, cfg.MetricsRegisterer)
	if err != nil {
		return false, nil, err
	}

	if nativeReads() {
		pool, err := openPgxPool(ctx, config, cfg.ConnectionPoolConfig)
		if err != nil {
			return false, nil, err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			pool.Close()
		}()
		dialect.PgxPool = pool
		logrus.Info("pgx-native read path enabled (KINE_PGSQL_NATIVE_READS)")
	}

	dialect.GetSizeSQL = query.New(`SELECT pg_total_relation_size('kine')`, "$", true, "GetSize")
	dialect.CompactSQL = query.New(`
		DELETE FROM kine AS kv
		USING	(
			SELECT kp.prev_revision AS id
			FROM kine AS kp
			WHERE
				kp.name != 'compact_rev_key' AND
				kp.prev_revision != 0 AND
				kp.id > $1 AND
				kp.id <= $2
			UNION ALL
			SELECT kd.id AS id
			FROM kine AS kd
			WHERE
				kd.deleted != 0 AND
				kd.id > $3 AND
				kd.id <= $4
		) AS ks
		WHERE kv.id = ks.id`, "$", true, "Compact")
	// Push the row limit into the key selection; see listFmt.
	dialect.ListCurrentSQL = query.New(fmt.Sprintf(listSQL, ""), "$", true, "ListCurrent")
	dialect.ListCurrentValSQL = query.New(fmt.Sprintf(listValSQL, ""), "$", true, "ListCurrentVal")
	dialect.GetRevisionAfterSQL = query.New(fmt.Sprintf(listSQL, "AND id <= ?"), "$", true, "GetRevisionAfter")
	dialect.GetRevisionAfterValSQL = query.New(fmt.Sprintf(listValSQL, "AND id <= ?"), "$", true, "GetRevisionAfterVal")
	dialect.FillRetryDuration = time.Millisecond + 5
	dialect.InsertRetry = func(err error) bool {
		if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UniqueViolation && err.ConstraintName == "kine_pkey" {
			return true
		}
		return false
	}
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(*pgconn.PgError); ok && err.Code == pgerrcode.UniqueViolation {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(*pgconn.PgError); ok {
			return err.Code
		}
		return err.Error()
	}
	dialect.TranslateStartKeyFunc = func(startKey string) string {
		// replace trailing null with 0x1a (substitute) as postgres does not allow null byte in UTF8 strings
		if s, ok := strings.CutSuffix(startKey, "\x00"); ok {
			return s + "\x1a"
		}
		return startKey
	}
	if err := setup(dialect.DB); err != nil {
		return false, nil, err
	}

	backend := server.Dialect(dialect)
	if currentStateReads() {
		if err := setupCurrentState(dialect.DB); err != nil {
			return false, nil, err
		}
		backend = newCurrentStateDialect(dialect)
		logrus.Info("current-state table reads enabled (KINE_PGSQL_CURRENT_STATE): current GET/LIST/COUNT are served from kine_current; includeDeleted and revision-bounded reads remain on the history log")
	} else if currentStateTeardown() && triggerInstalled(dialect.DB) {
		// OPTIMIZATION_ROUND_9.md D4: with mirror reads disabled, an operator
		// can request removal of the maintenance trigger so writes no longer
		// pay for it. The mirror table itself is kept; re-enabling the flag
		// re-creates the trigger and the startup check rebuilds the mirror
		// from the log (which is authoritative) before any read uses it.
		if _, err := dialect.DB.Exec(`DROP TRIGGER IF EXISTS kine_current_maintain ON kine`); err != nil {
			return false, nil, fmt.Errorf("current-state trigger teardown: %w", err)
		}
		logrus.Info("current-state trigger removed (KINE_PGSQL_CURRENT_STATE_TEARDOWN); the kine_current table is retained and will be rebuilt if the flag is re-enabled")
	}

	dialect.Migrate(context.Background())
	return true, logstructured.New(sqllog.New(backend, cfg.CompactInterval, cfg.CompactIntervalJitter, cfg.CompactTimeout, cfg.CompactMinRetain, cfg.CompactBatchSize, cfg.PollBatchSize)), nil
}

// nativeReads reports whether the pgx-native read path is enabled.
func nativeReads() bool {
	v := os.Getenv("KINE_PGSQL_NATIVE_READS")
	return v == "true" || v == "1" || v == "TRUE"
}

// openPgxPool creates the pgx-native connection pool used for read
// queries. Pool sizing: when the operator sets an explicit MaxOpen cap, both
// pools derive from it so the combined client-connection count stays within
// the server's max_connections budget (OPTIMIZATION_ROUND_9.md §3.2: with
// MaxIdle=64 and MaxOpen unset, stdlib (64) plus native (64) could reach 128
// against max_connections=100). MinConns is half of MaxConns, keeping warm
// connections on hand for read bursts (writes always use the stdlib pool,
// which keeps up to MaxIdle connections).
func openPgxPool(ctx context.Context, config *pgx.ConnConfig, connPoolConfig generic.ConnectionPoolConfig) (*pgxpool.Pool, error) {
	maxIdle := connPoolConfig.MaxIdle
	if maxIdle == 0 {
		maxIdle = 20
	} else if maxIdle < 0 {
		maxIdle = 0
	}

	poolCfg, err := pgxpool.ParseConfig(config.ConnString())
	if err != nil {
		return nil, err
	}
	// ConnString round-trips TLS but not the custom dialer
	poolCfg.ConnConfig.DialFunc = config.DialFunc
	poolCfg.MaxConns = int32(maxIdle)
	if connPoolConfig.MaxOpen > 0 {
		poolCfg.MaxConns = int32(connPoolConfig.MaxOpen)
	}
	poolCfg.MinConns = int32(poolCfg.MaxConns / 2)
	poolCfg.MaxConnLifetime = connPoolConfig.MaxLifetime
	poolCfg.MaxConnIdleTime = connPoolConfig.MaxIdleTime

	// OPTIMIZATION_ROUND_6.md R6-2: optional per-session cap on parallel
	// query workers. The R5 history matrix showed the planner flipping the
	// wide ListCurrentVal query to a parallel Gather Merge at low history
	// depth (EXPLAIN: 2 workers, external-merge sort spill of 5.4 MB, and
	// the slowest single-shot execution of the three depths) while the
	// non-parallel nested-loop plan served the same shape at depth >= 4.
	// Parallel workers are spawned per query, so under high client
	// concurrency the spawn/context-switch cost multiplies. Default is
	// unset (PostgreSQL's own default), so behavior is unchanged unless
	// the operator opts in.
	if v := os.Getenv("KINE_PGSQL_MAX_PARALLEL_WORKERS_PER_GATHER"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("KINE_PGSQL_MAX_PARALLEL_WORKERS_PER_GATHER must be a non-negative integer, got %q", v)
		}
		poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, "SET max_parallel_workers_per_gather = "+strconv.Itoa(n))
			return err
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}

	// warm the pool up front so the first read bursts don't pay for
	// connection establishment (min connections are otherwise established
	// lazily)
	for i := 0; i < int(poolCfg.MinConns); i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			pool.Close()
			return nil, err
		}
		conn.Release()
	}

	return pool, nil
}

func setup(db *sql.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")
	var version string
	collationSupported := true
	if err := db.QueryRow("select version()").Scan(&version); err == nil && strings.Contains(strings.ToLower(version), "cockroachdb") {
		// CockroadDB does not seem to support "C" as a collation
		// It looks like it's using golang.org/x/text/language and ends up calling something like v, err := language.Parse("C")
		// which parses it as a BCP47 language tag instead of a collation.
		collationSupported = false
	}

	for _, stmt := range schema {
		if !collationSupported {
			stmt = strings.ReplaceAll(stmt, ` COLLATE "C"`, "")
		}
		logrus.Tracef("SETUP EXEC : %v", query.Strip(stmt))
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	// Run enabled schama migrations.
	// Note that the schema created by the `schema` var is always the latest revision;
	// migrations should handle deltas between prior schema versions.
	schemaVersion, _ := strconv.ParseUint(os.Getenv("KINE_SCHEMA_MIGRATION"), 10, 64)
	for i, stmt := range schemaMigrations {
		if i >= int(schemaVersion) {
			break
		}
		if !collationSupported {
			stmt = strings.ReplaceAll(stmt, ` COLLATE "C"`, "")
		}
		if stmt == "" {
			continue
		}
		logrus.Tracef("SETUP EXEC MIGRATION %d: %v", i, query.Strip(stmt))
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

func createDBIfNotExist(ctx context.Context, config *pgx.ConnConfig, connector driver.Connector) error {
	createConfig := config.Copy()
	createConfig.Database = "postgres"
	connector = stdlib.GetConnector(*createConfig)
	conn, err := connector.Connect(ctx)
	if err != nil {
		logrus.Warnf("failed to ensure existence of database %s: unable to connect to default postgres database: %v", createConfig.Database, err)
		return nil
	}
	conn.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	var exists bool
	err = db.QueryRow("SELECT 1 FROM pg_database WHERE datname = $1", config.Database).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.Warnf("failed to check existence of database %s, going to attempt create: %v", config.Database, err)
	}

	if !exists {
		stmt := fmt.Sprintf(createDB, config.Database)
		logrus.Tracef("SETUP EXEC : %v", query.Strip(stmt))
		if _, err = db.Exec(stmt); err != nil {
			logrus.Warnf("failed to create database %s: %v", config.Database, err)
		} else {
			logrus.Tracef("created database: %s", config.Database)
		}
	}
	return nil
}

func prepareConfig(dataSourceName string, tlsInfo tls.Config) (*pgx.ConnConfig, error) {
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	} else {
		dataSourceName = "postgres://" + dataSourceName
	}
	u, err := util.ParseURL(dataSourceName)
	if err != nil {
		return nil, err
	}
	if len(u.Path) == 0 || u.Path == "/" {
		u.Path = "/kubernetes"
	}

	// makes quoting database and schema reference the same unquoted identifier
	// See: https://www.postgresql.org/docs/12/sql-syntax-lexical.html#:~:text=unquoted%20names%20are%20always%20folded%20to%20lower%20case
	u.Path = strings.ToLower(u.Path)

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, err
	}
	// set up tls dsn
	params := url.Values{}
	sslmode := ""
	if _, ok := queryMap["sslcert"]; tlsInfo.CertFile != "" && !ok {
		params.Add("sslcert", tlsInfo.CertFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslkey"]; tlsInfo.KeyFile != "" && !ok {
		params.Add("sslkey", tlsInfo.KeyFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslrootcert"]; tlsInfo.CAFile != "" && !ok {
		params.Add("sslrootcert", tlsInfo.CAFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslmode"]; !ok && sslmode != "" {
		params.Add("sslmode", sslmode)
	}
	for k, v := range queryMap {
		params.Add(k, v[0])
	}
	u.RawQuery = params.Encode()
	config, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, err
	}
	config.DialFunc = dialer.CachingDialer.DialContext
	return config, nil
}

func init() {
	drivers.Register("postgres", New)
	drivers.Register("postgresql", New)
}
