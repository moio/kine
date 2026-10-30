package generic

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/query"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/sirupsen/logrus"
)

const (
	defaultMaxIdleConns = 20 // database/sql default is 2, but profiling suggests that kine needs ~10-15 connections available under moderate load
)

// explicit interface check
var _ server.Dialect = (*Generic)(nil)

var (
	Columns    = "id, name, created, deleted, create_revision, prev_revision, lease"
	WithVal    = Columns + ", value"
	WithOldVal = WithVal + ", old_value"
	ListFmt    = `
		SELECT current_rev, compact_rev, %s
		FROM (%s) AS current_rev, (%s) AS compact_rev, kine
		INNER JOIN (%s) AS mkv USING (id)
		WHERE (deleted = 0 OR ?)
		ORDER BY name ASC
		`
	CurrentRevSQL  = `SELECT MAX(id) AS current_rev FROM kine`
	CompactRevSQL  = `SELECT MAX(prev_revision) AS compact_rev FROM kine WHERE name = 'compact_rev_key'`
	BetweenNameSQL = `SELECT MAX(id) AS id FROM kine WHERE name >= ? AND name < ? %s GROUP BY name`
	EqualsNameSQL  = `SELECT MAX(id) AS id FROM kine WHERE name = ? %s`
	AnyNameSQL     = `SELECT MAX(id) AS id FROM kine %s GROUP BY name`

	allSQL     = fmt.Sprintf(ListFmt, Columns, CurrentRevSQL, CompactRevSQL, AnyNameSQL)
	allValSQL  = fmt.Sprintf(ListFmt, WithVal, CurrentRevSQL, CompactRevSQL, AnyNameSQL)
	listSQL    = fmt.Sprintf(ListFmt, Columns, CurrentRevSQL, CompactRevSQL, BetweenNameSQL)
	listValSQL = fmt.Sprintf(ListFmt, WithVal, CurrentRevSQL, CompactRevSQL, BetweenNameSQL)
	getSQL     = fmt.Sprintf(ListFmt, Columns, CurrentRevSQL, CompactRevSQL, EqualsNameSQL)
	getValSQL  = fmt.Sprintf(ListFmt, WithVal, CurrentRevSQL, CompactRevSQL, EqualsNameSQL)
)

type (
	ErrRetry       func(error) bool
	TranslateErr   func(error) error
	ErrCode        func(error) string
	SubstituteFunc func(string) string
)

type ConnectionPoolConfig struct {
	MaxIdle     int           // zero means defaultMaxIdleConns; negative means 0
	MaxOpen     int           // <= 0 means unlimited
	MaxLifetime time.Duration // maximum amount of time a connection may be reused
	MaxIdleTime time.Duration // maximum amount of time a connection may remain idle
}

type Generic struct {
	sync.Mutex

	LockWrites              bool
	LastInsertID            bool
	DB                      *sql.DB
	GetSingleSQL            *query.Named
	GetSingleValSQL         *query.Named
	AllCurrentSQL           *query.Named
	AllCurrentValSQL        *query.Named
	ListCurrentSQL          *query.Named
	ListCurrentValSQL       *query.Named
	ListRevisionStartSQL    *query.Named
	ListRevisionStartValSQL *query.Named
	GetRevisionAfterSQL     *query.Named
	GetRevisionAfterValSQL  *query.Named
	CountAllCurrentSQL      *query.Named
	CountCurrentSQL         *query.Named
	CountAllRevisionSQL     *query.Named
	CountRevisionSQL        *query.Named
	AfterOldValSQL          *query.Named
	AfterAllOldValSQL       *query.Named
	AfterSingleOldValSQL    *query.Named
	CurrentRevSQL           *query.Named
	CompactRevSQL           *query.Named
	DeleteSQL               *query.Named
	CompactSQL              *query.Named
	UpdateCompactSQL        *query.Named
	PostCompactSQL          *query.Named
	InsertSQL               *query.Named
	FillSQL                 *query.Named
	InsertLastInsertIDSQL   *query.Named
	GetSizeSQL              *query.Named
	Retry                   ErrRetry
	InsertRetry             ErrRetry
	TranslateErr            TranslateErr
	TranslateStartKeyFunc   SubstituteFunc
	ErrCode                 ErrCode
	FillRetryDuration       time.Duration

	// --- cached revision substitution (OPTIMIZATION_ROUND_1.md T3) ---
	// Every read query embeds two scalar subqueries: the current revision
	// (MAX(id), an index-only backward scan) and the compaction revision
	// (MAX(prev_revision) WHERE name='compact_rev_key', a second index-only
	// scan). Both are recomputed server-side for every single query. When
	// enabled via KINE_T3_COMPACT_REV_REFRESH / KINE_T3_CURRENT_REV_REFRESH,
	// these subquery texts are replaced with cached literals before execution,
	// removing two index scans plus plan complexity from every query.
	//
	// Staleness: the compact revision moves only on compaction (bounded by
	// the refresh interval, also refreshed eagerly after UpdateCompact).
	// Binding the CURRENT revision (KINE_T3_CURRENT_REV_REFRESH) bounds
	// response-header freshness by the refresh interval; today each query's
	// MAX(id) subquery makes header and data a single consistent snapshot.
	// Single-writer instances are exact (refreshed eagerly on Insert). For
	// multi-writer deployments the current-rev cache is NOT safe by default —
	// it is off unless explicitly enabled. Only use it where the added
	// latency is acceptable.
	compactRevCache  atomic.Pointer[string] // nil = not loaded; else literal SQL text ("NULL" or int)
	currentRevCache  atomic.Pointer[string]
	revCacheOnce     sync.Once
	revCacheDBLoaded atomic.Bool
	// currentRevRefreshOn mirrors KINE_T3_CURRENT_REV_REFRESH>0; read in the
	// Insert hot path of execute() (see startRevCacheRefresher)
	currentRevRefreshOn atomic.Bool
}

func (d *Generic) Migrate(ctx context.Context) {
	var (
		count     = 0
		countKV   = d.queryRow(ctx, query.New("SELECT COUNT(*) FROM key_value", "?", false, ""))
		countKine = d.queryRow(ctx, query.New("SELECT COUNT(*) FROM kine", "?", false, ""))
	)

	if err := countKV.Scan(&count); err != nil || count == 0 {
		return
	}

	if err := countKine.Scan(&count); err != nil || count != 0 {
		return
	}

	logrus.Infof("Migrating content from old table")
	_, err := d.execute(ctx,
		query.New(`INSERT INTO kine(deleted, create_revision, prev_revision, name, value, created, lease)
					SELECT 0, 0, 0, kv.name, kv.value, 1, CASE WHEN kv.ttl > 0 THEN 15 ELSE 0 END
					FROM key_value kv
						WHERE kv.id IN (SELECT MAX(kvd.id) FROM key_value kvd GROUP BY kvd.name)`, "?", false, ""))
	if err != nil {
		logrus.Errorf("Migration failed: %v", err)
	}
}

func configureConnectionPooling(connPoolConfig ConnectionPoolConfig, db *sql.DB, driverName string) {
	// behavior copied from database/sql - zero means defaultMaxIdleConns; negative means 0
	if connPoolConfig.MaxIdle < 0 {
		connPoolConfig.MaxIdle = 0
	} else if connPoolConfig.MaxIdle == 0 {
		connPoolConfig.MaxIdle = defaultMaxIdleConns
	}

	logrus.Infof(
		"Configuring %s database connection pooling: maxIdle=%d, maxOpen=%d, maxLifetime=%s, maxIdleTime=%s",
		driverName, connPoolConfig.MaxIdle, connPoolConfig.MaxOpen, connPoolConfig.MaxLifetime, connPoolConfig.MaxIdleTime)
	db.SetMaxIdleConns(connPoolConfig.MaxIdle)
	db.SetMaxOpenConns(connPoolConfig.MaxOpen)
	db.SetConnMaxLifetime(connPoolConfig.MaxLifetime)
	db.SetConnMaxIdleTime(connPoolConfig.MaxIdleTime)
}

func openAndTest(connector driver.Connector) (*sql.DB, error) {
	db := sql.OpenDB(connector)

	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func OpenDB(ctx context.Context, wg *sync.WaitGroup, driverName string, connector driver.Connector, connPoolConfig ConnectionPoolConfig, paramCharacter string, numbered bool, metricsRegisterer prometheus.Registerer) (*Generic, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(connector)
		if err == nil {
			break
		}

		logrus.Errorf("Failed to ping database connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		logrus.Infof("Closing database connections...")
		if err := db.Close(); err != nil {
			logrus.Errorf("Failed to close database: %v", err)
		}
	}()

	configureConnectionPooling(connPoolConfig, db, driverName)

	if metricsRegisterer != nil {
		metricsRegisterer.MustRegister(collectors.NewDBStatsCollector(db, "kine"))
	}

	return &Generic{
		DB: db,

		AllCurrentSQL:           query.New(fmt.Sprintf(allSQL, ""), paramCharacter, numbered, "AllCurrent"),
		AllCurrentValSQL:        query.New(fmt.Sprintf(allValSQL, ""), paramCharacter, numbered, "AllCurrentVal"),
		ListCurrentSQL:          query.New(fmt.Sprintf(listSQL, ""), paramCharacter, numbered, "ListCurrent"),
		ListCurrentValSQL:       query.New(fmt.Sprintf(listValSQL, ""), paramCharacter, numbered, "ListCurrentVal"),
		ListRevisionStartSQL:    query.New(fmt.Sprintf(getSQL, "AND id <= ?"), paramCharacter, numbered, "ListRevisionStart"),
		ListRevisionStartValSQL: query.New(fmt.Sprintf(getValSQL, "AND id <= ?"), paramCharacter, numbered, "ListRevisionStartVal"),
		GetRevisionAfterSQL:     query.New(fmt.Sprintf(listSQL, "AND id <= ?"), paramCharacter, numbered, "GetRevisionAfter"),
		GetRevisionAfterValSQL:  query.New(fmt.Sprintf(listValSQL, "AND id <= ?"), paramCharacter, numbered, "GetRevisionAfterVal"),
		CurrentRevSQL:           query.New(CurrentRevSQL, paramCharacter, numbered, "CurrentRev"),
		CompactRevSQL:           query.New(CompactRevSQL, paramCharacter, numbered, "CompactRev"),

		GetSingleSQL: query.New(fmt.Sprintf(`
			SELECT current_rev, compact_rev, %s
			FROM (%s) AS current_rev, (%s) AS compact_rev, kine
			WHERE id = (%s)
			AND (deleted = 0 OR ?)`,
			Columns, CurrentRevSQL, CompactRevSQL, fmt.Sprintf(EqualsNameSQL, "")), paramCharacter, numbered, "GetSingle"),

		GetSingleValSQL: query.New(fmt.Sprintf(`
			SELECT current_rev, compact_rev, %s
			FROM (%s) AS current_rev, (%s) AS compact_rev, kine
			WHERE id = (%s)
			AND (deleted = 0 OR ?)`,
			WithVal, CurrentRevSQL, CompactRevSQL, fmt.Sprintf(EqualsNameSQL, "")), paramCharacter, numbered, "GetSingleVal"),

		CountAllCurrentSQL: query.New(fmt.Sprintf(`
			SELECT (%s), COUNT(id)
			FROM kine
			INNER JOIN (%s) AS mkv USING (id)
			WHERE (deleted = 0 OR ?)`,
			CurrentRevSQL, fmt.Sprintf(AnyNameSQL, "")), paramCharacter, numbered, "CountAllCurrent"),

		CountCurrentSQL: query.New(fmt.Sprintf(`
			SELECT (%s), COUNT(id)
			FROM kine
			INNER JOIN (%s) AS mkv USING (id)
			WHERE (deleted = 0 OR ?)`,
			CurrentRevSQL, fmt.Sprintf(BetweenNameSQL, "")), paramCharacter, numbered, "CountCurrent"),

		CountAllRevisionSQL: query.New(fmt.Sprintf(`
			SELECT (%s), (%s), COUNT(id)
			FROM kine
			INNER JOIN (%s) AS mkv USING (id)
			WHERE (deleted = 0 OR ?)`,
			CurrentRevSQL, CompactRevSQL, fmt.Sprintf(AnyNameSQL, "WHERE id <= ?")), paramCharacter, numbered, "CountAllRevision"),

		CountRevisionSQL: query.New(fmt.Sprintf(`
			SELECT (%s), (%s), COUNT(id)
			FROM kine
			INNER JOIN (%s) AS mkv USING (id)
			WHERE (deleted = 0 OR ?)`,
			CurrentRevSQL, CompactRevSQL, fmt.Sprintf(BetweenNameSQL, "AND id <= ?")), paramCharacter, numbered, "CountRevision"),

		AfterOldValSQL: query.New(fmt.Sprintf(`
			SELECT current_rev, compact_rev, %s
			FROM (%s) AS current_rev, (%s) AS compact_rev, kine
			WHERE	name >= ? AND name < ?
			AND	id > ?
			ORDER BY id ASC`,
			WithOldVal, CurrentRevSQL, CompactRevSQL), paramCharacter, numbered, "AfterOldVal"),

		AfterAllOldValSQL: query.New(fmt.Sprintf(`
			SELECT current_rev, compact_rev, %s
			FROM (%s) AS current_rev, (%s) AS compact_rev, kine
			WHERE id > ?
			ORDER BY id ASC`,
			WithOldVal, CurrentRevSQL, CompactRevSQL), paramCharacter, numbered, "AfterAllOldVal"),

		AfterSingleOldValSQL: query.New(fmt.Sprintf(`
			SELECT current_rev, compact_rev, %s
			FROM (%s) AS current_rev, (%s) AS compact_rev, kine
			WHERE name = ?
			AND id > ?
			ORDER BY id ASC`,
			WithOldVal, CurrentRevSQL, CompactRevSQL), paramCharacter, numbered, "AfterSingleOldVal"),

		DeleteSQL: query.New(`
			DELETE FROM kine
			WHERE id = ?`,
			paramCharacter, numbered, "Delete"),

		UpdateCompactSQL: query.New(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`,
			paramCharacter, numbered, "UpdateCompact"),

		InsertLastInsertIDSQL: query.New(`
			INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			SELECT ?, ?, ?, ?, ?, ?, ?, (SELECT value FROM kine WHERE id = ?) AS old_value`,
			paramCharacter, numbered, "InsertLastInsertID"),

		InsertSQL: query.New(`
			INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			SELECT ?, ?, ?, ?, ?, ?, ?, (SELECT value FROM kine WHERE id = ?) AS old_value RETURNING id`,
			paramCharacter, numbered, "Insert"),

		FillSQL: query.New(`
			INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			paramCharacter, numbered, "Fill"),
	}, err
}

func (d *Generic) query(ctx context.Context, sql *query.Named, args ...any) (result *sql.Rows, err error) {
	d.startRevCacheRefresher()
	q := d.substituteCachedRevs(sql.Name, sql.Query)
	query := sql.Fill(args)
	logrus.Tracef("QUERY: %s", query)
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(err), 0, query)
	}()
	return d.DB.QueryContext(ctx, q, args...)
}

func (d *Generic) queryRow(ctx context.Context, sql *query.Named, args ...any) (result *sql.Row) {
	d.startRevCacheRefresher()
	q := d.substituteCachedRevs(sql.Name, sql.Query)
	query := sql.Fill(args)
	logrus.Tracef("QUERY ROW: %s", query)
	startTime := time.Now()
	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(result.Err()), 0, query)
	}()
	return d.DB.QueryRowContext(ctx, q, args...)
}

func (d *Generic) execute(ctx context.Context, sql *query.Named, args ...any) (result sql.Result, err error) {
	retries := 0
	startTime := time.Now()
	queryText := d.substituteCachedRevs(sql.Name, sql.Query)
	query := sql.Fill(args)
	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	defer func() {
		metrics.ObserveSQL(startTime, d.ErrCode(err), retries, query)
	}()

	wait := strategy.Backoff(backoff.Linear(100 * time.Millisecond))
	for ; retries < 20; retries++ {
		logrus.Tracef("EXEC (try=%d): %s", retries, query)
		result, err = d.DB.ExecContext(ctx, queryText, args...)
		if err != nil && d.Retry != nil && d.Retry(err) {
			wait(uint(retries))
			continue
		}
		if err == nil && sql.Name == "UpdateCompact" {
			// the compaction revision just changed; refresh the cache eagerly
			// instead of waiting out the refresh interval
			go d.refreshCompactRev()
		}
		if err == nil && d.currentRevRefreshOn.Load() &&
			(sql.Name == "Insert" || sql.Name == "InsertLastInsertID") {
			// keep the cached current revision exact for single-writer
			// instances: the next request's header/compare must see this row
			d.refreshCurrentRev()
		}
		return result, err
	}
	return result, err
}

// --- cached revision substitution (OPTIMIZATION_ROUND_1.md T3) ---

// The exact subquery texts as they appear in executed queries — WITHOUT the
// wrapping parentheses, which the templates own. Replacing the inner text
// keeps the query shape identical: (...subquery...) -> (literal). The
// templates interpolate CurrentRevSQL/CompactRevSQL verbatim and query.New
// normalizes whitespace runs to single spaces, so these strings are stable.
const (
	currentRevSubSQL = "SELECT MAX(id) AS current_rev FROM kine"
	compactRevSubSQL = "SELECT MAX(prev_revision) AS compact_rev FROM kine WHERE name = 'compact_rev_key'"
)

// envRefreshInterval reads an interval from env; 0 (or invalid) disables.
func envRefreshInterval(env string) time.Duration {
	v := os.Getenv(env)
	if v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		logrus.Warnf("invalid %s=%q, cached revision substitution disabled", env, v)
		return 0
	}
	return d
}

// substituteCachedRevs replaces the current/compact revision subqueries with
// cached constant subqueries. Values not yet loaded (or feature disabled)
// leave the query untouched. The inner text is replaced with "SELECT <lit>
// AS ...", which is valid in every position the subquery appears: a FROM
// item (requires a subselect — a bare literal is a syntax error) and a
// scalar select-list item. The constant subquery is inlined by the planner.
//
// Substitution is restricted to the rev==0 "current state" read queries.
// Revision-bounded queries (GetRevisionAfter*, After*, Count*) MUST keep the
// live subquery: their ErrFutureRev/ErrCompacted checks compare a client- or
// internally-supplied revision against batch.CurrentRev, and a cached value
// lagging concurrent writes turns every update-of-existing-key into a
// spurious "required revision is a future revision" (measured 2026-10-29:
// put/c8 99.6k/100k requests errored before this restriction).
var revCacheSubstitutable = map[string]bool{
	"GetSingle":      true,
	"GetSingleVal":   true,
	"ListCurrent":    true,
	"ListCurrentVal": true,
	"AllCurrent":     true,
	"AllCurrentVal":  true,
}

func (d *Generic) substituteCachedRevs(name, q string) string {
	if !revCacheSubstitutable[name] {
		return q
	}
	if v := d.compactRevCache.Load(); v != nil {
		q = strings.ReplaceAll(q, compactRevSubSQL, "SELECT "+*v+" AS compact_rev")
	}
	if v := d.currentRevCache.Load(); v != nil {
		q = strings.ReplaceAll(q, currentRevSubSQL, "SELECT "+*v+" AS current_rev")
	}
	return q
}

// startRevCacheRefresher lazily starts the background refreshers, enabled via
// env: KINE_T3_COMPACT_REV_REFRESH (e.g. 30s) and KINE_T3_CURRENT_REV_REFRESH
// (e.g. 250ms). Both off by default — upstream behavior unchanged. The
// intervals must be captured at start time for the Insert-hook in execute();
// envRefreshInterval is called here and the enablement stored atomically.
func (d *Generic) startRevCacheRefresher() {
	d.revCacheOnce.Do(func() {
		compactInterval := envRefreshInterval("KINE_T3_COMPACT_REV_REFRESH")
		currentInterval := envRefreshInterval("KINE_T3_CURRENT_REV_REFRESH")
		if compactInterval == 0 && currentInterval == 0 {
			return
		}
		if currentInterval > 0 {
			d.currentRevRefreshOn.Store(true)
		}
		if compactInterval > 0 {
			if err := d.refreshCompactRev(); err == nil {
				go func() {
					for {
						time.Sleep(compactInterval)
						d.refreshCompactRev()
					}
				}()
			}
		}
		if currentInterval > 0 {
			if err := d.refreshCurrentRev(); err == nil {
				go func() {
					for {
						time.Sleep(currentInterval)
						d.refreshCurrentRev()
					}
				}()
			}
		}
		logrus.Infof("cached revision substitution enabled: compactRev refresh=%s, currentRev refresh=%s", compactInterval, currentInterval)
	})
}

// refreshCompactRev loads MAX(prev_revision) for the compact marker. NULL
// (no compaction yet, or the marker row not created yet during schema init)
// is cached as 0 — kine's "nothing compacted" semantic. Caching NULL here
// poisons put-path scans (compact_rev scanned into int64) until the marker
// row appears and the next refresh lands.
func (d *Generic) refreshCompactRev() error {
	var rev sql.NullInt64
	row := d.DB.QueryRowContext(context.Background(), CompactRevSQL)
	if err := row.Scan(&rev); err != nil {
		return err
	}
	lit := "0"
	if rev.Valid {
		lit = strconv.FormatInt(rev.Int64, 10)
	}
	d.compactRevCache.Store(&lit)
	return nil
}

// refreshCurrentRev loads MAX(id). An empty table caches 0.
func (d *Generic) refreshCurrentRev() error {
	var id sql.NullInt64
	row := d.DB.QueryRowContext(context.Background(), CurrentRevSQL)
	if err := row.Scan(&id); err != nil {
		return err
	}
	lit := "0"
	if id.Valid {
		lit = strconv.FormatInt(id.Int64, 10)
	}
	d.currentRevCache.Store(&lit)
	return nil
}

func (d *Generic) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, d.CompactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) SetCompactRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("SETCOMPACTREVISION %v", revision)
	_, err := d.execute(ctx, d.UpdateCompactSQL, revision)
	return err
}

func (d *Generic) Compact(ctx context.Context, from, to int64) (int64, error) {
	logrus.Tracef("COMPACT %v => %v", from, to)
	res, err := d.execute(ctx, d.CompactSQL, from, to, from, to)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (d *Generic) PostCompact(ctx context.Context) error {
	logrus.Trace("POSTCOMPACT")
	if d.PostCompactSQL != nil {
		_, err := d.execute(ctx, d.PostCompactSQL)
		return err
	}
	return nil
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	logrus.Tracef("DELETEREVISION %v", revision)
	_, err := d.execute(ctx, d.DeleteSQL, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, key, end string, limit int64, includeDeleted, keysOnly bool) (*sql.Rows, error) {
	var sql *query.Named
	if key == "" {
		if keysOnly {
			sql = d.AllCurrentSQL
		} else {
			sql = d.AllCurrentValSQL
		}
		return d.query(ctx, sql, includeDeleted)
	} else if end == "" {
		if keysOnly {
			sql = d.GetSingleSQL
		} else {
			sql = d.GetSingleValSQL
		}
		return d.query(ctx, sql, key, includeDeleted)
	}
	if keysOnly {
		sql = d.ListCurrentSQL
	} else {
		sql = d.ListCurrentValSQL
	}
	sql = sql.WithLimit(limit)
	return d.query(ctx, sql, key, end, includeDeleted)
}

func (d *Generic) List(ctx context.Context, key, end string, limit, revision int64, includeDeleted, keysOnly bool) (*sql.Rows, error) {
	var sql *query.Named
	if end == "" {
		if keysOnly {
			sql = d.ListRevisionStartSQL
		} else {
			sql = d.ListRevisionStartValSQL
		}
		sql = sql.WithLimit(limit)
		return d.query(ctx, sql, key, revision, includeDeleted)
	}
	if keysOnly {
		sql = d.GetRevisionAfterSQL
	} else {
		sql = d.GetRevisionAfterValSQL
	}
	sql = sql.WithLimit(limit)
	return d.query(ctx, sql, key, end, revision, includeDeleted)
}

func (d *Generic) CountCurrent(ctx context.Context, key, end string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	var row *sql.Row
	if key != "" {
		row = d.queryRow(ctx, d.CountCurrentSQL, key, end, false)
	} else {
		row = d.queryRow(ctx, d.CountAllCurrentSQL, false)
	}

	err := row.Scan(&rev, &id)
	return rev.Int64, id, err
}

func (d *Generic) Count(ctx context.Context, key, end string, revision int64) (int64, int64, int64, error) {
	var (
		rev     sql.NullInt64
		compact sql.NullInt64
		id      int64
	)

	var row *sql.Row
	if key != "" {
		row = d.queryRow(ctx, d.CountRevisionSQL, key, end, revision, false)
	} else {
		row = d.queryRow(ctx, d.CountAllRevisionSQL, revision, false)
	}
	err := row.Scan(&rev, &compact, &id)
	return rev.Int64, compact.Int64, id, err
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, d.CurrentRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) After(ctx context.Context, key, end string, rev, limit int64) (*sql.Rows, error) {
	var sql *query.Named
	if key == "" {
		sql = d.AfterAllOldValSQL
		if limit > 0 {
			sql = sql.Appendf("LIMIT %d", limit)
		}
		return d.query(ctx, sql, rev)
	}
	if end == "" {
		sql = d.AfterSingleOldValSQL
		if limit > 0 {
			sql = sql.Appendf("LIMIT %d", limit)
		}
		return d.query(ctx, sql, key, rev)
	}
	sql = d.AfterOldValSQL
	if limit > 0 {
		sql = sql.Appendf("LIMIT %d", limit)
	}
	return d.query(ctx, sql, key, end, rev)
}

func (d *Generic) Fill(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.FillSQL, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
	return err
}

func (d *Generic) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}

//nolint:revive
func (d *Generic) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value []byte) (id int64, err error) {
	if d.TranslateErr != nil {
		defer func() {
			if err != nil {
				err = d.TranslateErr(err)
			}
		}()
	}

	cVal := 0
	dVal := 0
	if create {
		cVal = 1
	}
	if delete {
		dVal = 1
	}

	if d.LastInsertID {
		row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, previousRevision)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	// Drivers without LastInsertID support may conflict on the serial id key when inserting rows,
	// as the ID is reserved at the beginning of the implicit transaction, but does not become
	// visible until the transaction completes, at which point we may have already created a gap fill record.
	// Retry the insert if the driver indicates a retriable insert error, to avoid presenting a spurious
	// duplicate key error to the client.
	wait := strategy.Backoff(backoff.Linear(100 * time.Millisecond))
	for i := uint(0); i < 20; i++ {
		row := d.queryRow(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, previousRevision)
		err = row.Scan(&id)

		if err != nil && d.InsertRetry != nil && d.InsertRetry(err) {
			logrus.Warnf("Retriable insert error for key %v: %v", key, err)
			metrics.InsertErrorsTotal.WithLabelValues("true").Inc()
			wait(i)
			continue
		}

		if err != nil {
			metrics.InsertErrorsTotal.WithLabelValues("false").Inc()
			logrus.WithField("key", key).WithField("createRevision", createRevision).WithField("previousRevision", previousRevision).Errorf("Insert error for key %v: %v", key, err)
		}

		return id, err
	}
	return
}

func (d *Generic) GetSize(ctx context.Context) (int64, error) {
	if d.GetSizeSQL == nil {
		return 0, errors.New("driver does not support size reporting")
	}
	var size int64
	row := d.queryRow(ctx, d.GetSizeSQL)
	if err := row.Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}

func (d *Generic) FillRetryDelay(ctx context.Context) {
	time.Sleep(d.FillRetryDuration)
}

func (d *Generic) TranslateStartKey(startKey string) string {
	if d.TranslateStartKeyFunc != nil {
		return d.TranslateStartKeyFunc(startKey)
	}
	return startKey
}
