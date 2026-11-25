package sqllog

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

const minCompactBatchSize = 100

type SQLLog struct {
	sync.RWMutex

	d                     server.Dialect
	broadcaster           broadcaster.Broadcaster
	ctx                   context.Context
	notify                chan int64
	currentRev            atomic.Int64
	compactInterval       time.Duration
	compactIntervalJitter int
	compactTimeout        time.Duration
	compactMinRetain      int64
	compactBatchSize      int64
	pollBatchSize         int64
}

func New(d server.Dialect, compactInterval time.Duration, compactIntervalJitter int, compactTimeout time.Duration, compactMinRetain int64, compactBatchSize int64, pollBatchSize int64) *SQLLog {
	l := &SQLLog{
		d:                     d,
		notify:                make(chan int64, 1024),
		compactInterval:       compactInterval,
		compactIntervalJitter: compactIntervalJitter,
		compactTimeout:        compactTimeout,
		compactMinRetain:      compactMinRetain,
		compactBatchSize:      compactBatchSize,
		pollBatchSize:         pollBatchSize,
	}
	return l
}

func (s *SQLLog) Start(ctx context.Context) error {
	if s.compactBatchSize < minCompactBatchSize {
		return fmt.Errorf("compact-batch-size %d too small: must be at least %d", s.compactBatchSize, minCompactBatchSize)
	}

	s.ctx = ctx
	return s.compactStart(s.ctx)
}

func (s *SQLLog) compactStart(ctx context.Context) error {
	logrus.Tracef("COMPACTSTART")

	rows, err := s.d.After(ctx, "compact_rev_key", "", 0, 0)
	if err != nil {
		return err
	}

	batch, _, err := RowsToEvents(rows, true, true)
	if err != nil {
		return err
	}

	logrus.Tracef("COMPACTSTART len(events)=%v", len(batch.Events))

	if len(batch.Events) == 0 {
		_, err := s.Append(ctx, &server.Event{
			Create: true,
			KV: &server.KeyValue{
				Key:   "compact_rev_key",
				Value: []byte(""),
			},
		})
		return err
	} else if len(batch.Events) == 1 {
		return nil
	}

	t, err := s.d.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer t.MustRollback()

	// this is to work around a bug in which we ended up with two compact_rev_key rows
	maxRev := int64(0)
	maxID := int64(0)
	for _, event := range batch.Events {
		if event.PrevKV != nil && event.PrevKV.ModRevision > maxRev {
			maxRev = event.PrevKV.ModRevision
			maxID = event.KV.ModRevision
		}
		logrus.Tracef("COMPACTSTART maxRev=%v maxID=%v", maxRev, maxID)
	}

	for _, event := range batch.Events {
		logrus.Tracef("COMPACTSTART event.KV.ModRevision=%v maxID=%v", event.KV.ModRevision, maxID)
		if event.KV.ModRevision == maxID {
			continue
		}
		if err := t.DeleteRevision(ctx, event.KV.ModRevision); err != nil {
			return err
		}
	}

	return t.Commit()
}

// compactor periodically compacts historical versions of keys.
// It will compact keys with versions older than given interval, but never within the last 1000 revisions.
// In other words, after compaction, it will only contain key revisions set during last interval.
// Any API call for the older versions of keys will return error.
// Interval is the time interval between each compaction. The first compaction happens after "interval".
// This logic is directly cribbed from k8s.io/apiserver/pkg/storage/etcd3/compact.go
func (s *SQLLog) compactor(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	compactRev, _ := s.d.GetCompactRevision(s.ctx)
	targetCompactRev, _ := s.CurrentRevision(s.ctx)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		compactRev, targetCompactRev = s.compactIter(compactRev, targetCompactRev)
	}
}

func (s *SQLLog) compactIter(compactRev, targetCompactRev int64) (int64, int64) {
	logrus.Tracef("COMPACT running compactRev=%d targetCompactRev=%d", compactRev, targetCompactRev)
	// Break up the compaction into smaller batches to avoid locking the database with excessively
	// long transactions. When things are working normally deletes should proceed quite quickly, but if
	// run against a database where compaction has stalled (see rancher/k3s#1311) it may take a long time
	// (several hundred ms) just for the database to execute the subquery to select the revisions to delete.

	var (
		resultLabel    string
		iterCompactRev int64
		iterStart      time.Time
		iterCount      int64
		compactedRev   int64
		currentRev     int64
		err            error
	)

	resultLabel = metrics.ResultSuccess
	iterCompactRev = compactRev
	compactedRev = compactRev
	iterStart = time.Now()
	iterCount = 0

	for iterCompactRev < targetCompactRev {
		// Set move iteration target compactBatchSize revisions forward, or
		// just as far as we need to hit the compaction target if that would
		// overshoot it.
		iterCompactRev += s.compactBatchSize
		if iterCompactRev > targetCompactRev {
			iterCompactRev = targetCompactRev
		}

		// only update the compacted and current revisions if they are valid,
		// but break out of the loop on any error.
		compacted, current, cerr := s.compact(compactedRev, iterCompactRev)
		if compacted != 0 && current != 0 {
			compactedRev = compacted
			currentRev = current
		}
		if cerr != nil {
			err = cerr
			break
		}
		iterCount++
	}

	if iterCount > 0 {
		logrus.Infof("COMPACT compacted from %d to %d in %d transactions over %s", compactRev, compactedRev, iterCount, time.Since(iterStart).Round(time.Millisecond))

		// post-compact operation errors are not critical, but should be reported
		if perr := s.postCompact(); perr != nil {
			logrus.Errorf("Post-compact operations failed: %v", perr)
		}
	}

	// Only store the final results for this compact interval if currentRev is
	// updated to the current compact revision.
	//
	// Note that one or more of the small-batch compact transactions may have
	// succeeded and moved the compact revision forward, even if err is non-nil.
	if currentRev > 0 {
		compactRev = compactedRev
		targetCompactRev = currentRev
	}

	// ErrCompacted indicates that no further work is necessary - either compactRev changed since the
	// last iteration because another client has compacted, or the requested revision has already been compacted.
	if err != nil && err != server.ErrCompacted {
		logrus.Errorf("Compact failed: %v", err)
		resultLabel = metrics.ResultError
	}
	metrics.CompactTotal.WithLabelValues(resultLabel).Inc()

	return compactRev, targetCompactRev
}

// compact removes deleted or replaced rows from the database, and updates the compact rev key.
// compactRev is the current compact revision; targetCompactRev is the revision to compact to.
// If compactRev does not match what's in the database, we know that someone else has compacted and we don't need to do it.
// Deletion of rows and update of the compact rev key is done within a single transaction. The transaction is rolled back on any error.
//
// On success, the function returns the revision compacted to, and the revision that we should try to compact to next time (the current revision).
// ErrCompacted is returned if the current revision is stale, or the target revision has already been compacted.
// In this case the compact and current revisions from the database are returned.
// On any other error, the returned compact and current revisions should not be used.
//
// This logic is cribbed from k8s.io/apiserver/pkg/storage/etcd3/compact.go
func (s *SQLLog) compact(compactRev int64, targetCompactRev int64) (int64, int64, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.compactTimeout)
	defer cancel()

	t, err := s.d.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer t.MustRollback()

	currentRev, err := t.CurrentRevision(s.ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current revision: %w", err)
	}

	dbCompactRev, err := t.GetCompactRevision(s.ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get compact revision: %w", err)
	}

	// Check to see if another node already compacted. This is normal on a multi-server cluster.
	if compactRev != dbCompactRev {
		logrus.Infof("COMPACT compact revision changed since last iteration: %d => %d", compactRev, dbCompactRev)
		return dbCompactRev, currentRev, server.ErrCompacted
	}

	// Ensure that we never compact the most recent 1000 revisions
	targetCompactRev = safeCompactRev(targetCompactRev, currentRev, s.compactMinRetain)

	// Don't bother compacting to a revision that has already been compacted
	if targetCompactRev <= compactRev {
		logrus.Tracef("COMPACT revision %d has already been compacted", targetCompactRev)
		return dbCompactRev, currentRev, server.ErrCompacted
	}

	logrus.Infof("COMPACT compactRev=%d targetCompactRev=%d currentRev=%d", compactRev, targetCompactRev, currentRev)

	start := time.Now()

	// Set compact revision before compacting - if the compact transaction times
	// out this will get rolled back. Otherwise it is possible for Compact to
	// succeed within the time limit, but SetCompactRevision occurs too late, and
	// the work is wasted.
	if err := t.SetCompactRevision(s.ctx, targetCompactRev); err != nil {
		return 0, 0, fmt.Errorf("failed to record compact revision: %w", err)
	}

	// compact revisions from old compact revision to new target
	deletedRows, err := t.Compact(s.ctx, compactRev, targetCompactRev)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to compact to revision %d: %w", targetCompactRev, err)
	}

	// Commit the transaction if we make it all the way through deleting without any
	// errors. The deferred rollback becomes a no-op if the transaction is committed.
	t.MustCommit()
	logrus.Infof("COMPACT deleted %d rows from %d revisions in %s - compacted to %d/%d", deletedRows, (targetCompactRev - compactRev), time.Since(start), targetCompactRev, currentRev)

	return targetCompactRev, currentRev, nil
}

// postCompact executes any driver-specific cleanup after a successful compaction pass.
// The actual operation depends on the Dialect implementation; for example, the SQLite driver
// uses this to run a WAL checkpoint (PRAGMA wal_checkpoint) to flush committed pages back
// into the main database file (this does NOT reclaim free pages or shrink the database file;
// disk space reclamation is handled separately via VACUUM at startup).
func (s *SQLLog) postCompact() error {
	return s.d.PostCompact(s.ctx)
}

func (s *SQLLog) CurrentRevision(ctx context.Context) (int64, error) {
	currRev := s.currentRev.Load()
	if currRev != 0 {
		return currRev, nil
	}
	lastRev, err := s.d.CurrentRevision(ctx)
	if err != nil {
		return lastRev, err
	}
	if s.currentRev.CompareAndSwap(currRev, lastRev) {
		return lastRev, nil
	}
	return s.currentRev.Load(), nil
}

func (s *SQLLog) CompactRevision(ctx context.Context) (int64, error) {
	return s.d.GetCompactRevision(ctx)
}

func (s *SQLLog) After(ctx context.Context, key, end string, revision, limit int64) (server.EventBatch, error) {
	var (
		batch   server.EventBatch
		compact int64
	)
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return batch, err
	}
	rows, err := s.d.After(ctx, key, end, revision, limit)
	if err != nil {
		return batch, err
	}

	batch, compact, err = RowsToEvents(rows, true, true)

	if batch.CurrentRev == 0 {
		// without rows we won't have the compact or current revisions
		batch.CurrentRev = currentRev
		compact, err = s.d.GetCompactRevision(ctx)
		if err != nil {
			return batch, err
		}
	}

	// The compact revision itself can still be queried, so it is only an error to watch anything before compactRev - 1
	if revision > 0 && revision < compact-1 {
		return batch, server.ErrCompacted
	}

	// PrevKV should be nil if revision has been compacted; ref:
	// https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/storage/etcd3/event.go#L66-L67
	for _, event := range batch.Events {
		if event.PrevKV != nil && event.PrevKV.ModRevision < compact {
			event.PrevKV = nil
		}
	}

	return batch, err
}

func (s *SQLLog) List(ctx context.Context, key, end string, limit, revision int64, includeDeleted, keysOnly bool) (server.EventBatch, error) {
	var (
		rows    server.RowSet
		batch   server.EventBatch
		compact int64
	)
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return batch, err
	}

	key = s.d.TranslateStartKey(key)

	if revision == 0 {
		rows, err = s.d.ListCurrent(ctx, key, end, limit, includeDeleted, keysOnly)
	} else {
		rows, err = s.d.List(ctx, key, end, limit, revision, includeDeleted, keysOnly)
	}
	if err != nil {
		return batch, err
	}

	batch, compact, err = RowsToEvents(rows, !keysOnly, false)
	if err != nil {
		return batch, err
	}

	if batch.CurrentRev == 0 {
		// without rows we won't have the compact or current revisions
		batch.CurrentRev = currentRev
		compact, err = s.d.GetCompactRevision(ctx)
		if err != nil {
			return batch, err
		}
	}

	if revision > batch.CurrentRev {
		return batch, server.ErrFutureRev
	}

	if revision > 0 && revision < compact {
		return batch, server.ErrCompacted
	}

	select {
	case s.notify <- batch.CurrentRev:
	default:
	}

	return batch, err
}

func (s *SQLLog) ListStream(ctx context.Context, key, end string, limit, revision int64, includeDeleted, keysOnly bool) server.ListResult {
	var (
		rows server.RowSet
		kvc  = make(chan *server.KeyValue, 1)
		errc = make(chan error, 1)
	)
	errorResult := func(err error) server.ListResult {
		if err != nil {
			errc <- err
		}
		if rows != nil {
			rows.Close()
		}
		close(kvc)
		close(errc)
		return server.ListResult{KVc: kvc, Errorc: errc}
	}

	key = s.d.TranslateStartKey(key)

	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return errorResult(err)
	}

	// one arena for the whole streamed result set
	arena := &byteArena{}

	if revision == 0 {
		rows, err = s.d.ListCurrent(ctx, key, end, limit, includeDeleted, keysOnly)
	} else {
		rows, err = s.d.List(ctx, key, end, limit, revision, includeDeleted, keysOnly)
	}
	if err != nil {
		return errorResult(err)
	}

	// scan first row now, to get current and compact revs
	var current, compact int64
	if rows.Next() {
		event, err := scan(rows, &current, &compact, !keysOnly, false, arena)
		if err != nil {
			return errorResult(err)
		}
		kvc <- event.KV
	}

	if current == 0 {
		// without rows we won't have the compact or current revisions
		current = currentRev
		compact, err = s.d.GetCompactRevision(ctx)
		if err != nil {
			return errorResult(err)
		}
	}

	if revision > current {
		return errorResult(server.ErrFutureRev)
	}

	if revision > 0 && revision < compact {
		return errorResult(server.ErrCompacted)
	}

	// stream rows into kv channel
	go func() {
		defer rows.Close()
		defer close(kvc)
		defer close(errc)
		for rows.Next() {
			event, err := scan(rows, &current, &compact, !keysOnly, false, arena)
			if err != nil {
				errc <- err
				return
			}
			kvc <- event.KV
		}
		// Next returns false for a read that ended early exactly as it does for
		// the last row, so without this a list cut short by a cancelled query or
		// a dropped connection reaches the client as a complete one.
		if err := rows.Err(); err != nil {
			errc <- err
		}
	}()

	return server.ListResult{KVc: kvc, Errorc: errc, CurrentRevision: current}
}

// byteArena batches the byte copies of one result set's scanned values into
// a small number of backing chunks. Scanning through database/sql hands us a
// freshly allocated copy of every bytea column; routing those bytes through
// the arena replaces one allocation per value with one per chunk, and puts
// the values of a result set in contiguous memory, which keeps the later
// protobuf marshal pass cache-friendly.
type byteArena struct {
	cur []byte // current chunk; len(cur) is the used prefix
}

const arenaChunkSize = 64 << 10

// clone returns a copy of b owned by the arena. The returned slice must be
// treated as read-only (it shares backing with other values in the chunk).
func (a *byteArena) clone(b []byte) []byte {
	if len(b) == 0 {
		if b == nil {
			return nil
		}
		return []byte{}
	}
	if cap(a.cur)-len(a.cur) < len(b) {
		size := arenaChunkSize
		if a.cur == nil {
			// the first chunk is sized to the first value so that small
			// results (point reads) only pay for what they need
			size = len(b)
		} else if len(b) > size {
			size = len(b)
		}
		a.cur = make([]byte, 0, size)
	}
	start := len(a.cur)
	a.cur = a.cur[:start+len(b)]
	copy(a.cur[start:], b)
	return a.cur[start : start+len(b) : start+len(b)]
}

// scanDests is per-row scratch destination storage. The RawBytes fields alias
// driver-owned buffers that are only valid until the next call to
// rows.Next(), so their contents must be consumed (copied) immediately.
type scanDests struct {
	cur     sql.NullInt64
	compact sql.NullInt64
	prevMod int64
	create  int64
	delete  int64
	value   sql.RawBytes
	prevVal sql.RawBytes
}

// rowsToEvents converts database rows to KV store events.
// if val is false, rows must not include the current value
// if prev is false, rows must additionally not include the previous value
func RowsToEvents(rows server.RowSet, val, prev bool) (server.EventBatch, int64, error) {
	var (
		batch   server.EventBatch
		compact int64
		arena   byteArena
	)
	defer rows.Close()

	// pgx-native rows expose the raw wire-format values; when they do and
	// every field is binary-encoded, scan each row directly from the wire
	// bytes instead of routing through Scan's interface boxing.
	// RawValues() is populated per row and only valid after the first Next(),
	// so the gate is evaluated lazily from the first row: all rows of one
	// result set share the field descriptions, and the pre-Next check used
	// before OPTIMIZATION_ROUND_4.md forced every result set down the
	// fallback scanner.
	var (
		fast    bool
		checked bool
	)

	for rows.Next() {
		if !checked {
			checked = true
			switch {
			case rows.Binary() && len(rows.RawValues()) > 0:
				fast = true
				metrics.ScanMode.WithLabelValues("fast", "").Inc()
			case rows.Binary():
				metrics.ScanMode.WithLabelValues("scan", "no_raw_values").Inc()
			default:
				metrics.ScanMode.WithLabelValues("scan", "text_format").Inc()
			}
		}
		var (
			event *server.Event
			err   error
		)
		if fast {
			event, err = scanFast(rows.RawValues(), &batch.CurrentRev, &compact, val, prev, &arena)
		} else {
			event, err = scan(rows, &batch.CurrentRev, &compact, val, prev, &arena)
		}
		if err != nil {
			return batch, 0, err
		}
		batch.Events = append(batch.Events, event)
	}
	// a read cut short by a cancelled query or a dropped connection must not
	// reach the caller as a complete result
	if err := rows.Err(); err != nil {
		return batch, 0, err
	}

	return batch, compact, nil
}

func (s *SQLLog) Watch(ctx context.Context) <-chan server.EventBatch {
	return s.broadcaster.Watch(ctx, s.startWatch)
}

func (s *SQLLog) startWatch() (chan server.EventBatch, error) {
	pollStart, err := s.d.CurrentRevision(s.ctx)
	if err != nil {
		return nil, err
	}

	c := make(chan server.EventBatch)

	if s.compactIntervalJitter < 0 || s.compactIntervalJitter > 100 {
		panic("jitterPercent must be between 0 and 100")
	}
	maxJitter := float64(s.compactIntervalJitter) / 100.0 * float64(s.compactInterval)
	jitter := time.Duration(rand.Float64()*2*maxJitter - maxJitter)

	// start compaction and polling at the same time to watch starts
	// at the oldest revision, but compaction doesn't create gaps
	if s.compactInterval <= 0 {
		logrus.Debugf("COMPACT disabled; automatic compaction will not occur")
	} else {
		go s.compactor(s.compactInterval + jitter)
	}

	go s.poll(c, pollStart)
	return c, nil
}

func (s *SQLLog) poll(result chan server.EventBatch, pollStart int64) {
	var (
		skip         int64
		skipTime     time.Time
		waitForMore  = true
		pollRevision = pollStart
		trace        = logrus.IsLevelEnabled(logrus.TraceLevel)
	)

	wait := time.NewTicker(time.Second)
	defer wait.Stop()
	defer close(result)

	for {
		if waitForMore {
			select {
			case <-s.ctx.Done():
				return
			case check := <-s.notify:
				if check <= pollRevision {
					continue
				}
			case <-wait.C:
			}
		}

		rows, err := s.d.After(s.ctx, "", "", pollRevision, s.pollBatchSize)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logrus.Errorf("Failed to list latest changes: %v", err)
			}
			continue
		}

		batch, _, err := RowsToEvents(rows, true, true)
		waitForMore = int64(len(batch.Events)) < s.pollBatchSize
		if err != nil {
			logrus.Errorf("fail to convert rows changes: %v", err)
			continue
		}

		if trace {
			logrus.Tracef("POLL AFTER %d, limit=%d, events=%d", pollRevision, s.pollBatchSize, len(batch.Events))
		}

		if len(batch.Events) == 0 {
			continue
		}

		var (
			rev        = pollRevision
			saveLast   = false
			sequential = make([]*server.Event, 0, len(batch.Events))
		)

		for _, event := range batch.Events {
			next := rev + 1
			// Ensure that we are notifying events in a sequential fashion. For example if we find row 4 before 3
			// we don't want to notify row 4 because 3 is essentially dropped forever.
			if event.KV.ModRevision != next {
				if trace {
					logrus.Tracef("MODREVISION GAP: expected %v, got %v", next, event.KV.ModRevision)
				}
				if canSkipRevision(next, skip, skipTime) {
					// This situation should never happen, but we have it here as a fallback just for unknown reasons
					// we don't want to pause all watches forever
					logrus.Errorf("GAP %s, revision=%d, delete=%v, next=%d", event.KV.Key, event.KV.ModRevision, event.Delete, next)
				} else if skip != next {
					// This is the first time we have encountered this missing revision, so record time start
					// and trigger a quick retry for simple out of order events
					skip = next
					skipTime = time.Now()
					select {
					case s.notify <- next:
					default:
					}
					// Some drivers increment the revision sequence at the start of the insert
					// transaction, but the row does not become visible to us until the transaction
					// completes. This looks like a skip, but creating a fill record too quickly
					// will cause the insert to fail and the transaction to roll back. Allow the
					// driver to inject an extra delay into the retry before filling.
					s.d.FillRetryDelay(s.ctx)
					break
				} else {
					if err := s.d.Fill(s.ctx, next); err == nil {
						if trace {
							logrus.Tracef("FILL, revision=%d, err=%v", next, err)
						}
						select {
						case s.notify <- next:
						default:
						}
					} else {
						if trace {
							logrus.Tracef("FILL FAILED, revision=%d, err=%v", next, err)
						}
					}
					break
				}
			}

			// we have done something now that we should save the last revision.  We don't save here now because
			// the next loop could fail leading to saving the reported revision without reporting it.  In practice this
			// loop right now has no error exit so the next loop shouldn't fail, but if we for some reason add a method
			// that returns error, that would be a tricky bug to find.  So instead we only save the last revision at
			// the same time we write to the channel.
			saveLast = true
			rev = event.KV.ModRevision
			if s.d.IsFill(event.KV.Key) {
				if trace {
					logrus.Tracef("BROADCAST SKIPPED FOR FILL %s, revision=%d, delete=%v", event.KV.Key, event.KV.ModRevision, event.Delete)
				}
			} else {
				sequential = append(sequential, event)
				if trace {
					logrus.Tracef("BROADCAST %s, revision=%d, delete=%v", event.KV.Key, event.KV.ModRevision, event.Delete)
				}
			}
		}

		if saveLast {
			s.currentRev.CompareAndSwap(pollRevision, rev)
			pollRevision = rev
			if len(sequential) > 0 {
				result <- server.EventBatch{CurrentRev: pollRevision, Events: sequential}
			}
		}
	}
}

func canSkipRevision(rev, skip int64, skipTime time.Time) bool {
	return rev == skip && time.Since(skipTime) > time.Second
}

func (s *SQLLog) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	key = s.d.TranslateStartKey(key)

	if revision == 0 {
		return s.d.CountCurrent(ctx, key, end)
	}

	rev, compact, rows, err := s.d.Count(ctx, key, end, revision)
	if err != nil {
		return 0, 0, err
	}
	if revision > rev {
		return rev, 0, server.ErrFutureRev
	}
	if revision < compact {
		return rev, 0, server.ErrCompacted
	}
	return rev, rows, nil
}

func (s *SQLLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	e := *event
	if e.KV == nil {
		e.KV = &server.KeyValue{}
	}
	if e.PrevKV == nil {
		e.PrevKV = &server.KeyValue{}
	}

	currentRev := s.currentRev.Load()
	rev, err := s.d.Insert(ctx, e.KV.Key,
		e.Create,
		e.Delete,
		e.KV.CreateRevision,
		e.PrevKV.ModRevision,
		e.KV.Lease,
		e.KV.Value,
	)
	if err != nil {
		return 0, err
	}

	s.appended(currentRev, rev)
	return rev, nil
}

// CASInserter is the opt-in write capability of OPTIMIZATION_ROUND_17.md:
// one statement verifies the key's latest row is a live row at the expected
// revision and appends the successor, arbitrated by the backend's unique
// index. Only implemented by databases that can do both in a single
// statement (PostgreSQL).
type CASInserter interface {
	FusedCASEnabled() bool
	InsertCAS(ctx context.Context, key string, expectedRevision, lease int64, value []byte) (id, createRevision int64, ok bool, err error)
}

// CASFusable reports whether Update should attempt the fused CAS path.
func (s *SQLLog) CASFusable() bool {
	if ci, can := s.d.(CASInserter); can {
		return ci.FusedCASEnabled()
	}
	return false
}

// BarePutFastEnabled forwards the dialect's bare-put fast-path opt-in
// (OPTIMIZATION_ROUND_18.md); SQLLog has no statement of its own for it —
// the fast path is one existence read (a plain log read) plus either an
// ordinary Append (create branch) or UpdateCAS (update branch).
func (s *SQLLog) BarePutFastEnabled() bool {
	if d, can := s.d.(interface{ BarePutFastEnabled() bool }); can {
		return d.BarePutFastEnabled()
	}
	return false
}

// OneStmtPutter is the one-statement bare-put capability of
// OPTIMIZATION_ROUND_19.md, overriding BarePutFast when enabled.
type OneStmtPutter interface {
	OneStmtPutEnabled() bool
	PutOne(ctx context.Context, key string, value []byte, lease int64) (id, prevRevision, createRevision int64, err error)
}

// OneStmtPutEnabled forwards the dialect's one-statement put opt-in.
func (s *SQLLog) OneStmtPutEnabled() bool {
	if d, can := s.d.(interface{ OneStmtPutEnabled() bool }); can {
		return d.OneStmtPutEnabled()
	}
	return false
}

// PutOne performs the one-statement bare put and publishes the new revision to
// the poller exactly as Append does.
func (s *SQLLog) PutOne(ctx context.Context, key string, value []byte, lease int64) (id, prevRevision, createRevision int64, err error) {
	osp, can := s.d.(OneStmtPutter)
	if !can {
		return 0, 0, 0, errors.New("one-statement put not supported by this log")
	}
	id, prevRevision, createRevision, err = osp.PutOne(ctx, key, value, lease)
	if err != nil {
		return 0, 0, 0, err
	}
	s.appended(s.currentRev.Load(), id)
	return id, prevRevision, createRevision, nil
}

// UpdateCAS performs the fused check-and-append for a supported CAS update;
// on success it returns the updated KV (ModRevision = new revision,
// CreateRevision from the appended row). ok=false means the CAS check did
// not hold or was lost in a race — the caller falls back to the original
// read-then-append path, which reproduces the pre-fusion semantics exactly.
func (s *SQLLog) UpdateCAS(ctx context.Context, key string, value []byte, expectedRevision, lease int64) (*server.KeyValue, bool, error) {
	currentRev := s.currentRev.Load()
	rev, createRevision, ok, err := s.d.(CASInserter).InsertCAS(ctx, key, expectedRevision, lease, value)
	if err != nil || !ok {
		return nil, false, err
	}
	s.appended(currentRev, rev)
	return &server.KeyValue{
		Key:            key,
		Value:          value,
		Lease:          lease,
		ModRevision:    rev,
		CreateRevision: createRevision,
	}, true, nil
}

// appended publishes a completed append to the poller and advances the
// cached current revision; shared by Append and UpdateCAS.
func (s *SQLLog) appended(previousRevision, rev int64) {
	// notify the polling loop of the new revision.
	select {
	case s.notify <- rev:
	default:
	}

	// previousRevision may have moved ahead due to other inserts between when
	// the insert returned and now; ensure that we don't roll it back if it
	// has changed elsewhere.
	s.currentRev.CompareAndSwap(previousRevision, rev)
}

// wire-format column decoders. PostgreSQL binary encoding: int8 and
// int4 are big-endian two's complement, bytea carries a 4-byte length
// prefix, and text is the raw UTF-8 bytes.

func decInt8(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

func decInt4(b []byte) int64 {
	if len(b) != 4 {
		return 0
	}
	return int64(int32(binary.BigEndian.Uint32(b)))
}

// Note on bytea columns: pgx strips the wire framing before RawValues, so a
// binary-format bytea value is the raw payload with NO in-band 4-byte length
// prefix (a decoder that strips one corrupts data; pgtype.Bytea behaves the
// same). NULLs are reported as nil slices and stay nil through the arena.

// scanFast builds a server.Event directly from a row's raw wire-format
// values (PostgreSQL binary format, as exposed by pgx Rows.RawValues).
// Column layout: current_rev, compact_rev, id, name, created, deleted,
// create_revision, prev_revision, lease [, value [, old_value]].
// The raw values alias the connection buffer and must be copied before
// the next rows.Next() call, which the arena does immediately.
func scanFast(raws [][]byte, rev *int64, compact *int64, val, prev bool, arena *byteArena) (*server.Event, error) {
	if len(raws) < 9 {
		return nil, fmt.Errorf("expected at least 9 columns, got %d", len(raws))
	}
	event := &server.Event{
		KV: &server.KeyValue{},
	}
	if prev {
		event.PrevKV = &server.KeyValue{}
	}

	event.KV.ModRevision = decInt8(raws[2])
	event.KV.Key = string(raws[3])
	event.Create = decInt4(raws[4]) != 0
	event.Delete = decInt4(raws[5]) != 0
	event.KV.CreateRevision = decInt8(raws[6])
	prevMod := decInt8(raws[7])
	event.KV.Lease = decInt4(raws[8])
	if prev {
		event.PrevKV.ModRevision = prevMod
	}
	if val && len(raws) > 9 {
		event.KV.Value = arena.clone(raws[9]) // bytea: raw payload, nil for NULL
	}
	if prev && len(raws) > 10 {
		event.PrevKV.Value = arena.clone(raws[10])
	}

	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	} else if prev {
		event.PrevKV.CreateRevision = event.KV.CreateRevision
		event.PrevKV.Lease = event.KV.Lease
		event.PrevKV.Key = event.KV.Key
	}

	*rev = decInt8(raws[0])
	*compact = decInt8(raws[1])
	return event, nil
}

// scan scans the current row's columns into a server.Event struct.
// If a valid event is scanned, the passed rev and compact vars are also updated.
func scan(rows server.RowSet, rev *int64, compact *int64, val, prev bool, arena *byteArena) (*server.Event, error) {
	event := &server.Event{
		KV: &server.KeyValue{},
	}
	if prev {
		event.PrevKV = &server.KeyValue{}
	}
	dest := scanDests{}
	colCount := 9
	if val {
		colCount++
		if prev {
			colCount++
		}
	}

	var dests [11]any
	dests[0] = &dest.cur
	dests[1] = &dest.compact
	dests[2] = &event.KV.ModRevision
	dests[3] = &event.KV.Key
	// created/deleted are int4 columns; scan into int64 scratch and
	// convert (database/sql would convert to *bool implicitly, pgx will
	// not)
	dests[4] = &dest.create
	dests[5] = &dest.delete
	dests[6] = &event.KV.CreateRevision
	if prev {
		dests[7] = &event.PrevKV.ModRevision
	} else {
		// prev_revision is still scanned (it drives pagination and delete
		// detection), but the PrevKV row itself is not needed off the watch
		// path
		dests[7] = &dest.prevMod
	}
	dests[8] = &event.KV.Lease
	if val {
		dests[9] = &dest.value
		if prev {
			dests[10] = &dest.prevVal
		}
	}

	err := rows.Scan(dests[:colCount]...)
	if err != nil {
		return nil, err
	}
	event.Create = dest.create != 0
	event.Delete = dest.delete != 0

	if val {
		event.KV.Value = arena.clone(dest.value)
		if prev {
			event.PrevKV.Value = arena.clone(dest.prevVal)
		}
	}

	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	} else if prev {
		event.PrevKV.CreateRevision = event.KV.CreateRevision
		event.PrevKV.Lease = event.KV.Lease
		event.PrevKV.Key = event.KV.Key
	}

	*rev = dest.cur.Int64
	*compact = dest.compact.Int64
	return event, nil
}

// safeCompactRev ensures that we never compact the most recent 1000 revisions.
func safeCompactRev(targetCompactRev int64, currentRev int64, compactMinRetain int64) int64 {
	safeRev := currentRev - compactMinRetain
	if targetCompactRev < safeRev {
		safeRev = targetCompactRev
	}
	if safeRev < 0 {
		safeRev = 0
	}
	return safeRev
}

func (s *SQLLog) DbSize(ctx context.Context) (int64, error) {
	return s.d.GetSize(ctx)
}

func (s *SQLLog) Compact(ctx context.Context, targetCompactRev int64) (int64, error) {
	currentRev, _ := s.CurrentRevision(ctx)
	if targetCompactRev > currentRev {
		return 0, server.ErrFutureRev
	}
	compactRev, _ := s.d.GetCompactRevision(s.ctx)
	if targetCompactRev <= compactRev {
		return 0, server.ErrCompacted
	}
	// manual compact is a no-op unless automatic compaction is disabled
	if s.compactInterval <= 0 {
		s.compactIter(compactRev, targetCompactRev)
		return s.CurrentRevision(ctx)
	}
	return currentRev, nil
}
