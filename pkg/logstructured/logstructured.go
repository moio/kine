package logstructured

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/ttl"
)

type Log interface {
	Start(ctx context.Context) error
	CompactRevision(ctx context.Context) (int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	List(ctx context.Context, key, end string, limit, revision int64, includeDeletes, keysOnly bool) (server.EventBatch, error)
	ListStream(ctx context.Context, key, end string, limit, revision int64, includeDeletes, keysOnly bool) server.ListResult
	Count(ctx context.Context, key, end string, revision int64) (int64, int64, error)
	After(ctx context.Context, key, end string, revision, limit int64) (server.EventBatch, error)
	Watch(ctx context.Context) <-chan server.EventBatch
	Append(ctx context.Context, event *server.Event) (int64, error)
	DbSize(ctx context.Context) (int64, error)
	Compact(ctx context.Context, revision int64) (int64, error)
}

// CASUpdater is the optional fused check-and-append capability of
// OPTIMIZATION_ROUND_17.md, implemented by logs over backends that can
// arbitrate a CAS check inside one INSERT statement (PostgreSQL).
type CASUpdater interface {
	CASFusable() bool
	UpdateCAS(ctx context.Context, key string, value []byte, expectedRevision, lease int64) (*server.KeyValue, bool, error)
}

type LogStructured struct {
	log Log
}

func New(log Log) *LogStructured {
	return &LogStructured{
		log: log,
	}
}

func (l *LogStructured) Start(ctx context.Context) error {
	if err := l.log.Start(ctx); err != nil {
		return err
	}
	// See https://github.com/kubernetes/kubernetes/blob/442a69c3bdf6fe8e525b05887e57d89db1e2f3a5/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L97
	if _, err := l.Create(ctx, server.HealthKey, []byte(server.HealthVal), 0); err != nil && err != server.ErrKeyExists {
		logrus.Errorf("Failed to create health check key: %v", err)
	}
	go ttl.Run(ctx, l)
	return nil
}

func (l *LogStructured) Get(ctx context.Context, key string, revision int64, keysOnly bool) (revRet int64, kvRet *server.KeyValue, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("GET %s, rev=%d => rev=%d, kv=%v, err=%v", key, revision, revRet, kvRet != nil, errRet)
	}()

	rev, event, err := l.get(ctx, key, revision, false, keysOnly)
	if event == nil {
		return rev, nil, err
	}
	return rev, event.KV, err
}

func (l *LogStructured) get(ctx context.Context, key string, revision int64, includeDeletes, keysOnly bool) (int64, *server.Event, error) {
	batch, err := l.log.List(ctx, key, "", 1, revision, includeDeletes, keysOnly)
	if err != nil {
		return batch.CurrentRev, nil, err
	}
	if len(batch.Events) == 0 {
		return batch.CurrentRev, nil, nil
	}
	return batch.CurrentRev, batch.Events[0], nil
}

func (l *LogStructured) adjustRevision(ctx context.Context, rev *int64) {
	if *rev != 0 {
		return
	}

	if newRev, err := l.log.CurrentRevision(ctx); err == nil {
		*rev = newRev
	}
}

func (l *LogStructured) Create(ctx context.Context, key string, value []byte, lease int64) (revRet int64, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, revRet, errRet)
	}()

	_, prevEvent, err := l.get(ctx, key, 0, true, true)
	if err != nil {
		return 0, err
	}
	createEvent := &server.Event{
		Create: true,
		KV: &server.KeyValue{
			Key:   key,
			Value: value,
			Lease: lease,
		},
		PrevKV: &server.KeyValue{},
	}
	if prevEvent != nil {
		if !prevEvent.Delete {
			return 0, server.ErrKeyExists
		}
		createEvent.PrevKV.ModRevision = prevEvent.KV.ModRevision
	}

	revRet, errRet = l.log.Append(ctx, createEvent)
	return revRet, errRet
}

func (l *LogStructured) Delete(ctx context.Context, key string, revision int64) (revRet int64, kvRet *server.KeyValue, deletedRet bool, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		logrus.Tracef("DELETE %s, rev=%d => rev=%d, kv=%v, deleted=%v, err=%v", key, revision, revRet, kvRet != nil, deletedRet, errRet)
	}()

	rev, event, err := l.get(ctx, key, 0, true, false)
	if err != nil {
		return 0, nil, false, err
	}

	if event == nil {
		// The key has no history rows: it never existed, or its entire
		// history (including the latest tombstone) was compacted away.
		// etcd evaluates a ModRevision compare against unknown or compacted
		// history as false, so the compare-and-delete fails and nothing is
		// written. Returning deleted=true here made a stale-rev txn delete
		// of an already-deleted, compacted key report success where etcd
		// reports deleted=false.
		return rev, nil, false, nil
	}

	if event.Delete {
		return rev, nil, false, nil
	}

	if revision != 0 && event.KV.ModRevision != revision {
		return rev, event.KV, false, nil
	}
	deleteEvent := &server.Event{
		Delete: true,
		KV:     &server.KeyValue{Key: key},
		PrevKV: &server.KeyValue{ModRevision: event.KV.ModRevision},
	}

	rev, err = l.log.Append(ctx, deleteEvent)
	if err != nil {
		// If error on Append we assume it's a UNIQUE constraint error, so we fetch the latest (if we can)
		// and return that the delete failed
		latestRev, latestEvent, latestErr := l.get(ctx, key, 0, true, false)
		if latestErr != nil || latestEvent == nil {
			return rev, event.KV, false, nil
		}
		return latestRev, latestEvent.KV, false, nil
	}
	return rev, event.KV, true, err
}

func (l *LogStructured) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	defer func() {
		logrus.Tracef("LIST %s, end=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v", key, end, limit, revision, revRet, len(kvRet), errRet)
	}()

	batch, err := l.log.List(ctx, key, end, limit, revision, false, keysOnly)
	kvs := make([]*server.KeyValue, 0, len(batch.Events))
	for _, event := range batch.Events {
		kvs = append(kvs, event.KV)
	}
	return batch.CurrentRev, kvs, err
}

func (l *LogStructured) ListStream(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) server.ListResult {
	return l.log.ListStream(ctx, key, end, limit, revision, false, keysOnly)
}

func (l *LogStructured) Count(ctx context.Context, key, end string, revision int64) (revRet int64, count int64, err error) {
	defer func() {
		logrus.Tracef("COUNT %s, end=%s, rev=%d => rev=%d, count=%d, err=%v", key, end, revision, revRet, count, err)
	}()
	return l.log.Count(ctx, key, end, revision)
}

func (l *LogStructured) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, kvRet *server.KeyValue, updateRet bool, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		kvRev := int64(0)
		if kvRet != nil {
			kvRev = kvRet.ModRevision
		}
		if logrus.IsLevelEnabled(logrus.TraceLevel) {
			logrus.Tracef("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, kvrev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, kvRev, updateRet, errRet)
		}
	}()

	// OPTIMIZATION_ROUND_17.md: when the backing log supports the fused
	// check-and-append, the CAS fast path collapses the pre-read and the
	// append into one statement. A failed check falls through to the
	// original read-then-append path below, which reproduces the original
	// failure semantics (including the KV carried back) without a second
	// copy of that logic.
	if cu, canFuse := l.log.(CASUpdater); canFuse && cu.CASFusable() {
		kv, updated, err := cu.UpdateCAS(ctx, key, value, revision, lease)
		if err != nil {
			return 0, nil, false, err
		}
		if updated {
			return kv.ModRevision, kv, true, nil
		}
	}

	rev, event, err := l.get(ctx, key, 0, false, false)
	if err != nil {
		return 0, nil, false, err
	}

	if event == nil {
		return 0, nil, false, nil
	}

	if event.KV.ModRevision != revision {
		return rev, event.KV, false, nil
	}

	updateEvent := &server.Event{
		KV: &server.KeyValue{
			Key:            key,
			CreateRevision: event.KV.CreateRevision,
			Value:          value,
			Lease:          lease,
		},
		PrevKV: &server.KeyValue{ModRevision: event.KV.ModRevision},
	}

	rev, err = l.log.Append(ctx, updateEvent)
	if err != nil {
		rev, event, err := l.get(ctx, key, 0, false, false)
		if event == nil {
			return rev, nil, false, err
		}
		return rev, event.KV, false, err
	}

	updateEvent.KV.ModRevision = rev
	return rev, updateEvent.KV, true, err
}

// BarePutFastEnabled reports whether PutFast may short-circuit unconditional
// puts (OPTIMIZATION_ROUND_18.md): the dialect must opt in and the fused CAS
// machinery must be available, since the update branch rides on UpdateCAS.
func (l *LogStructured) BarePutFastEnabled() bool {
	d, can := l.log.(interface{ BarePutFastEnabled() bool })
	return can && d.BarePutFastEnabled()
}

// PutFast resolves an unconditional put in at most two SQL statements: one
// tombstone-inclusive existence read, then either a create append or the fused
// CAS append seeded with the revision the read returned. Any race or failed
// capability returns done=false and the caller reruns the standard
// create→get→update flow, which reproduces the original semantics exactly.
// The failure KV contract mirrors the standard flow: the update branch returns
// the appended row's KV (as Update does), the create branch returns nil prev
// (as the Create path does).
func (l *LogStructured) PutFast(ctx context.Context, key string, value []byte, lease int64) (revRet int64, prev *server.KeyValue, done bool, errRet error) {
	defer func() {
		l.adjustRevision(ctx, &revRet)
		if logrus.IsLevelEnabled(logrus.TraceLevel) {
			logrus.Tracef("PUTFAST %s, size=%d, lease=%v => rev=%d, done=%v, err=%v", key, len(value), lease, revRet, done, errRet)
		}
	}()

	if !l.BarePutFastEnabled() {
		return 0, nil, false, nil
	}
	cu, canCAS := l.log.(CASUpdater)
	if !canCAS || !cu.CASFusable() {
		return 0, nil, false, nil
	}

	_, event, err := l.get(ctx, key, 0, true, false)
	if err != nil {
		// surface read errors through the standard flow, which reports them identically
		return 0, nil, false, nil
	}

	if event == nil || event.Delete {
		// create branch: Create's own event construction verbatim
		createEvent := &server.Event{
			Create: true,
			KV: &server.KeyValue{
				Key:   key,
				Value: value,
				Lease: lease,
			},
			PrevKV: &server.KeyValue{},
		}
		if event != nil {
			createEvent.PrevKV.ModRevision = event.KV.ModRevision
		}
		rev, err := l.log.Append(ctx, createEvent)
		if err != nil {
			// a lost create race (unique violation → ErrKeyExists) reruns the
			// standard loop; other errors surface identically through it too
			return 0, nil, false, nil
		}
		return rev, nil, true, nil
	}

	// live row: the read already provided everything the CAS needs
	kv, updated, err := cu.UpdateCAS(ctx, key, value, event.KV.ModRevision, lease)
	if err != nil || !updated {
		return 0, nil, false, nil
	}
	return kv.ModRevision, kv, true, nil
}

func (l *LogStructured) Watch(ctx context.Context, revision int64) server.WatchResult {
	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := l.log.Watch(ctx)

	result := make(chan server.EventBatch, 100)
	errc := make(chan error, 1)
	wr := server.WatchResult{Eventc: result, Errorc: errc}

	// include the current revision in list
	if revision > 1 {
		revision--
	}

	var batch server.EventBatch
	var err error

	if revision == 0 {
		wr.CurrentRevision, err = l.CurrentRevision(ctx)
		batch.CurrentRev = wr.CurrentRevision
	} else {
		batch, err = l.log.After(ctx, "", "", revision, 0)
		wr.CurrentRevision = batch.CurrentRev
	}

	if err != nil {
		if !errors.Is(err, context.Canceled) {
			if err == server.ErrCompacted {
				wr.CompactRevision, _ = l.log.CompactRevision(ctx)
			} else {
				errc <- server.ErrGRPCUnhealthy
			}
		}
		cancel()
	}

	result <- batch

	rev := wr.CurrentRevision
	go func() {
		// always ensure we fully read the channel
		for i := range readChan {
			result <- i.After(rev)
		}
		close(result)
		cancel()
	}()

	return wr
}

func (l *LogStructured) DbSize(ctx context.Context) (int64, error) {
	return l.log.DbSize(ctx)
}

func (l *LogStructured) CurrentRevision(ctx context.Context) (int64, error) {
	return l.log.CurrentRevision(ctx)
}

func (l *LogStructured) Compact(ctx context.Context, revision int64) (int64, error) {
	return l.log.Compact(ctx, revision)
}
