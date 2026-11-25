package server

import (
	"context"
	"database/sql"
	"fmt"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotSupported = status.New(codes.InvalidArgument, "etcdserver: unsupported operations in txn request").Err()
	ErrInvalidWatch = status.New(codes.InvalidArgument, "etcdserver: unsupported options in watch request").Err()

	ErrKeyExists     = rpctypes.ErrGRPCDuplicateKey
	ErrCompacted     = rpctypes.ErrGRPCCompacted
	ErrFutureRev     = rpctypes.ErrGRPCFutureRev
	ErrNoLeader      = rpctypes.ErrGRPCNoLeader
	ErrGRPCUnhealthy = rpctypes.ErrGRPCUnhealthy
)

const (
	HealthKey = `/registry/health`
	HealthVal = `{"health":"true"}`
)

type Backend interface {
	Start(ctx context.Context) error
	Get(ctx context.Context, key string, revision int64, keysOnly bool) (int64, *KeyValue, error)
	Create(ctx context.Context, key string, value []byte, lease int64) (int64, error)
	Delete(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error)
	List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (int64, []*KeyValue, error)
	ListStream(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) ListResult
	Count(ctx context.Context, key, end string, revision int64) (int64, int64, error)
	Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error)
	Watch(ctx context.Context, revision int64) WatchResult
	DbSize(ctx context.Context) (int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	Compact(ctx context.Context, revision int64) (int64, error)
}

// FastPutter is the optional two-statement bare-put fast path of
// OPTIMIZATION_ROUND_18.md: an unconditional put resolves in one existence
// check plus one append (create or fused CAS update). done=false means the
// fast path did not complete (race or capability disabled) and the caller
// must run the standard create→get→update flow.
type FastPutter interface {
	BarePutFastEnabled() bool
	PutFast(ctx context.Context, key string, value []byte, lease int64) (revision int64, prev *KeyValue, done bool, err error)
}

// OneStmtPut is the optional one-statement bare put of OPTIMIZATION_ROUND_19.md
// (strictly stronger than FastPutter): one statement classifies the key and
// appends the successor, arbitrated by the backend's unique index. A returned
// ErrKeyExists (or any other error) is handled by the caller's standard loop.
type OneStmtPutter interface {
	OneStmtPutEnabled() bool
	PutOne(ctx context.Context, key string, value []byte, lease int64) (revision, prevRevision, createRevision int64, err error)
}

type Dialect interface {
	ListCurrent(ctx context.Context, key, end string, limit int64, includeDeleted, keysOnly bool) (RowSet, error)
	List(ctx context.Context, key, end string, limit, revision int64, includeDeleted, keysOnly bool) (RowSet, error)
	CountCurrent(ctx context.Context, key, end string) (int64, int64, error)
	Count(ctx context.Context, key, end string, revision int64) (int64, int64, int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	After(ctx context.Context, key, end string, rev, limit int64) (RowSet, error)
	//nolint:revive
	Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value []byte) (int64, error)
	DeleteRevision(ctx context.Context, revision int64) error
	GetCompactRevision(ctx context.Context) (int64, error)
	SetCompactRevision(ctx context.Context, revision int64) error
	Compact(ctx context.Context, from, to int64) (int64, error)
	PostCompact(ctx context.Context) error
	Fill(ctx context.Context, revision int64) error
	IsFill(key string) bool
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
	GetSize(ctx context.Context) (int64, error)
	FillRetryDelay(ctx context.Context)
	TranslateStartKey(startKey string) string
}

type Transaction interface {
	Commit() error
	MustCommit()
	Rollback() error
	MustRollback()
	GetCompactRevision(ctx context.Context) (int64, error)
	SetCompactRevision(ctx context.Context, revision int64) error
	Compact(ctx context.Context, from, to int64) (int64, error)
	DeleteRevision(ctx context.Context, revision int64) error
	CurrentRevision(ctx context.Context) (int64, error)
}

// RowSet is the minimal row-consumption interface returned by the
// Dialect read methods. It is satisfied by both *sql.Rows (through an
// adapter) and by pgx-native rows, which additionally expose raw
// wire-format values for zero-copy scanning.
type RowSet interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
	// RawValues returns the raw wire-format column values of the current
	// row. The returned slices alias the driver's internal buffers and are
	// only valid until the next call to Next. Implementations that cannot
	// expose raw values return nil.
	RawValues() [][]byte
	// Binary reports whether every result field uses the binary wire
	// format (a precondition for interpreting RawValues).
	Binary() bool
}

// KeyValue is the primary KV representation.
type KeyValue struct {
	Key            string
	Value          []byte
	Version        int64
	CreateRevision int64
	ModRevision    int64
	Lease          int64
}

type EventBatch struct {
	Events     []*Event
	CurrentRev int64
}

func (e EventBatch) After(rev int64) EventBatch {
	events := e.Events
	for len(events) > 0 && events[0] != nil && events[0].KV.ModRevision <= rev {
		events = events[1:]
	}
	if len(events) != len(e.Events) {
		return EventBatch{Events: events, CurrentRev: e.CurrentRev}
	}
	return e
}

func (e EventBatch) String() string {
	return fmt.Sprintf("{current_rev:%d events:%v}", e.CurrentRev, e.Events)
}

type Event struct {
	Delete bool
	Create bool
	KV     *KeyValue
	PrevKV *KeyValue
}

func (e *Event) InRange(key, end string) bool {
	return e != nil && e.KV != nil && (key == "" || (end != "" && e.KV.Key >= key && e.KV.Key < end) || e.KV.Key == key)
}

func (e *Event) String() string {
	op := "="
	if e.Create {
		op = "+"
	} else if e.Delete {
		op = "-"
	}
	return fmt.Sprintf("%s%s@%d", op, e.KV.Key, e.KV.ModRevision)
}

type WatchResult struct {
	CurrentRevision int64
	CompactRevision int64
	Eventc          <-chan EventBatch
	Errorc          <-chan error
}

type ListResult struct {
	CurrentRevision int64
	KVc             <-chan *KeyValue
	Errorc          <-chan error
}

func unsupported(field string) error {
	return status.New(codes.Unimplemented, field+" is not implemented by kine").Err()
}
