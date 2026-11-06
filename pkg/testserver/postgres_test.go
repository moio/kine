package testserver

// Feature-enabled PostgreSQL tests for the current-state mirror
// (OPTIMIZATION_ROUND_9.md §3 / sub-plan OPTIMIZATION_ROUND_9_0.md).
//
// These tests run the real kine gRPC endpoint against a real PostgreSQL
// server, through the same endpoint.Listen path used in production, and
// exercise public client semantics (clientv3 + the kubernetes typed
// wrapper) rather than driver internals. They are skipped unless
// KINE_TEST_PG_DSN is set, so a plain `go test ./...` without a database
// stays green. Each test gets its own database (setDatabasePath appends a
// content hash to the DSN path), created by the pgsql driver's setup.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/app"
	"github.com/k3s-io/kine/pkg/endpoint"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const testDSNEnv = "KINE_TEST_PG_DSN"

// startPostgresKineRaw starts a kine endpoint and returns the client plus an
// explicit stop function, for tests that need to control the process
// lifecycle (restart tests). The caller must invoke stop exactly once.
func startPostgresKineRaw(t *testing.T, dsn string) (*kubernetes.Client, func()) {
	t.Helper()
	if dsn == "" {
		t.Skipf("%s not set; skipping PostgreSQL feature test", testDSNEnv)
	}
	logrus.SetLevel(logrus.ErrorLevel)
	logrus.SetOutput(t.Output())

	t.Setenv("KINE_PGSQL_NATIVE_READS", "true")
	t.Setenv("KINE_PGSQL_CURRENT_STATE", "true")

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	config := app.Config(nil)
	config.WaitGroup = wg
	config.Listener = "unix://" + t.TempDir() + "/kine.sock"
	config.NotifyInterval = 5 * time.Second
	config.CompactInterval = 0
	config.CompactMinRetain = 0
	config.Endpoint = dsn

	e, err := endpoint.Listen(ctx, config)
	if err != nil {
		cancel()
		return nil, func() {}
	}

	tlsConfig, err := e.TLSConfig.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	client, err := kubernetes.New(clientv3.Config{
		TLS:         tlsConfig,
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		Logger:      zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("kine-client"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, func() {
		cancel()
		wg.Wait()
	}
}

// startPostgresKine starts a kine endpoint against the given PostgreSQL DSN
// with the current-state mirror enabled, and returns a typed client whose
// embedded *clientv3.Client provides raw KV/Txn/Compact access.
func startPostgresKine(t *testing.T, dsn string) *kubernetes.Client {
	t.Helper()
	if dsn == "" {
		t.Skipf("%s not set; skipping PostgreSQL feature test", testDSNEnv)
	}
	logrus.SetLevel(logrus.ErrorLevel)
	logrus.SetOutput(t.Output())

	// Mirror + native reads on for every test; individual tests may add
	// more environment via t.Setenv before calling this.
	t.Setenv("KINE_PGSQL_NATIVE_READS", "true")
	t.Setenv("KINE_PGSQL_CURRENT_STATE", "true")

	ctx, cancel := context.WithCancel(t.Context())
	wg := &sync.WaitGroup{}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	config := app.Config(nil)
	config.WaitGroup = wg
	config.Listener = "unix://" + t.TempDir() + "/kine.sock"
	config.NotifyInterval = 5 * time.Second
	// Compaction tests drive Compact explicitly; no background compaction.
	config.CompactInterval = 0
	config.CompactMinRetain = 0
	config.Endpoint = dsn

	e, err := endpoint.Listen(ctx, config)
	if err != nil {
		t.Fatalf("failed to start kine endpoint: %v", err)
	}

	tlsConfig, err := e.TLSConfig.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	client, err := kubernetes.New(clientv3.Config{
		TLS:         tlsConfig,
		Endpoints:   e.Endpoints,
		DialTimeout: 10 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		Logger:      zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("kine-client"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// pgTestDSN returns the per-test database DSN. The hash suffix gives every
// test its own database, so tests are order-independent.
func pgTestDSN(t *testing.T) string {
	t.Helper()
	base := os.Getenv(testDSNEnv)
	if base == "" {
		t.Skipf("%s not set; skipping PostgreSQL feature test", testDSNEnv)
	}
	dsn, err := setDatabasePath(base, t.TempDir())
	if err != nil {
		t.Fatalf("failed to derive test database DSN: %v", err)
	}
	return dsn
}

// openRawDB opens a direct database/sql connection to the test database for
// mirror-content inspection. The pgx stdlib driver is registered by the pgsql
// driver package.
func openRawDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("raw DB open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestPGT2DeleteCompactRestart covers decision D1: after compaction removes
// a key's tombstone from the history log (PostgreSQL CompactSQL deletes even
// latest-revision tombstones), the retained mirror tombstone must NOT be
// treated as mirror corruption at startup. Before the D1 repair the startup
// count check sees COUNT(DISTINCT name) history < COUNT(*) mirror and
// destructively rebuilds the mirror from the log, losing the tombstone.
func TestPGT2DeleteCompactRestart(t *testing.T) {
	dsn := pgTestDSN(t)

	client, stop := startPostgresKineRaw(t, dsn)
	ctx := context.Background()

	// seed two keys, delete one
	r1, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("kA"), "=", 0)).
		Then(clientv3.OpPut("kA", "va")).Commit()
	if err != nil || !r1.Succeeded {
		t.Fatalf("seed kA: err=%v succ=%v", err, r1 != nil && r1.Succeeded)
	}
	r2, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("kB"), "=", 0)).
		Then(clientv3.OpPut("kB", "vb")).Commit()
	if err != nil || !r2.Succeeded {
		t.Fatalf("seed kB: err=%v succ=%v", err, r2 != nil && r2.Succeeded)
	}
	// delete kA via the supported CAS txn shape (plain Delete RPC is
	// Unimplemented on kine)
	gk, err := client.Client.Get(ctx, "kA")
	if err != nil || len(gk.Kvs) != 1 {
		t.Fatalf("get kA before delete: err=%v kvs=%v", err, gk.Kvs)
	}
	dr, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("kA"), "=", gk.Kvs[0].ModRevision)).
		Then(clientv3.OpDelete("kA")).Else(clientv3.OpGet("kA")).Commit()
	if err != nil || !dr.Succeeded {
		t.Fatalf("delete kA: err=%v succ=%v", err, dr != nil && dr.Succeeded)
	}
	var deletedCount int64
	for _, resp := range dr.Responses {
		if d := resp.GetResponseDeleteRange(); d != nil {
			deletedCount += d.Deleted
		}
	}
	if deletedCount != 1 {
		t.Fatalf("delete kA: deleted=%d", deletedCount)
	}
	compactRev := dr.Header.Revision

	// compact everything up to and including the tombstone
	_, err = client.Compact(ctx, compactRev)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	// wait for physical compaction to remove the log rows <= compactRev
	raw := openRawDB(t, dsn)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := raw.QueryRow(`SELECT COUNT(*) FROM kine WHERE id <= $1 AND name = 'kA'`, compactRev).Scan(&n); err != nil {
			t.Fatalf("compaction poll: %v", err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("log rows for kA not compacted within 10s")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// a fresh write keeps MAX(id) equal on log and mirror while the count
	// check disagrees (kA's tombstone survives only in the mirror)
	r3, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("kC"), "=", 0)).
		Then(clientv3.OpPut("kC", "vc")).Commit()
	if err != nil || !r3.Succeeded {
		t.Fatalf("seed kC: err=%v succ=%v", err, r3 != nil && r3.Succeeded)
	}
	stop()

	// restart: setupCurrentState runs the sync check
	client2, stop2 := startPostgresKineRaw(t, dsn)
	defer stop2()

	// D1: the retained tombstone must survive the restart
	var tomb int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM kine_current WHERE name = 'kA' AND deleted = 1`).Scan(&tomb); err != nil {
		t.Fatalf("tombstone check: %v", err)
	}
	if tomb != 1 {
		t.Errorf("D1 BUG: mirror tombstone for kA lost across restart (count=%d) — destructive rebuild ran", tomb)
	}

	// public semantics after restart
	g, err := client2.Client.Get(ctx, "kA")
	if err != nil || len(g.Kvs) != 0 {
		t.Fatalf("GET deleted kA after restart: err=%v kvs=%v", err, g.Kvs)
	}
	// delete-of-deleted via the supported get+delete txn shape (Then-only
	// with Get+Delete on one key) must report deleted=false
	tr, err := client2.Client.Txn(ctx).Then(clientv3.OpGet("kA"), clientv3.OpDelete("kA")).Commit()
	if err != nil {
		t.Fatalf("delete-of-deleted txn: %v", err)
	}
	var delDeleted int64
	if len(tr.Responses) >= 2 {
		if d := tr.Responses[1].GetResponseDeleteRange(); d != nil {
			delDeleted = d.Deleted
		}
	}
	if delDeleted != 0 {
		t.Errorf("delete-of-deleted reported deleted=%d, want 0 (etcd semantics)", delDeleted)
	}
	// recreate works
	rr, err := client2.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("kA"), "=", 0)).
		Then(clientv3.OpPut("kA", "va2")).Commit()
	if err != nil || !rr.Succeeded {
		t.Fatalf("recreate kA after restart: err=%v succ=%v", err, rr != nil && rr.Succeeded)
	}
}

// TestPGT1BasicSemantics covers create/update/failed-CAS/delete/recreate,
// binary and NUL-containing values, and write-ack -> GET/LIST/COUNT
// visibility on the mirror path (sub-plan 9.0 T1).
func TestPGT1BasicSemantics(t *testing.T) {
	client := startPostgresKine(t, pgTestDSN(t))
	ctx := context.Background()

	// create shape: Compare(ModRevision==0), Then(Put), no Else
	create := func(key, val string) *clientv3.TxnResponse {
		resp, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, val)).Commit()
		if err != nil {
			t.Fatalf("create %s: %v", key, err)
		}
		return resp
	}
	update := func(key, val string, rev int64) *clientv3.TxnResponse {
		resp, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
			Then(clientv3.OpPut(key, val)).Else(clientv3.OpGet(key)).Commit()
		if err != nil {
			t.Fatalf("update %s: %v", key, err)
		}
		return resp
	}
	casDelete := func(key string, rev int64) *clientv3.TxnResponse {
		resp, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
			Then(clientv3.OpDelete(key)).Else(clientv3.OpGet(key)).Commit()
		if err != nil {
			t.Fatalf("casDelete %s: %v", key, err)
		}
		return resp
	}

	// create
	r1 := create("k1", "v1")
	if !r1.Succeeded {
		t.Fatal("create of absent key did not succeed")
	}
	rev1 := r1.Header.Revision

	// binary + NUL-containing values are legal in values (keys stay NUL-free)
	rbin := create("kbin", "\x00\x01\xff")
	if !rbin.Succeeded {
		t.Fatal("create with NUL/binary value failed")
	}
	got, err := client.Client.Get(ctx, "kbin")
	if err != nil || string(got.Kvs[0].Value) != "\x00\x01\xff" {
		t.Fatalf("binary value round-trip: err=%v val=%q", err, got.Kvs)
	}

	// update with correct revision
	r2 := update("k1", "v2", rev1)
	if !r2.Succeeded {
		t.Fatalf("update with matching revision failed")
	}

	// failed CAS: stale revision
	r3 := update("k1", "v3-stale", rev1)
	if r3.Succeeded {
		t.Fatal("update with stale revision succeeded")
	}

	// write-ack -> GET
	got, err = client.Client.Get(ctx, "k1")
	if err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].Value) != "v2" {
		t.Fatalf("GET after ack: err=%v kvs=%v", err, got.Kvs)
	}
	if got.Header.Revision < got.Kvs[0].ModRevision {
		t.Fatalf("header revision %d predates returned KV mod revision %d", got.Header.Revision, got.Kvs[0].ModRevision)
	}

	// casDelete with matching revision
	r4 := casDelete("k1", got.Kvs[0].ModRevision)
	if !r4.Succeeded {
		t.Fatal("CAS delete with matching revision failed")
	}

	// GET after delete -> not found, valid header
	got, err = client.Client.Get(ctx, "k1")
	if err != nil {
		t.Fatalf("GET deleted key: %v", err)
	}
	if len(got.Kvs) != 0 {
		t.Fatalf("GET deleted key returned kvs: %v", got.Kvs)
	}
	if got.Count != 0 {
		t.Fatalf("GET deleted key count=%d", got.Count)
	}

	// recreate
	r5 := create("k1", "v4")
	if !r5.Succeeded {
		t.Fatal("recreate after delete failed")
	}

	// LIST / COUNT visibility
	ls, err := client.Client.Get(ctx, "k", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("LIST: %v", err)
	}
	if len(ls.Kvs) != 2 {
		t.Fatalf("LIST expected 2 live keys, got %d", len(ls.Kvs))
	}
	cnt, err := client.Client.Get(ctx, "k", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if cnt.Count != 2 {
		t.Fatalf("COUNT expected 2, got %d", cnt.Count)
	}
}

// TestPGT3AllKeyCountOnly covers the all-key CountOnly case (sub-plan 9.0
// T3 / decision D2). The all-key request normalizes to key="", end="".
// Two facts established here:
//
//  1. Before the D2 repair the mirror returned 0 (bounded count query over
//     an empty range); after the repair it counts all live mirror rows.
//  2. kine's all-key reads include internal keys (compact_rev_key,
//     /registry/health) — the history path's AnyNameSQL does the same — so
//     the correct invariant is COUNT(all-key) == len(LIST(all-key)), not a
//     count of user keys only. The D2 repair is also strictly better than
//     the history path, whose all-key count (base Generic.CountCurrent with
//     BetweenNameSQL) returns 0 for key="", end="".
func TestPGT3AllKeyCountOnly(t *testing.T) {
	client := startPostgresKine(t, pgTestDSN(t))
	ctx := context.Background()

	const n = 7
	for i := 0; i < n; i++ {
		_, err := client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(fmt.Sprintf("k%02d", i)), "=", 0)).
			Then(clientv3.OpPut(fmt.Sprintf("k%02d", i), "v")).Commit()
		if err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	// all-key COUNT must equal the all-key LIST length (internal keys are
	// part of kine's all-key reads on both paths)
	resp, err := client.Client.Get(ctx, "\x00", clientv3.WithFromKey(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatalf("all-key COUNT: %v", err)
	}
	all, err := client.Client.Get(ctx, "\x00", clientv3.WithFromKey())
	if err != nil {
		t.Fatalf("all-key LIST: %v", err)
	}
	if resp.Count != int64(len(all.Kvs)) {
		t.Errorf("all-key COUNT %d != all-key LIST length %d", resp.Count, len(all.Kvs))
	}
	if resp.Count < n {
		t.Errorf("all-key COUNT %d < seeded user keys %d (D2 bug if it was 0)", resp.Count, n)
	}

	// bounded-range count still correct and excludes internals
	bc, err := client.Client.Get(ctx, "k", clientv3.WithRange("l"), clientv3.WithCountOnly())
	if err != nil {
		t.Fatalf("bounded COUNT: %v", err)
	}
	if bc.Count != n {
		t.Errorf("bounded COUNT: got %d, want %d", bc.Count, n)
	}
}

// TestPGT4EmptyReads covers empty GET and empty LIST metadata behavior
// (sub-plan 9.0 T4 / decision D3). Characterization: an empty result must
// not error and must carry a valid header revision.
func TestPGT4EmptyReads(t *testing.T) {
	client := startPostgresKine(t, pgTestDSN(t))
	ctx := context.Background()

	// empty single-key GET
	got, err := client.Client.Get(ctx, "never-created")
	if err != nil {
		t.Fatalf("empty GET errored: %v", err)
	}
	if len(got.Kvs) != 0 {
		t.Fatalf("empty GET returned kvs: %v", got.Kvs)
	}

	// empty range LIST — a range that cannot contain kine's internal keys
	// (compact_rev_key, /registry/health). Empty ranges that DO overlap the
	// internal keys return them on both the history and mirror paths
	// (AnyNameSQL upstream behavior), covered by TestPGT3AllKeyCountOnly.
	ls, err := client.Client.Get(ctx, "zzza", clientv3.WithRange("zzzz"))
	if err != nil {
		t.Fatalf("empty LIST errored: %v", err)
	}
	if len(ls.Kvs) != 0 {
		t.Fatalf("empty LIST returned kvs: %v", ls.Kvs)
	}

	// empty all-key LIST on a fresh database: no error, valid header. The
	// two internal keys (compact_rev_key, /registry/health) are expected —
	// kine's all-key reads include them on the history path too (see
	// TestPGT3AllKeyCountOnly).
	al, err := client.Client.Get(ctx, "\x00", clientv3.WithFromKey())
	if err != nil {
		t.Fatalf("empty all-key LIST errored: %v", err)
	}
	for _, kv := range al.Kvs {
		if k := string(kv.Key); k != "compact_rev_key" && k != "/registry/health" {
			t.Fatalf("empty all-key LIST returned unexpected key %q", k)
		}
	}
}
