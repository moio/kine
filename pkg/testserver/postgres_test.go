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
)

const testDSNEnv = "KINE_TEST_PG_DSN"

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

// restartClient closes the current client (but not the kine process) and
// connects a fresh client to the same database. Used for restart tests:
// the kine process itself stays up, so mirror re-verification happens
// through a new driver instance below, not here. For a full process
// restart, start a new endpoint against the same DSN.
func restartClient(t *testing.T, dsn string) *kubernetes.Client {
	t.Helper()
	return startPostgresKine(t, dsn)
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
