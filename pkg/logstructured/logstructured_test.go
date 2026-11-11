package logstructured

import (
	"context"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
)

// fakeLog implements the Log interface with scripted Append/List behaviour.
type fakeLog struct {
	// appendRev/appendErr simulate the driver-level insert CAS: a failed CAS
	// returns rev=0 with a nil error (rows-affected == 0), matching
	// Generic.Insert semantics.
	appendRev int64
	appendErr error
	// listEvents is returned by List for the fallback lookup.
	listEvents []*server.Event
}

func (f *fakeLog) Start(ctx context.Context) error                    { return nil }
func (f *fakeLog) CompactRevision(ctx context.Context) (int64, error) { return 0, nil }
func (f *fakeLog) CurrentRevision(ctx context.Context) (int64, error) { return 10, nil }
func (f *fakeLog) List(ctx context.Context, key, end string, limit, revision int64, includeDeletes, keysOnly bool) (server.EventBatch, error) {
	return server.EventBatch{CurrentRev: 10, Events: f.listEvents}, nil
}
func (f *fakeLog) ListStream(ctx context.Context, key, end string, limit, revision int64, includeDeletes, keysOnly bool) server.ListResult {
	return server.ListResult{}
}
func (f *fakeLog) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	return 0, 0, nil
}
func (f *fakeLog) After(ctx context.Context, key, end string, revision, limit int64) (server.EventBatch, error) {
	return server.EventBatch{}, nil
}
func (f *fakeLog) Watch(ctx context.Context) <-chan server.EventBatch { return nil }
func (f *fakeLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	return f.appendRev, f.appendErr
}
func (f *fakeLog) DbSize(ctx context.Context) (int64, error) { return 0, nil }
func (f *fakeLog) Compact(ctx context.Context, revision int64) (int64, error) {
	return 0, nil
}

// TestDeleteOfKeyWithoutHistory asserts etcd semantics for the txn
// compare-and-delete fallback: when the CAS insert fails and the key has no
// history rows (never existed, or fully compacted including the tombstone),
// the compare is evaluated as FALSE, so the delete reports deleted=false.
// kine previously reported deleted=true here, which made a stale-rev txn
// delete of an already-deleted compacted key succeed where etcd fails the
// compare.
func TestDeleteOfKeyWithoutHistory(t *testing.T) {
	ls := New(&fakeLog{appendRev: 0}) // CAS insert fails (0 rows), no history
	rev, kv, deleted, err := ls.Delete(context.Background(), "k", 4)
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if deleted {
		t.Fatalf("deleted=true for a key without history rows; etcd evaluates the compare as false")
	}
	if rev != 10 {
		t.Fatalf("unexpected revision %d, want current rev 10", rev)
	}
	if kv != nil {
		t.Fatalf("unexpected KV returned for a failed compare")
	}
}

// TestDeleteOfDeletedKeyRetainedTombstone asserts the tombstone-present case:
// kine and etcd both evaluate the compare as false for an already-deleted key.
func TestDeleteOfDeletedKeyRetainedTombstone(t *testing.T) {
	ls := New(&fakeLog{
		appendRev:  0, // CAS insert fails
		listEvents: []*server.Event{{Delete: true, KV: &server.KeyValue{Key: "k"}}},
	})
	_, _, deleted, err := ls.Delete(context.Background(), "k", 4)
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if deleted {
		t.Fatalf("deleted=true for an already-deleted key; etcd evaluates the compare as false")
	}
}

// TestDeleteSuccess asserts the normal path: compare passes, CAS insert
// succeeds, delete=true.
func TestDeleteSuccess(t *testing.T) {
	ls := New(&fakeLog{
		appendRev:  11,
		listEvents: []*server.Event{{KV: &server.KeyValue{Key: "k", ModRevision: 4}}},
	})
	rev, _, deleted, err := ls.Delete(context.Background(), "k", 4)
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if !deleted {
		t.Fatalf("deleted=false for a successful delete")
	}
	if rev != 11 {
		t.Fatalf("unexpected revision %d, want 11", rev)
	}
}

// TestDeleteLiveKeyStaleCompare asserts the live-key stale compare case.
func TestDeleteLiveKeyStaleCompare(t *testing.T) {
	ls := New(&fakeLog{
		appendRev:  0, // CAS insert fails (key moved on)
		listEvents: []*server.Event{{KV: &server.KeyValue{Key: "k", ModRevision: 6}}},
	})
	_, kv, deleted, err := ls.Delete(context.Background(), "k", 4)
	if err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if deleted {
		t.Fatalf("deleted=true for a stale compare on a live key")
	}
	if kv == nil || kv.ModRevision != 6 {
		t.Fatalf("expected current KV with modrev 6, got %+v", kv)
	}
}
