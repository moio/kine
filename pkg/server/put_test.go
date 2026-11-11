package server

import (
	"context"
	"errors"
	"testing"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// fakeBackend implements Backend with a scripted create/get/update sequence.
// All other methods are unsupported for this test.
type fakeBackend struct {
	// creates are consumed in order; ErrKeyExists entries trigger the
	// get/update path.
	creates []error
	// gets returns the ModRevision handed to the subsequent Update.
	getModRev int64
	// updates are consumed in order: (rev, updated, err) per call.
	updates []fakeUpdate

	createCalls int
	updateCalls int
}

type fakeUpdate struct {
	rev     int64
	updated bool
	err     error
}

func (f *fakeBackend) Start(ctx context.Context) error                       { return nil }
func (f *fakeBackend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	if len(f.creates) == 0 {
		return 0, errors.New("unexpected Create call")
	}
	e := f.creates[0]
	f.creates = f.creates[1:]
	f.createCalls++
	if e == nil {
		// successful create: return a fresh revision
		return int64(100 + f.createCalls), nil
	}
	return int64(f.createCalls), e
}
func (f *fakeBackend) Get(ctx context.Context, key string, revision int64, keysOnly bool) (int64, *KeyValue, error) {
	return revision, &KeyValue{ModRevision: f.getModRev}, nil
}
func (f *fakeBackend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error) {
	if len(f.updates) == 0 {
		return 0, nil, false, errors.New("unexpected Update call")
	}
	u := f.updates[0]
	f.updates = f.updates[1:]
	f.updateCalls++
	return u.rev, nil, u.updated, u.err
}
func (f *fakeBackend) Delete(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error) {
	return 0, nil, false, errors.New("unexpected Delete call")
}
func (f *fakeBackend) List(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) (int64, []*KeyValue, error) {
	return 0, nil, errors.New("unexpected List call")
}
func (f *fakeBackend) ListStream(ctx context.Context, key, end string, limit, revision int64, keysOnly bool) ListResult {
	return ListResult{}
}
func (f *fakeBackend) Count(ctx context.Context, key, end string, revision int64) (int64, int64, error) {
	return 0, 0, errors.New("unexpected Count call")
}
func (f *fakeBackend) Watch(ctx context.Context, revision int64) WatchResult {
	return WatchResult{}
}
func (f *fakeBackend) DbSize(ctx context.Context) (int64, error)    { return 0, nil }
func (f *fakeBackend) CurrentRevision(ctx context.Context) (int64, error) {
	return 0, nil
}
func (f *fakeBackend) Compact(ctx context.Context, revision int64) (int64, error) {
	return 0, nil
}

// TestPutLostUpdateIsRetried verifies that a Put whose compare-and-swap in
// Update fails (because another writer changed the key between Get and
// Update) retries the create/update sequence instead of returning success
// for a write that never happened. etcd Put is unconditional
// last-writer-wins; kine must behave the same.
func TestPutLostUpdateIsRetried(t *testing.T) {
	backend := &fakeBackend{
		creates:   []error{ErrKeyExists, nil},
		getModRev: 2,
		updates: []fakeUpdate{
			// first attempt loses the race: CAS mismatch, no error,
			// nothing written (matches LogStructured.Update semantics)
			{rev: 3, updated: false, err: nil},
		},
	}
	ls := &LimitedServer{backend: backend}

	resp, err := ls.Put(context.Background(), &etcdserverpb.PutRequest{
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if backend.updateCalls != 1 {
		t.Fatalf("expected exactly one failed Update attempt, got %d", backend.updateCalls)
	}
	if resp.Header.Revision != 102 { // revision of the retry Create
		t.Fatalf("Put reported revision %d from a failed CAS instead of the retrying create", resp.Header.Revision)
	}
	if len(backend.creates) != 0 || len(backend.updates) != 0 {
		t.Fatal("scripted calls not fully consumed")
	}
}

// TestPutUpdateErrorPropagates verifies that a real Update error is returned.
func TestPutUpdateErrorPropagates(t *testing.T) {
	sentinel := errors.New("boom")
	backend := &fakeBackend{
		creates:   []error{ErrKeyExists},
		getModRev: 2,
		updates:   []fakeUpdate{{rev: 3, updated: false, err: sentinel}},
	}
	ls := &LimitedServer{backend: backend}

	_, err := ls.Put(context.Background(), &etcdserverpb.PutRequest{
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected Update error to propagate, got %v", err)
	}
}

// TestPutCreateSuccess verifies the plain create path is unchanged.
func TestPutCreateSuccess(t *testing.T) {
	backend := &fakeBackend{
		creates: []error{nil},
	}
	ls := &LimitedServer{backend: backend}

	resp, err := ls.Put(context.Background(), &etcdserverpb.PutRequest{
		Key:   []byte("k"),
		Value: []byte("v"),
	})
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if resp.Header.Revision != 101 {
		t.Fatalf("unexpected revision %d", resp.Header.Revision)
	}
}
