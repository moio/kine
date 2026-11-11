package server

import (
	"bytes"
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func (l *LimitedServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if r.IgnoreValue {
		return nil, unsupported("ignoreValue")
	}
	if r.IgnoreLease {
		return nil, unsupported("ignoreLease")
	}

	var kv *KeyValue
	// redirect apiserver get to the substitute compact revision key
	// response is fixed up in toKV()
	if bytes.Equal(r.Key, compactRevKey) {
		r.Key = compactRevAPI
	}

	key := string(r.Key)
	rev, err := l.backend.Create(ctx, key, r.Value, r.Lease)
	for err == ErrKeyExists {
		rev, kv, err = l.backend.Get(ctx, key, rev, false)
		if err != nil {
			return nil, err
		}
		var updated bool
		rev, kv, updated, err = l.backend.Update(ctx, key, r.Value, kv.ModRevision, r.Lease)
		if err != nil {
			return nil, err
		}
		if updated {
			break
		}
		// The key was modified or deleted by another writer between Get and
		// Update, so the compare-and-swap in Update failed and nothing was
		// written. etcd Put is an unconditional last-writer-wins operation;
		// retry the create/update sequence instead of returning success for
		// a write that never happened.
		kv = nil
		rev, err = l.backend.Create(ctx, key, r.Value, r.Lease)
	}
	if err != nil {
		return nil, err
	}

	if !r.PrevKv {
		kv = nil
	}

	return &etcdserverpb.PutResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: rev},
		PrevKv: toKV(kv),
	}, nil
}
