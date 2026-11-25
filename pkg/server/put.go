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

	// OPTIMIZATION_ROUND_19.md: the one-statement bare put, when supported,
	// resolves the whole put in a single statement. ErrKeyExists (lost arbiter
	// race) falls through to the standard create→get→update loop, which is
	// also how the standard path behaves on a lost race.
	if osp, ok := l.backend.(OneStmtPutter); ok && osp.OneStmtPutEnabled() {
		rev, prevRev, createRev, err := osp.PutOne(ctx, key, r.Value, r.Lease)
		if err == nil {
			var kv *KeyValue
			if prevRev != 0 {
				// loop-entered success: the standard path returns the new KV
				// as PrevKv; a first-ever create (prevRev == 0) returns nil
				kv = &KeyValue{
					Key:            key,
					Value:          r.Value,
					Lease:          r.Lease,
					ModRevision:    rev,
					CreateRevision: createRev,
				}
			}
			if !r.PrevKv {
				kv = nil
			}
			return &etcdserverpb.PutResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: rev},
				PrevKv: toKV(kv),
			}, nil
		}
		if err != ErrKeyExists {
			return nil, err
		}
		// lost race → standard loop handles it identically
	}

	// OPTIMIZATION_ROUND_18.md: when the backend supports the two-statement
	// bare-put fast path, try it first. done=false (race, capability off or
	// read error) falls through to the standard create→get→update loop, which
	// reproduces the original semantics exactly.
	if fp, ok := l.backend.(FastPutter); ok && fp.BarePutFastEnabled() {
		rev, kv, done, err := fp.PutFast(ctx, key, r.Value, r.Lease)
		if err != nil {
			return nil, err
		}
		if done {
			if !r.PrevKv {
				kv = nil
			}
			return &etcdserverpb.PutResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: rev},
				PrevKv: toKV(kv),
			}, nil
		}
	}

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
