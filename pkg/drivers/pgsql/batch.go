// Copyright 2026 The kine Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgsql

// Coalesced current-state point reads (OPTIMIZATION_ROUND_11.md B1).
//
// Current-state point GETs (revision=0, end="", tombstone-exclusive) that
// arrive while another point query is in flight are merged into one
// `WHERE name = ANY($1) AND deleted = 0` query issued after they arrived.
// Each request still observes a PostgreSQL snapshot taken at its batch's
// execution (the current_rev subquery is evaluated once per statement, so
// every member of a batch sees the same revision header — identical
// semantics to the unbatched single-key query's own snapshot). PostgreSQL
// stays authoritative; nothing is cached. With no query in flight a request
// is dispatched immediately, so single-client (c1) latency is unchanged.
//
// Opt-in via KINE_PGSQL_POINT_GET_BATCH=true; batch size
// KINE_PGSQL_POINT_GET_BATCH_SIZE (default 64), concurrent batched queries
// KINE_PGSQL_POINT_GET_BATCH_INFLIGHT (default 4). Requires the pgx-native
// read path (KINE_PGSQL_NATIVE_READS): the merged result is routed as raw
// binary wire values to per-request memory row sets.

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/server"
)

// pointBatch is one open or executing merged query.
type pointBatch struct {
	keys    []string
	members []chan batchResult
}

type batchResult struct {
	rs  server.RowSet
	err error
}

type pointGetBatcher struct {
	mu      sync.Mutex
	open    []*pointBatch // queued, not yet dispatched (FIFO head first)
	drawn   int           // batches currently executing
	maxSize int
	maxExec int
	exec    batchExecFunc
}

type batchExecFunc func(ctx context.Context, keys []string) ([][][]byte, error)

func newPointGetBatcher(maxSize, maxExec int, exec batchExecFunc) *pointGetBatcher {
	return &pointGetBatcher{maxSize: maxSize, maxExec: maxExec, exec: exec}
}

// get joins (or creates and possibly dispatches) a batch for one point read.
// The goroutine that dispatches a batch runs the merged query and delivers
// every member's result through its channel, including its own.
func (b *pointGetBatcher) get(ctx context.Context, key string) (server.RowSet, error) {
	metrics.CurrentStateReads.WithLabelValues("get_value_batched").Inc()
	ch := make(chan batchResult, 1)

	b.mu.Lock()
	// join the oldest open batch that still has room
	var batch *pointBatch
	for _, open := range b.open {
		if len(open.keys) < b.maxSize {
			batch = open
			break
		}
	}
	if batch == nil {
		batch = &pointBatch{keys: []string{key}, members: []chan batchResult{ch}}
		b.open = append(b.open, batch)
	} else {
		batch.keys = append(batch.keys, key)
		batch.members = append(batch.members, ch)
	}

	runKeys, runMembers := (*[]string)(nil), (*[]chan batchResult)(nil)
	if b.drawn < b.maxExec && (b.drawn == 0 || len(batch.keys) == b.maxSize) {
		// no batched query in flight (or the batch filled): issue now, after
		// the requests that already arrived; this goroutine runs the query
		b.dispatchLocked(batch)
		runKeys, runMembers = &batch.keys, &batch.members
	}
	b.mu.Unlock()

	if runKeys != nil {
		b.run(context.WithoutCancel(ctx), *runKeys, *runMembers)
	}

	select {
	case res := <-ch:
		return res.rs, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dispatchLocked marks batch as executing and drops it from the open queue.
// The caller still holds b.mu.
func (b *pointGetBatcher) dispatchLocked(batch *pointBatch) {
	b.drawn++
	for i, open := range b.open {
		if open == batch {
			b.open = append(b.open[:i], b.open[i+1:]...)
			break
		}
	}
}

// run executes one merged query, delivers every member its row set, then
// promotes queued batches while the in-flight limit allows. Iterative: each
// promoted batch executes in this same goroutine's loop (no recursion).
func (b *pointGetBatcher) run(ctx context.Context, keys []string, members []chan batchResult) {
	for {
		rows, err := b.exec(ctx, keys)
		if err != nil {
			for _, m := range members {
				m <- batchResult{rs: nil, err: err}
			}
		} else {
			// route each row to the member whose key it answers (column 4
			// is name: varchar wire value, framing already stripped by pgx).
			// kine_current holds one row per key, so byKey never accumulates.
			byKey := make(map[string][][]byte, len(rows))
			for _, raw := range rows {
				byKey[string(raw[3])] = raw
			}
			for i, key := range keys {
				if raws, ok := byKey[key]; ok {
					members[i] <- batchResult{rs: &memRowSet{rows: [][][]byte{raws}}, err: nil}
				} else {
					members[i] <- batchResult{rs: &memRowSet{}, err: nil}
				}
			}
		}

		b.mu.Lock()
		b.drawn--
		if len(b.open) > 0 && b.drawn < b.maxExec {
			next := b.open[0]
			b.dispatchLocked(next)
			keys, members = next.keys, next.members
			b.mu.Unlock()
			continue
		}
		b.mu.Unlock()
		return
	}
}

// memRowSet serves one member of a merged point query from raw binary
// wire-format values already copied out of the pgx buffer. Binary() is
// always true, so sqllog's scanFast path consumes RawValues directly with
// zero re-encoding; Scan is implemented for completeness.
type memRowSet struct {
	rows [][][]byte
	i    int // index of the row the consumer has consumed up to
}

func (m *memRowSet) Next() bool {
	if m.i < len(m.rows) {
		m.i++
		return true
	}
	return false
}

// RawValues returns the raw values of the current row; they are owned
// copies, so the "valid until next Next" contract of RowSet is trivially met.
func (m *memRowSet) RawValues() [][]byte {
	if m.i == 0 || m.i > len(m.rows) {
		return nil
	}
	return m.rows[m.i-1]
}

func (m *memRowSet) Binary() bool { return true }

func (m *memRowSet) Close() {}

func (m *memRowSet) Err() error { return nil }

// Scan mirrors pgx's binary decoding for the current-state get column
// layout: current_rev (int8), compact_rev (int8), id (int8), name (varchar),
// created (int4), deleted (int4), create_revision (int8), prev_revision
// (int8), lease (int4)[, value (bytea)]. Used only by the database/sql-style
// fallback scanner; the raw-value fast path never calls it.
func (m *memRowSet) Scan(dest ...any) error {
	raw := m.RawValues()
	if raw == nil {
		return fmt.Errorf("memRowSet: Scan called before Next")
	}
	if len(dest) > len(raw) {
		return fmt.Errorf("memRowSet: expected at least %d columns, got %d", len(dest), len(raw))
	}
	for i, d := range dest {
		if raw[i] == nil {
			continue
		}
		switch out := d.(type) {
		case *int64:
			// int8 columns decode directly; int4 columns (created, deleted,
			// lease) arrive as 4-byte values
			if len(raw[i]) == 4 {
				*out = int64(int32(binary.BigEndian.Uint32(raw[i])))
			} else if len(raw[i]) == 8 {
				*out = int64(binary.BigEndian.Uint64(raw[i]))
			} else {
				return fmt.Errorf("memRowSet: int column %d: unexpected width %d", i, len(raw[i]))
			}
		case *string:
			*out = string(raw[i])
		case *[]byte:
			*out = raw[i]
		default:
			return fmt.Errorf("memRowSet: unsupported dest type at column %d", i)
		}
	}
	return nil
}
