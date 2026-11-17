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

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeExec answers keys with one synthetic row per key: columns are
// [current_rev, compact_rev, id, name, created, deleted, create_revision,
// prev_revision, lease] as binary wire values.
func fakeExec() (batchExecFunc, *int) {
	calls := 0
	var mu sync.Mutex
	return func(ctx context.Context, keys []string) ([][][]byte, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		var out [][][]byte
		for i, k := range keys {
			row := [][]byte{
				i8(100 + int64(i)), i8(0), i8(10 + int64(i)),
				[]byte(k), i4(1), i4(0), i8(10 + int64(i)), i8(0), i4(0),
			}
			out = append(out, row)
		}
		return out, nil
	}, &calls
}

func i8(v int64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[7-i] = byte(v >> (8 * i))
	}
	return b
}

func i4(v int64) []byte {
	b := make([]byte, 4)
	for i := 0; i < 4; i++ {
		b[3-i] = byte(v >> (8 * i))
	}
	return b
}

// consume drains a row set the way sqllog's scanFast would (RawValues path).
func consume(rs interface {
	Next() bool
	RawValues() [][]byte
	Binary() bool
	Close()
	Err() error
}) (key string, modrev int64, ok bool) {
	if !rs.Next() {
		return "", 0, false
	}
	raw := rs.RawValues()
	rev := int64(0)
	for i := 0; i < 8; i++ {
		rev = rev<<8 | int64(raw[2][i])
	}
	return string(raw[3]), rev, true
}

func TestSequentialDispatchesImmediately(t *testing.T) {
	exec, calls := fakeExec()
	b := newPointGetBatcher(64, 4, exec)
	for i := 0; i < 3; i++ {
		rs, err := b.get(context.Background(), fmt.Sprintf("k%d", i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		key, _, ok := consume(rs)
		if !ok || key != fmt.Sprintf("k%d", i) {
			t.Fatalf("get %d: got key %q ok=%v", i, key, ok)
		}
	}
	if *calls != 3 {
		t.Fatalf("expected 3 dispatches (one per sequential get), got %d", *calls)
	}
}

func TestConcurrentGetsCoalesce(t *testing.T) {
	exec, calls := fakeExec()
	// inflight=1: the first request dispatches at once; the others must join
	// its (open) batch and be answered by the same query
	b := newPointGetBatcher(64, 1, exec)
	var wg sync.WaitGroup
	results := make([]string, 8)
	errs := make([]error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rs, err := b.get(context.Background(), fmt.Sprintf("k%d", i))
			errs[i] = err
			if err == nil {
				results[i], _, _ = consume(rs)
			}
		}(i)
	}
	// first request could complete before the others join, so expect 1-2
	// queries; everything that joined the open batch must share one
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: concurrent gets did not complete")
	}
	for i := 0; i < 8; i++ {
		if errs[i] != nil {
			t.Fatalf("get %d: %v", i, errs[i])
		}
		if results[i] != fmt.Sprintf("k%d", i) {
			t.Fatalf("get %d: routed wrong row %q", i, results[i])
		}
	}
	if *calls >= 8 {
		t.Fatalf("expected coalescing (fewer queries than requests), got %d", *calls)
	}
}

func TestMissingKeyYieldsEmptyRowSet(t *testing.T) {
	exec, _ := fakeExec()
	b := newPointGetBatcher(64, 4, func(ctx context.Context, keys []string) ([][][]byte, error) {
		rows, err := exec(ctx, keys)
		if err != nil {
			return nil, err
		}
		// drop rows for the absent key so it answers with no row
		var kept [][][]byte
		for _, row := range rows {
			if string(row[3]) != "absent" {
				kept = append(kept, row)
			}
		}
		return kept, nil
	})
	rs, err := b.get(context.Background(), "present")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := consume(rs); !ok {
		t.Fatal("present key should return a row")
	}
	rs2, err := b.get(context.Background(), "absent")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := consume(rs2); ok {
		t.Fatal("absent key should return an empty row set")
	}
	if rs2.Binary() != true {
		t.Fatal("memRowSet must report binary")
	}
}

func TestExecErrorDeliveredToAll(t *testing.T) {
	b := newPointGetBatcher(64, 1, func(ctx context.Context, keys []string) ([][][]byte, error) {
		return nil, fmt.Errorf("boom")
	})
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = b.get(context.Background(), fmt.Sprintf("k%d", i))
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock on exec error")
	}
	for i := 0; i < 4; i++ {
		if errs[i] == nil || errs[i].Error() != "boom" {
			t.Fatalf("get %d: expected boom, got %v", i, errs[i])
		}
	}
}

func TestMemRowSetScan(t *testing.T) {
	m := &memRowSet{rows: [][][]byte{{i8(42), i8(0), i8(10), []byte("somekey"), i4(1), i4(0), i8(10), i8(0), i4(7)}}}
	var rev, compact, id int64
	var key string
	if !m.Next() {
		t.Fatal("no row")
	}
	if err := m.Scan(&rev, &compact, &id, &key, new(int64), new(int64), new(int64), new(int64), new(int64)); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if rev != 42 || id != 10 || key != "somekey" {
		t.Fatalf("scan mismatch: rev=%d id=%d key=%q", rev, id, key)
	}
}
