package sqllog

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"testing"
)

// Regression tests for the pgx-native raw wire-format scan path
// (OPTIMIZATION_ROUND_4.md R4-0B/R4-2). They encode a mock row set that
// follows pgx's semantics:
//
//   - RawValues() is only populated after the first Next(), is nil before
//     it, and reports NULLs as nil slices;
//   - binary-format bytea values are the raw payload with no in-band
//     length prefix;
//   - text-format result fields make the raw fast path unusable.

const (
	fInt8   = "i8"
	fInt4   = "i4"
	fText   = "text"
	fBytea  = "bytea"
	colText = 0
	colBin  = 1
)

type fakeRowSet struct {
	kinds     []string // per-column encoding kind
	formats   []int16  // per-column wire format (0 text, 1 binary)
	rows      [][][]byte
	i         int // 0 before the first Next()
	scanCalls int
	err       error // returned by Err() once rows are exhausted
}

func (f *fakeRowSet) Next() bool {
	f.i++
	return f.i <= len(f.rows)
}

func (f *fakeRowSet) RawValues() [][]byte {
	if f.i == 0 || f.i > len(f.rows) {
		return nil
	}
	return f.rows[f.i-1]
}

func (f *fakeRowSet) Binary() bool {
	for _, fm := range f.formats {
		if fm != colBin {
			return false
		}
	}
	return true
}

func (f *fakeRowSet) Close() {}

func (f *fakeRowSet) Err() error { return f.err }

// Scan mimics the driver's decode of the current row into dests. It fills
// each dest from the row's raw values according to the column kind.
func (f *fakeRowSet) Scan(dest ...any) error {
	f.scanCalls++
	if f.i == 0 || f.i > len(f.rows) {
		return errors.New("Scan called outside of a row")
	}
	raws := f.rows[f.i-1]
	for j, d := range dest {
		if j >= len(raws) || raws[j] == nil {
			continue // NULL leaves dest zeroed (mirrors database/sql)
		}
		v := raws[j]
		switch dv := d.(type) {
		case *sql.NullInt64:
			dv.Valid = true
			dv.Int64 = decodeKind(f.kinds[j], v)
		case *int64:
			*dv = decodeKind(f.kinds[j], v)
		case *string:
			*dv = string(v)
		case *sql.RawBytes:
			*dv = append((*dv)[:0], v...)
		default:
			return errors.New("unexpected dest type")
		}
	}
	return nil
}

func decodeKind(kind string, v []byte) int64 {
	switch kind {
	case fInt8:
		return decInt8(v)
	case fInt4:
		return decInt4(v)
	default:
		return 0
	}
}

// binRow builds one row's raw values in the kine column layout:
// cur, compact, modrev, name, created, deleted, create_rev, prev_mod, lease
// [, value [, old_value]].
func binRow(cur, compact, modrev int64, name string, created, deleted, createRev, prevMod, lease int64, val, prev []byte) [][]byte {
	i8 := func(v int64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(v))
		return b
	}
	i4 := func(v int64) []byte {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v))
		return b
	}
	raws := [][]byte{
		i8(cur), i8(compact), i8(modrev), []byte(name),
		i4(created), i4(deleted), i8(createRev), i8(prevMod), i4(lease),
	}
	raws = append(raws, val, prev)
	return raws
}

func binKinds() []string {
	return []string{fInt8, fInt8, fInt8, fText, fInt4, fInt4, fInt8, fInt8, fInt4, fBytea, fBytea}
}

func allBinary(n int) []int16 {
	f := make([]int16, n)
	for i := range f {
		f[i] = colBin
	}
	return f
}

// The value payload deliberately starts with bytes that look like a 4-byte
// length prefix: a decoder that strips one corrupts the value (the old
// decBytea did, because pgx already removes the wire framing).
var trickyPayload = append([]byte{0x00, 0x00, 0x00, 0x0f}, []byte("example payload")...)

func TestRowsToEventsFastPathDecodesRawWireValues(t *testing.T) {
	prev := []byte("previous")
	rows := &fakeRowSet{
		kinds:   binKinds(),
		formats: allBinary(11),
		rows: [][][]byte{
			binRow(11, 2, 11, "key-a", 0, 0, 3, 10, 7, trickyPayload, prev), // update
			binRow(12, 2, 12, "key-b", 1, 0, 0, 0, 7, trickyPayload, nil),   // create
			binRow(13, 2, 13, "key-c", 0, 1, 4, 12, 7, nil, nil),            // delete
		},
	}

	batch, compact, err := RowsToEvents(rows, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if compact != 2 {
		t.Errorf("compact = %d, want 2", compact)
	}
	if len(batch.Events) != 3 {
		t.Fatalf("got %d events, want 3", len(batch.Events))
	}

	a := batch.Events[0]
	if a.KV.Key != "key-a" || a.KV.ModRevision != 11 || a.Create || a.Delete {
		t.Errorf("event A header wrong: %+v", a.KV)
	}
	if a.KV.CreateRevision != 3 || a.KV.Lease != 7 {
		t.Errorf("event A create/lease wrong: %+v", a.KV)
	}
	if a.PrevKV == nil || a.PrevKV.ModRevision != 10 {
		t.Fatalf("event A PrevKV wrong: %+v", a.PrevKV)
	}
	if string(a.KV.Value) != string(trickyPayload) {
		t.Errorf("value corrupted by decoder: %q", a.KV.Value)
	}
	if string(a.PrevKV.Value) != "previous" {
		t.Errorf("prev value corrupted: %q", a.PrevKV.Value)
	}

	b := batch.Events[1]
	if !b.Create || b.PrevKV != nil || b.KV.CreateRevision != 12 {
		t.Errorf("event B create/prev wrong: create=%v createRev=%d prev=%+v", b.Create, b.KV.CreateRevision, b.PrevKV)
	}
	if string(b.KV.Value) != string(trickyPayload) {
		t.Errorf("value corrupted by decoder: %q", b.KV.Value)
	}

	c := batch.Events[2]
	if !c.Delete {
		t.Errorf("event C not marked deleted: %+v", c.KV)
	}
	if c.PrevKV == nil || c.PrevKV.ModRevision != 12 || c.PrevKV.Value != nil {
		t.Errorf("event C PrevKV wrong: %+v", c.PrevKV)
	}
	if rows.scanCalls != 0 {
		t.Errorf("fast path must not route through Scan (called %d times)", rows.scanCalls)
	}
	if batch.CurrentRev != 13 {
		t.Errorf("batch CurrentRev = %d, want 13", batch.CurrentRev)
	}
}

// The scan-mode gate must be evaluated after the first Next(): RawValues is
// nil before it, and the pre-Next check used before R4 sent every result set
// down the fallback scanner. Detect the mode via Scan being called or not.
func TestRowsToEventsFastGateCheckedAfterFirstNext(t *testing.T) {
	rows := &fakeRowSet{
		kinds:   binKinds(),
		formats: allBinary(10),
		rows:    [][][]byte{binRow(5, 0, 5, "k", 1, 0, 0, 0, 0, []byte("v"), nil)},
	}
	if _, _, err := RowsToEvents(rows, true, false); err != nil {
		t.Fatal(err)
	}
	if rows.scanCalls != 0 {
		t.Error("binary row set still used the fallback scanner: gate not rechecked after Next()")
	}
}

// Text-format result fields (pgx defaults text columns to text) must fall
// back to the scanner, not be interpreted as wire bytes.
func TestRowsToEventsTextFieldsFallBackToScan(t *testing.T) {
	rows := &fakeRowSet{
		kinds:   binKinds(),
		formats: allBinary(11),
		rows: [][][]byte{
			func() [][]byte {
				r := binRow(3, 0, 3, "k", 1, 0, 0, 0, 0, []byte("v"), nil)
				r[3] = []byte("k") // text wire bytes; same as binary text
				return r
			}(),
		},
	}
	rows.formats[3] = colText // the `name` column comes back as text

	batch, _, err := RowsToEvents(rows, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if rows.scanCalls == 0 {
		t.Error("text-format result set must use the fallback scanner")
	}
	if len(batch.Events) != 1 || batch.Events[0].KV.Key != "k" || string(batch.Events[0].KV.Value) != "v" {
		t.Errorf("fallback scan produced wrong event: %+v", batch.Events)
	}
}

func TestRowsToEventsNULLValueStaysNil(t *testing.T) {
	rows := &fakeRowSet{
		kinds:   binKinds(),
		formats: allBinary(11),
		rows:    [][][]byte{binRow(4, 0, 4, "k", 1, 0, 0, 0, 0, nil, nil)},
	}
	batch, _, err := RowsToEvents(rows, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Events[0].KV.Value != nil {
		t.Errorf("NULL value decoded to %q, want nil", batch.Events[0].KV.Value)
	}
}

// A result set that ends in a driver error must not be reported as complete.
func TestRowsToEventsSurfacesRowsErr(t *testing.T) {
	rows := &fakeRowSet{
		kinds:   binKinds(),
		formats: allBinary(10),
		rows:    [][][]byte{binRow(5, 0, 5, "k", 1, 0, 0, 0, 0, []byte("v"), nil)},
		err:     errors.New("conn closed mid-result"),
	}
	if _, _, err := RowsToEvents(rows, true, false); err == nil {
		t.Error("rows.Err() after iteration was not surfaced")
	}
}

func TestRowsToEventsEmptyResultIsComplete(t *testing.T) {
	rows := &fakeRowSet{kinds: binKinds(), formats: allBinary(10)}
	batch, compact, err := RowsToEvents(rows, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Events) != 0 {
		t.Errorf("got %d events for empty result", len(batch.Events))
	}
	if compact != 0 {
		t.Errorf("compact = %d, want 0", compact)
	}
}
