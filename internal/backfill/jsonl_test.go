package backfill

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJSONLFileReaderRoundTrip(t *testing.T) {
	t.Log("=== scenario: JSONL file source re-opens with identical content ===")
	dir := t.TempDir()
	path := filepath.Join(dir, "in.jsonl")
	base := time.Date(2026, 2, 2, 2, 2, 2, 0, time.UTC)
	content := "" +
		`{"seq":1,"tx":{"event_id":"a","tx_id":"ta","account_id":"acct","amount":1,"currency":"USD","country":"US","event_type":"PAY","occurred_at":"` + base.Format(time.RFC3339) + `","ingested_at":"` + base.Add(time.Second).Format(time.RFC3339) + `"}}` + "\n" +
		`{"tx":{"event_id":"b","tx_id":"tb","account_id":"acct","amount":2,"currency":"EUR","country":"ZZ","event_type":"PAY","occurred_at":"` + base.Add(time.Minute).Format(time.RFC3339) + `","ingested_at":"` + base.Add(2*time.Minute).Format(time.RFC3339) + `"}}` + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	openAndRead := func() []Record {
		it, err := NewJSONLFileReader(path).Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()
		out := []Record{}
		for {
			r, ok, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			out = append(out, r)
		}
		return out
	}
	first := openAndRead()
	if len(first) != 2 || first[0].Transaction.EventID != "a" || first[1].Seq != 2 {
		t.Fatalf("decode wrong: %+v", first)
	}
	if first[1].Transaction.Currency != "EUR" || first[1].Transaction.Country != "ZZ" {
		t.Fatalf("second record fields wrong: %+v", first[1].Transaction)
	}
	// Reopen (used after mid-sort crash) must yield identical records.
	second := openAndRead()
	if len(second) != 2 || second[0].Seq != first[0].Seq ||
		!second[1].Transaction.OccurredAt.Equal(first[1].Transaction.OccurredAt) {
		t.Fatalf("reopen mismatch:\n%+v\n%+v", first, second)
	}

	// End-to-end: JSONL source through the runner; b (ZZ) must BLOCK.
	p, _ := newStack(t)
	rn := NewRunner(NewMemStore(), scenarioLogger{t})
	res, err := rn.Run(context.Background(), p, Config{
		JobID: "j-jsonl", ChunkSize: 10, Source: NewJSONLFileReader(path),
	})
	if err != nil || !res.Completed {
		t.Fatalf("run: %+v %v", res, err)
	}
	vb := p.Process(first[1].Transaction)
	if vb.Action != "BLOCK" || vb.IsDuplicate != true {
		t.Fatalf("JSONL backfill verdict wrong: %+v", vb)
	}
	t.Log("result: JSONL decodes, derives seq, re-opens identically, runs end-to-end")
}

func TestSliceReaderHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	it, err := SliceReader([]Record{{Seq: 1}}).Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if _, ok, err := it.Next(); err != context.Canceled {
		t.Fatalf("want context.Canceled, got ok=%v err=%v", ok, err)
	}
}
