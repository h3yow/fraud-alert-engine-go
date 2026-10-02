package backfill

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/h3yow/fraud-alert-engine-go/internal/domain"
)

// sortKey orders transactions deterministically for replay: business time
// first, EventID as the stable tie-break.
type sortKey struct {
	occurred time.Time
	eventID  string
}

func keyOf(tx domain.Transaction) sortKey {
	return sortKey{occurred: tx.OccurredAt, eventID: tx.EventID}
}

func lessKey(a, b sortKey) bool {
	if !a.occurred.Equal(b.occurred) {
		return a.occurred.Before(b.occurred)
	}
	return a.eventID < b.eventID
}

// sortAndDedupe sorts one in-memory chunk by sortKey and drops adjacent
// exact duplicates (same EventID and same OccurredAt), returning the number
// of dropped duplicates.
func sortAndDedupe(chunk []domain.Transaction) ([]domain.Transaction, int) {
	sorted := append([]domain.Transaction(nil), chunk...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return lessKey(keyOf(sorted[i]), keyOf(sorted[j]))
	})
	out := sorted[:0]
	dups := 0
	for i, tx := range sorted {
		if i > 0 && tx.EventID == sorted[i-1].EventID && tx.OccurredAt.Equal(sorted[i-1].OccurredAt) {
			dups++
			continue
		}
		out = append(out, tx)
	}
	return out, dups
}

// runWriter streams one sorted run to disk as JSON lines.
type runWriter struct {
	f   *os.File
	buf *bufio.Writer
	enc *json.Encoder
	n   int64
}

func newRunWriter(path string) (*runWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	buf := bufio.NewWriterSize(f, 1<<20)
	return &runWriter{f: f, buf: buf, enc: json.NewEncoder(buf)}, nil
}

func (w *runWriter) write(tx domain.Transaction) error {
	if err := w.enc.Encode(tx); err != nil {
		return err
	}
	w.n++
	return nil
}

func (w *runWriter) close() error {
	if err := w.buf.Flush(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// runReader streams one sorted run back from disk.
type runReader struct {
	f   *os.File
	dec *json.Decoder
}

func openRunReader(path string) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &runReader{f: f, dec: json.NewDecoder(bufio.NewReaderSize(f, 1<<20))}, nil
}

// next returns the next transaction; ok=false means the run is exhausted.
func (r *runReader) next() (tx domain.Transaction, ok bool, err error) {
	if err := r.dec.Decode(&tx); err != nil {
		if err.Error() == "EOF" {
			return domain.Transaction{}, false, nil
		}
		return domain.Transaction{}, false, err
	}
	return tx, true, nil
}

func (r *runReader) close() error { return r.f.Close() }

// mergeItem is one heap entry during the k-way merge.
type mergeItem struct {
	tx  domain.Transaction
	run int // index into the readers slice
}

// mergeHeap is a min-heap ordered by sortKey, then run index for stability.
type mergeHeap struct {
	items []mergeItem
}

func (h *mergeHeap) len() int { return len(h.items) }

func (h *mergeHeap) less(i, j int) bool {
	ki, kj := keyOf(h.items[i].tx), keyOf(h.items[j].tx)
	if lessKey(ki, kj) {
		return true
	}
	if lessKey(kj, ki) {
		return false
	}
	return h.items[i].run < h.items[j].run
}

func (h *mergeHeap) swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *mergeHeap) push(it mergeItem) {
	h.items = append(h.items, it)
	h.up(len(h.items) - 1)
}

func (h *mergeHeap) pop() mergeItem {
	top := h.items[0]
	last := len(h.items) - 1
	h.items[0] = h.items[last]
	h.items = h.items[:last]
	if len(h.items) > 0 {
		h.down(0)
	}
	return top
}

func (h *mergeHeap) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			return
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *mergeHeap) down(i int) {
	for {
		l, r := 2*i+1, 2*i+2
		smallest := i
		if l < len(h.items) && h.less(l, smallest) {
			smallest = l
		}
		if r < len(h.items) && h.less(r, smallest) {
			smallest = r
		}
		if smallest == i {
			return
		}
		h.swap(i, smallest)
		i = smallest
	}
}

// mergeRuns streams the k-way merge of the sorted run files at paths,
// invoking emit for every record in global (OccurredAt, EventID) order.
// Adjacent exact duplicates (same EventID and OccurredAt appearing in more
// than one run) are emitted only once; dups counts the suppressed copies.
// Memory usage is bounded by one record per open run file.
func mergeRuns(paths []string, emit func(tx domain.Transaction) error) (dups int64, err error) {
	readers := make([]*runReader, 0, len(paths))
	defer func() {
		for _, r := range readers {
			r.close()
		}
	}()
	h := &mergeHeap{}
	for i, p := range paths {
		r, err := openRunReader(p)
		if err != nil {
			return 0, fmt.Errorf("open run %s: %w", p, err)
		}
		readers = append(readers, r)
		tx, ok, err := r.next()
		if err != nil {
			return 0, fmt.Errorf("read run %s: %w", p, err)
		}
		if ok {
			h.push(mergeItem{tx: tx, run: i})
		}
	}
	var prev *sortKey
	for h.len() > 0 {
		it := h.pop()
		k := keyOf(it.tx)
		if prev != nil && prev.eventID == k.eventID && prev.occurred.Equal(k.occurred) {
			dups++
		} else {
			if err := emit(it.tx); err != nil {
				return dups, err
			}
			kc := k
			prev = &kc
		}
		tx, ok, err := readers[it.run].next()
		if err != nil {
			return dups, err
		}
		if ok {
			h.push(mergeItem{tx: tx, run: it.run})
		}
	}
	return dups, nil
}
