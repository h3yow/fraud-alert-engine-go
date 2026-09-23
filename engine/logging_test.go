package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// stringsLogger 捕获日志输出，供断言“输入与判定依据被打印”。
type stringsLogger struct{ sb strings.Builder }

func (l *stringsLogger) Printf(format string, args ...any) {
	l.sb.WriteString(strings.TrimSpace(fmt.Sprintf(format, args...)) + "\n")
}

// TestEngine_LogsInputAndRationale 验证日志打印了输入字段、命中依据与最终定论。
func TestEngine_LogsInputAndRationale(t *testing.T) {
	logger := &stringsLogger{}
	reg := NewRegistry()
	if err := reg.Register(NewBlacklistRule("RULE_BLACKLIST", 10)); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	e := NewEngine(reg, Config{
		Blacklist: NewMemoryBlacklistStore([]string{"ACCT-LOG"}),
		Policy:    ConservativeDegradePolicy(),
		Logger:    logger,
		Now:       func() time.Time { return base },
	})
	txn := Transaction{TxnID: "LOG1", AccountID: "ACCT-LOG", Amount: 20000,
		Currency: "USD", Country: "US", EventTime: base}
	e.Ingest(context.Background(), txn)

	out := logger.sb.String()
	required := []string{
		"输入 txn=LOG1",
		"account=ACCT-LOG",
		"amount=20000.00 USD",
		"eventTime=2026-09-23T10:00:00Z",
		"命中 txn=LOG1 rule=RULE_BLACKLIST",
		"依据=",
		"定论 txn=LOG1 action=BLOCK",
	}
	for _, sub := range required {
		if !strings.Contains(out, sub) {
			t.Fatalf("日志缺少 %q；实际输出:\n%s", sub, out)
		}
	}
}

// TestEngine_LogsDegradationReason 验证降级原因被打印且可区分。
func TestEngine_LogsDegradationReason(t *testing.T) {
	logger := &stringsLogger{}
	e := NewEngine(NewRegistry(), Config{
		Policy: ConservativeDegradePolicy(),
		Logger: logger,
		Now:    func() time.Time { return time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC) },
	})
	e.Ingest(context.Background(), Transaction{
		TxnID: "BAD", AccountID: "ACCT-X", Amount: -1, Currency: "USD",
		Country: "US", EventTime: time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC),
	})
	out := logger.sb.String()
	if !strings.Contains(out, "降级 txn=BAD action=BLOCK reason=INVALID_FIELD") {
		t.Fatalf("降级日志须打印可区分原因，实际:\n%s", out)
	}
}
