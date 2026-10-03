package backfill

import (
	"fmt"
	"os"
	"testing"
)

// coverageMatrix lists every business the backfill package guarantees and
// the test (case) that covers it. TestMain prints the matrix before running
// anything, and the post-run summary prints the per-case PASS/FAIL results,
// so `go test -v ./internal/backfill/` makes coverage legible at a glance.
var coverageMatrix = []struct {
	Business string
	Cases    []string
}{
	{"回填结果等价于去重后按业务时间逐条处理", []string{"TestBackfillMatchesSequentialByBusinessTime"}},
	{"批内乱序+重复、分块落盘与压实不影响结果", []string{"TestBackfillChunkedStreamingAndGuards"}},
	{"跑到一半停止、空状态重启后续跑：逐笔结论/命中规则/告警归属/计数完全一致", []string{"TestBackfillRestartEmptyComponentResume"}},
	{"重启保留告警生命周期(OPEN/ACKED/RESOLVED)与首末发生时间", []string{"TestBackfillRestartEmptyComponentResume", "TestStoreSnapshotRestoreRoundTrip(alert 包)"}},
	{"重启恢复水位线，后续实时迟到/乱序平滑衔接", []string{"TestBackfillRestartEmptyComponentResume", "TestProcessorSnapshotRestoreRoundTrip"}},
	{"重启恢复有状态规则(velocity)窗口，扎堆判定不丢失", []string{"TestBackfillRestartEmptyComponentResume", "TestVelocitySnapshotRestore(rule 包)"}},
	{"同一 EventID 批内多次/跨块撞车(含不同时间戳)只算一次", []string{"TestBackfillEventIDCollisionAcrossChunks", "TestBackfillChunkedStreamingAndGuards"}},
	{"断点游标前缀不重计、后续不漏", []string{"TestBackfillRestartEmptyComponentResume", "TestBackfillStopAndResumeCountConservation"}},
	{"进程内(非重启)中断续跑仍守恒", []string{"TestBackfillStopAndResumeCountConservation"}},
	{"挂错非空处理器续跑被显式拒绝", []string{"TestBackfillRestartRejectsForeignNonEmptyProcessor"}},
	{"回填与实时并发：定论唯一、计数守恒、无回退", []string{"TestBackfillConcurrentWithRealtime"}},
	{"处理器/告警/规则快照往返与空状态恢复保护", []string{"TestProcessorSnapshotRestoreRoundTrip", "TestProcessorRestoreGuards", "TestStoreSnapshotRestoreRoundTrip", "TestVelocitySnapshotRestore"}},
}

func TestMain(m *testing.M) {
	fmt.Println("================ backfill 业务覆盖矩阵 ================")
	for i, row := range coverageMatrix {
		fmt.Printf("%2d. business=%-40s cases=%v\n", i+1, row.Business, row.Cases)
	}
	fmt.Println("======================================================")
	code := m.Run()
	fmt.Println("---------------- backfill 用例结果 --------------------")
	printed := map[string]bool{}
	for _, row := range coverageMatrix {
		for _, c := range row.Cases {
			if printed[c] {
				continue
			}
			printed[c] = true
			if _, ok := ranCases[c]; !ok {
				continue // cases in other packages are logged by those packages
			}
			status := "FAIL"
			if ranCases[c] {
				status = "PASS"
			}
			fmt.Printf("case=%-55s result=%s\n", c, status)
		}
	}
	fmt.Println("------------------------------------------------------")
	os.Exit(code)
}

// ranCases is populated by each test via markCase and read by TestMain's
// post-run summary. true=passed, false=failed.
var ranCases = map[string]bool{}

func markCase(name string, passed bool) { ranCases[name] = passed }
