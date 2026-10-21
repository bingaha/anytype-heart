package backuphealth

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/core/domain"
)

// TestExitFlowSim 模拟退出流程：StateChange(closing) → 快速自检通过且<1s。
func TestExitFlowSim(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 2, "space1": 3})
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	svc := &service{repoRoot: root, layout: layout, periodic: &periodicExport{}}

	start := time.Now()
	svc.StateChange(int(domain.CompStateAppClosingInitiated))
	el := time.Since(start)

	rep := svc.LastQuickReport()
	require.NotNil(t, rep)
	require.True(t, rep.OK, "failures: %v", rep.Failures)
	assert.Less(t, el, time.Second, "退出钩子阻塞必须<1s")

	// 后台切换同样触发
	svc.StateChange(int(domain.CompStateAppWentBackground))
	require.True(t, svc.LastQuickReport().OK)

	// 前台切换不触发（报告保持上一次）
	before := svc.LastQuickReport()
	svc.StateChange(int(domain.CompStateAppWentForeground))
	assert.Same(t, before, svc.LastQuickReport())

	// 报告落盘，停机后通用工具可查
	assert.FileExists(t, root+"/backuphealth/last-quickcheck.json")
}

// TestExitFlowSimCorrupt 模拟“弄坏后退出”：必须报警且报告记失败。
func TestExitFlowSimCorrupt(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 2, "space1": 3})
	p := root + "/objectstore/space1/objects.db"
	require.NoError(t, truncate(p, 512))
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	svc := &service{repoRoot: root, layout: layout, periodic: &periodicExport{}}

	svc.StateChange(int(domain.CompStateAppClosingInitiated))
	rep := svc.LastQuickReport()
	require.NotNil(t, rep)
	require.False(t, rep.OK, "弄坏后退出必须报警")
	require.NotEmpty(t, rep.Failures)
}

// TestServiceManualTriggers 服务级手动触发接口（08 号按钮直接调这些）。
func TestServiceManualTriggers(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 1, "space1": 1})
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	svc := &service{repoRoot: root, layout: layout, periodic: &periodicExport{}}

	q := svc.QuickCheckNow(context.Background())
	require.True(t, q.OK)

	d := svc.DeepVerify(context.Background())
	require.True(t, d.OK)
	require.NotNil(t, svc.LastDeepReport())

	// 无 objStore 时导出记 skipped（完整布局路径见 plainexport_test）
	svc.exporter = &fakeExporter{t: t, mdName: "n.md", mdBody: "# n\n", fileNam: "a.bin", fileDat: []byte("DATA")}
	sum := svc.ExportAllSpacesNow(context.Background(), root+"/plaintext")
	require.NotNil(t, sum)
	require.NotNil(t, svc.LastExport())

	svc.SetPeriodicExport(time.Hour, root+"/plaintext")
	svc.StopPeriodicExport()
}

func truncate(p string, n int64) error {
	f, err := os.OpenFile(p, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(n)
}
