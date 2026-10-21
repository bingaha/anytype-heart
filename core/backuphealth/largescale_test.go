package backuphealth

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
)

// TestQuickCheckLargeScale 两三百 MB 量级的退出流程模拟。
// 默认跳过：BACKUPHEALTH_LARGE=1 时运行（生成约 260MB fixture，需数 GB 磁盘与数十秒）。
func TestQuickCheckLargeScale(t *testing.T) {
	if os.Getenv("BACKUPHEALTH_LARGE") == "" {
		t.Skip("set BACKUPHEALTH_LARGE=1 to run the ~250MB timing simulation")
	}
	root := t.TempDir()
	const (
		rowPayload = 1 << 20 // 1MB/行
		rowCount   = 220     // ~220MB
		fileSize   = 2 << 20 // 2MB/文件
		fileCount  = 20      // ~40MB
	)
	// 大库：通用 5 行小数据 + space1 220 行大 payload
	require.NoError(t, os.MkdirAll(filepath.Join(root, "objectstore", "space1"), 0755))
	writeBigDb(t, filepath.Join(root, "objectstore", "objects.db"), 5, 1024)
	writeBigDb(t, filepath.Join(root, "objectstore", "space1", "objects.db"), rowCount, rowPayload)
	writeBigDb(t, filepath.Join(root, "objectstore", "space1", "crdt.db"), 0, 0)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "flatfs", "zz"), 0755))
	chunk := make([]byte, fileSize)
	for i := range chunk {
		chunk[i] = byte(i * 7)
	}
	for i := 0; i < fileCount; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(root, "flatfs", "zz", fmt.Sprintf("blob%02d", i)), chunk, 0644))
	}
	var total int64
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	t.Logf("fixture total: %.1f MB", float64(total)/(1<<20))

	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	expected := ExpectedCounts{
		"\x00" + "pages":       5,
		"space1\x00" + "pages": rowCount,
	}

	start := time.Now()
	rep := RunQuickCheck(context.Background(), layout, expected)
	el := time.Since(start)
	t.Logf("quickcheck: ok=%v duration=%s reported=%dms dbs=%d files=%d", rep.OK, el, rep.DurationMs, len(rep.Dbs), rep.Files.FileCount)
	if !rep.OK {
		t.Logf("failures: %v", rep.Failures)
	}
	require.True(t, rep.OK, "大规模下快速自检必须通过")
	assert.Less(t, el, time.Second, "两三百MB量级下阻塞必须<1秒")

	dstart := time.Now()
	drep := RunDeepVerify(context.Background(), layout)
	t.Logf("deepverify: ok=%v duration=%s", drep.OK, time.Since(dstart))
	require.True(t, drep.OK)
}

func writeBigDb(t *testing.T, path string, rows, payload int) {
	t.Helper()
	conn, err := sqlite.OpenConn(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	require.NoError(t, execNoResult(conn, `CREATE TABLE 'ns_pages_docs' (id BLOB PRIMARY KEY, data JSONB)`))
	stmt, err := conn.Prepare(`INSERT INTO 'ns_pages_docs' (id, data) VALUES (?, ?)`)
	require.NoError(t, err)
	defer func() { require.NoError(t, stmt.Finalize()) }()
	buf := make([]byte, payload)
	for i := 0; i < rows; i++ {
		for j := range buf {
			buf[j] = byte(i + j*13)
		}
		id := []byte{byte(i), byte(i >> 8), byte(i >> 16), 0xA0}
		stmt.BindBytes(1, id)
		stmt.BindBytes(2, buf)
		_, err := stmt.Step()
		require.NoError(t, err)
		require.NoError(t, stmt.Reset())
	}
}
