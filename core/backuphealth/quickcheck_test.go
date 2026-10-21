package backuphealth

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
)

// makeFixtureRepo 造一个最小冷备份目录：通用库 + 一个 space 的 index/crdt + flatfs 文件。
// 表按 any-store 命名 `<ns>_<coll>_docs(id BLOB, data JSONB)`，行数可查。
func makeFixtureRepo(t *testing.T, collections map[string]int) (root string) {
	t.Helper()
	root = t.TempDir()
	mustWriteDb := func(path string, rows int) {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		conn, err := sqlite.OpenConn(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, conn.Close()) }()
		require.NoError(t, execNoResult(conn, `CREATE TABLE 'ns_pages_docs' (id BLOB PRIMARY KEY, data JSONB)`))
		for i := 0; i < rows; i++ {
			require.NoError(t, execNoResult(conn, `INSERT INTO 'ns_pages_docs' (id, data) VALUES (?, ?)`,
				[]byte{byte(i), 0, 1}, []byte(`{"n":1}`)))
		}
		if rows > 0 {
			require.NoError(t, execNoResult(conn, `CREATE TABLE 'ns_files_docs' (id BLOB PRIMARY KEY, data JSONB)`))
			require.NoError(t, execNoResult(conn, `INSERT INTO 'ns_files_docs' (id, data) VALUES (?, ?)`,
				[]byte{9}, []byte(`{"f":1}`)))
		}
	}
	require.NoError(t, os.MkdirAll(filepath.Join(root, "objectstore"), 0755))
	mustWriteDb(filepath.Join(root, "objectstore", "objects.db"), collections["common"])
	mustWriteDb(filepath.Join(root, "objectstore", "space1", "objects.db"), collections["space1"])
	mustWriteDb(filepath.Join(root, "objectstore", "space1", "crdt.db"), 0)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "flatfs", "aB"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "flatfs", "aB", "blob1"), []byte("hello-attachment"), 0644))
	// 让 blob1 成为“最新文件”
	newest := time.Now().Add(time.Hour)
	require.NoError(t, os.Chtimes(filepath.Join(root, "flatfs", "aB", "blob1"), newest, newest))
	return root
}

func execNoResult(conn *sqlite.Conn, q string, args ...any) error {
	stmt, err := conn.Prepare(q)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Finalize() }()
	for i, a := range args {
		switch v := a.(type) {
		case []byte:
			stmt.BindBytes(i+1, v)
		case string:
			stmt.BindText(i+1, v)
		}
	}
	_, err = stmt.Step()
	return err
}

func TestQuickCheckPassAndFast(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 3, "space1": 5})
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	expected := ExpectedCounts{
		"\x00" + "pages":       3,
		"space1\x00" + "pages": 5,
		"space1\x00" + "files": 1,
	}
	start := time.Now()
	rep := RunQuickCheck(context.Background(), layout, expected)
	el := time.Since(start)
	require.True(t, rep.OK, "failures: %v", rep.Failures)
	assert.Less(t, el, time.Second, "快速自检必须<1s")
	assert.Less(t, rep.DurationMs, int64(1000))
	require.Len(t, rep.Dbs, 3) // common + space1 index/crdt
	assert.Equal(t, 1, rep.Files.FileCount)
	assert.NotEmpty(t, rep.Files.NewestFile)
	assert.Equal(t, int64(len("hello-attachment")), rep.Files.NewestSize)
}

func TestQuickCheckRowMismatchAlarms(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 3, "space1": 5})
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	// 在线说 5 行，模拟落盘少了（期望故意写错）→ 必须报警
	rep := RunQuickCheck(context.Background(), layout, ExpectedCounts{"space1\x00pages": 4})
	require.False(t, rep.OK)
	require.NotEmpty(t, rep.Failures)
	assert.Contains(t, rep.Failures[0], "rows mismatch")
}

func TestQuickCheckCorruptedDbAlarms(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 3, "space1": 5})
	// 人为弄坏：覆盖 space 库文件头（magic 损坏，open 即失败）+ 截断通用库
	p := filepath.Join(root, "objectstore", "space1", "objects.db")
	f, err := os.OpenFile(p, os.O_RDWR, 0644)
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, 100), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	q := filepath.Join(root, "objectstore", "objects.db")
	require.NoError(t, os.Truncate(q, 1024))
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	start := time.Now()
	rep := RunQuickCheck(context.Background(), layout, nil)
	el := time.Since(start)
	require.False(t, rep.OK, "弄坏文件后必须报警")
	assert.Less(t, el, time.Second)
}

func TestQuickCheckMissingDbAlarms(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 1, "space1": 1})
	require.NoError(t, os.Remove(filepath.Join(root, "objectstore", "space1", "crdt.db")))
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	rep := RunQuickCheck(context.Background(), layout, nil)
	require.False(t, rep.OK)
}

func TestQuickCheckBudgetHardCap(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 1, "space1": 1})
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	old := quickCheckBudget
	quickCheckBudget = time.Nanosecond
	defer func() { quickCheckBudget = old }()
	start := time.Now()
	rep := RunQuickCheck(context.Background(), layout, nil)
	el := time.Since(start)
	require.False(t, rep.OK, "预算耗尽必须按失败返回")
	assert.Less(t, el, time.Second, "预算外也必须<1s 返回")
}
