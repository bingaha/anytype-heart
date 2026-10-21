package backuphealth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
)

func updateOneRow(t *testing.T, dbPath string) {
	t.Helper()
	conn, err := sqlite.OpenConn(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	require.NoError(t, execNoResult(conn, `UPDATE 'ns_pages_docs' SET data = ? WHERE rowid = 1`, `{"n":2}`))
}

func TestDeepVerifyReport(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 2, "space1": 3})
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	rep := RunDeepVerify(context.Background(), layout)
	require.True(t, rep.OK, "failures: %v", rep.Failures)
	require.Len(t, rep.Dbs, 3)
	for _, d := range rep.Dbs {
		assert.Equal(t, "ok", d.IntegrityCheck, d.Path)
		assert.NotEmpty(t, d.TableHashes, d.Path)
	}
	assert.Equal(t, 1, rep.Files.FileCount)
	assert.NotEmpty(t, rep.Files.RootHex)
	assert.NotEmpty(t, rep.Files.FileHashes)
}

func TestDeepVerifyDetectsSilentChange(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 2, "space1": 3})
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	before := RunDeepVerify(context.Background(), layout)
	require.True(t, before.OK)

	// 静默篡改一行（结构完好，quick_check 看不出），深校验哈希必须变
	updateOneRow(t, filepath.Join(root, "objectstore", "space1", "objects.db"))
	after := RunDeepVerify(context.Background(), layout)
	require.True(t, after.OK, "结构未坏，不应判失败，但哈希必须变")
	assert.NotEqual(t, tableHash(before, "space1", "index"), tableHash(after, "space1", "index"))

	// 删掉一个附件，RootHex 必须变
	require.NoError(t, os.Remove(filepath.Join(root, "flatfs", "aB", "blob1")))
	afterFiles := RunDeepVerify(context.Background(), layout)
	require.True(t, afterFiles.OK)
	assert.NotEqual(t, before.Files.RootHex, afterFiles.Files.RootHex)
	assert.NotContains(t, afterFiles.Files.FileHashes, "aB/blob1")
}

func TestDeepVerifyCorruptFails(t *testing.T) {
	root := makeFixtureRepo(t, map[string]int{"common": 2, "space1": 3})
	p := filepath.Join(root, "objectstore", "space1", "objects.db")
	require.NoError(t, os.Truncate(p, 512))
	layout, err := DiscoverLayout(root)
	require.NoError(t, err)
	rep := RunDeepVerify(context.Background(), layout)
	require.False(t, rep.OK)
	require.NotEmpty(t, rep.Failures)
}

func tableHash(rep *DeepReport, spaceId, kind string) string {
	for _, d := range rep.Dbs {
		if d.SpaceId == spaceId && d.Kind == kind {
			for t, h := range d.TableHashes {
				if len(t) >= len("_pages_docs") && t[len(t)-len("_pages_docs"):] == "_pages_docs" {
					return h
				}
			}
		}
	}
	return ""
}
