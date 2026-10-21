package backuphealth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/pb"
	"github.com/anyproto/anytype-heart/pkg/lib/pb/model"
)

// fakeExporter 模仿真实导出链的目录产物：<staging>/<export名>/xxx.md + files/原文件名。
type fakeExporter struct {
	t       *testing.T
	mdName  string
	mdBody  string
	fileNam string
	fileDat []byte
	failFor map[string]bool
}

func (f *fakeExporter) Export(_ context.Context, req pb.RpcObjectListExportRequest) (string, *model.ExportReport, error) {
	if f.failFor[req.SpaceId] {
		return "", nil, errFakeExport
	}
	out := filepath.Join(req.Path, "anytype-testspace-20240101")
	requireMk(f.t, filepath.Join(out, "files"))
	mustWrite(f.t, filepath.Join(out, f.mdName), f.mdBody)
	mustWrite(f.t, filepath.Join(out, "files", f.fileNam), string(f.fileDat))
	return out, nil, nil
}

type fakeErr string

func (e fakeErr) Error() string { return string(e) }

const errFakeExport = fakeErr("fake export boom")

func requireMk(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0755))
}

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(body), 0644))
}

type fakeLister struct {
	ids   []string
	names map[string]string
}

func (f *fakeLister) OpenedSpaceIds() []string      { return f.ids }
func (f *fakeLister) GetSpaceName(id string) string { return f.names[id] }

func TestExportLayoutReadable(t *testing.T) {
	dest := t.TempDir()
	exp := &fakeExporter{t: t, mdName: "我的笔记.md", mdBody: "# 我的笔记\n\n正文在此。\n", fileNam: "photo.png", fileDat: []byte("PNG-DATA")}
	lister := &fakeLister{ids: []string{"space1"}, names: map[string]string{"space1": "My Space"}}
	sum := ExportAllSpacesMarkdown(context.Background(), exp, lister, dest)
	require.Len(t, sum.Spaces, 1)
	require.Empty(t, sum.Spaces[0].Error, sum.Spaces[0])
	spaceDir := sum.Spaces[0].Dir

	// 每 Space 一文件夹：xxx.md + attachments/原文件名
	mdPath := filepath.Join(spaceDir, "我的笔记.md")
	attPath := filepath.Join(spaceDir, "attachments", "photo.png")
	// 通用工具可读：纯 os/file 命令，不跑软件
	mdBytes, err := os.ReadFile(mdPath)
	require.NoError(t, err, "md 必须用通用工具直接读出")
	assert.Contains(t, string(mdBytes), "正文在此")
	attBytes, err := os.ReadFile(attPath)
	require.NoError(t, err, "附件必须双击即开（字节一致）")
	assert.Equal(t, []byte("PNG-DATA"), attBytes)
	assert.NoFileExists(t, filepath.Join(spaceDir, "files", "photo.png"), "files/ 必须已改名为 attachments/")

	// manifest.json 可读
	manBytes, err := os.ReadFile(filepath.Join(dest, "manifest.json"))
	require.NoError(t, err)
	var man ExportSummary
	require.NoError(t, json.Unmarshal(manBytes, &man))
	require.Len(t, man.Spaces, 1)
	assert.Equal(t, 1, man.Spaces[0].MdCount)
	assert.Equal(t, 1, man.Spaces[0].AttachmentCount)
}

func TestExportNoSpaces(t *testing.T) {
	dest := t.TempDir()
	sum := ExportAllSpacesMarkdown(context.Background(), nil, &fakeLister{}, dest)
	require.Len(t, sum.Spaces, 1)
	assert.NotEmpty(t, sum.Spaces[0].Skipped)
}
