package backuphealth

// 定期明文导出：复用 core/block/export 导出链，把每个 Space 全量导出为
// Markdown + 附件目录，当第二副本。
//
// 目标布局（通用工具直接可读，不跑软件）：
//
//	<dest>/<space文件夹>/xxx.md            每篇笔记一篇 Markdown（原文件名语义）
//	<dest>/<space文件夹>/attachments/原文件名  附件原字节，双击即开
//	<dest>/manifest.json                   本次导出的清单（给 08 号按钮展示用）
//
// 实现：调一次现成的 Markdown 目录导出（IncludeFiles=true），再把它的
// `files/` 重命名为 `attachments/`。不改导出链一行代码。

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gosimple/slug"

	"github.com/anyproto/anytype-heart/pb"
	"github.com/anyproto/anytype-heart/pkg/lib/pb/model"
)

// MarkdownExporter 是对 export.Export 的最小依赖面（08 号实现 RPC 时同理）。
type MarkdownExporter interface {
	Export(ctx context.Context, req pb.RpcObjectListExportRequest) (path string, diagnostics *model.ExportReport, err error)
}

// SpaceLister 提供待导出的 Space 集合与展示名。objectstore.ObjectStore 天然满足。
type SpaceLister interface {
	OpenedSpaceIds() []string
	GetSpaceName(spaceId string) string
}

// SpaceExport 单个 Space 的导出结果。
type SpaceExport struct {
	SpaceId         string `json:"spaceId"`
	SpaceName       string `json:"spaceName"`
	Dir             string `json:"dir"`
	MdCount         int    `json:"mdCount"`
	AttachmentCount int    `json:"attachmentCount"`
	Bytes           int64  `json:"bytes"`
	Skipped         string `json:"skipped,omitempty"`
	Error           string `json:"error,omitempty"`
}

// ExportSummary 一次全量导出的清单，同时落盘为 manifest.json。
type ExportSummary struct {
	At         time.Time     `json:"at"`
	Dest       string        `json:"dest"`
	Spaces     []SpaceExport `json:"spaces"`
	DurationMs int64         `json:"durationMs"`
}

// ExportAllSpacesMarkdown 把全部已打开 Space 导出为 Markdown+attachments 目录。
// exporter/lister 传 nil 则只做目录准备（测试/降级用，Spaces 记 skipped）。
func ExportAllSpacesMarkdown(ctx context.Context, exporter MarkdownExporter, lister SpaceLister, destDir string) *ExportSummary {
	start := time.Now()
	sum := &ExportSummary{At: start, Dest: destDir}
	if err := os.MkdirAll(destDir, 0755); err != nil {
		sum.Spaces = append(sum.Spaces, SpaceExport{Error: fmt.Sprintf("mkdir dest: %v", err)})
		return sum
	}
	var spaceIds []string
	names := map[string]string{}
	if lister != nil {
		spaceIds = lister.OpenedSpaceIds()
		for _, id := range spaceIds {
			names[id] = lister.GetSpaceName(id)
		}
	}
	if len(spaceIds) == 0 {
		sum.Spaces = append(sum.Spaces, SpaceExport{Skipped: "no opened spaces"})
		sum.DurationMs = time.Since(start).Milliseconds()
		_ = writeManifest(destDir, sum)
		return sum
	}
	used := map[string]bool{}
	for _, spaceId := range spaceIds {
		if err := ctx.Err(); err != nil {
			sum.Spaces = append(sum.Spaces, SpaceExport{SpaceId: spaceId, Error: fmt.Sprintf("canceled: %v", err)})
			continue
		}
		sum.Spaces = append(sum.Spaces, exportOneSpace(ctx, exporter, spaceId, names[spaceId], destDir, used))
	}
	sum.DurationMs = time.Since(start).Milliseconds()
	_ = writeManifest(destDir, sum)
	return sum
}

func exportOneSpace(ctx context.Context, exporter MarkdownExporter, spaceId, spaceName, destDir string, used map[string]bool) SpaceExport {
	se := SpaceExport{SpaceId: spaceId, SpaceName: spaceName}
	if exporter == nil {
		se.Skipped = "no exporter"
		return se
	}
	dirName := spaceDirName(spaceName, spaceId, used)
	finalDir := filepath.Join(destDir, dirName)
	se.Dir = finalDir
	staging, err := os.MkdirTemp(destDir, ".staging-"+dirName+"-")
	if err != nil {
		se.Error = fmt.Sprintf("mkstaging: %v", err)
		return se
	}
	defer os.RemoveAll(staging)
	outPath, _, err := exporter.Export(ctx, pb.RpcObjectListExportRequest{
		SpaceId:         spaceId,
		Format:          model.Export_Markdown,
		IncludeFiles:    true,
		IncludeNested:   true,
		IncludeArchived: true,
		Zip:             false,
		NoProgress:      true,
		Path:            staging,
	})
	if err != nil {
		se.Error = fmt.Sprintf("export: %v", err)
		return se
	}
	if err := reshapeMarkdownExport(outPath, finalDir); err != nil {
		se.Error = fmt.Sprintf("reshape: %v", err)
		return se
	}
	se.MdCount, se.AttachmentCount, se.Bytes = surveyDir(finalDir)
	return se
}

// spaceDirName 生成人类可读且唯一的 Space 文件夹名。
func spaceDirName(spaceName, spaceId string, used map[string]bool) string {
	base := slug.Make(spaceName)
	if base == "" {
		base = "space"
	}
	name := base
	if used[name] {
		short := spaceId
		if len(short) > 8 {
			short = short[:8]
		}
		name = base + "-" + short
	}
	used[name] = true
	return name
}

// reshapeMarkdownExport 把导出链产物搬成目标布局：
// <out>/*.md -> <final>/ ; <out>/files/ -> <final>/attachments/。
func reshapeMarkdownExport(outPath, finalDir string) error {
	st, err := os.Stat(outPath)
	if err != nil {
		return fmt.Errorf("stat export out: %w", err)
	}
	src := outPath
	if !st.IsDir() {
		return fmt.Errorf("expected dir export, got file: %s", outPath)
	}
	if err := os.MkdirAll(finalDir, 0755); err != nil {
		return fmt.Errorf("mkdir final: %w", err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("read export out: %w", err)
	}
	for _, e := range entries {
		from := filepath.Join(src, e.Name())
		toName := e.Name()
		if e.Name() == "files" {
			toName = "attachments"
		}
		to := filepath.Join(finalDir, toName)
		_ = os.RemoveAll(to)
		if err := os.Rename(from, to); err != nil {
			// 跨盘时 rename 不可用，退化为拷贝。
			if cerr := copyTree(from, to); cerr != nil {
				return fmt.Errorf("move %s: %v (copy fallback: %v)", e.Name(), err, cerr)
			}
			_ = os.RemoveAll(from)
		}
	}
	return nil
}

func copyTree(from, to string) error {
	st, err := os.Stat(from)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return copyFile(from, to, st.Mode())
	}
	if err := os.MkdirAll(to, 0755); err != nil {
		return err
	}
	entries, err := os.ReadDir(from)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := copyTree(filepath.Join(from, e.Name()), filepath.Join(to, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(from, to string, mode os.FileMode) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.Perm())
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// surveyDir 统计目录里的 md 数、附件数与总字节（attachments 下的都算附件）。
func surveyDir(dir string) (md, att int, bytes int64) {
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		bytes += info.Size()
		rel, _ := filepath.Rel(dir, p)
		first := strings.Split(rel, string(filepath.Separator))[0]
		switch {
		case first == "attachments":
			att++
		case strings.HasSuffix(strings.ToLower(info.Name()), ".md"):
			md++
		default:
			att++
		}
		return nil
	})
	return md, att, bytes
}

func writeManifest(destDir string, sum *ExportSummary) error {
	data, err := json.MarshalIndent(sum, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(destDir, "manifest.json"), data, 0644)
}
