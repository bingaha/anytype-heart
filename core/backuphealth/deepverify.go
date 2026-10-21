package backuphealth

// 按需深校验：全量哈希核对。空闲或设置页手动触发，无时间预算，
// ctx 由调用方（空闲调度/手动取消）控制。
//
//   - 每个 sqlite：PRAGMA integrity_check + 逐表行数 + 表内容流式 SHA256
//     （按 id 排序扫 id/data 两列，不 whole-file 哈希——WAL/checkpoint
//     会改文件字节但不改逻辑内容）
//   - flatfs 每个块文件：SHA256 全量
//
// 两次报告逐项对比即可发现静默损坏；人为弄坏任一文件后重跑，
// 对应项必报警。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"zombiezen.com/go/sqlite"
)

// DeepDbReport 单库深校验结果。
type DeepDbReport struct {
	SpaceId string `json:"spaceId"`
	Kind    string `json:"kind"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	// IntegrityCheck 为 "ok" 时库结构无损。
	IntegrityCheck string `json:"integrityCheck"`
	// TableHashes key 为裸表名，value 为 "rows=<n> sha256=<hex>"。
	TableHashes map[string]string `json:"tableHashes"`
}

// DeepFileReport 附件区深校验结果。
type DeepFileReport struct {
	Dir        string `json:"dir"`
	Skipped    bool   `json:"skipped,omitempty"`
	FileCount  int    `json:"fileCount"`
	TotalBytes int64  `json:"totalBytes"`
	// RootHex 把全部文件哈希排序后拼接再哈希，一眼看出附件区是否变化。
	RootHex string `json:"rootHex,omitempty"`
	// FileHashes key 为相对 flatfs 的路径。文件多了会很大，写文件报告用，
	// 内存里照常保留（两三百 MB 量级约数千文件，可接受）。
	FileHashes map[string]string `json:"fileHashes,omitempty"`
}

// DeepReport 全量哈希报告。
type DeepReport struct {
	At         time.Time      `json:"at"`
	DurationMs int64          `json:"durationMs"`
	OK         bool           `json:"ok"`
	Failures   []string       `json:"failures,omitempty"`
	Dbs        []DeepDbReport `json:"dbs"`
	Files      DeepFileReport `json:"files"`
}

// RunDeepVerify 执行全量深校验。可多次运行后对比报告找静默损坏。
func RunDeepVerify(ctx context.Context, layout RepoLayout) *DeepReport {
	start := time.Now()
	rep := &DeepReport{At: start, OK: true}
	dbFiles, err := layout.ListDbFiles()
	if err != nil {
		rep.OK = false
		rep.Failures = []string{err.Error()}
		rep.DurationMs = time.Since(start).Milliseconds()
		return rep
	}
	for _, df := range dbFiles {
		if err := ctx.Err(); err != nil {
			rep.OK = false
			rep.Failures = append(rep.Failures, fmt.Sprintf("canceled: %v", err))
			break
		}
		rep.Dbs = append(rep.Dbs, deepCheckDb(ctx, df))
	}
	rep.Files = deepCheckFiles(ctx, layout)
	for _, d := range rep.Dbs {
		if d.IntegrityCheck != "ok" {
			rep.OK = false
			rep.Failures = append(rep.Failures, fmt.Sprintf("integrity failed: %s: %s", d.Path, d.IntegrityCheck))
		}
	}
	if rep.Files.Skipped {
		rep.OK = false
		rep.Failures = append(rep.Failures, fmt.Sprintf("filestore missing: %s", rep.Files.Dir))
	}
	rep.DurationMs = time.Since(start).Milliseconds()
	return rep
}

func deepCheckDb(ctx context.Context, df DbFile) DeepDbReport {
	dr := DeepDbReport{
		SpaceId: df.SpaceId, Kind: df.Kind, Path: df.Path,
		Size: df.Size, TableHashes: map[string]string{},
	}
	if df.Size < 0 {
		dr.IntegrityCheck = "file missing"
		return dr
	}
	conn, err := sqlite.OpenConn(df.Path, sqlite.OpenReadOnly)
	if err != nil {
		dr.IntegrityCheck = fmt.Sprintf("open: %v", err)
		return dr
	}
	defer func() { _ = conn.Close() }()
	msg, err := pragmaFirstRow(conn, "PRAGMA integrity_check")
	if err != nil {
		dr.IntegrityCheck = fmt.Sprintf("pragma: %v", err)
		return dr
	}
	dr.IntegrityCheck = msg
	if msg != "ok" {
		return dr
	}
	tables, err := listTables(conn)
	if err != nil {
		dr.IntegrityCheck = fmt.Sprintf("list tables: %v", err)
		return dr
	}
	for _, t := range tables {
		if err := ctx.Err(); err != nil {
			dr.TableHashes[t] = fmt.Sprintf("canceled: %v", err)
			break
		}
		n, sum, err := hashTable(conn, t)
		if err != nil {
			dr.TableHashes[t] = fmt.Sprintf("error: %v", err)
			continue
		}
		dr.TableHashes[t] = fmt.Sprintf("rows=%d sha256=%s", n, sum)
	}
	return dr
}

func hashTable(conn *sqlite.Conn, table string) (int64, string, error) {
	quoted := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
	stmt, err := conn.Prepare(fmt.Sprintf(`SELECT id, data FROM %s ORDER BY id`, quoted))
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = stmt.Finalize() }()
	h := sha256.New()
	var n int64
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return n, "", err
		}
		if !hasRow {
			break
		}
		n++
		for col := 0; col < 2; col++ {
			if err := feedColumn(h, stmt, col); err != nil {
				return n, "", err
			}
			h.Write([]byte{0})
		}
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// feedColumn 把一列喂进哈希。小列一次拷贝，大列走 ColumnReader 流式读
// （ColumnBytes 总是从列头拷贝，不能分片循环）。
func feedColumn(h io.Writer, stmt *sqlite.Stmt, col int) error {
	n := stmt.ColumnLen(col)
	if n <= 1<<20 {
		buf := make([]byte, n)
		stmt.ColumnBytes(col, buf)
		_, err := h.Write(buf)
		return err
	}
	_, err := io.Copy(h, stmt.ColumnReader(col))
	return err
}

func deepCheckFiles(ctx context.Context, layout RepoLayout) DeepFileReport {
	fr := DeepFileReport{Dir: layout.FlatfsDir, FileHashes: map[string]string{}}
	if !layout.HasFlatfs {
		fr.Skipped = true
		return fr
	}
	var names []string
	err := filepath.Walk(layout.FlatfsDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rel, rerr := filepath.Rel(layout.FlatfsDir, p)
		if rerr != nil {
			return nil
		}
		names = append(names, rel)
		fr.FileCount++
		fr.TotalBytes += info.Size()
		return nil
	})
	if err != nil {
		fr.Skipped = true
		return fr
	}
	root := sha256.New()
	for _, rel := range names {
		if err := ctx.Err(); err != nil {
			break
		}
		sum, serr := hashFile(filepath.Join(layout.FlatfsDir, rel))
		if serr != nil {
			fr.FileHashes[rel] = fmt.Sprintf("error: %v", serr)
			continue
		}
		fr.FileHashes[rel] = sum
		root.Write([]byte(rel))
		root.Write([]byte{0})
		root.Write([]byte(sum))
	}
	fr.RootHex = hex.EncodeToString(root.Sum(nil))
	return fr
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
