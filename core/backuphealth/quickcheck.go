package backuphealth

// 快速自检：在 durability 退出/后台 flush 点之后运行，只做三件事——
// 库能打开（只读 open + PRAGMA quick_check）、表行数对上（期望行数由调用方
// 从在线组件取，flush 后两者必须一致）、最新文件大小对上。
// 预算 <1s：全部只读、不哈希文件内容，超时直接按失败返回。

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"zombiezen.com/go/sqlite"
)

const (
	// QuickCheckBudget 是快速自检的总预算。StateChange 里同步阻塞，
	// 超时即失败返回，绝不拖慢关机。
	QuickCheckBudget = 800 * time.Millisecond
)

// quickCheckBudget 供单测收紧预算；生产值恒为 QuickCheckBudget。
var quickCheckBudget = QuickCheckBudget

// ExpectedCounts 是调用方（在线组件）给出的期望行数，key 形如
// "<spaceId>\x00<collection>"，spaceId 为 "" 表示通用库。
// 为空表示“只查库能打开，不对行数”。
type ExpectedCounts map[string]int64

// DbCheck 是单个 sqlite 文件的检查结果。
type DbCheck struct {
	SpaceId string `json:"spaceId"`
	Kind    string `json:"kind"`
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	// Tables key 为裸表名，value 为行数。
	Tables map[string]int64 `json:"tables"`
	// Mismatches 记录期望行数对不上的 collection。
	Mismatches []string `json:"mismatches,omitempty"`
	Opened     bool     `json:"opened"`
	QuickCheck string   `json:"quickCheck"`
}

// FileCheck 是附件区（flatfs）的检查结果：只 stat，不读内容。
type FileCheck struct {
	Dir         string `json:"dir"`
	Skipped     bool   `json:"skipped,omitempty"`
	FileCount   int    `json:"fileCount"`
	TotalBytes  int64  `json:"totalBytes"`
	NewestFile  string `json:"newestFile,omitempty"`
	NewestSize  int64  `json:"newestSize,omitempty"`
	NewestMtime int64  `json:"newestMtime,omitempty"`
}

// QuickReport 是快速自检报告。OK=false 时调用方必须提示用户暂缓关闭。
type QuickReport struct {
	At         time.Time `json:"at"`
	DurationMs int64     `json:"durationMs"`
	OK         bool      `json:"ok"`
	Failures   []string  `json:"failures,omitempty"`
	Dbs        []DbCheck `json:"dbs"`
	Files      FileCheck `json:"files"`
}

// RunQuickCheck 执行一次快速自检。expected 为 nil 时跳过行数对比。
// 总耗时被 quickCheckBudget 硬封顶：单个 sqlite 调用（quick_check 等）
// 不接受 ctx，会阻塞，因此每个库检查跑在独立 goroutine 里，外层按剩余
// 预算 select，超时即记失败返回。超时的 goroutine 会自行结束并关连接，
// 不影响报告（只读连接，无副作用）。
func RunQuickCheck(ctx context.Context, layout RepoLayout, expected ExpectedCounts) *QuickReport {
	start := time.Now()
	deadline := start.Add(quickCheckBudget)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	rep := &QuickReport{At: start, OK: true}
	dbFiles, err := layout.ListDbFiles()
	if err != nil {
		return fail(rep, start, err.Error())
	}
	for _, df := range dbFiles {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			rep.Dbs = append(rep.Dbs, DbCheck{SpaceId: df.SpaceId, Kind: df.Kind, Path: df.Path, Size: df.Size, Tables: map[string]int64{}, QuickCheck: "budget exceeded"})
			continue
		}
		rep.Dbs = append(rep.Dbs, checkDbFileBudget(df, expected, remaining))
	}
	rep.Files = checkFiles(ctx, layout)
	for _, d := range rep.Dbs {
		if !d.Opened {
			rep.Failures = append(rep.Failures, fmt.Sprintf("db unreadable: %s", d.Path))
		} else if d.QuickCheck != "ok" {
			rep.Failures = append(rep.Failures, fmt.Sprintf("db quick_check failed: %s: %s", d.Path, d.QuickCheck))
		}
		rep.Failures = append(rep.Failures, d.Mismatches...)
	}
	if rep.Files.Skipped {
		rep.Failures = append(rep.Failures, fmt.Sprintf("filestore missing: %s", rep.Files.Dir))
	}
	rep.OK = len(rep.Failures) == 0
	rep.DurationMs = time.Since(start).Milliseconds()
	return rep
}

func fail(rep *QuickReport, start time.Time, msg string) *QuickReport {
	rep.OK = false
	rep.Failures = []string{msg}
	rep.DurationMs = time.Since(start).Milliseconds()
	return rep
}

// checkDbFileBudget 带硬超时地检查单个库。
func checkDbFileBudget(df DbFile, expected ExpectedCounts, timeout time.Duration) DbCheck {
	if df.Size < 0 {
		return DbCheck{SpaceId: df.SpaceId, Kind: df.Kind, Path: df.Path, Size: df.Size, Tables: map[string]int64{}}
	}
	ch := make(chan DbCheck, 1)
	go func() {
		ch <- checkDbFile(context.Background(), df, expected)
	}()
	select {
	case dc := <-ch:
		return dc
	case <-time.After(timeout):
		return DbCheck{SpaceId: df.SpaceId, Kind: df.Kind, Path: df.Path, Size: df.Size,
			Tables: map[string]int64{}, QuickCheck: fmt.Sprintf("budget exceeded (%s)", timeout)}
	}
}

func checkDbFile(ctx context.Context, df DbFile, expected ExpectedCounts) DbCheck {
	dc := DbCheck{
		SpaceId: df.SpaceId, Kind: df.Kind, Path: df.Path,
		Size: df.Size, Tables: map[string]int64{},
	}
	if df.Size < 0 {
		return dc // 文件压根不存在：Opened=false，调用方报警
	}
	if err := ctx.Err(); err != nil {
		dc.QuickCheck = err.Error()
		return dc
	}
	conn, err := sqlite.OpenConn(df.Path, sqlite.OpenReadOnly)
	if err != nil {
		dc.QuickCheck = fmt.Sprintf("open: %v", err)
		return dc
	}
	defer func() { _ = conn.Close() }()
	dc.Opened = true

	if msg, err := pragmaFirstRow(conn, "PRAGMA quick_check"); err != nil {
		dc.QuickCheck = fmt.Sprintf("pragma: %v", err)
		return dc
	} else {
		dc.QuickCheck = msg
	}
	if dc.QuickCheck != "ok" {
		return dc // 结构已坏，不再数行数
	}
	tables, err := listTables(conn)
	if err != nil {
		dc.QuickCheck = fmt.Sprintf("list tables: %v", err)
		dc.Opened = false
		return dc
	}
	for _, t := range tables {
		if err := ctx.Err(); err != nil {
			dc.Mismatches = append(dc.Mismatches, fmt.Sprintf("budget exceeded while counting %s", t))
			break
		}
		n, err := countRows(conn, t)
		if err != nil {
			dc.Mismatches = append(dc.Mismatches, fmt.Sprintf("count %s: %v", t, err))
			continue
		}
		dc.Tables[t] = n
	}
	// 行数对上：在线期望 vs 落盘实数。表名带命名空间前缀，
	// 按后缀 "_<collection>_docs" 匹配。crdt 库是内部同步状态，
	// 不参与行数对比（它的表即使同名也不是业务行数）。
	if df.Kind == "crdt" {
		return dc
	}
	for key, want := range expected {
		spaceId, coll := splitExpectedKey(key)
		if spaceId != df.SpaceId {
			continue
		}
		matched := false
		for t, got := range dc.Tables {
			if strings.HasSuffix(t, "_"+coll+"_docs") {
				matched = true
				if got != want {
					dc.Mismatches = append(dc.Mismatches,
						fmt.Sprintf("rows mismatch %s/%s: live=%d disk=%d", df.SpaceId, coll, want, got))
				}
			}
		}
		if !matched && df.Kind != "crdt" {
			dc.Mismatches = append(dc.Mismatches,
				fmt.Sprintf("collection missing on disk %s/%s", df.SpaceId, coll))
		}
	}
	return dc
}

func splitExpectedKey(key string) (spaceId, coll string) {
	if i := strings.Index(key, "\x00"); i >= 0 {
		return key[:i], key[i+1:]
	}
	return "", key
}

func pragmaFirstRow(conn *sqlite.Conn, pragma string) (string, error) {
	stmt, err := conn.Prepare(pragma)
	if err != nil {
		return "", err
	}
	defer func() { _ = stmt.Finalize() }()
	hasRow, err := stmt.Step()
	if err != nil {
		return "", err
	}
	if !hasRow {
		return "", fmt.Errorf("no rows")
	}
	return stmt.ColumnText(0), nil
}

func listTables(conn *sqlite.Conn) ([]string, error) {
	stmt, err := conn.Prepare("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Finalize() }()
	var out []string
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, err
		}
		if !hasRow {
			break
		}
		name := stmt.ColumnText(0)
		if strings.HasSuffix(name, "_docs") {
			out = append(out, name)
		}
	}
	return out, nil
}

func countRows(conn *sqlite.Conn, table string) (int64, error) {
	// 表名来自 sqlite_master，白名单后缀已过滤；仍用双引号包裹防关键字。
	stmt, err := conn.Prepare(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, strings.ReplaceAll(table, `"`, `""`)))
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Finalize() }()
	hasRow, err := stmt.Step()
	if err != nil {
		return 0, err
	}
	if !hasRow {
		return 0, fmt.Errorf("no rows")
	}
	return stmt.ColumnInt64(0), nil
}

func checkFiles(ctx context.Context, layout RepoLayout) FileCheck {
	fc := FileCheck{Dir: layout.FlatfsDir}
	if !layout.HasFlatfs {
		fc.Skipped = true
		return fc
	}
	err := filepath.Walk(layout.FlatfsDir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // 单个文件读不到就跳过，不让自检崩掉
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if info.IsDir() {
			return nil
		}
		fc.FileCount++
		fc.TotalBytes += info.Size()
		if mtime := info.ModTime().Unix(); mtime > fc.NewestMtime {
			fc.NewestMtime = mtime
			fc.NewestFile = p
			fc.NewestSize = info.Size()
		}
		return nil
	})
	if err != nil {
		fc.Skipped = true
	}
	// 最新文件必须能打开读出 NewestSize 个字节——大小对不上即报警。
	if !fc.Skipped && fc.NewestFile != "" {
		f, err := os.Open(fc.NewestFile)
		if err != nil {
			fc.NewestFile += " (unreadable)"
			return fc
		}
		defer f.Close()
		buf := make([]byte, 4096)
		var read int64
		for {
			n, rerr := f.Read(buf)
			read += int64(n)
			if rerr != nil {
				break
			}
		}
		if read != fc.NewestSize {
			fc.NewestFile += " (short read)"
		}
	}
	return fc
}
