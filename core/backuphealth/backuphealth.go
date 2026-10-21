package backuphealth

// Package backuphealth 是明文全量同步的备份健康层（07 号票，全部新增）。
//
// 三件事：
//  1. 快速自检：StateChange 监听到退出/切后台时运行。注册顺序排在 durability
//     之后（见 bootstrap），flush 已发生，随后只读核对——库能打开、表行数对上、
//     最新文件大小对上。阻塞 <1s，失败记报告并打 ERROR 日志提示暂缓关闭。
//  2. 按需深校验：DeepVerify(ctx) 全量哈希报告，空闲或设置页手动触发。
//  3. 定期明文导出：ExportAllSpacesNow + SetPeriodicExport，复用 export 链。
//
// 留给 08 号票接按钮的触发接口就是 Service 的方法集；08 号只需包一层
// middleware RPC 透出 LastQuickReport/LastDeepReport/LastExport 即可。

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"go.uber.org/zap"

	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/core/wallet"
	"github.com/anyproto/anytype-heart/pkg/lib/datastore/anystoreprovider"
	"github.com/anyproto/anytype-heart/pkg/lib/localstore/objectstore"
	"github.com/anyproto/anytype-heart/pkg/lib/logging"
)

// CName 是组件名。
const CName = "backuphealth"

var log = logging.LoggerNotSugared(CName)

// Service 是留给 08 号票接按钮的触发接口。本票已全部实现。
type Service interface {
	app.Component
	// QuickCheckNow 手动触发一次快速自检（设置页“检查备份”按钮用）。
	QuickCheckNow(ctx context.Context) *QuickReport
	// DeepVerify 手动触发一次全量深校验（设置页“深校验”按钮用）。
	DeepVerify(ctx context.Context) *DeepReport
	// ExportAllSpacesNow 手动触发一次全量明文导出（设置页“立即导出”按钮用）。
	ExportAllSpacesNow(ctx context.Context, destDir string) *ExportSummary
	// SetPeriodicExport 打开/调整定期导出；interval<=0 或 destDir=="" 为关闭。
	SetPeriodicExport(interval time.Duration, destDir string)
	// StopPeriodicExport 关闭定期导出。
	StopPeriodicExport()
	// LastQuickReport / LastDeepReport / LastExport 供 08 号展示用。
	LastQuickReport() *QuickReport
	LastDeepReport() *DeepReport
	LastExport() *ExportSummary
}

type service struct {
	repoRoot   string
	layout     RepoLayout
	layoutErr  error
	provider   anystoreprovider.Provider
	objStore   objectstore.ObjectStore
	exporter   MarkdownExporter
	periodic   *periodicExport
	mu         sync.RWMutex
	lastQuick  *QuickReport
	lastDeep   *DeepReport
	lastExport *ExportSummary
}

// New 创建备份健康组件。
func New() Service {
	return &service{periodic: &periodicExport{}}
}

func (s *service) Name() string { return CName }

func (s *service) Init(a *app.App) error {
	if w, err := app.GetComponent[wallet.Wallet](a); err == nil {
		s.repoRoot = w.RepoPath()
	} else {
		return fmt.Errorf("wallet missing: %w", err)
	}
	s.layout, s.layoutErr = DiscoverLayout(s.repoRoot)
	// 以下全是可选依赖：缺了就降级（行数不对/只做目录级检查），不炸 Init。
	if p, err := app.GetComponent[anystoreprovider.Provider](a); err == nil {
		s.provider = p
	}
	if o, err := app.GetComponent[objectstore.ObjectStore](a); err == nil {
		s.objStore = o
	}
	if e, err := app.GetComponent[MarkdownExporter](a); err == nil {
		s.exporter = e
	}
	return nil
}

func (s *service) Run(ctx context.Context) error { return nil }

func (s *service) Close(ctx context.Context) error {
	s.periodic.stop()
	return nil
}

// StateChange 挂在 durability 退出/后台 flush 点之后（注册顺序保证）。
// 同步阻塞但自带 800ms 预算，超时即失败返回，不拖关机。
func (s *service) StateChange(state int) {
	switch domain.CompState(state) {
	case domain.CompStateAppClosingInitiated, domain.CompStateAppWentBackground:
	default:
		return
	}
	if s.layoutErr != nil {
		log.Error("backuphealth layout broken, advise user to keep app open",
			zap.Error(s.layoutErr))
		return
	}
	rep := RunQuickCheck(context.Background(), s.layout, s.liveExpectedCounts())
	s.mu.Lock()
	s.lastQuick = rep
	s.mu.Unlock()
	_ = persistReport(s.repoRoot, "last-quickcheck.json", rep)
	if rep.OK {
		log.Info("backup alive",
			zap.Int64("durationMs", rep.DurationMs),
			zap.Int("dbs", len(rep.Dbs)),
			zap.Int("files", rep.Files.FileCount))
		return
	}
	// 失败：ERROR 日志即“提示用户暂缓关闭”在后端侧的落地；
	// 前端弹窗由 08 号读 LastQuickReport 实现。
	log.Error("backup quickcheck FAILED, advise user to keep app open and retry",
		zap.Strings("failures", rep.Failures),
		zap.Int64("durationMs", rep.DurationMs))
}

// liveExpectedCounts 从在线组件取期望行数。取不到就返回空——
// 此时只查“库能打开”，不对行数。
func (s *service) liveExpectedCounts() ExpectedCounts {
	exp := ExpectedCounts{}
	if s.provider == nil {
		return exp
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	// 通用库
	if db := s.provider.GetCommonDb(); db != nil {
		names, err := db.GetCollectionNames(ctx)
		if err == nil {
			for _, c := range names {
				coll, err := db.Collection(ctx, c)
				if err != nil {
					continue
				}
				n, err := coll.Count(ctx)
				if err != nil {
					continue
				}
				exp["\x00"+c] = int64(n)
			}
		}
	}
	// 各 Space 库：只查已打开的，避免在退出路径上现开库。
	var spaces []string
	if s.objStore != nil {
		spaces = s.objStore.OpenedSpaceIds()
	} else {
		dbs, err := s.layout.ListDbFiles()
		if err == nil {
			seen := map[string]bool{}
			for _, d := range dbs {
				if d.SpaceId != "" && !seen[d.SpaceId] {
					seen[d.SpaceId] = true
					spaces = append(spaces, d.SpaceId)
				}
			}
		}
	}
	for _, spaceId := range spaces {
		db, err := s.provider.GetSpaceIndexDb(spaceId)
		if err != nil {
			continue
		}
		names, err := db.GetCollectionNames(ctx)
		if err != nil {
			continue
		}
		for _, c := range names {
			coll, err := db.Collection(ctx, c)
			if err != nil {
				continue
			}
			n, err := coll.Count(ctx)
			if err != nil {
				continue
			}
			exp[spaceId+"\x00"+c] = int64(n)
		}
		if err := ctx.Err(); err != nil {
			break
		}
	}
	return exp
}

// QuickCheckNow 手动触发快速自检。
func (s *service) QuickCheckNow(ctx context.Context) *QuickReport {
	rep := RunQuickCheck(ctx, s.layout, s.liveExpectedCounts())
	s.mu.Lock()
	s.lastQuick = rep
	s.mu.Unlock()
	_ = persistReport(s.repoRoot, "last-quickcheck.json", rep)
	return rep
}

// DeepVerify 手动/空闲触发全量深校验。
func (s *service) DeepVerify(ctx context.Context) *DeepReport {
	rep := RunDeepVerify(ctx, s.layout)
	s.mu.Lock()
	s.lastDeep = rep
	s.mu.Unlock()
	_ = persistReport(s.repoRoot, "last-deepverify.json", rep)
	return rep
}

// ExportAllSpacesNow 手动触发全量明文导出。
func (s *service) ExportAllSpacesNow(ctx context.Context, destDir string) *ExportSummary {
	var lister SpaceLister
	if s.objStore != nil {
		lister = s.objStore
	}
	sum := ExportAllSpacesMarkdown(ctx, s.exporter, lister, destDir)
	s.mu.Lock()
	s.lastExport = sum
	s.mu.Unlock()
	_ = persistReport(s.repoRoot, "last-export.json", sum)
	return sum
}

// SetPeriodicExport 打开/调整定期导出。
func (s *service) SetPeriodicExport(interval time.Duration, destDir string) {
	s.periodic.set(s, interval, destDir)
}

// StopPeriodicExport 关闭定期导出。
func (s *service) StopPeriodicExport() {
	s.periodic.stop()
}

func (s *service) LastQuickReport() *QuickReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastQuick
}

func (s *service) LastDeepReport() *DeepReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastDeep
}

func (s *service) LastExport() *ExportSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastExport
}

// persistReport 把报告落到 <repo>/backuphealth/ 下，停机也能用通用工具看。
func persistReport(repoRoot, name string, v any) error {
	if repoRoot == "" {
		return nil
	}
	dir := filepath.Join(repoRoot, "backuphealth")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), data, 0644)
}
