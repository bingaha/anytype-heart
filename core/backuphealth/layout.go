package backuphealth

// 明文全量同步 07 号票：备份健康层（只碰备份层，其它票的文件一律不动）。
//
// 停机冷备份就是运行数据目录本身（停机一致点）：
//   - <repo>/objectstore/objects.db            全 Space 通用索引库
//   - <repo>/objectstore/<spaceId>/objects.db  每 Space 索引库
//   - <repo>/objectstore/<spaceId>/crdt.db      每 Space CRDT 库
//   - <repo>/flatfs/                           整文件明文块（IPFS flatfs 分片存放）
//
// 本文件只做“目录长什么样”的发现，不读业务语义。

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const (
	objectStoreDirName = "objectstore"
	flatfsDirName      = "flatfs"
	commonDbName       = "objects.db"
	crdtDbName         = "crdt.db"
)

// RepoLayout 是停机冷备份的目录视图。FlatfsDir 可能不存在
// （自定义 IPFSStorageAddr 的情况），此时与文件相关的检查跳过并注明。
type RepoLayout struct {
	RepoRoot       string
	ObjectStoreDir string
	FlatfsDir      string
	HasFlatfs      bool
}

// DiscoverLayout 从钱包数据目录推导备份目录布局。不存在 objectstore 视为致命问题。
func DiscoverLayout(repoRoot string) (RepoLayout, error) {
	l := RepoLayout{
		RepoRoot:       repoRoot,
		ObjectStoreDir: filepath.Join(repoRoot, objectStoreDirName),
		FlatfsDir:      filepath.Join(repoRoot, flatfsDirName),
	}
	st, err := os.Stat(l.ObjectStoreDir)
	if err != nil {
		return RepoLayout{}, fmt.Errorf("objectstore dir missing: %w", err)
	}
	if !st.IsDir() {
		return RepoLayout{}, fmt.Errorf("objectstore is not a dir: %s", l.ObjectStoreDir)
	}
	if st, err := os.Stat(l.FlatfsDir); err == nil && st.IsDir() {
		l.HasFlatfs = true
	}
	return l, nil
}

// DbFile 是一份待查的 sqlite 文件。
type DbFile struct {
	// SpaceId 为 "" 表示通用库 objects.db。
	SpaceId string
	Kind    string // "index" | "crdt" | "common"
	Path    string
	Size    int64
}

// ListDbFiles 枚举全部应检查的 sqlite 文件（common + 每 space 的 index/crdt）。
// 文件不存在会被上层记为失败，而不是静默跳过——丢库必须报警。
func (l RepoLayout) ListDbFiles() ([]DbFile, error) {
	var out []DbFile
	common := filepath.Join(l.ObjectStoreDir, commonDbName)
	if st, err := os.Stat(common); err != nil {
		out = append(out, DbFile{Kind: "common", Path: common, Size: -1})
	} else {
		out = append(out, DbFile{Kind: "common", Path: common, Size: st.Size()})
	}
	entries, err := os.ReadDir(l.ObjectStoreDir)
	if err != nil {
		return nil, fmt.Errorf("read objectstore dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		spaceId := e.Name()
		for kind, name := range map[string]string{"index": commonDbName, "crdt": crdtDbName} {
			p := filepath.Join(l.ObjectStoreDir, spaceId, name)
			df := DbFile{SpaceId: spaceId, Kind: kind, Path: p, Size: -1}
			if st, err := os.Stat(p); err == nil && !st.IsDir() {
				df.Size = st.Size()
			}
			out = append(out, df)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}
