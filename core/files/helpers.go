package files

import (
	"context"
	"fmt"
	"io"

	"github.com/anyproto/any-sync/commonfile/fileblockstore"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"

	"github.com/anyproto/anytype-heart/core/files/filehelper"
	"github.com/anyproto/anytype-heart/pkg/lib/crypto/symmetric"
	"github.com/anyproto/anytype-heart/pkg/lib/ipfs/helpers"
)

// dagServiceForSpace returns a DAGService that is namespaced for the given spaceID.
// if fileHandler is nil the default dagService is used
func (s *service) dagServiceForSpace(spaceID string, customFileHandler *fileservice.FileHandler) ipld.DAGService {
	if customFileHandler != nil {
		return filehelper.NewDAGServiceWithSpaceID(spaceID, customFileHandler.DAGService())
	}
	return filehelper.NewDAGServiceWithSpaceID(spaceID, s.dagService)
}

func (s *service) addFileData(ctx context.Context, spaceID string, r io.Reader, fileHandler *fileservice.FileHandler) (ipld.Node, error) {
	// 明文全量同步：文件内容（与元数据）以整文件一块明文存放，不再按 unixfs 切分。
	// 整块以 raw codec 单块写入本地块存储；读取经由统一的 dagServiceForSpace，
	// 下游（GetFile/DagReader、filesync walker、offloader 链 walk）均兼容 raw 叶子。
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read file data: %w", err)
	}
	node := merkledag.NewRawNode(data)
	dagService := s.dagServiceForSpace(spaceID, fileHandler)
	if err := dagService.Add(ctx, node); err != nil {
		return nil, fmt.Errorf("add raw block: %w", err)
	}
	return node, nil
}

func (s *service) getFile(ctx context.Context, spaceID string, c cid.Cid) (ufsio.ReadSeekCloser, error) {
	cctx := fileblockstore.CtxWithSpaceId(ctx, spaceID)
	return s.commonFile.GetFile(cctx, c)
}

func (s *service) dataAtPath(ctx context.Context, spaceID string, pth string) (cid.Cid, symmetric.ReadSeekCloser, error) {
	dagService := s.dagServiceForSpace(spaceID, nil)
	newPath, err := path.NewPath("/ipfs/" + pth)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to resolve path %s: %w", pth, err)
	}
	rCid, err := helpers.ResolveCid(ctx, dagService, newPath)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to resolve path %s: %w", pth, err)
	}

	r, err := s.getFile(ctx, spaceID, rCid)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("failed to resolve path %s: %w", pth, err)
	}

	return rCid, r, nil
}
