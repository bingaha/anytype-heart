package filesync

// 明文全量同步（06）：带 chunk 清单的 pair DAG 只推 chunk，整块不上报
// （单块超推批次上限会被 batcher 硬拒绝），下载端按清单取块。

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/anyproto/any-store/query"
	"github.com/globalsign/mgo/bson"
	"github.com/ipfs/boxo/ipld/merkledag"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync/commonfile/fileblockstore"

	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/core/files/chunkspec"
	"github.com/anyproto/anytype-heart/core/files/filesync/filequeue"
)

// givenChunkedPairDAG builds a heart-style pair DAG (meta + whole content +
// chunk manifest) in the local DAG, mimicking what core/files writes for
// over-threshold contents.
func givenChunkedPairDAG(t *testing.T, fx *fixture, spaceId string, size int) (domain.FileId, cid.Cid, []cid.Cid) {
	t.Helper()
	ctx := fileblockstore.CtxWithSpaceId(context.Background(), spaceId)
	dagService := fx.fileService.DAGService()

	content := make([]byte, size)
	_, err := rand.Read(content)
	require.NoError(t, err)

	whole := merkledag.NewRawNode(content)
	require.NoError(t, dagService.Add(ctx, whole))
	meta := merkledag.NewRawNode([]byte(`{"name":"big.bin"}`))
	require.NoError(t, dagService.Add(ctx, meta))

	var chunks []cid.Cid
	cs := chunkspec.ChunkSizeBytes()
	for off := int64(0); off < int64(len(content)); off += cs {
		end := min(off+cs, int64(len(content)))
		node := merkledag.NewRawNode(content[off:end])
		require.NoError(t, dagService.Add(ctx, node))
		chunks = append(chunks, node.Cid())
	}
	require.Greater(t, len(chunks), 1, "test file must exceed one chunk")

	dir, err := uio.NewDirectory(dagService)
	require.NoError(t, err)
	require.NoError(t, dir.AddChild(ctx, chunkspec.MetaLinkName, meta))
	require.NoError(t, dir.AddChild(ctx, chunkspec.ContentLinkName, whole))
	for i, c := range chunks {
		node, err := dagService.Get(ctx, c)
		require.NoError(t, err)
		require.NoError(t, dir.AddChild(ctx, chunkspec.ChunkLinkName(i), node))
	}
	pairNode, err := dir.GetNode()
	require.NoError(t, err)
	require.NoError(t, dagService.Add(ctx, pairNode))

	// Real file DAGs nest the pair under an outer directory (link "0").
	outer, err := uio.NewDirectory(dagService)
	require.NoError(t, err)
	require.NoError(t, outer.AddChild(ctx, "0", pairNode))
	outerNode, err := outer.GetNode()
	require.NoError(t, err)
	require.NoError(t, dagService.Add(ctx, outerNode))

	return domain.FileId(outerNode.Cid().String()), whole.Cid(), chunks
}

func waitFileDone(t *testing.T, fx *fixture, fileId domain.FileId, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	it, err := fx.queue.GetNext(ctx, filequeue.GetNextRequest[FileInfo]{
		Subscribe: true,
		StoreFilter: query.And{
			filterByFileId(fileId.String()),
			filterByState(FileStateDone),
		},
		Filter: func(info FileInfo) bool { return info.FileId == fileId && info.State == FileStateDone },
	})
	require.NoError(t, err, "file must reach Done instead of looping on batch errors")
	require.NoError(t, fx.queue.ReleaseAndUpdate(it.ObjectId, it))
}

func TestChunkedFileUploadPushesChunksNotWhole(t *testing.T) {
	fx := newFixture(t, 1024*1024*1024)
	defer fx.Finish(t)

	spaceId := "space1"
	fileId, wholeCid, chunkCids := givenChunkedPairDAG(t, fx, spaceId, 2<<20)

	err := fx.AddFile(AddFileRequest{
		FileObjectId:   bson.NewObjectId().Hex(),
		FileId:         domain.FullFileId{SpaceId: spaceId, FileId: fileId},
		UploadedByUser: true,
	})
	require.NoError(t, err)

	waitFileDone(t, fx, fileId, 30*time.Second)

	// 整块从未上节点：按 cid 取应失败，否则说明又回到了整块推送
	_, err = fx.rpcStore.Get(ctx, wholeCid)
	require.Error(t, err, "whole %d-byte block must not be pushed (batcher would reject it)", 2<<20)

	// chunk 全在节点上且字节一致
	for _, c := range chunkCids {
		got, err := fx.rpcStore.Get(ctx, c)
		require.NoError(t, err, "chunk %s must be on the node", c)
		want, err := fx.localFileStorage.Get(ctx, c)
		require.NoError(t, err)
		assert.Equal(t, want.RawData(), got.RawData())
	}

	// 节点侧 FilesInfo 可见该文件（chunks 已绑定）
	infos, err := fx.rpcStore.FilesInfo(ctx, spaceId, fileId)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	assert.NotZero(t, infos[0].UsageBytes)
}

func TestChunkedLargeFileUpload70MB(t *testing.T) {
	fx := newFixture(t, 1024*1024*1024)
	defer fx.Finish(t)

	spaceId := "space1"
	fileId, wholeCid, chunkCids := givenChunkedPairDAG(t, fx, spaceId, 70<<20)

	err := fx.AddFile(AddFileRequest{
		FileObjectId:   bson.NewObjectId().Hex(),
		FileId:         domain.FullFileId{SpaceId: spaceId, FileId: fileId},
		UploadedByUser: true,
	})
	require.NoError(t, err)

	waitFileDone(t, fx, fileId, 120*time.Second)

	_, err = fx.rpcStore.Get(ctx, wholeCid)
	require.Error(t, err, "whole 70MB block must not be pushed")

	require.Len(t, chunkCids, chunkspec.ChunkCount(70<<20, chunkspec.ChunkSizeBytes()))
	for _, c := range chunkCids {
		_, err := fx.rpcStore.Get(ctx, c)
		require.NoError(t, err, "chunk %s must be on the node", c)
	}
}

func TestSmallPairDAGUploadsWholeBlock(t *testing.T) {
	// 无清单的 pair DAG（小文件/legacy）保持原行为：整块推送。
	fx := newFixture(t, 1024*1024*1024)
	defer fx.Finish(t)

	spaceId := "space1"
	content := []byte("small file, one shot")
	ctx := fileblockstore.CtxWithSpaceId(context.Background(), spaceId)
	dagService := fx.fileService.DAGService()

	whole := merkledag.NewRawNode(content)
	require.NoError(t, dagService.Add(ctx, whole))
	meta := merkledag.NewRawNode([]byte(`{"name":"small.txt"}`))
	require.NoError(t, dagService.Add(ctx, meta))
	dir, err := uio.NewDirectory(dagService)
	require.NoError(t, err)
	require.NoError(t, dir.AddChild(ctx, chunkspec.MetaLinkName, meta))
	require.NoError(t, dir.AddChild(ctx, chunkspec.ContentLinkName, whole))
	pairNode, err := dir.GetNode()
	require.NoError(t, err)
	require.NoError(t, dagService.Add(ctx, pairNode))
	fileId := domain.FileId(pairNode.Cid().String())

	err = fx.AddFile(AddFileRequest{
		FileObjectId:   bson.NewObjectId().Hex(),
		FileId:         domain.FullFileId{SpaceId: spaceId, FileId: fileId},
		UploadedByUser: true,
	})
	require.NoError(t, err)

	waitFileDone(t, fx, fileId, 30*time.Second)

	got, err := fx.rpcStore.Get(ctx, whole.Cid())
	require.NoError(t, err, "small whole block must be pushed as before")
	assert.Equal(t, content, got.RawData())
}
