package fileoffloader

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/core/block/object/idresolver/mock_idresolver"
	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/core/files/chunkspec"
	"github.com/anyproto/anytype-heart/core/files/filestorage"
	"github.com/anyproto/anytype-heart/core/syncstatus/filesyncstatus"
	"github.com/anyproto/anytype-heart/pkg/lib/bundle"
	"github.com/anyproto/anytype-heart/pkg/lib/datastore"
	"github.com/anyproto/anytype-heart/pkg/lib/ipfs/helpers"
	"github.com/anyproto/anytype-heart/pkg/lib/localstore/objectstore"
	"github.com/anyproto/anytype-heart/tests/testutil"
)

type fixture struct {
	Service

	commonFile  fileservice.FileService
	objectStore *objectstore.StoreFixture
}

func newFixture(t *testing.T) *fixture {
	blockStorage := filestorage.NewInMemory()
	commonFileService := fileservice.New()
	objectStore := objectstore.NewStoreFixture(t)
	dataStoreProvider, err := datastore.NewInMemory()
	spaceIdResolver := mock_idresolver.NewMockResolver(t)

	require.NoError(t, err)
	offloader := New()

	ctx := context.Background()
	a := new(app.App)
	a.Register(dataStoreProvider)
	a.Register(blockStorage)
	a.Register(commonFileService)
	a.Register(objectStore)
	a.Register(offloader)
	a.Register(testutil.PrepareMock(ctx, a, spaceIdResolver))

	err = a.Start(ctx)
	require.NoError(t, err)

	return &fixture{
		Service:     offloader,
		commonFile:  commonFileService,
		objectStore: objectStore,
	}
}

func TestOffloadAllFiles(t *testing.T) {
	fx := newFixture(t)

	ctx := context.Background()
	fileNode1, err := fx.commonFile.AddFile(ctx, generateTestFileData(t, 2*1024*1024))
	require.NoError(t, err)

	fileNode2, err := fx.commonFile.AddFile(ctx, generateTestFileData(t, 2*1024*1024))
	require.NoError(t, err)

	fx.objectStore.AddObjects(t, "space1", []objectstore.TestObject{
		{
			bundle.RelationKeyId:               domain.String("fileObjectId1"),
			bundle.RelationKeyFileId:           domain.String(fileNode1.Cid().String()),
			bundle.RelationKeyFileBackupStatus: domain.Int64(int64(filesyncstatus.Synced)),
		},
		{
			bundle.RelationKeyId:               domain.String("fileObjectId2"),
			bundle.RelationKeyFileId:           domain.String(fileNode2.Cid().String()),
			bundle.RelationKeyFileBackupStatus: domain.Int64(int64(filesyncstatus.Limited)),
		},
	})

	err = fx.FilesOffload(ctx, nil, false)
	require.NoError(t, err)

	_, err = fx.commonFile.GetFile(ctx, fileNode1.Cid())
	require.Error(t, err)

	_, err = fx.commonFile.GetFile(ctx, fileNode2.Cid())
	require.NoError(t, err)
}

func mustCid(t *testing.T, hash string) cid.Cid {
	t.Helper()
	c, err := cid.Decode(hash)
	require.NoError(t, err)
	return c
}

func TestChunkedManifestCountsContentOnce(t *testing.T) {
	// 明文全量同步（06 分片清单）：pair 目录含 content + chunk-* 链接时，
	// 释放统计只计 content 一次（chunk 是传输副本），但删除仍覆盖全部块。
	fx := newFixture(t)
	ctx := context.Background()
	dagSvc := fx.commonFile.DAGService()

	putRaw := func(data []byte) string {
		n := merkledag.NewRawNode(data)
		require.NoError(t, dagSvc.Add(ctx, n))
		return n.Cid().String()
	}

	content := make([]byte, 100)
	_, err := rand.Read(content)
	require.NoError(t, err)
	chunkA := make([]byte, 50)
	_, err = rand.Read(chunkA)
	require.NoError(t, err)
	chunkB := make([]byte, 50)
	_, err = rand.Read(chunkB)
	require.NoError(t, err)

	contentHash := putRaw(content)
	metaHash := putRaw([]byte(`{"name":"big.bin"}`))
	chunkAHash := putRaw(chunkA)
	chunkBHash := putRaw(chunkB)

	pair, err := uio.NewDirectory(dagSvc)
	require.NoError(t, err)
	require.NoError(t, helpers.AddLinkToDirectory(ctx, dagSvc, pair, "meta", metaHash))
	require.NoError(t, helpers.AddLinkToDirectory(ctx, dagSvc, pair, "content", contentHash))
	require.NoError(t, helpers.AddLinkToDirectory(ctx, dagSvc, pair, chunkspec.ChunkLinkName(0), chunkAHash))
	require.NoError(t, helpers.AddLinkToDirectory(ctx, dagSvc, pair, chunkspec.ChunkLinkName(1), chunkBHash))
	pairNode, err := pair.GetNode()
	require.NoError(t, err)
	require.NoError(t, dagSvc.Add(ctx, pairNode))

	totalSize, cids, err := fx.Service.(*service).getAllExistingFileBlocksCids(ctx, domain.FullFileId{
		SpaceId: "space1",
		FileId:  domain.FileId(pairNode.Cid().String()),
	})
	require.NoError(t, err)

	rawLen := func(hash string) uint64 {
		n, err := dagSvc.Get(ctx, mustCid(t, hash))
		require.NoError(t, err)
		return uint64(len(n.RawData()))
	}
	// 计入：pair 目录自身 + meta + content；不计入：chunk-* 传输副本。
	wantSize := rawLen(pairNode.Cid().String()) + rawLen(metaHash) + rawLen(contentHash)
	assert.Equal(t, wantSize, totalSize, "chunk bytes must not be double-counted")

	got := make(map[string]struct{}, len(cids))
	for _, c := range cids {
		got[c.String()] = struct{}{}
	}
	for _, want := range []string{pairNode.Cid().String(), contentHash, metaHash, chunkAHash, chunkBHash} {
		assert.Contains(t, got, want, "offload deletion must still cover every block")
	}
}

func TestSpaceOffload(t *testing.T) {
	fx := newFixture(t)

	ctx := context.Background()
	fileNode1, err := fx.commonFile.AddFile(ctx, generateTestFileData(t, 2*1024*1024))
	require.NoError(t, err)

	fileNode2, err := fx.commonFile.AddFile(ctx, generateTestFileData(t, 2*1024*1024))
	require.NoError(t, err)

	fx.objectStore.AddObjects(t, "space1", []objectstore.TestObject{
		{
			bundle.RelationKeyId:               domain.String("fileObjectId1"),
			bundle.RelationKeySpaceId:          domain.String("space1"),
			bundle.RelationKeyFileId:           domain.String(fileNode1.Cid().String()),
			bundle.RelationKeyFileBackupStatus: domain.Int64(int64(filesyncstatus.Synced)),
		},
		{
			bundle.RelationKeyId:               domain.String("fileObjectId2"),
			bundle.RelationKeySpaceId:          domain.String("space1"),
			bundle.RelationKeyFileId:           domain.String(fileNode2.Cid().String()),
			bundle.RelationKeyFileBackupStatus: domain.Int64(int64(filesyncstatus.Synced)),
		},
	})
	fx.objectStore.AddObjects(t, "space2", []objectstore.TestObject{
		{
			bundle.RelationKeyId:               domain.String("fileObjectId3"),
			bundle.RelationKeySpaceId:          domain.String("space2"),
			bundle.RelationKeyFileId:           domain.String(fileNode2.Cid().String()),
			bundle.RelationKeyFileBackupStatus: domain.Int64(int64(filesyncstatus.Synced)),
		},
	})

	offloaded, _, err := fx.FileSpaceOffload(ctx, "space1", false)
	require.NoError(t, err)
	assert.True(t, 1 == offloaded)

	_, err = fx.commonFile.GetFile(ctx, fileNode1.Cid())
	require.Error(t, err)

	_, err = fx.commonFile.GetFile(ctx, fileNode2.Cid())
	require.NoError(t, err)
}

func generateTestFileData(t *testing.T, size int) io.Reader {
	buf := make([]byte, size)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	return bytes.NewReader(buf)
}
