package files

// 明文全量同步（06）：超阈值文件的内容写整文件一块 + chunk 块，pair 目录带
// chunk 清单；小文件路径保持原样。

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/core/files/chunkspec"
	"github.com/anyproto/anytype-heart/pkg/lib/bundle"
)

func TestChunkedLargeFileManifest(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)

	content := make([]byte, 2<<20)
	_, err := rand.Read(content)
	require.NoError(t, err)

	got, err := fx.FileAdd(ctx, spaceId,
		WithName("big.bin"),
		WithReader(bytes.NewReader(content)),
	)
	require.NoError(t, err)
	got.Commit()

	rootCid := cid.MustParse(got.FileId.String())
	root, err := fx.commonFileService.DAGService().Get(ctx, rootCid)
	require.NoError(t, err)

	// FileId 指向外层包裹目录，pair 嵌在里面：用清单发现定位。
	pairs, err := chunkspec.FindManifestPairs(root, func(c cid.Cid) (ipld.Node, error) {
		return fx.commonFileService.DAGService().Get(ctx, c)
	}, 2)
	require.NoError(t, err)
	require.Len(t, pairs, 1, "large file must carry exactly one chunk manifest")

	links := root.Links()
	_ = links
	contentLinkCid := pairs[0].ContentCid
	chunkCids := pairs[0].Chunks
	wantChunks := chunkspec.ChunkCount(int64(len(content)), chunkspec.ChunkSizeBytes())
	require.Greater(t, wantChunks, 1, "2MB must exceed one chunk")
	require.Len(t, chunkCids, wantChunks, "manifest must list every chunk")

	// 按清单顺序组装 == 源字节，且整块 CID 与 content 链接一致
	var assembled []byte
	for _, c := range chunkCids {
		node, err := fx.commonFileService.DAGService().Get(ctx, c)
		require.NoError(t, err)
		assembled = append(assembled, node.RawData()...)
	}
	require.Equal(t, content, assembled, "chunks must reassemble to source bytes")

	whole, err := fx.commonFileService.DAGService().Get(ctx, contentLinkCid)
	require.NoError(t, err)
	assert.Equal(t, content, whole.RawData(), "content link must stay the whole raw block")
	assert.Equal(t, contentLinkCid.String(), whole.Cid().String())

	// 本地读者走 content 主块：字节一致
	reader, err := fx.GetContentReader(ctx, spaceId, contentLinkCid.String(), "")
	require.NoError(t, err)
	byReader, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, byReader)
}

func TestSmallFileHasNoManifest(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)

	content := []byte("small file, one shot")
	got, err := fx.FileAdd(ctx, spaceId,
		WithName("small.txt"),
		WithReader(bytes.NewReader(content)),
	)
	require.NoError(t, err)
	got.Commit()

	root, err := fx.commonFileService.DAGService().Get(ctx, cid.MustParse(got.FileId.String()))
	require.NoError(t, err)
	pairs, err := chunkspec.FindManifestPairs(root, func(c cid.Cid) (ipld.Node, error) {
		return fx.commonFileService.DAGService().Get(ctx, c)
	}, 2)
	require.NoError(t, err)
	assert.Empty(t, pairs, "small file must not carry a chunk manifest")

	// 整文件一块：DAG 里内容原文只出现在一块
	entries := collectDAGBlocksForTest(t, fx, got.FileId)
	var hits int
	for _, b := range entries {
		if b.Cid().Prefix().Codec == cid.Raw && bytes.Equal(b.RawData(), content) {
			hits++
		}
	}
	assert.Equal(t, 1, hits)
}

func TestChunkedManifestKeepsFileObjectDetails(t *testing.T) {
	// 文件对象侧不受清单影响：Details 仍能读出名/size（读的是 content 主块）。
	ctx := context.Background()
	fx := newFixture(t)

	content := make([]byte, 3*chunkspec.DefaultChunkSize/2)
	_, err := rand.Read(content)
	require.NoError(t, err)

	got, err := fx.FileAdd(ctx, spaceId,
		WithName("mid.bin"),
		WithReader(bytes.NewReader(content)),
	)
	require.NoError(t, err)
	got.Commit()

	file, err := NewFile(fx.Service, domain.FullFileId{SpaceId: spaceId, FileId: got.FileId}, got.Variants)
	require.NoError(t, err)
	details, _, err := file.Details(ctx)
	require.NoError(t, err)
	assert.Equal(t, "mid", details.GetString(bundle.RelationKeyName))
}
