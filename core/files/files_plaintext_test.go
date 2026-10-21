package files

// 明文全量同步：整文件一块明文的端到端断言。
// 写入 -> 本地块为单 raw 块且字节与源一致 -> 块级推拉到第二节点 -> 对端无密钥离线读出一致。

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/pkg/lib/bundle"
)

func TestPlaintextWholeFileRoundtrip(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)

	content := "it's my favorite file, stored as one plaintext block"
	opts := []AddOption{
		WithName(testFileName),
		WithLastModifiedDate(time.Now().Unix()),
		WithReader(strings.NewReader(content)),
	}
	got, err := fx.FileAdd(ctx, spaceId, opts...)
	require.NoError(t, err)
	require.NotEmpty(t, got.FileId)
	got.Commit()

	require.Len(t, got.Variants, 1)
	variant := got.Variants[0]

	// 无密钥：Key 留空
	assert.Empty(t, variant.Key)

	// 内容块是单 raw 块：无子链，字节与源一致
	contentCid := cid.MustParse(variant.Hash)
	require.Equal(t, uint64(cid.Raw), contentCid.Prefix().Codec)
	contentNode, err := fx.commonFileService.DAGService().Get(ctx, contentCid)
	require.NoError(t, err)
	assert.Empty(t, contentNode.Links())
	assert.Equal(t, content, string(contentNode.RawData()))

	// 整文件一块：从根 walk 整棵 DAG，内容原文只出现在一块里
	entries := collectDAGBlocksForTest(t, fx, got.FileId)
	var contentHits int
	for _, b := range entries {
		if b.Cid().Prefix().Codec == cid.Raw && bytes.Equal(b.RawData(), []byte(content)) {
			contentHits++
		}
	}
	assert.Equal(t, 1, contentHits, "file content must be stored as exactly one raw block")

	// 文件对象侧记录文件名/sha256/size（经由 File.Details）
	file, err := NewFile(fx.Service, domain.FullFileId{SpaceId: spaceId, FileId: got.FileId}, got.Variants)
	require.NoError(t, err)
	details, _, err := file.Details(ctx)
	require.NoError(t, err)
	assert.Equal(t, testFileName, details.GetString(bundle.RelationKeyName))
	assert.Equal(t, float64(len(content)), details.GetFloat64(bundle.RelationKeySizeInBytes))
	assert.NotEmpty(t, variant.Checksum, "sha256 checksum must be recorded")
}

func TestPlaintextWholeFileTwoNodeTransfer(t *testing.T) {
	ctx := context.Background()
	fxA := newFixture(t)
	fxB := newFixture(t)

	content := "attachment bytes that travel from node A to node B as plaintext"
	addOpts := []AddOption{
		WithName("attachment.bin"),
		WithLastModifiedDate(time.Now().Unix()),
		WithReader(strings.NewReader(content)),
	}
	added, err := fxA.FileAdd(ctx, spaceId, addOpts...)
	require.NoError(t, err)
	added.Commit()

	// A 的整棵文件 DAG 按块推给 B（模拟 filesync 推拉：只搬块，不搬密钥）
	entries := collectDAGBlocksForTest(t, fxA, added.FileId)
	require.NotEmpty(t, entries)
	for _, b := range entries {
		node, err := decodeBlockForTest(b)
		require.NoError(t, err)
		require.NoError(t, fxB.commonFileService.DAGService().Add(ctx, node))
	}

	// B 无密钥离线读出：元数据可解、内容字节与源一致
	variants, err := fxB.GetFileVariants(ctx, domain.FullFileId{SpaceId: spaceId, FileId: added.FileId}, nil)
	require.NoError(t, err)
	require.Len(t, variants, 1)
	assert.Equal(t, "attachment.bin", variants[0].Name)

	reader, err := fxB.GetContentReader(ctx, spaceId, variants[0].Hash, "")
	require.NoError(t, err)
	gotContent, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, string(gotContent))
}

func collectDAGBlocksForTest(t *testing.T, fx *fixture, fileId domain.FileId) []blocks.Block {
	t.Helper()
	ctx := context.Background()
	rootCid := cid.MustParse(fileId.String())
	rootNode, err := fx.commonFileService.DAGService().Get(ctx, rootCid)
	require.NoError(t, err)
	rootBlock, err := blocks.NewBlockWithCid(rootNode.RawData(), rootNode.Cid())
	require.NoError(t, err)
	result := []blocks.Block{rootBlock}
	walker := ipld.NewWalker(ctx, ipld.NewNavigableIPLDNode(rootNode, fx.commonFileService.DAGService()))
	err = walker.Iterate(func(navNode ipld.NavigableNode) error {
		node := navNode.GetIPLDNode()
		if node.Cid() == rootNode.Cid() {
			return nil
		}
		b, err := blocks.NewBlockWithCid(node.RawData(), node.Cid())
		if err != nil {
			return err
		}
		result = append(result, b)
		return nil
	})
	if !errors.Is(err, ipld.EndOfDag) {
		require.NoError(t, err)
	}
	return result
}

func decodeBlockForTest(b blocks.Block) (ipld.Node, error) {
	switch b.Cid().Prefix().Codec {
	case cid.DagProtobuf:
		return merkledag.DecodeProtobufBlock(b)
	case cid.Raw:
		return merkledag.DecodeRawBlock(b)
	default:
		return nil, fmt.Errorf("unsupported codec %d", b.Cid().Prefix().Codec)
	}
}
