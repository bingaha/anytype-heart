package chunkspec

import (
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/boxo/ipld/merkledag"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunkLinkNameRoundtrip(t *testing.T) {
	for _, i := range []int{0, 1, 7, 42, 139} {
		name := ChunkLinkName(i)
		assert.True(t, IsChunkLink(name), name)
	}
	for _, bad := range []string{"", "chunk-", "chunk-1", "chunk-00000a", "meta", "content", "chunk-000000x", "Chunk-000001"} {
		assert.False(t, IsChunkLink(bad), bad)
	}
}

func TestChunkCount(t *testing.T) {
	const cs = int64(512 << 10)
	assert.Equal(t, 1, ChunkCount(0, cs))
	assert.Equal(t, 1, ChunkCount(cs, cs))
	assert.Equal(t, 2, ChunkCount(cs+1, cs))
	assert.Equal(t, 3, ChunkCount(2*cs+1, cs))
}

func TestNeedsChunkingDefault(t *testing.T) {
	SetChunkSizeBytes(0) // restore default/env
	assert.False(t, NeedsChunking(DefaultChunkSize))
	assert.True(t, NeedsChunking(DefaultChunkSize+1))
}

func TestFindManifestPairsNested(t *testing.T) {
	newRaw := func(b byte) ipld.Node { return merkledag.NewRawNode([]byte{b}) }

	// pair with manifest: meta + content + 2 chunks
	pair := new(merkledag.ProtoNode)
	require.NoError(t, pair.AddNodeLink("meta", newRaw('m')))
	whole := newRaw('w')
	require.NoError(t, pair.AddNodeLink("content", whole))
	c0, c1 := newRaw('0'), newRaw('1')
	require.NoError(t, pair.AddRawLink(ChunkLinkName(0), &ipld.Link{Name: ChunkLinkName(0), Cid: c0.Cid()}))
	require.NoError(t, pair.AddRawLink(ChunkLinkName(1), &ipld.Link{Name: ChunkLinkName(1), Cid: c1.Cid()}))

	// outer -> "0" -> pair, plus a legacy small pair without manifest
	legacy := new(merkledag.ProtoNode)
	require.NoError(t, legacy.AddNodeLink("meta", newRaw('M')))
	require.NoError(t, legacy.AddNodeLink("content", newRaw('W')))
	outer := new(merkledag.ProtoNode)
	require.NoError(t, outer.AddNodeLink("0", pair))
	require.NoError(t, outer.AddNodeLink("1", legacy))

	byCid := map[string]ipld.Node{
		pair.Cid().KeyString():   pair,
		legacy.Cid().KeyString(): legacy,
	}
	fetch := func(c cid.Cid) (ipld.Node, error) {
		n, ok := byCid[c.KeyString()]
		if !ok {
			return nil, fmt.Errorf("unknown %s", c)
		}
		return n, nil
	}

	// depth 0 on the pair itself matches directly
	got, err := FindManifestPairs(pair, fetch, 2)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, pair.Cid(), got[0].PairCid)
	assert.Equal(t, whole.Cid(), got[0].ContentCid)
	require.Len(t, got[0].Chunks, 2)

	// nested under outer, legacy pair ignored
	got, err = FindManifestPairs(outer, fetch, 2)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, pair.Cid(), got[0].PairCid)

	// depth 0 on outer finds nothing (no descent)
	got, err = FindManifestPairs(outer, fetch, 0)
	require.NoError(t, err)
	assert.Empty(t, got)

	// unixfs-shaped node: unnamed dag-pb children must not be descended
	// into (fetch would pull leaf data over the network).
	unx := new(merkledag.ProtoNode)
	require.NoError(t, unx.AddNodeLink("", pair))
	calls := 0
	_, err = FindManifestPairs(unx, func(c cid.Cid) (ipld.Node, error) {
		calls++
		return nil, fmt.Errorf("must not fetch unnamed child %s", c)
	}, 2)
	require.NoError(t, err)
	assert.Zero(t, calls)
}

func TestSortedChunkCids(t *testing.T) {
	newCid := func(b byte) cid.Cid {
		return merkledag.NewRawNode([]byte{b}).Cid()
	}
	links := []*ipld.Link{
		{Name: "content", Cid: newCid('c')},
		{Name: ChunkLinkName(2), Cid: newCid('2')},
		{Name: "meta", Cid: newCid('m')},
		{Name: ChunkLinkName(0), Cid: newCid('0')},
		{Name: ChunkLinkName(1), Cid: newCid('1')},
	}
	got := SortedChunkCids(links)
	require.Len(t, got, 3)
	assert.Equal(t, newCid('0'), got[0])
	assert.Equal(t, newCid('1'), got[1])
	assert.Equal(t, newCid('2'), got[2])

	assert.Nil(t, SortedChunkCids([]*ipld.Link{{Name: "content", Cid: newCid('c')}}))
	assert.Nil(t, SortedChunkCids(nil))
}
