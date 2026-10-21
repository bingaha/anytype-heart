// Package chunkspec defines the transport-chunking contract of the
// 明文全量同步 fork.
//
// A file whose content exceeds ChunkSize is stored locally as one whole raw
// block (unchanged) PLUS one raw block per fixed-size chunk, with an ordered
// manifest of chunk links ("chunk-000000", …) on the file pair directory
// next to the existing "meta" and "content" links. The manifest is the only
// cross-device discovery channel for chunks: CIDs are hashes and cannot be
// derived, and the file protocol has no list-blocks or range RPCs.
//
// Rules:
//   - Writers (core/files) split with ChunkSize; readers (filedownloader) and
//     the push planner (filesync) only follow the manifest, so the chunk size
//     stays configurable without breaking existing DAGs.
//   - ChunkSize must stay comfortably under the filesync push batch limit
//     (~1MiB per BlockPushMany), otherwise a single transport piece is
//     rejected with "block size is too big" and the file can never upload.
//   - Local readers keep using the "content" link (whole raw block);
//     storage semantics are unchanged, chunking is transport-only.
package chunkspec

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

const (
	// DefaultChunkSize is the default transport piece size: 512KiB, i.e. half
	// of the ~1MiB filesync push batch limit, leaving ample headroom for RPC
	// envelopes.
	DefaultChunkSize = 512 << 10

	// ContentLinkName and MetaLinkName mirror the pair-directory link names in
	// core/files (files.MetaLinkName/files.ContentLinkName); restated here so
	// transport code does not depend on the files service package.
	ContentLinkName = "content"
	MetaLinkName    = "meta"

	// ChunkLinkPrefix prefixes every manifest link on the pair directory.
	// The numeric suffix is zero-padded (see ChunkLinkName) so lexicographic
	// order matches chunk order.
	ChunkLinkPrefix = "chunk-"

	// chunkLinkDigits is the zero-padded width of the manifest suffix.
	chunkLinkDigits = 6
)

var (
	chunkSizeOnce sync.Once
	chunkSize     int64
)

// ChunkSizeBytes returns the transport piece size in bytes. It defaults to
// DefaultChunkSize and can be overridden with the
// ANYTYPE_SYNC_CHUNK_SIZE_BYTES environment variable (parsed once, invalid
// values fall back to the default). Writers consult it when splitting;
// readers follow the on-DAG manifest and never need it, so changing it only
// affects newly written files.
func ChunkSizeBytes() int64 {
	chunkSizeOnce.Do(func() {
		chunkSize = DefaultChunkSize
		if v := os.Getenv("ANYTYPE_SYNC_CHUNK_SIZE_BYTES"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 64<<10 {
				chunkSize = n
			}
		}
	})
	return chunkSize
}

// SetChunkSizeBytes overrides ChunkSizeBytes; tests use it to exercise the
// chunked paths with small fixtures. A non-positive value restores the
// default/environment resolution.
func SetChunkSizeBytes(n int64) {
	if n <= 0 {
		chunkSizeOnce = sync.Once{}
		chunkSize = 0
		return
	}
	chunkSize = n
}

// ChunkLinkName returns the manifest link name for chunk i.
func ChunkLinkName(i int) string {
	return fmt.Sprintf("%s%06d", ChunkLinkPrefix, i)
}

// IsChunkLink reports whether a pair-directory link name is a chunk manifest
// entry.
func IsChunkLink(name string) bool {
	if !strings.HasPrefix(name, ChunkLinkPrefix) {
		return false
	}
	rest := strings.TrimPrefix(name, ChunkLinkPrefix)
	if len(rest) != chunkLinkDigits {
		return false
	}
	for _, r := range rest {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ChunkCount returns how many chunks data of total bytes splits into with the
// given chunk size. Totals at or below one chunk yield 1 (single block, no
// manifest needed — callers treat that as the legacy one-shot path).
func ChunkCount(total int64, chunkSize int64) int {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if total <= chunkSize {
		return 1
	}
	return int((total + chunkSize - 1) / chunkSize)
}

// NeedsChunking reports whether content of size total must be transported as
// chunks rather than one whole block.
func NeedsChunking(total int64) bool {
	return total > ChunkSizeBytes()
}

// SortedChunkCids extracts the chunk CIDs from pair-directory links in chunk
// order. It returns nil when the DAG carries no manifest (legacy one-shot
// file).
func SortedChunkCids(links []*ipld.Link) []cid.Cid {
	var indexed []struct {
		idx int
		cid cid.Cid
	}
	for _, l := range links {
		if l == nil || !IsChunkLink(l.Name) {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(l.Name, ChunkLinkPrefix))
		if err != nil {
			continue
		}
		indexed = append(indexed, struct {
			idx int
			cid cid.Cid
		}{idx: n, cid: l.Cid})
	}
	if len(indexed) == 0 {
		return nil
	}
	sort.Slice(indexed, func(i, j int) bool { return indexed[i].idx < indexed[j].idx })
	out := make([]cid.Cid, 0, len(indexed))
	for _, e := range indexed {
		out = append(out, e.cid)
	}
	return out
}

// PairRef describes one pair directory carrying a chunk manifest: the
// whole-content block plus the ordered transport chunks that reassemble to
// it.
type PairRef struct {
	PairCid    cid.Cid
	ContentCid cid.Cid
	// MetaCid is the pair's meta block, if the pair carries one.
	MetaCid cid.Cid
	Chunks  []cid.Cid
}

// FindManifestPairs locates pair directories carrying a chunk manifest at or
// below root, descending at most maxDepth levels. Real file DAGs nest the
// pair under an outer directory (outer -> "0" -> pair), so callers pass a
// depth of at least 2; a pair used directly as root matches at depth 0.
//
// fetch retrieves child nodes. Only named directory-codec children of
// manifest-less directories are descended into: the unixfs chunker names
// file-chunk links "" (boxo importer helpers), while directory entries
// always carry names — so raw leaves, and unnamed intermediate chunks, are
// identified by (codec, name) and never fetched. Directories already
// carrying a manifest are not descended into (their children are leaves by
// construction).
// Fetch errors are propagated: callers cannot decide whole-vs-chunked
// transport without the pair directory, so failing closed (retry) is safer
// than guessing.
func FindManifestPairs(root ipld.Node, fetch func(cid.Cid) (ipld.Node, error), maxDepth int) ([]PairRef, error) {
	type item struct {
		node  ipld.Node
		depth int
	}
	var pairs []PairRef
	queue := []item{{node: root, depth: 0}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		links := cur.node.Links()
		content := LinkByName(links, ContentLinkName)
		chunks := SortedChunkCids(links)
		if content != nil && len(chunks) > 0 {
			var metaCid cid.Cid
			if m := LinkByName(links, MetaLinkName); m != nil {
				metaCid = m.Cid
			}
			pairs = append(pairs, PairRef{
				PairCid:    cur.node.Cid(),
				ContentCid: content.Cid,
				MetaCid:    metaCid,
				Chunks:     chunks,
			})
			continue
		}
		if cur.depth >= maxDepth {
			continue
		}
		for _, l := range links {
			if l == nil || l.Name == "" || l.Cid.Prefix().Codec != cid.DagProtobuf {
				continue
			}
			child, err := fetch(l.Cid)
			if err != nil {
				return nil, fmt.Errorf("resolve %s: %w", l.Name, err)
			}
			queue = append(queue, item{node: child, depth: cur.depth + 1})
		}
	}
	return pairs, nil
}

// LinkByName returns the first link with the given name, or nil.
func LinkByName(links []*ipld.Link, name string) *ipld.Link {
	for _, l := range links {
		if l != nil && l.Name == name {
			return l
		}
	}
	return nil
}
