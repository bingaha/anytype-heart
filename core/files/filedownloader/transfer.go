package filedownloader

// Chunked, resumable large-file reception for the 明文全量同步 fork.
//
// Files whose pair directory carries a chunk manifest (see
// core/files/chunkspec — written by core/files for contents larger than the
// transport chunk size) are fetched chunk by chunk instead of one whole
// block:
//
//   - each chunk is an independent small block, so a network stall only
//     fails the in-flight pieces; completed chunks are already cached in the
//     local blockstore (the proxy caches every remotely fetched block), and
//     the next round fetches only the still-missing ones — resume is at chunk
//     granularity without re-sending the whole file;
//   - once every chunk is present, the bytes are concatenated in manifest
//     order, verified by recomputing the whole-file raw CID (must equal the
//     "content" link CID), and stored as ONE raw block — the on-disk format
//     stays a single whole block, identical to a small file.
//
// Files without a manifest (small files, legacy DAGs) keep the original
// one-shot walk path untouched.

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync/commonfile/fileblockstore"

	"github.com/anyproto/anytype-heart/core/files/chunkspec"
)

const (
	// maxTransferRounds bounds one download attempt: each round fetches the
	// still-missing chunks in parallel and rechecks. A round that makes no
	// progress still counts, so a persistently unreachable source fails fast
	// and the worker-level retry (with backoff and requeue) takes over.
	maxTransferRounds = 8
	// transferRoundDelay is the pause between fetch rounds.
	transferRoundDelay = time.Second
	// maxDownloadAttempts bounds worker-level retries of one download task
	// before it is dropped (a poison task must not wedge a worker forever;
	// the subscription re-emits still-missing files on restart).
	maxDownloadAttempts = 8
)

// manifestSearchDepth covers the real nesting (outer -> pair) with margin.
const manifestSearchDepth = 2

// downloadRetryDelay is the worker backoff before requeueing a failed task.
// A var so tests can shrink it.
var downloadRetryDelay = func(attempt int) time.Duration {
	d := time.Second << (attempt - 1)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

// downloadFileResumable fetches one file, resuming from previously fetched
// chunks when the DAG carries chunk manifests. It returns the number of
// content bytes confirmed present, so a failed attempt can requeue with its
// offset preserved.
func (s *service) downloadFileResumable(ctx context.Context, t downloadTask) (completed int64, err error) {
	dagService := s.dagServiceForSpace(t.spaceId)

	rootCid, err := cid.Parse(t.fileId.String())
	if err != nil {
		return t.resumeOffset, fmt.Errorf("parse file cid: %w", err)
	}
	root, err := dagService.Get(ctx, rootCid)
	if err != nil {
		return t.resumeOffset, fmt.Errorf("get file root: %w", err)
	}
	pairs, err := chunkspec.FindManifestPairs(root, func(c cid.Cid) (ipld.Node, error) {
		return dagService.Get(ctx, c)
	}, manifestSearchDepth)
	if err != nil {
		return t.resumeOffset, fmt.Errorf("resolve chunk manifest: %w", err)
	}
	if len(pairs) == 0 {
		// No manifest (small file, legacy or foreign DAG): original one-shot
		// path, behavior unchanged.
		if err := s.DownloadToLocalStore(ctx, t.spaceId, t.fileId, 0); err != nil {
			return t.resumeOffset, err
		}
		return 0, nil
	}
	return s.downloadChunkedPairs(ctx, dagService, t.spaceId, pairs, t.resumeOffset)
}

// downloadChunkedPairs fetches every manifest chunk (plus the tiny meta
// blocks), assembles each pair's chunks in manifest order, verifies the
// whole-file CID, and stores the result as one raw block.
func (s *service) downloadChunkedPairs(ctx context.Context, dagService ipld.DAGService, spaceID string, pairs []chunkspec.PairRef, resumeOffset int64) (int64, error) {
	spaceCtx := fileblockstore.CtxWithSpaceId(ctx, spaceID)

	var required []cid.Cid
	for _, p := range pairs {
		if p.MetaCid.Defined() {
			required = append(required, p.MetaCid)
		}
		required = append(required, p.Chunks...)
	}
	if err := s.fetchMissingRounds(spaceCtx, dagService, required); err != nil {
		return s.presentPairsBytes(spaceCtx, dagService, pairs), err
	}

	var total int64
	for _, p := range pairs {
		assembled := make([]byte, 0, len(p.Chunks)*chunkspec.DefaultChunkSize)
		for _, c := range p.Chunks {
			node, err := dagService.Get(spaceCtx, c)
			if err != nil {
				return total + int64(len(assembled)),
					fmt.Errorf("get chunk %s: %w", c, err)
			}
			assembled = append(assembled, node.RawData()...)
		}
		whole := merkledag.NewRawNode(assembled)
		if !whole.Cid().Equals(p.ContentCid) {
			return total + int64(len(assembled)), fmt.Errorf(
				"chunk assembly cid mismatch: got %s want %s (size %d)",
				whole.Cid(), p.ContentCid, len(assembled))
		}
		if err := dagService.Add(spaceCtx, whole); err != nil {
			return total + int64(len(assembled)),
				fmt.Errorf("store assembled block: %w", err)
		}
		total += int64(len(assembled))
	}
	return total, nil
}

// fetchMissingRounds fetches the not-yet-local blocks in parallel rounds,
// rechecking presence after each round so only still-missing chunks are
// refetched — a mid-transfer cut resumes from the completed prefix instead
// of resending the whole file.
func (s *service) fetchMissingRounds(ctx context.Context, dagService ipld.DAGService, required []cid.Cid) error {
	for round := 0; round < maxTransferRounds; round++ {
		var missing []cid.Cid
		for _, c := range required {
			ok, err := s.commonFile.HasCid(ctx, c)
			if err != nil {
				log.Warn("chunk presence check", zap.String("cid", c.String()), zap.Error(err))
				missing = append(missing, c)
				continue
			}
			if !ok {
				missing = append(missing, c)
			}
		}
		if len(missing) == 0 {
			return nil
		}
		// Parallel fetch; per-item failures are intentionally ignored here —
		// the presence recheck at the next round is the source of truth.
		for range dagService.GetMany(ctx, missing) {
		}
		if round+1 < maxTransferRounds {
			select {
			case <-ctx.Done():
				return fmt.Errorf("fetch chunks: %w", ctx.Err())
			case <-time.After(transferRoundDelay):
			}
		}
	}
	return fmt.Errorf("fetch chunks: still missing blocks after %d rounds", maxTransferRounds)
}

// presentPairsBytes reports how many chunk bytes are already local. It is the
// offset a failed attempt hands back for requeueing.
func (s *service) presentPairsBytes(ctx context.Context, dagService ipld.DAGService, pairs []chunkspec.PairRef) int64 {
	var total int64
	for _, p := range pairs {
		for _, c := range p.Chunks {
			ok, err := s.commonFile.HasCid(ctx, c)
			if err != nil || !ok {
				continue
			}
			node, err := dagService.Get(ctx, c)
			if err != nil {
				continue
			}
			total += int64(len(node.RawData()))
		}
	}
	return total
}
