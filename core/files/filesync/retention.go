package filesync

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anyproto/anytype-heart/core/domain"
)

// Byte-level deletion retention.
//
// Deleting a note/attachment removes the index row (tombstone, isDeleted)
// immediately, so the object disappears from lists at once. The underlying
// bytes — both on the backup file node and in the local block store — are
// only reclaimed after FileBytesRetention, giving a recovery window during
// which the bytes can still be pulled back. EraseFileNow skips the wait for
// explicit user erasure.
//
// Both durations are vars (not consts) so tests and the settings UI (ticket
// 08) can tune them without touching the deletion chain itself.
const (
	// FileBytesRetentionDays is the default number of days file bytes survive
	// a delete before the deleter reclaims them locally and on the node.
	FileBytesRetentionDays = 7
	// TombstoneRetentionDays mirrors the index-side tombstone retention
	// (core/block/deletionaudit): deletion audit rows stay queryable for 30
	// days. filesync does not enforce index tombstones; the constant lives
	// here so the whole deletion timeline is visible in one place.
	TombstoneRetentionDays = 30
)

// FileBytesRetention gates processDeletion: remote DeleteFiles and local
// block removal only run once the queue item is due. Default 7 days.
var FileBytesRetention = time.Duration(FileBytesRetentionDays) * 24 * time.Hour

// TombstoneRetention documents the index-side audit window. Default 30 days.
// Enforcement lives in core/block/deletionaudit (retention.go).
var TombstoneRetention = time.Duration(TombstoneRetentionDays) * 24 * time.Hour

// deletionDueTime returns the earliest time the bytes of a file deleted at
// `from` may be reclaimed.
func deletionDueTime(from time.Time) time.Time {
	return from.Add(FileBytesRetention)
}

// deletionDue reports whether the retention window ending at scheduledAt has
// passed as of now. A zero scheduledAt (legacy queue items, internal orphan
// cleanups) is always due.
func deletionDue(scheduledAt, now time.Time) bool {
	if scheduledAt.IsZero() {
		return true
	}
	return !now.Before(scheduledAt)
}

// deleteLocalFileBytes removes every locally stored block of the file DAG.
// It reuses enumerateFileCids, so leaf chunks that were never downloaded cost
// nothing: only blocks present in the local store are deleted.
//
// errBlockNotFound (structural nodes exist neither locally nor on the node)
// means there is nothing local to reclaim and is not an error.
func (s *fileSync) deleteLocalFileBytes(ctx context.Context, spaceId string, fileId domain.FullFileId) error {
	entries, err := s.enumerateFileCids(ctx, spaceId, fileId.FileId, nil)
	if err != nil {
		if errors.Is(err, errBlockNotFound) {
			return nil
		}
		return fmt.Errorf("enumerate file cids: %w", err)
	}
	for _, entry := range entries {
		if err := s.fileStorage.Delete(ctx, entry.cid); err != nil {
			// Best effort per block: a missing block is already gone, a real
			// failure is logged and surfaced so the caller can retry.
			if isBlockNotFoundError(err) {
				continue
			}
			return fmt.Errorf("delete local block %s: %w", entry.cid, err)
		}
	}
	return nil
}
