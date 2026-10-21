package filesync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/anyproto/any-store/query"
	"go.uber.org/zap"

	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/core/files/filesync/filequeue"
)

func (s *fileSync) DeleteFile(objectId string, fileId domain.FullFileId) error {
	if s.cfg.IsLocalOnlyMode() {
		return nil
	}

	return s.process(objectId, func(exists bool, info FileInfo) (FileInfo, bool, error) {
		if exists {
			info.State = FileStatePendingDeletion
			// Deletion must not wait out a long retry backoff the item may
			// carry from MissingBlocks parking; it waits out the byte
			// retention window instead, so deleted bytes stay recoverable.
			info.ScheduledAt = deletionDueTime(time.Now())
			return info, true, nil
		}

		info = FileInfo{
			FileId:      fileId.FileId,
			SpaceId:     fileId.SpaceId,
			ObjectId:    objectId,
			State:       FileStatePendingDeletion,
			ScheduledAt: deletionDueTime(time.Now()),
		}
		return info, true, nil
	})
}

// EraseFileNow queues an immediate, retention-skipping erase of the file's
// bytes both on the backup node and locally. It reuses the ordinary deletion
// chain (processNextToDelete/runDeleter): the only difference to DeleteFile
// is that the item is due at once. Explicit entry point for the "erase now"
// button (ticket 08 wires the UI to it via fileobject.EraseFileData).
func (s *fileSync) EraseFileNow(objectId string, fileId domain.FullFileId) error {
	if s.cfg.IsLocalOnlyMode() {
		return nil
	}

	return s.process(objectId, func(exists bool, info FileInfo) (FileInfo, bool, error) {
		if exists {
			info.State = FileStatePendingDeletion
			info.ScheduledAt = time.Now()
			return info, true, nil
		}

		info = FileInfo{
			FileId:      fileId.FileId,
			SpaceId:     fileId.SpaceId,
			ObjectId:    objectId,
			State:       FileStatePendingDeletion,
			ScheduledAt: time.Now(),
		}
		return info, true, nil
	})
}

func (s *fileSync) runDeleter() {
	for {
		select {
		case <-s.loopCtx.Done():
			return
		default:
		}

		err := s.processNextToDelete(s.loopCtx)
		if err != nil && !errors.Is(err, filequeue.ErrClosed) {
			log.Error("process next to delete", zap.Error(err))
		}
	}
}

func (s *fileSync) processNextToDelete(ctx context.Context) error {
	item, err := s.queue.GetNextScheduled(ctx, filequeue.GetNextScheduledRequest[FileInfo]{
		Subscribe:   true,
		StoreFilter: filterByState(FileStatePendingDeletion),
		StoreOrder: &query.SortField{
			Field:   "scheduledAt",
			Path:    []string{"scheduledAt"},
			Reverse: false,
		},
		Filter: func(info FileInfo) bool {
			return info.State == FileStatePendingDeletion
		},
		ScheduledAt: func(info FileInfo) time.Time {
			return info.ScheduledAt
		},
	})
	if err != nil {
		return fmt.Errorf("get next scheduled item: %w", err)
	}

	next, err := s.processDeletion(ctx, item)

	releaseErr := s.queue.ReleaseAndUpdate(item.ObjectId, next)

	return errors.Join(releaseErr, err)
}

func (s *fileSync) processDeletion(ctx context.Context, it FileInfo) (FileInfo, error) {
	// Retention gate: the index tombstone is already gone (the object
	// disappeared from lists at once), but the bytes stay recoverable until
	// the retention window ends. The queue wakes the item when it is due; an
	// early sighting simply keeps it queued.
	if !deletionDue(it.ScheduledAt, time.Now()) {
		return it, nil
	}

	err := s.rpcStore.DeleteFiles(ctx, it.SpaceId, it.FileId)
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "space is deleted") || strings.Contains(errStr, "code: 502") {
			it.State = FileStateDeleted
			return it, nil
		}
		it = it.Reschedule()
		return it, err
	}

	mngr, err := s.limitManager.getSpace(it.SpaceId)
	if err == nil {
		mngr.deallocateFile(it.Key())
	} else {
		log.Error("processDeletion: get space limit manager", zap.Error(err))
	}

	// The backup node copy is gone; now reclaim the local blocks. A local
	// failure reschedules (the remote delete is idempotent) so bytes are not
	// silently leaked to disk.
	if err := s.deleteLocalFileBytes(ctx, it.SpaceId, it.FullFileId()); err != nil {
		it = it.Reschedule()
		return it, err
	}

	it.State = FileStateDeleted
	return it, nil
}
