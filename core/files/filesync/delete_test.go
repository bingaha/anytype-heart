package filesync

import (
	"testing"
	"time"

	"github.com/anyproto/any-store/query"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/core/files/filesync/filequeue"
)

// withRetention overrides FileBytesRetention for the duration of a test.
func withRetention(t *testing.T, d time.Duration) {
	t.Helper()
	prev := FileBytesRetention
	FileBytesRetention = d
	t.Cleanup(func() { FileBytesRetention = prev })
}

func TestDeleteFile(t *testing.T) {
	t.Run("if all is ok, delete file", func(t *testing.T) {
		withRetention(t, 0)
		fx := newFixture(t, 1024*1024*1024)
		defer fx.Finish(t)
		spaceId := "space1"

		fileId, _ := fx.givenFileAddedToDAG(t, 1024)
		fx.givenFileUploaded(t, spaceId, fileId)

		entries, err := fx.enumerateFileCids(ctx, spaceId, fileId, nil)
		require.NoError(t, err)
		require.NotEmpty(t, entries)
		var cids []cid.Cid
		for _, e := range entries {
			cids = append(cids, e.cid)
		}

		err = fx.DeleteFile("objectId", domain.FullFileId{SpaceId: spaceId, FileId: fileId})
		require.NoError(t, err)

		it, err := fx.queue.GetNext(ctx, filequeue.GetNextRequest[FileInfo]{
			Subscribe:   true,
			StoreFilter: filterByState(FileStateDeleted),
			Filter:      func(info FileInfo) bool { return info.State == FileStateDeleted },
		})

		require.NoError(t, err)
		assert.Equal(t, fileId, it.FileId)

		err = fx.queue.ReleaseAndUpdate(it.ObjectId, it)

		resp, err := fx.rpcStore.FilesInfo(ctx, spaceId, fileId)
		require.NoError(t, err)
		require.Empty(t, resp)

		// Local blocks are reclaimed together with the backup node copy.
		for _, c := range cids {
			exists, err := fx.localFileStorage.ExistsCids(ctx, []cid.Cid{c})
			require.NoError(t, err)
			require.Empty(t, exists)
		}
	})

	t.Run("with error while deleting, add to retry queue", func(t *testing.T) {
		withRetention(t, 0)
		fx := newFixture(t, 1024*1024*1024)
		defer fx.Finish(t)
		spaceId := "space1"

		testFileId := domain.FileId("bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku")

		// Just try to delete missing file, in-memory RPC store will return error.
		// In case of a missing file the real file node returns OK
		err := fx.DeleteFile("objectId", domain.FullFileId{SpaceId: spaceId, FileId: testFileId})
		require.NoError(t, err)

		inFuture := time.Now().Add(30 * time.Second)
		it, err := fx.queue.GetNext(ctx, filequeue.GetNextRequest[FileInfo]{
			Subscribe: true,
			StoreFilter: query.And{
				filterByState(FileStatePendingDeletion),
				// Means it's scheduled for retry
				query.Key{
					Path:   []string{"scheduledAt"},
					Filter: query.NewComp(query.CompOpGte, inFuture.Unix()),
				},
			},
			Filter: func(info FileInfo) bool {
				return info.State == FileStatePendingDeletion && info.ScheduledAt.After(inFuture)
			},
		})

		require.NoError(t, err)
		assert.Equal(t, testFileId, it.FileId)

		err = fx.queue.ReleaseAndUpdate(it.ObjectId, it)
		require.NoError(t, err)
	})

	t.Run("bytes are kept until the retention window ends", func(t *testing.T) {
		withRetention(t, time.Hour)
		fx := newFixture(t, 1024*1024*1024)
		defer fx.Finish(t)
		spaceId := "space1"

		fileId, _ := fx.givenFileAddedToDAG(t, 1024)
		fx.givenFileUploaded(t, spaceId, fileId)

		err := fx.DeleteFile("objectId", domain.FullFileId{SpaceId: spaceId, FileId: fileId})
		require.NoError(t, err)

		// The item waits out the retention window instead of deleting at once.
		it := findQueueItem(t, fx, "objectId")
		assert.Equal(t, FileStatePendingDeletion, it.State)
		assert.WithinDuration(t, time.Now().Add(time.Hour), it.ScheduledAt, 5*time.Minute)

		// The deleter must not touch the backup node copy while gated.
		next, err := fx.processDeletion(ctx, it)
		require.NoError(t, err)
		assert.Equal(t, FileStatePendingDeletion, next.State)

		resp, err := fx.rpcStore.FilesInfo(ctx, spaceId, fileId)
		require.NoError(t, err)
		require.Len(t, resp, 1)
	})

	t.Run("erase now skips the retention window on both ends", func(t *testing.T) {
		withRetention(t, time.Hour)
		fx := newFixture(t, 1024*1024*1024)
		defer fx.Finish(t)
		spaceId := "space1"

		fileId, _ := fx.givenFileAddedToDAG(t, 1024)
		fx.givenFileUploaded(t, spaceId, fileId)

		entries, err := fx.enumerateFileCids(ctx, spaceId, fileId, nil)
		require.NoError(t, err)
		require.NotEmpty(t, entries)

		err = fx.EraseFileNow("objectId", domain.FullFileId{SpaceId: spaceId, FileId: fileId})
		require.NoError(t, err)

		// Due at once, regardless of the retention window. List (not GetById)
		// so the read never contends with the running deleter for the lock.
		it := findQueueItem(t, fx, "objectId")
		assert.True(t, !it.ScheduledAt.After(time.Now().Add(time.Minute)),
			"erase must be due at once, got %v", it.ScheduledAt)

		it, err = fx.queue.GetNext(ctx, filequeue.GetNextRequest[FileInfo]{
			Subscribe:   true,
			StoreFilter: filterByState(FileStateDeleted),
			Filter:      func(info FileInfo) bool { return info.State == FileStateDeleted },
		})
		require.NoError(t, err)
		assert.Equal(t, fileId, it.FileId)
		require.NoError(t, fx.queue.ReleaseAndUpdate(it.ObjectId, it))

		resp, err := fx.rpcStore.FilesInfo(ctx, spaceId, fileId)
		require.NoError(t, err)
		require.Empty(t, resp)

		// Local blocks are erased together with the backup node copy.
		for _, e := range entries {
			exists, err := fx.localFileStorage.ExistsCids(ctx, []cid.Cid{e.cid})
			require.NoError(t, err)
			require.Empty(t, exists)
		}
	})
}

// findQueueItem returns the persisted queue item without taking its lock,
// so the read never contends with the background workers.
func findQueueItem(t *testing.T, fx *fixture, objectId string) FileInfo {
	t.Helper()
	items, err := fx.queue.List()
	require.NoError(t, err)
	for _, it := range items {
		if it.ObjectId == objectId {
			return it
		}
	}
	t.Fatalf("queue item %s not found", objectId)
	return FileInfo{}
}

func TestDeletionRetentionDefaults(t *testing.T) {
	assert.Equal(t, 7, FileBytesRetentionDays)
	assert.Equal(t, 7*24*time.Hour, FileBytesRetention)
	assert.Equal(t, 30, TombstoneRetentionDays)
	assert.Equal(t, 30*24*time.Hour, TombstoneRetention)

	now := time.Now()
	assert.WithinDuration(t, now.Add(7*24*time.Hour), deletionDueTime(now), time.Second)
	assert.True(t, deletionDue(time.Time{}, now))
	assert.False(t, deletionDue(now.Add(time.Hour), now))
	assert.True(t, deletionDue(now.Add(-time.Hour), now))
}
