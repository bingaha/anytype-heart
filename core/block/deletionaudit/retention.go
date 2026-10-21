package deletionaudit

import (
	"time"
)

// Index tombstone retention.
//
// When an object is deleted, its index row is stripped to a tombstone
// ({id, spaceId, isDeleted} plus the preserved audit facts) and the object's
// tree — the only place holding the full content — goes away with it. The
// tombstone is what the audit List serves and what keeps "deleted" distinct
// from "never loaded" for sync and backlink bookkeeping.
//
// Tombstones are kept for TombstoneRetention (default 30 days) so a deleted
// object stays visible in the audit and recoverable-by-identity for the
// regret window, while history does not grow without bound: rows older than
// the window are eligible for hard purge (spaceindex.DeleteDetails).
//
// The duration is a var (not a const) so tests and the settings UI (ticket
// 08) can tune it without touching the audit query path.
const TombstoneRetentionDays = 30

// TombstoneRetention is the index-tombstone audit window. Default 30 days.
var TombstoneRetention = time.Duration(TombstoneRetentionDays) * 24 * time.Hour

// ExpiredBefore returns the deletedDate (unix seconds) threshold: tombstones
// stamped at or before it are older than the retention window as of now.
func ExpiredBefore(now time.Time) int64 {
	return now.Add(-TombstoneRetention).Unix()
}

// TombstoneExpired reports whether a tombstone stamped with deletedDate
// (unix seconds, bundle.RelationKeyDeletedDate) has aged out of the
// retention window as of now.
func TombstoneExpired(deletedDate int64, now time.Time) bool {
	return deletedDate <= ExpiredBefore(now)
}

// TombstoneExpiryTime returns the time at which a tombstone stamped at
// deletedAt leaves the retention window.
func TombstoneExpiryTime(deletedAt time.Time) time.Time {
	return deletedAt.Add(TombstoneRetention)
}
