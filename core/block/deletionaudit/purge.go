package deletionaudit

import (
	"context"
	"time"

	"github.com/anyproto/anytype-heart/pkg/lib/bundle"
	"github.com/anyproto/anytype-heart/pkg/lib/database"
	"github.com/anyproto/anytype-heart/pkg/lib/localstore/objectstore/spaceindex"
	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/pkg/lib/logging"
	"github.com/anyproto/anytype-heart/pkg/lib/pb/model"
)

var purgeLog = logging.Logger("deletionaudit.purge")

// purgeInterval controls how often expired tombstones are hard-purged.
// A var (not a const) so tests can shorten the loop.
var purgeInterval = 24 * time.Hour

// PurgeExpiredTombstones hard-deletes index tombstones older than the
// retention window (see retention.go). Only audited removals carry
// deletedDate, so only they can age out; bare index churn without an audit
// stamp is never touched. Uninstalled types/templates are excluded so
// reinstall keeps working off the preserved tombstone.
func (s *service) PurgeExpiredTombstones(ctx context.Context) (int, error) {
	threshold := ExpiredBefore(time.Now())
	filters := &database.Filters{
		FilterObj: database.FiltersAnd{
			database.FilterEq{
				Key: bundle.RelationKeyIsDeleted, Cond: model.BlockContentDataviewFilter_Equal,
				Value: domain.Bool(true),
			},
			database.FilterNotNull{Key: bundle.RelationKeyDeletedDate},
			database.FilterEq{
				Key: bundle.RelationKeyDeletedDate, Cond: model.BlockContentDataviewFilter_LessOrEqual,
				Value: domain.Int64(threshold),
			},
			database.FilterNot{Filter: database.FilterEq{
				Key: bundle.RelationKeyIsUninstalled, Cond: model.BlockContentDataviewFilter_Equal,
				Value: domain.Bool(true),
			}},
		},
	}

	purged := 0
	err := s.objectStore.IterateSpaceIndex(ctx, func(store spaceindex.Store) error {
		records, err := store.QueryRaw(filters, 0, 0)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		ids := make([]string, 0, len(records))
		for _, r := range records {
			if r.Details == nil {
				continue
			}
			if id, ok := r.Details.TryString(bundle.RelationKeyId); ok {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return nil
		}
		if err := store.DeleteDetails(ctx, ids); err != nil {
			return err
		}
		purged += len(ids)
		return nil
	})
	if err != nil {
		return purged, err
	}
	return purged, nil
}

// Run starts the daily hard-purge loop for expired tombstones.
// First purge happens one interval after start so boot stays fast;
// PurgeExpiredTombstones stays public for tests and manual triggers.
func (s *service) Run(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(purgeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if n, err := s.PurgeExpiredTombstones(ctx); err != nil {
					purgeLog.With("error", err).Error("purge expired tombstones")
				} else if n > 0 {
					purgeLog.With("purged", n).Info("purged expired tombstones")
				}
			}
		}
	}()
	return nil
}

// Close stops the purge loop.
func (s *service) Close(ctx context.Context) error {
	return nil
}
