package deletionaudit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/pkg/lib/bundle"
	"github.com/anyproto/anytype-heart/core/domain"
	"github.com/anyproto/anytype-heart/pkg/lib/localstore/objectstore"
)

func TestPurgeExpiredTombstones(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	old := now.Add(-31 * 24 * time.Hour).Unix()
	fresh := now.Add(-24 * time.Hour).Unix()

	store := objectstore.NewStoreFixture(t)
	svc := &service{objectStore: store}
	spaceId := "space1"

	store.AddObjects(t, spaceId, []objectstore.TestObject{
		{ // expired audited removal -> purged
			bundle.RelationKeyId:          domain.String("gone-old"),
			bundle.RelationKeySpaceId:     domain.String(spaceId),
			bundle.RelationKeyIsDeleted:   domain.Bool(true),
			bundle.RelationKeyDeletedDate: domain.Int64(old),
		},
		{ // fresh removal -> kept
			bundle.RelationKeyId:          domain.String("gone-fresh"),
			bundle.RelationKeySpaceId:     domain.String(spaceId),
			bundle.RelationKeyIsDeleted:   domain.Bool(true),
			bundle.RelationKeyDeletedDate: domain.Int64(fresh),
		},
		{ // expired but uninstallable -> kept for reinstall
			bundle.RelationKeyId:            domain.String("gone-uninstalled"),
			bundle.RelationKeySpaceId:       domain.String(spaceId),
			bundle.RelationKeyIsDeleted:     domain.Bool(true),
			bundle.RelationKeyDeletedDate:   domain.Int64(old),
			bundle.RelationKeyIsUninstalled: domain.Bool(true),
		},
		{ // bare index churn without audit stamp -> never purged
			bundle.RelationKeyId:        domain.String("gone-bare"),
			bundle.RelationKeySpaceId:   domain.String(spaceId),
			bundle.RelationKeyIsDeleted: domain.Bool(true),
		},
		{ // live object -> untouched
			bundle.RelationKeyId:        domain.String("live"),
			bundle.RelationKeySpaceId:   domain.String(spaceId),
			bundle.RelationKeyIsDeleted: domain.Bool(false),
		},
	})

	n, err := svc.PurgeExpiredTombstones(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only the expired audited tombstone should purge")

	idx := store.SpaceIndex(spaceId)
	for id, want := range map[string]bool{
		"gone-old": false, "gone-fresh": true, "gone-uninstalled": true, "gone-bare": true, "live": true,
	} {
		d, err := idx.GetDetails(id)
		require.NoError(t, err)
		_, hasId := d.TryString(bundle.RelationKeyId)
		assert.Equal(t, want, hasId, "presence of %s", id)
	}
}
