package deletionaudit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTombstoneRetention(t *testing.T) {
	t.Run("defaults are 30 days", func(t *testing.T) {
		assert.Equal(t, 30, TombstoneRetentionDays)
		assert.Equal(t, 30*24*time.Hour, TombstoneRetention)
	})

	t.Run("fresh tombstone is not expired", func(t *testing.T) {
		now := time.Now()
		assert.False(t, TombstoneExpired(now.Unix(), now))
		assert.False(t, TombstoneExpired(now.Add(-29*24*time.Hour).Unix(), now))
	})

	t.Run("old tombstone is expired", func(t *testing.T) {
		now := time.Now()
		assert.True(t, TombstoneExpired(now.Add(-31*24*time.Hour).Unix(), now))
	})

	t.Run("expiry time is stamped plus retention", func(t *testing.T) {
		at := time.Now()
		assert.WithinDuration(t, at.Add(TombstoneRetention), TombstoneExpiryTime(at), time.Second)
	})
}
