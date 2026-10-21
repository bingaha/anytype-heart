package spacecore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/anytype-heart/space/spacecore/localdiscovery"
)

// TestWholeNetworkPassphraseBridge 一处设置同时喂给握手加盐与发现后缀：
// 口令变了，握手盐与发现名必须一起变（即接上了 04 号票 admissionSaltFor
// 的 networkPassphrase 与 01/08 号票的 EffectiveServiceName）。
func TestWholeNetworkPassphraseBridge(t *testing.T) {
	t.Setenv("MYAPP_NETWORK_PASSPHARSE", "")
	defer ClearWholeNetworkPassphrase()

	SetWholeNetworkPassphrase("bridge-pp")
	assert.True(t, WholeNetworkPassphraseConfigured())

	got, err := networkPassphrase()
	require.NoError(t, err)
	assert.Equal(t, "bridge-pp", got)

	name1 := DiscoveryServiceName()
	assert.Equal(t, localdiscovery.EffectiveServiceName(), name1)
	assert.Contains(t, name1, "-", "configured passphrase must suffix the discovery name")

	nonce := []byte("nonce-1")
	salt1 := deriveAdmissionSalt("net1", got, nonce)

	SetWholeNetworkPassphrase("other-pp")
	got2, err := networkPassphrase()
	require.NoError(t, err)
	assert.Equal(t, "other-pp", got2)

	name2 := DiscoveryServiceName()
	assert.NotEqual(t, name1, name2, "passphrase change must re-suffix discovery")
	assert.NotEqual(t, salt1, deriveAdmissionSalt("net1", got2, nonce),
		"passphrase change must re-salt the handshake")
}

// TestWholeNetworkPassphraseEmpty 空口令=未组网：握手 fail-closed，发现退回 base 名。
func TestWholeNetworkPassphraseEmpty(t *testing.T) {
	t.Setenv("MYAPP_NETWORK_PASSPHARSE", "")
	defer ClearWholeNetworkPassphrase()

	SetWholeNetworkPassphrase("")
	assert.False(t, WholeNetworkPassphraseConfigured())
	assert.NotContains(t, DiscoveryServiceName(), "-", "empty passphrase must fall back to the bare base name")
}
