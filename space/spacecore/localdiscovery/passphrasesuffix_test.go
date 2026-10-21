package localdiscovery

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEffectiveServiceNameNoPassphrase(t *testing.T) {
	SetDiscoveryPassphraseProvider(nil)
	assert.Equal(t, "", DiscoveryNameSuffix())
	assert.Equal(t, serviceName, EffectiveServiceName())
}

func TestEffectiveServiceNameWithPassphrase(t *testing.T) {
	SetDiscoveryPassphraseProvider(func() (string, error) { return "correct-horse", nil })
	defer SetDiscoveryPassphraseProvider(nil)

	sum := sha256.Sum256([]byte(discoveryNameDomain + "correct-horse"))
	want := serviceName + "-" + hex.EncodeToString(sum[:])[:discoverySuffixHexLen]
	assert.Equal(t, want, EffectiveServiceName())
	assert.Len(t, DiscoveryNameSuffix(), 1+discoverySuffixHexLen)
}

func TestEffectiveServiceNameMismatch(t *testing.T) {
	SetDiscoveryPassphraseProvider(func() (string, error) { return "pp-one", nil })
	one := EffectiveServiceName()
	SetDiscoveryPassphraseProvider(func() (string, error) { return "pp-two", nil })
	two := EffectiveServiceName()
	SetDiscoveryPassphraseProvider(nil)
	assert.NotEqual(t, one, two)
	assert.NotEqual(t, one, serviceName)
	assert.Equal(t, serviceName, EffectiveServiceName())
}

// TestEffectiveServiceNameIsStable 同一口令多次求值必须稳定（Register 与
// Browse 共用，否则自发自收两边名字对不上）。
func TestEffectiveServiceNameIsStable(t *testing.T) {
	SetDiscoveryPassphraseProvider(func() (string, error) { return "stable-pp", nil })
	defer SetDiscoveryPassphraseProvider(nil)
	assert.Equal(t, EffectiveServiceName(), EffectiveServiceName())
}
