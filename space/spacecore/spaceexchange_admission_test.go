package spacecore

// 准入层（04票）握手单测：成员签名 + 整网口令 + NetworkId 绑定。
// 沙箱组播受限，live 发现跑不了，这里用生产函数做双边对拍代替：
// 调用方 requestMemberTokens → 响应方 respondMemberTokens → 调用方
// matchMemberProofs，与 peer.go / rpchandler.go 的调用顺序一致。

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/commonspace/clientspaceproto"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// admissionFixture gives two devices of one account (same SignKey, different
// PeerKeys) plus a stranger account, and the member-key map derived from a
// real in-memory ACL the way admissionMemberKeys does.
type admissionFixture struct {
	devA, devB *accountdata.AccountKeys
	stranger   *accountdata.AccountKeys
	members    map[string][]crypto.PubKey
}

func newAdmissionFixture(t *testing.T, spaces ...string) *admissionFixture {
	t.Helper()
	accPriv, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	devPrivA, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	devPrivB, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	devA := accountdata.New(devPrivA, accPriv)
	devB := accountdata.New(devPrivB, accPriv)
	strangerKeys, err := accountdata.NewRandom()
	require.NoError(t, err)

	// the ACL the verifier reads from disk: owner-only, so the only member
	// key is the account public key both devices share.
	aclList, _ := newKeylessAcl(t, devA, nil)
	members := map[string][]crypto.PubKey{}
	for _, spaceId := range spaces {
		members[spaceId] = activeMemberKeys(aclList.AclState())
	}
	require.NotEmpty(t, members[spaces[0]], "owner must be an active member")

	return &admissionFixture{devA: devA, devB: devB, stranger: strangerKeys, members: members}
}

func admissionSaltFor(t *testing.T, networkId, passphrase string) (nonce, salt []byte) {
	t.Helper()
	var err error
	nonce, err = clientspaceproto.NewNonceV2()
	require.NoError(t, err)
	salt = deriveAdmissionSalt(networkId, passphrase, nonce)
	return nonce, salt
}

// admissionRoundTrip drives both sides the way production does: the caller
// (devA) signs for callerIds, the responder (devB, same account) intersects
// against responderIds using its own ACL copy, the caller matches the proofs.
func admissionRoundTrip(
	t *testing.T, fx *admissionFixture,
	callerIds, responderIds []string,
	nonce, salt []byte,
	callerPeer, responderPeer string,
) (shared []string, matched []string) {
	t.Helper()
	request, err := requestMemberTokens(callerIds, fx.devA.SignKey, salt, callerPeer, responderPeer)
	require.NoError(t, err)

	shared, proofs := respondMemberTokens(responderIds, fx.members, request, salt, callerPeer, responderPeer, fx.devB.SignKey)
	matched = matchMemberProofs(callerIds, fx.members, proofs, salt, callerPeer, responderPeer)
	return shared, matched
}

func TestAdmissionSamePassphraseConnects(t *testing.T) {
	fx := newAdmissionFixture(t, "spaceA", "spaceB", "spaceC")

	t.Run("only the shared space is learned", func(t *testing.T) {
		nonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		salt := deriveAdmissionSalt("net1", "pp1", nonce)

		shared, matched := admissionRoundTrip(t, fx,
			[]string{"spaceA", "spaceB"}, []string{"spaceB", "spaceC"},
			nonce, salt, fx.devA.PeerId, fx.devB.PeerId)

		assert.Equal(t, []string{"spaceB"}, shared)
		assert.Equal(t, []string{"spaceB"}, matched)
	})

	t.Run("every space shared", func(t *testing.T) {
		nonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		salt := deriveAdmissionSalt("net1", "pp1", nonce)

		_, matched := admissionRoundTrip(t, fx,
			[]string{"spaceA", "spaceB"}, []string{"spaceA", "spaceB"},
			nonce, salt, fx.devA.PeerId, fx.devB.PeerId)

		assert.Equal(t, []string{"spaceA", "spaceB"}, matched)
	})

	t.Run("no space in common admits nothing", func(t *testing.T) {
		nonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		salt := deriveAdmissionSalt("net1", "pp1", nonce)

		shared, matched := admissionRoundTrip(t, fx,
			[]string{"spaceA"}, []string{"spaceC"},
			nonce, salt, fx.devA.PeerId, fx.devB.PeerId)

		assert.Empty(t, shared)
		assert.Empty(t, matched)
	})
}

func TestAdmissionGateRejects(t *testing.T) {
	newRoundTrip := func(t *testing.T, networkId, callerPP, responderPP string) (shared, matched []string) {
		t.Helper()
		fx := newAdmissionFixture(t, "spaceA")
		nonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		callerSalt := deriveAdmissionSalt(networkId, callerPP, nonce)
		responderSalt := deriveAdmissionSalt(networkId, responderPP, nonce)
		request, err := requestMemberTokens([]string{"spaceA"}, fx.devA.SignKey, callerSalt, fx.devA.PeerId, fx.devB.PeerId)
		require.NoError(t, err)
		shared, proofs := respondMemberTokens([]string{"spaceA"}, fx.members, request, responderSalt, fx.devA.PeerId, fx.devB.PeerId, fx.devB.SignKey)
		matched = matchMemberProofs([]string{"spaceA"}, fx.members, proofs, callerSalt, fx.devA.PeerId, fx.devB.PeerId)
		return shared, matched
	}

	t.Run("wrong passphrase admits nothing", func(t *testing.T) {
		shared, matched := newRoundTrip(t, "net1", "pp1", "pp2")
		assert.Empty(t, shared)
		assert.Empty(t, matched)
	})

	t.Run("wrong network id admits nothing", func(t *testing.T) {
		fx := newAdmissionFixture(t, "spaceA")
		nonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		callerSalt := deriveAdmissionSalt("net1", "pp1", nonce)
		otherNetSalt := deriveAdmissionSalt("net2", "pp1", nonce)
		request, err := requestMemberTokens([]string{"spaceA"}, fx.devA.SignKey, callerSalt, fx.devA.PeerId, fx.devB.PeerId)
		require.NoError(t, err)
		shared, _ := respondMemberTokens([]string{"spaceA"}, fx.members, request, otherNetSalt, fx.devA.PeerId, fx.devB.PeerId, fx.devB.SignKey)
		assert.Empty(t, shared)
	})

	t.Run("non-member stranger admits nothing", func(t *testing.T) {
		fx := newAdmissionFixture(t, "spaceA")
		nonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		salt := deriveAdmissionSalt("net1", "pp1", nonce)
		// the stranger knows the passphrase and the space id, but its
		// account key is in no member list.
		request, err := requestMemberTokens([]string{"spaceA"}, fx.stranger.SignKey, salt, fx.stranger.PeerId, fx.devB.PeerId)
		require.NoError(t, err)
		shared, _ := respondMemberTokens([]string{"spaceA"}, fx.members, request, salt, fx.stranger.PeerId, fx.devB.PeerId, fx.devB.SignKey)
		assert.Empty(t, shared)
	})

	t.Run("proofs bound to another peer pair do not match", func(t *testing.T) {
		fx := newAdmissionFixture(t, "spaceA")
		nonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		salt := deriveAdmissionSalt("net1", "pp1", nonce)
		request, err := requestMemberTokens([]string{"spaceA"}, fx.devA.SignKey, salt, fx.devA.PeerId, fx.devB.PeerId)
		require.NoError(t, err)
		_, proofs := respondMemberTokens([]string{"spaceA"}, fx.members, request, salt, fx.devA.PeerId, fx.devB.PeerId, fx.devB.SignKey)
		require.Len(t, proofs, 1)
		got := matchMemberProofs([]string{"spaceA"}, fx.members, proofs, salt, "otherCaller", fx.devB.PeerId)
		assert.Empty(t, got)
	})

	t.Run("proofs under another nonce do not match", func(t *testing.T) {
		fx := newAdmissionFixture(t, "spaceA")
		nonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		salt := deriveAdmissionSalt("net1", "pp1", nonce)
		request, err := requestMemberTokens([]string{"spaceA"}, fx.devA.SignKey, salt, fx.devA.PeerId, fx.devB.PeerId)
		require.NoError(t, err)
		_, proofs := respondMemberTokens([]string{"spaceA"}, fx.members, request, salt, fx.devA.PeerId, fx.devB.PeerId, fx.devB.SignKey)
		otherNonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		otherSalt := deriveAdmissionSalt("net1", "pp1", otherNonce)
		got := matchMemberProofs([]string{"spaceA"}, fx.members, proofs, otherSalt, fx.devA.PeerId, fx.devB.PeerId)
		assert.Empty(t, got)
	})
}

func TestDeriveAdmissionSalt(t *testing.T) {
	nonce, err := clientspaceproto.NewNonceV2()
	require.NoError(t, err)

	t.Run("deterministic in its inputs", func(t *testing.T) {
		assert.Equal(t,
			deriveAdmissionSalt("net1", "pp1", nonce),
			deriveAdmissionSalt("net1", "pp1", nonce))
	})

	t.Run("any input change changes the salt", func(t *testing.T) {
		base := deriveAdmissionSalt("net1", "pp1", nonce)
		otherNonce, err := clientspaceproto.NewNonceV2()
		require.NoError(t, err)
		assert.NotEqual(t, base, deriveAdmissionSalt("net1", "pp2", nonce), "passphrase")
		assert.NotEqual(t, base, deriveAdmissionSalt("net2", "pp1", nonce), "network id")
		assert.NotEqual(t, base, deriveAdmissionSalt("net1", "pp1", otherNonce), "nonce")
	})

	t.Run("request and response roles do not cross-verify", func(t *testing.T) {
		fx := newAdmissionFixture(t, "spaceA")
		salt := deriveAdmissionSalt("net1", "pp1", nonce)
		req, err := requestMemberTokens([]string{"spaceA"}, fx.devA.SignKey, salt, fx.devA.PeerId, fx.devB.PeerId)
		require.NoError(t, err)
		// a request token offered back as a response proof must not match.
		// req is padded+sorted, so locate the real token by verifying it.
		var real [][]byte
		for _, tok := range req {
			if verifyAgainstMembers(fx.members["spaceA"],
				admissionMessage(salt, admissionReqLabel, fx.devA.PeerId, fx.devB.PeerId, "spaceA"), tok) {
				real = append(real, tok)
			}
		}
		require.Len(t, real, 1, "exactly the one real request token must be identifiable")
		got := matchMemberProofs([]string{"spaceA"}, fx.members, real, salt, fx.devA.PeerId, fx.devB.PeerId)
		assert.Empty(t, got)
	})
}

func TestRequestMemberTokensPadding(t *testing.T) {
	fx := newAdmissionFixture(t, "spaceA")
	nonce, err := clientspaceproto.NewNonceV2()
	require.NoError(t, err)
	salt := deriveAdmissionSalt("net1", "pp1", nonce)

	t.Run("single space still yields a full sorted bucket of signature-sized tokens", func(t *testing.T) {
		got, err := requestMemberTokens([]string{"spaceA"}, fx.devA.SignKey, salt, fx.devA.PeerId, fx.devB.PeerId)
		require.NoError(t, err)
		require.Len(t, got, 16)
		for _, tok := range got {
			assert.Len(t, tok, 64)
		}
		strs := make([]string, 0, len(got))
		for _, tok := range got {
			strs = append(strs, string(tok))
		}
		assert.IsIncreasing(t, strs)
	})
}

func TestNetworkPassphraseProvider(t *testing.T) {
	t.Run("custom provider wins", func(t *testing.T) {
		SetNetworkPassphraseProvider(func() (string, error) { return "from-keychain", nil })
		defer SetNetworkPassphraseProvider(nil)
		got, err := networkPassphrase()
		require.NoError(t, err)
		assert.Equal(t, "from-keychain", got)
	})

	t.Run("environment fallback", func(t *testing.T) {
		SetNetworkPassphraseProvider(nil)
		t.Setenv("MYAPP_NETWORK_PASSPHRASE", "from-env")
		got, err := networkPassphrase()
		require.NoError(t, err)
		assert.Equal(t, "from-env", got)
	})

	t.Run("nothing configured fails closed", func(t *testing.T) {
		SetNetworkPassphraseProvider(nil)
		t.Setenv("MYAPP_NETWORK_PASSPHRASE", "")
		_, err := networkPassphrase()
		require.ErrorIs(t, err, errAdmissionNoPassphrase)
	})
}

// TestSpaceExchangeV1Disabled is the static guard for the downgrade path:
// the legacy plaintext handler must answer an empty list even when asked
// with space ids and a local server (no service needed — it touches none).
func TestSpaceExchangeV1Disabled(t *testing.T) {
	h := &rpcHandler{}
	resp, err := h.SpaceExchange(context.Background(), &clientspaceproto.SpaceExchangeRequest{
		SpaceIds:    []string{"spaceA"},
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"127.0.0.1"}, Port: 1},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Empty(t, resp.SpaceIds)
}
