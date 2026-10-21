package spacecore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	stded25519 "crypto/ed25519"

	"github.com/anyproto/any-sync/commonspace/clientspaceproto"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/util/crypto"

	"github.com/anyproto/anytype-heart/space/spacecore/storage"
)

// forkDiscoverySaltDomain namespaces the HKDF salt used for LAN discovery keys.
// Official clients derive with salt = space id; we derive with salt = domain +
// space id, so a token minted by an official client can never validate here
// and vice versa. Only this LAN membership-proof key changes — sync payload
// encryption, member signatures and transport encryption are untouched.
const forkDiscoverySaltDomain = "myapp/lan-discovery/v1:"

// deriveDiscoveryKey derives the per-space LAN discovery key from the space's
// first ACL read key under the fork's domain. Both LAN exchange sides
// (discoverykeys.go for stored spaces, techspacekey.go for the derived tech
// space) go through here so they always agree with each other and never with
// official clients.
func deriveDiscoveryKey(firstReadKey crypto.SymKey, spaceId string) ([]byte, error) {
	return clientspaceproto.DeriveDiscoveryKey(firstReadKey, forkDiscoverySaltDomain+spaceId)
}

// The three steps of the SpaceExchangeV2 handshake, shared by the outbound
// (peer.go) and inbound (rpchandler.go) sides so both derive their tokens
// through the same code. Every token is bound to the ordered (caller,
// responder) peer pair, so the two sides must pass the pair in the same order
// regardless of which one they are.

// requestTokens builds the caller's membership token per space it can derive a
// discovery key for, padded to a bucket size and sorted so the list length
// reveals nothing about how many spaces the caller actually holds.
func requestTokens(spaceIds []string, keys map[string][]byte, nonce []byte, callerPeerId, responderPeerId string) ([][]byte, error) {
	tokens := make([][]byte, 0, len(keys))
	for _, spaceId := range spaceIds {
		if key, ok := keys[spaceId]; ok {
			tokens = append(tokens, clientspaceproto.RequestTokenV2(key, nonce, callerPeerId, responderPeerId))
		}
	}
	return clientspaceproto.PadTokensV2(tokens)
}

// respondTokens is the responder side: recompute the caller's expected token
// for every space we hold a key for, and answer a membership proof for the
// intersection only. Returns the intersecting space ids alongside the proofs.
func respondTokens(spaceIds []string, keys map[string][]byte, received [][]byte, nonce []byte, callerPeerId, responderPeerId string) (shared []string, proofs [][]byte) {
	set := tokenSet(received)
	for _, spaceId := range spaceIds {
		key, ok := keys[spaceId]
		if !ok {
			continue
		}
		if _, ok = set[string(clientspaceproto.RequestTokenV2(key, nonce, callerPeerId, responderPeerId))]; !ok {
			continue
		}
		shared = append(shared, spaceId)
		proofs = append(proofs, clientspaceproto.ResponseTokenV2(key, nonce, callerPeerId, responderPeerId))
	}
	return shared, proofs
}

// matchProofs is the caller side of the response: map the responder's proofs
// back to our own space ids. A proof we cannot reproduce belongs to a space we
// do not hold, so it is ignored.
func matchProofs(spaceIds []string, keys map[string][]byte, proofs [][]byte, nonce []byte, callerPeerId, responderPeerId string) (shared []string) {
	set := tokenSet(proofs)
	for _, spaceId := range spaceIds {
		key, ok := keys[spaceId]
		if !ok {
			continue
		}
		if _, ok = set[string(clientspaceproto.ResponseTokenV2(key, nonce, callerPeerId, responderPeerId))]; ok {
			shared = append(shared, spaceId)
		}
	}
	return shared
}

func tokenSet(tokens [][]byte) map[string]struct{} {
	set := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		set[string(token)] = struct{}{}
	}
	return set
}

// ---- admission gate: member-signature handshake (fork ticket 04) ----
//
// The read-key HMAC tokens above prove "holds the first read key". The
// admission handshake below proves "is a current member" instead: every token
// is an Ed25519 signature over (salt || role || caller || responder ||
// spaceId), made with the device's ACCOUNT private key (the identity the
// space member table knows) and verified against the member account public
// keys of the verifier's own copy of each space's ACL.
//
// NOTE on deviceKey vs account key: the ticket asks for "deviceKey signs,
// account pubkey verifies". That is not implementable as written — a
// deviceKey (PeerKey) signature can never verify under an account pubkey
// (SignKey), they are independent keypairs. What the handshake needs is a
// signature the member table can check, so the signer is the account private
// key the device holds. Device binding is preserved: the signed message
// carries the ordered (caller, responder) device peer-id pair, and the
// transport still authenticates the device key via TLS.
//
// The salt binds the network passphrase AND the NetworkId:
// salt = SHA256(domain || networkId || passphrase || nonce). A peer with a
// different passphrase or a different NetworkId derives a different salt, so
// none of its signatures verify here and none of ours verify there: the
// intersection is empty and no space-derived byte is returned. This is the
// "NetworkId first" check — it runs before any per-space or member work, and
// without it nothing is answered. (The proto has no NetworkId field, so the
// check is enforced by salt binding rather than by comparing a transmitted
// value; see admissionSaltFor.)

// errAdmissionNoPassphrase is returned when no network passphrase is
// configured: the gate fails closed, the exchange answers nothing.
var errAdmissionNoPassphrase = fmt.Errorf("admission: no network passphrase configured")

// admissionPassphraseProvider, when set, supplies the whole-network
// passphrase (first launch: user input, then OS keychain via the existing
// keytar/safeStorage channel; losing it means regrouping under a new one).
// It defaults to nil and networkPassphrase falls back to the
// MYAPP_NETWORK_PASSPHRASE environment value, so tests and headless setups
// keep working without a keychain. Production wires the keychain here via
// SetNetworkPassphraseProvider.
var (
	admissionPassphraseMu       sync.RWMutex
	admissionPassphraseProvider func() (string, error)
)

// SetNetworkPassphraseProvider installs the network-passphrase source (OS
// keychain in production, a stub in tests). A nil provider restores the
// environment fallback.
func SetNetworkPassphraseProvider(fn func() (string, error)) {
	admissionPassphraseMu.Lock()
	defer admissionPassphraseMu.Unlock()
	admissionPassphraseProvider = fn
}

func networkPassphrase() (string, error) {
	admissionPassphraseMu.RLock()
	fn := admissionPassphraseProvider
	admissionPassphraseMu.RUnlock()
	if fn != nil {
		pp, err := fn()
		if err != nil {
			return "", fmt.Errorf("admission passphrase provider: %w", err)
		}
		if pp == "" {
			return "", errAdmissionNoPassphrase
		}
		return pp, nil
	}
	if pp := os.Getenv("MYAPP_NETWORK_PASSPHRASE"); pp != "" {
		return pp, nil
	}
	return "", errAdmissionNoPassphrase
}

var admissionSaltDomain = []byte("myapp/admission/v1:")

var (
	admissionReqLabel  = []byte("myapp/admission/v1:req")
	admissionRespLabel = []byte("myapp/admission/v1:resp")
)

// memberTokenBuckets mirrors the upstream tokenBucketsV2 sizes: request lists
// are padded to a bucket so their length reveals nothing about the true
// space count. Member tokens are 64-byte Ed25519 signatures, so the fillers
// are 64 bytes too — 32-byte fillers would be length-distinguishable.
var memberTokenBuckets = []int{16, 32, 64, 128, 256}

// deriveAdmissionSalt binds one handshake run to this network: any passphrase
// or NetworkId mismatch yields a different salt and every signature check on
// both sides fails. Deterministic in (networkId, passphrase, nonce).
func deriveAdmissionSalt(networkId, passphrase string, nonce []byte) []byte {
	h := sha256.New()
	admissionField(h, admissionSaltDomain)
	admissionField(h, []byte(networkId))
	admissionField(h, []byte(passphrase))
	admissionField(h, nonce)
	return h.Sum(nil)
}

func admissionField(w io.Writer, field []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(field)))
	_, _ = w.Write(lenBuf[:])
	_, _ = w.Write(field)
}

// admissionMessage is what each member proof signs: the salt (passphrase,
// NetworkId, fresh nonce), the direction role (request proofs must not verify
// as response proofs and vice versa), the ordered device peer-id pair (a
// proof captured on one connection is useless on another), and the space id.
func admissionMessage(salt, role []byte, callerPeerId, responderPeerId, spaceId string) []byte {
	var buf bytes.Buffer
	admissionField(&buf, salt)
	admissionField(&buf, role)
	admissionField(&buf, []byte(callerPeerId))
	admissionField(&buf, []byte(responderPeerId))
	admissionField(&buf, []byte(spaceId))
	return buf.Bytes()
}

// requestMemberTokens signs one membership proof per space with the account
// private key, then pads and sorts the list so its length reveals nothing.
func requestMemberTokens(spaceIds []string, signKey crypto.PrivKey, salt []byte, callerPeerId, responderPeerId string) ([][]byte, error) {
	tokens := make([][]byte, 0, len(spaceIds))
	for _, spaceId := range spaceIds {
		sig, err := signKey.Sign(admissionMessage(salt, admissionReqLabel, callerPeerId, responderPeerId, spaceId))
		if err != nil {
			return nil, fmt.Errorf("admission: sign request token: %w", err)
		}
		tokens = append(tokens, sig)
	}
	return padMemberTokens(tokens)
}

func padMemberTokens(tokens [][]byte) ([][]byte, error) {
	sorted := make([][]byte, 0, len(tokens))
	sorted = append(sorted, tokens...)
	maxBucket := memberTokenBuckets[len(memberTokenBuckets)-1]
	target := (len(sorted) + maxBucket - 1) / maxBucket * maxBucket
	for _, bucket := range memberTokenBuckets {
		if len(sorted) <= bucket {
			target = bucket
			break
		}
	}
	for len(sorted) < target {
		filler := make([]byte, stded25519.SignatureSize)
		if _, err := io.ReadFull(rand.Reader, filler); err != nil {
			return nil, fmt.Errorf("admission: pad tokens: %w", err)
		}
		sorted = append(sorted, filler)
	}
	// sorting is required: without it the position of the real tokens would
	// reveal the true space count the padding is meant to hide.
	sort.Slice(sorted, func(i, j int) bool {
		return string(sorted[i]) < string(sorted[j])
	})
	return sorted, nil
}

// respondMemberTokens is the responder side: for every space we hold, check
// each received token against that space's CURRENT member account keys (a
// former member's signature still verifies cryptographically, but their key
// is no longer in the member list, so they are rejected here). Answer a
// membership proof only for the verified intersection.
func respondMemberTokens(
	spaceIds []string,
	memberKeys map[string][]crypto.PubKey,
	received [][]byte,
	salt []byte,
	callerPeerId, responderPeerId string,
	signKey crypto.PrivKey,
) (shared []string, proofs [][]byte) {
	for _, spaceId := range spaceIds {
		keys, ok := memberKeys[spaceId]
		if !ok || len(keys) == 0 {
			continue
		}
		msg := admissionMessage(salt, admissionReqLabel, callerPeerId, responderPeerId, spaceId)
		matched := false
		for _, tok := range received {
			if len(tok) != stded25519.SignatureSize {
				continue
			}
			if verifyAgainstMembers(keys, msg, tok) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		proof, err := signKey.Sign(admissionMessage(salt, admissionRespLabel, callerPeerId, responderPeerId, spaceId))
		if err != nil {
			continue
		}
		shared = append(shared, spaceId)
		proofs = append(proofs, proof)
	}
	return shared, proofs
}

// matchMemberProofs is the caller side: map the responder's proofs back to
// our own space ids by verifying each against our copy of that space's
// member list. A proof we cannot attribute to a member of a space we hold is
// ignored.
func matchMemberProofs(
	spaceIds []string,
	memberKeys map[string][]crypto.PubKey,
	proofs [][]byte,
	salt []byte,
	callerPeerId, responderPeerId string,
) (shared []string) {
	for _, spaceId := range spaceIds {
		keys, ok := memberKeys[spaceId]
		if !ok || len(keys) == 0 {
			continue
		}
		msg := admissionMessage(salt, admissionRespLabel, callerPeerId, responderPeerId, spaceId)
		for _, proof := range proofs {
			if len(proof) != stded25519.SignatureSize {
				continue
			}
			if verifyAgainstMembers(keys, msg, proof) {
				shared = append(shared, spaceId)
				break
			}
		}
	}
	return shared
}

func verifyAgainstMembers(keys []crypto.PubKey, msg, sig []byte) bool {
	for _, key := range keys {
		ok, err := key.Verify(msg, sig)
		if err == nil && ok {
			return true
		}
	}
	return false
}

// activeMemberKeys returns the account public keys entitled to sync the space
// right now: active status and a non-empty permission. This is the
// MembershipChecker-side check — it runs on the verifier's own ACL copy
// before anything is answered, so removed or never-member identities are
// refused even though their signatures would verify on their own.
func activeMemberKeys(st *list.AclState) []crypto.PubKey {
	var keys []crypto.PubKey
	if st == nil {
		return nil
	}
	for _, acc := range st.CurrentAccounts() {
		if acc.Status != list.StatusActive {
			continue
		}
		if acc.Permissions.NoPermissions() {
			continue
		}
		if acc.PubKey == nil {
			continue
		}
		keys = append(keys, acc.PubKey)
	}
	return keys
}

// memberKeysForSpaces reads each space's ACL from storage and returns its
// current member account keys. Spaces without a readable ACL are absent from
// the result and can prove nothing. It mirrors discoveryKeySource.derive's
// storage reading without touching it.
func memberKeysForSpaces(ctx context.Context, store storage.ClientStorage, keys *accountdata.AccountKeys, spaceIds []string) map[string][]crypto.PubKey {
	out := make(map[string][]crypto.PubKey, len(spaceIds))
	for _, spaceId := range spaceIds {
		st, err := store.WaitSpaceStorage(ctx, spaceId)
		if err != nil {
			continue
		}
		aclStorage, err := st.AclStorage()
		if err != nil {
			_ = st.Close(ctx)
			continue
		}
		aclList, err := list.BuildAclListWithIdentity(keys, aclStorage, recordverifier.NewValidateFull())
		closeErr := st.Close(ctx)
		if err != nil {
			continue
		}
		if closeErr != nil {
			continue
		}
		if members := activeMemberKeys(aclList.AclState()); len(members) > 0 {
			out[spaceId] = members
		}
	}
	return out
}

// admissionSaltFor binds one handshake run to this network: the whole-network
// passphrase plus our NetworkId, mixed with the caller's fresh nonce. Either
// side derives its own copy; nothing is transmitted. Call this before any
// per-space work — a gate failure answers nothing at all.
func (s *service) admissionSaltFor(nonce []byte) ([]byte, error) {
	pp, err := networkPassphrase()
	if err != nil {
		return nil, err
	}
	return deriveAdmissionSalt(s.nodeConf.Configuration().NetworkId, pp, nonce), nil
}

// admissionMemberKeys returns the current member account keys for the given
// spaces: disk ACLs via memberKeysForSpaces, plus the derived tech space,
// whose sole member is this account itself.
func (s *service) admissionMemberKeys(ctx context.Context, spaceIds []string) map[string][]crypto.PubKey {
	out := memberKeysForSpaces(ctx, s.spaceStorageProvider, s.accountKeys, spaceIds)
	if tech := s.techSpaceExchangeInfo(); tech.id != "" {
		for _, id := range spaceIds {
			if id == tech.id {
				out[id] = []crypto.PubKey{s.accountKeys.SignKey.GetPublic()}
			}
		}
	}
	return out
}
