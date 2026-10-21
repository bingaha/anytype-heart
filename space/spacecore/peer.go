package spacecore

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"

	"storj.io/drpc"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace"
	"github.com/anyproto/any-sync/commonspace/clientspaceproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/transport"

	"github.com/anyproto/anytype-heart/space/spacecore/localdiscovery"
)

// PeerDiscoveryObserver receives every LAN discovery, before the peer is
// dialed. The recovery status tracker (core.recovery) implements it; this
// package owns the single localdiscovery.SetNotifier slot, so the forward
// lives here rather than competing for it — Android's SetNotifierProvider
// path lands here too. Looked up by name because the tracker lives above this
// package; a drift test keeps the constant equal to recovery.CName.
type PeerDiscoveryObserver interface {
	OnLocalPeerDiscovered(peerId string, addrs []string)
}

const recoveryCName = "core.recovery"

func lookupPeerDiscoveryObserver(a *app.App) PeerDiscoveryObserver {
	c := a.Component(recoveryCName)
	if c == nil {
		return nil
	}
	observer, _ := c.(PeerDiscoveryObserver)
	return observer
}

// lookupPullObserver resolves the same tracker as a commonspace.PullObserver;
// loadSpace injects it into every space's Deps (tech, personal, shareable,
// streamable, one-to-one all route through it).
func lookupPullObserver(a *app.App) commonspace.PullObserver {
	c := a.Component(recoveryCName)
	if c == nil {
		return nil
	}
	observer, _ := c.(commonspace.PullObserver)
	return observer
}

func (s *service) PeerDiscovered(ctx context.Context, discovered localdiscovery.DiscoveredPeer, own localdiscovery.OwnAddresses) {
	if s.peerDiscoveryObserver != nil {
		s.peerDiscoveryObserver.OnLocalPeerDiscovered(discovered.PeerId, discovered.Addrs)
	}
	s.localAddrsMu.Lock()
	if s.localAddrsClosed {
		s.localAddrsMu.Unlock()
		return
	}
	if discovered.Link == "" {
		s.setBaseAddrs(discovered.PeerId, discovered.Addrs)
	} else {
		s.setRoute(discovered.PeerId, routeMDNS+discovered.Link, discovered.Addrs)
	}
	s.installLocalAddrs(discovered.PeerId)
	s.localAddrsMu.Unlock()
	s.rememberOwn(own)
	// every discovery exchanges, as on develop. A cable answers again every
	// two to three minutes, so a cable peer is exchanged with at that pace,
	// which also teaches a restarted peer, or one whose route to us
	// expired, our address on the cable again
	shared, proof, err := s.exchangeFn(ctx, discovered.PeerId, &own)
	if err != nil {
		return
	}
	s.publishLocalPeer(discovered.PeerId, shared, proof, directionOutbound)
}

// exchangeOutbound is the outbound handshake with one LAN peer: the admission
// handshake over the request list (disk plus the derived tech space). own may
// be nil (re-exchange before local discovery reported our addresses): the
// peer then answers without recording us. The same function serves discovery
// and re-exchange, so the two cannot drift. proof is always true: every token
// is a member-signature proof, and an empty intersection answers nothing.
// There is no v1 fallback: the legacy plaintext handshake hands our full
// space id list to whoever asks, which the admission gate exists to prevent.
func (s *service) exchangeOutbound(ctx context.Context, peerId string, own *localdiscovery.OwnAddresses) (shared []string, proof bool, err error) {
	unaryPeer, err := s.poolManager.UnaryPeerPool().Get(ctx, peerId)
	if err != nil {
		return nil, false, fmt.Errorf("get peer: %w", err)
	}
	allIds, err := s.exchangeRequestIds()
	if err != nil {
		return nil, false, err
	}
	var localServer *clientspaceproto.LocalServer
	if own != nil {
		localServer = localServerOf(*own, s.peerIPsOf(peerId, peer.CtxPeerAddr(unaryPeer.Context())))
	}
	shared, err = s.spaceExchangeV2(ctx, unaryPeer, peerId, allIds, localServer)
	if err != nil {
		return nil, false, err
	}
	return shared, true, nil
}

// spaceExchangeV2 runs the admission handshake: send one member-signature
// token per space we hold (account key over salt || req || peer pair ||
// spaceId, padded and sorted so the true space count stays hidden), then
// match the peer's proof tokens back to our own spaces via our member lists.
// The salt binds the network passphrase, our NetworkId and the fresh nonce,
// so a peer with another passphrase or NetworkId — or no membership —
// verifies nothing and the intersection comes back empty. Neither side ever
// transmits a space id.
func (s *service) spaceExchangeV2(ctx context.Context, unaryPeer peer.Peer, peerId string, allIds []string, localServer *clientspaceproto.LocalServer) ([]string, error) {
	nonce, err := clientspaceproto.NewNonceV2()
	if err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}
	// the gate first: without a passphrase (or with a NetworkId we cannot
	// bind) there is nothing to prove and nothing is sent.
	salt, err := s.admissionSaltFor(nonce)
	if err != nil {
		return nil, err
	}
	selfPeerId := s.wallet.GetDevicePrivkey().GetPublic().PeerId()
	// we are the caller, so the pair is ordered (us, them)
	tokens, err := requestMemberTokens(allIds, s.wallet.GetAccountPrivkey(), salt, selfPeerId, peerId)
	if err != nil {
		return nil, fmt.Errorf("build request tokens: %w", err)
	}
	var resp *clientspaceproto.SpaceExchangeV2Response
	err = unaryPeer.DoDrpc(ctx, func(conn drpc.Conn) error {
		var dErr error
		resp, dErr = clientspaceproto.NewDRPCClientSpaceClient(conn).SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{
			Nonce:       nonce,
			SpaceTokens: tokens,
			LocalServer: localServer,
		})
		return dErr
	})
	if err != nil {
		return nil, fmt.Errorf("space exchange v2: %w", err)
	}
	memberKeys := s.admissionMemberKeys(ctx, allIds)
	return matchMemberProofs(allIds, memberKeys, resp.SpaceTokens, salt, selfPeerId, peerId), nil
}

// localServerOf describes this device's listening endpoint to a LAN peer
// known at peerIPs.
func localServerOf(own localdiscovery.OwnAddresses, peerIPs []netip.Addr) *clientspaceproto.LocalServer {
	return &clientspaceproto.LocalServer{
		Ips: own.For(peerIPs),
		// our own drpc listener port, so always within uint16
		Port: int32(own.Port), // #nosec G115
	}
}

// addSchema expands each discovered ip:port into explicit transport URLs.
// Local peers are dialed over yamux (TCP) only: a live host whose app has
// quit answers the dial with an RST in one RTT, while a QUIC dial to a dead
// port has to wait out the whole handshake timeout (quic-go has no
// ICMP/ECONNREFUSED handling on the unconnected sockets any-sync uses). A
// host that left the network answers neither transport — that case still
// costs the full dial timeout. The QUIC listener stays bound so
// not-yet-updated clients can still dial us; that inbound connection is
// bidirectional, so mixed versions keep syncing.
func (s *service) addSchema(addrs []string) (res []string) {
	res = make([]string, 0, len(addrs))
	for _, addr := range addrs {
		res = append(res, transport.Yamux+"://"+addr)
	}
	return res
}

// setLocalPeerAddrs makes hostPorts ("ip:port") the addresses a LAN peer is
// dialed at, and remembers its IPs for the subnet match in localServerOf.
func (s *service) setLocalPeerAddrs(peerId string, hostPorts []string) {
	ips := make([]netip.Addr, 0, len(hostPorts))
	for _, hostPort := range hostPorts {
		if ip, ok := hostIP(hostPort); ok {
			ips = append(ips, ip)
		}
	}
	s.ownMu.Lock()
	if s.peerIPs == nil {
		s.peerIPs = map[string][]netip.Addr{}
	}
	s.peerIPs[peerId] = ips
	s.ownMu.Unlock()
	// addSchema pins the transport for local peers (yamux); see its comment
	s.peerService.SetPeerAddrs(peerId, s.addSchema(hostPorts))
}

// peerIPsOf is the peer's known addresses plus the one the live connection
// to it uses (connAddr, from peer.CtxPeerAddr; empty when unknown).
func (s *service) peerIPsOf(peerId, connAddr string) []netip.Addr {
	s.ownMu.Lock()
	ips := slices.Clone(s.peerIPs[peerId])
	s.ownMu.Unlock()
	if ip, ok := hostIP(connAddr); ok && !slices.Contains(ips, ip) {
		ips = append(ips, ip)
	}
	return ips
}

func (s *service) forgetPeerIPs(peerId string) {
	s.localAddrsMu.Lock()
	s.forgetLocalPeerAddrs(peerId)
	s.localAddrsMu.Unlock()
	s.ownMu.Lock()
	defer s.ownMu.Unlock()
	delete(s.peerIPs, peerId)
}

// isLinkLocalAddr reports whether hostPort is a dialable IPv6 link-local
// address; an IPv4 link-local one (169.254/16) is just another IPv4 address.
func isLinkLocalAddr(hostPort string) bool {
	ip, ok := hostIP(hostPort)
	return ok && ip.Is6() && ip.IsLinkLocalUnicast() && ip.Zone() != ""
}

// hostIP extracts the IP from "ip:port" or a transport URL such as
// "yamux://ip:port".
func hostIP(addr string) (netip.Addr, bool) {
	if _, rest, found := strings.Cut(addr, "://"); found {
		addr = rest
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return netip.Addr{}, false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return ip.Unmap(), true
}
