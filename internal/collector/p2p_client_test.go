package collector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Snipa22/go-tari-lib/p2p"
	pb "github.com/Snipa22/go-tari-lib/p2p/proto"
	rpcpkg "github.com/Snipa22/go-tari-lib/p2p/rpc"
)

// fakeP2PProbeFuncs is a fixture p2pProbeFuncs implementation used to
// exercise p2pNodeClient's GetInfo/GetPeers mapping logic without any real
// P2P network calls.
type fakeP2PProbeFuncs struct {
	chainMetadata    *p2p.ChainMetadataInfo
	chainMetadataErr error

	identity    *p2p.PeerInfo
	identityErr error

	peers    []*pb.PeerInfo
	peersErr error
}

func (f *fakeP2PProbeFuncs) probeChainMetadata(ctx context.Context, addr string) (*p2p.ChainMetadataInfo, error) {
	if f.chainMetadataErr != nil {
		return nil, f.chainMetadataErr
	}
	return f.chainMetadata, nil
}

func (f *fakeP2PProbeFuncs) probeIdentity(ctx context.Context, addr string) (*p2p.PeerInfo, error) {
	if f.identityErr != nil {
		return nil, f.identityErr
	}
	return f.identity, nil
}

func (f *fakeP2PProbeFuncs) probeGetPeers(ctx context.Context, addr string, req rpcpkg.GetPeersRequest) ([]*pb.PeerInfo, error) {
	if f.peersErr != nil {
		return nil, f.peersErr
	}
	return f.peers, nil
}

func TestP2PClientGetInfo(t *testing.T) {
	fake := &fakeP2PProbeFuncs{
		chainMetadata: &p2p.ChainMetadataInfo{
			BestBlockHeight: 54321,
			Latency:         42 * time.Millisecond,
		},
		identity: &p2p.PeerInfo{
			RemoteStaticPubKey: []byte("node-own-pubkey"),
		},
	}
	client := &p2pNodeClient{probes: fake}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.GetInfo(ctx, "127.0.0.1:18189")
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if !info.Reachable {
		t.Errorf("Reachable = false, want true")
	}
	if info.Height == nil || *info.Height != 54321 {
		t.Errorf("Height = %v, want 54321", info.Height)
	}
	if info.ChainTipHeight == nil || *info.ChainTipHeight != 54321 {
		t.Errorf("ChainTipHeight = %v, want 54321", info.ChainTipHeight)
	}
	if info.LatencyMS == nil || *info.LatencyMS != 42 {
		t.Errorf("LatencyMS = %v, want 42", info.LatencyMS)
	}
	if string(info.PublicKey) != "node-own-pubkey" {
		t.Errorf("PublicKey = %q, want %q", info.PublicKey, "node-own-pubkey")
	}
	// Version is intentionally always nil for the P2P path — see
	// p2pNodeClient.GetInfo's doc comment.
	if info.Version != nil {
		t.Errorf("Version = %v, want nil", *info.Version)
	}
	if info.RxtHashrate != nil || info.C29Hashrate != nil || info.Sha3xHashrate != nil {
		t.Errorf("expected all hashrate fields nil, got Rxt=%v C29=%v Sha3x=%v",
			info.RxtHashrate, info.C29Hashrate, info.Sha3xHashrate)
	}
}

// TestP2PClientGetInfoIdentityProbeFailureIsNonFatal verifies that a
// failing probeIdentity call does not fail GetInfo overall: Reachable/
// Height must still reflect the successful probeChainMetadata call, only
// PublicKey is left nil.
func TestP2PClientGetInfoIdentityProbeFailureIsNonFatal(t *testing.T) {
	fake := &fakeP2PProbeFuncs{
		chainMetadata: &p2p.ChainMetadataInfo{
			BestBlockHeight: 111,
		},
		identityErr: errors.New("boom: identity handshake failed"),
	}
	client := &p2pNodeClient{probes: fake}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.GetInfo(ctx, "127.0.0.1:18189")
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if !info.Reachable {
		t.Errorf("Reachable = false, want true (identity probe failure must not affect Reachable)")
	}
	if info.Height == nil || *info.Height != 111 {
		t.Errorf("Height = %v, want 111", info.Height)
	}
	if info.PublicKey != nil {
		t.Errorf("PublicKey = %x, want nil", info.PublicKey)
	}
}

func TestP2PClientGetInfoPropagatesProbeError(t *testing.T) {
	fake := &fakeP2PProbeFuncs{chainMetadataErr: errors.New("boom: handshake failed")}
	client := &p2pNodeClient{probes: fake}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.GetInfo(ctx, "127.0.0.1:18189"); err == nil {
		t.Fatal("GetInfo: expected error from failing ProbeChainMetadata, got nil")
	}
}

func TestP2PClientGetPeers(t *testing.T) {
	// One peer with a single text-multiaddr claim, one peer with two
	// claims that both advertise the SAME address (dedup across claims),
	// one peer with a mix of a parseable and an unparseable address
	// (unparseable skipped, parseable kept), and one peer whose only
	// address is unparseable (contributes nothing).
	binaryIP4TCP := encodeTestBinaryMultiaddrIP4TCP([4]byte{10, 0, 0, 1}, 18189)

	fake := &fakeP2PProbeFuncs{
		peers: []*pb.PeerInfo{
			{
				PublicKey: []byte("peer1"),
				Claims: []*pb.PeerIdentityClaim{
					{Addresses: [][]byte{[]byte("/ip4/1.2.3.4/tcp/18189")}},
				},
			},
			{
				PublicKey: []byte("peer2-dup-across-claims"),
				Claims: []*pb.PeerIdentityClaim{
					{Addresses: [][]byte{binaryIP4TCP}},
					{Addresses: [][]byte{binaryIP4TCP}}, // same address, second claim
				},
			},
			{
				PublicKey: []byte("peer3-mixed"),
				Claims: []*pb.PeerIdentityClaim{
					{Addresses: [][]byte{
						{0xff, 0xff, 0xff}, // garbage, unparseable
						[]byte("/ip4/9.9.9.9/tcp/1234"),
					}},
				},
			},
			{
				PublicKey: []byte("peer4-unparseable-only"),
				Claims: []*pb.PeerIdentityClaim{
					{Addresses: [][]byte{{0x00, 0x01, 0x02}}},
				},
			},
		},
	}
	client := &p2pNodeClient{probes: fake}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peers, err := client.GetPeers(ctx, "127.0.0.1:18189")
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}

	want := map[string]string{
		"1.2.3.4:18189":  "peer1",
		"10.0.0.1:18189": "peer2-dup-across-claims",
		"9.9.9.9:1234":   "peer3-mixed",
	}
	if len(peers) != len(want) {
		t.Fatalf("GetPeers returned %d peers, want %d: %v", len(peers), len(want), peers)
	}
	seen := map[string]bool{}
	for _, p := range peers {
		wantPubKey, ok := want[p.Address]
		if !ok {
			t.Errorf("unexpected peer address %q", p.Address)
			continue
		}
		if seen[p.Address] {
			t.Errorf("peer address %q returned more than once (dedup failed)", p.Address)
		}
		if string(p.PublicKey) != wantPubKey {
			t.Errorf("peer %q PublicKey = %q, want %q", p.Address, p.PublicKey, wantPubKey)
		}
		seen[p.Address] = true
	}
	for addr := range want {
		if !seen[addr] {
			t.Errorf("expected peer address %q not found in result %v", addr, peers)
		}
	}
}

func TestP2PClientGetPeersPropagatesProbeError(t *testing.T) {
	fake := &fakeP2PProbeFuncs{peersErr: errors.New("boom: rpc negotiation failed")}
	client := &p2pNodeClient{probes: fake}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.GetPeers(ctx, "127.0.0.1:18189"); err == nil {
		t.Fatal("GetPeers: expected error from failing ProbeGetPeers, got nil")
	}
}

func TestNewP2PClientReturnsRealProbes(t *testing.T) {
	client, ok := NewP2PClient().(*p2pNodeClient)
	if !ok {
		t.Fatalf("NewP2PClient() = %T, want *p2pNodeClient", NewP2PClient())
	}
	if _, ok := client.probes.(realP2PProbeFuncs); !ok {
		t.Errorf("NewP2PClient().probes = %T, want realP2PProbeFuncs", client.probes)
	}
}
