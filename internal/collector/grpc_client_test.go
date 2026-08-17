package collector

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Snipa22/go-tari-grpc-lib/v3/tari_generated"
)

// fakeBaseNodeServer is a fixture BaseNodeServer used to exercise
// grpcNodeClient over real wire-level gRPC/protobuf serialization, without
// needing a live Tari node. Embedding UnimplementedBaseNodeServer means
// only the RPCs grpcNodeClient actually calls need overriding.
type fakeBaseNodeServer struct {
	tari_generated.UnimplementedBaseNodeServer

	tipInfo      *tari_generated.TipInfoResponse
	version      *tari_generated.BaseNodeGetVersionResponse
	identity     *tari_generated.NodeIdentity
	connPeers    *tari_generated.ListConnectedPeersResponse
	failTipInfo  bool
	failIdentify bool
}

func (f *fakeBaseNodeServer) GetTipInfo(ctx context.Context, in *tari_generated.Empty) (*tari_generated.TipInfoResponse, error) {
	if f.failTipInfo {
		return nil, context.DeadlineExceeded
	}
	return f.tipInfo, nil
}

func (f *fakeBaseNodeServer) GetVersion(ctx context.Context, in *tari_generated.Empty) (*tari_generated.BaseNodeGetVersionResponse, error) {
	return f.version, nil
}

func (f *fakeBaseNodeServer) Identify(ctx context.Context, in *tari_generated.Empty) (*tari_generated.NodeIdentity, error) {
	if f.failIdentify {
		return nil, context.DeadlineExceeded
	}
	return f.identity, nil
}

func (f *fakeBaseNodeServer) ListConnectedPeers(ctx context.Context, in *tari_generated.Empty) (*tari_generated.ListConnectedPeersResponse, error) {
	return f.connPeers, nil
}

// startFakeBaseNode starts an in-process gRPC server backed by srv, serving
// over a bufconn listener, and returns a grpcNodeClient wired to dial it
// plus a cleanup func to stop the server.
func startFakeBaseNode(t *testing.T, srv *fakeBaseNodeServer) (*grpcNodeClient, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	tari_generated.RegisterBaseNodeServer(s, srv)

	go func() {
		_ = s.Serve(lis)
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.Dial()
	}

	client := &grpcNodeClient{
		dial: grpc.NewClient,
		dialOpts: []grpc.DialOption{
			grpc.WithContextDialer(dialer),
		},
	}

	cleanup := func() {
		s.Stop()
		_ = lis.Close()
	}

	return client, cleanup
}

func TestGRPCClientGetInfo(t *testing.T) {
	fake := &fakeBaseNodeServer{
		tipInfo: &tari_generated.TipInfoResponse{
			Metadata: &tari_generated.MetaData{
				BestBlockHeight: 12345,
			},
			InitialSyncAchieved: true,
		},
		version: &tari_generated.BaseNodeGetVersionResponse{
			Version: "1.2.3-test",
			Network: 42,
		},
		identity: &tari_generated.NodeIdentity{
			PublicKey: []byte("node-own-pubkey"),
		},
	}
	client, cleanup := startFakeBaseNode(t, fake)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.GetInfo(ctx, "passthrough:///bufconn")
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if !info.Reachable {
		t.Errorf("Reachable = false, want true")
	}
	if info.Height == nil || *info.Height != 12345 {
		t.Errorf("Height = %v, want 12345", info.Height)
	}
	if info.ChainTipHeight == nil || *info.ChainTipHeight != 12345 {
		t.Errorf("ChainTipHeight = %v, want 12345", info.ChainTipHeight)
	}
	if info.Version == nil || *info.Version != "1.2.3-test" {
		t.Errorf("Version = %v, want 1.2.3-test", info.Version)
	}
	if info.LatencyMS == nil || *info.LatencyMS < 0 {
		t.Errorf("LatencyMS = %v, want a non-negative measured value", info.LatencyMS)
	}
	if string(info.PublicKey) != "node-own-pubkey" {
		t.Errorf("PublicKey = %q, want %q", info.PublicKey, "node-own-pubkey")
	}
}

// TestGRPCClientGetInfoIdentifyFailureIsNonFatal verifies that a failing
// Identify call (the node's own gRPC BaseNode server not implementing/
// erroring on Identify) does not fail GetInfo overall: Reachable/Height/
// Version must still reflect the successful GetTipInfo/GetVersion calls,
// only PublicKey is left nil.
func TestGRPCClientGetInfoIdentifyFailureIsNonFatal(t *testing.T) {
	fake := &fakeBaseNodeServer{
		tipInfo: &tari_generated.TipInfoResponse{
			Metadata: &tari_generated.MetaData{BestBlockHeight: 999},
		},
		version:      &tari_generated.BaseNodeGetVersionResponse{Version: "9.9.9"},
		failIdentify: true,
	}
	client, cleanup := startFakeBaseNode(t, fake)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.GetInfo(ctx, "passthrough:///bufconn")
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if !info.Reachable {
		t.Errorf("Reachable = false, want true (Identify failure must not affect Reachable)")
	}
	if info.Height == nil || *info.Height != 999 {
		t.Errorf("Height = %v, want 999", info.Height)
	}
	if info.PublicKey != nil {
		t.Errorf("PublicKey = %x, want nil", info.PublicKey)
	}
}

func TestGRPCClientGetInfoPropagatesRPCError(t *testing.T) {
	fake := &fakeBaseNodeServer{failTipInfo: true}
	client, cleanup := startFakeBaseNode(t, fake)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.GetInfo(ctx, "passthrough:///bufconn"); err == nil {
		t.Fatal("GetInfo: expected error from failing GetTipInfo, got nil")
	}
}

func TestGRPCClientGetPeers(t *testing.T) {
	// One peer with a text multiaddr address, one with a binary-encoded
	// ip4/tcp multiaddr, one with a binary-encoded ip6/tcp multiaddr, and
	// one with a garbage address that should be silently skipped (but
	// the peer itself still contributes its other, parseable address).
	binaryIP4TCP := encodeTestBinaryMultiaddrIP4TCP([4]byte{10, 0, 0, 1}, 18189)
	binaryIP6TCP := encodeTestBinaryMultiaddrIP6TCP(
		[16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 18189,
	)

	fake := &fakeBaseNodeServer{
		connPeers: &tari_generated.ListConnectedPeersResponse{
			ConnectedPeers: []*tari_generated.Peer{
				{
					PublicKey: []byte("peer1"),
					Addresses: []*tari_generated.Address{
						{Address: []byte("/ip4/1.2.3.4/tcp/18189")},
					},
				},
				{
					PublicKey: []byte("peer2"),
					Addresses: []*tari_generated.Address{
						{Address: binaryIP4TCP},
					},
				},
				{
					PublicKey: []byte("peer3"),
					Addresses: []*tari_generated.Address{
						{Address: binaryIP6TCP},
					},
				},
				{
					PublicKey: []byte("peer4-mixed"),
					Addresses: []*tari_generated.Address{
						{Address: []byte{0xff, 0xff, 0xff}}, // garbage, unparseable
						{Address: []byte("/ip4/9.9.9.9/tcp/1234")},
					},
				},
			},
		},
	}
	client, cleanup := startFakeBaseNode(t, fake)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peers, err := client.GetPeers(ctx, "passthrough:///bufconn")
	if err != nil {
		t.Fatalf("GetPeers: %v", err)
	}

	want := map[string]string{
		"1.2.3.4:18189":  "peer1",
		"10.0.0.1:18189": "peer2",
		"[::1]:18189":    "peer3",
		"9.9.9.9:1234":   "peer4-mixed",
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

func TestGRPCClientDialFailureReturnsError(t *testing.T) {
	// No fake server listening: a real dial (not bufconn) to an address
	// nothing is bound to should fail relatively quickly. grpc.NewClient
	// itself doesn't dial eagerly, so the failure surfaces on the first
	// RPC call once the short dialTimeout context deadline is hit.
	client := &grpcNodeClient{dial: grpc.NewClient}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := client.GetInfo(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("GetInfo against an unreachable address: expected error, got nil")
	}
}
