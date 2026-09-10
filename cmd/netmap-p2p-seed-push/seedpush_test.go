package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Snipa22/go-tari-lib/p2p"
	pb "github.com/Snipa22/go-tari-lib/p2p/proto"

	"github.com/Snipa22/go-tari-netmap/internal/p2pidentity"
)

// startLoopbackResponder starts a real go-tari-lib/p2p.Serve responder on a fresh loopback TCP
// listener, mirroring cmd/netmap-p2p-responder/responder_test.go's startTestResponder exactly
// (same package, same pattern) -- this is the "spin up a real go-tari-lib responder in-process"
// half of BRIEF8.md's most valuable test. onIdentity is called once per successful identity
// exchange the responder receives, reporting exactly what the dialing side (this tool's own
// PushOne) advertised.
func startLoopbackResponder(t *testing.T, onIdentity func(remoteAddr net.Addr, staticKey []byte, identity *p2p.PeerInfo)) (addr string, cleanup func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("starting loopback listener: %v", err)
	}

	responderStatic, err := p2p.GenerateRistrettoKeypair()
	if err != nil {
		listener.Close()
		t.Fatalf("generating responder static keypair: %v", err)
	}

	cfg := p2p.ResponderConfig{
		StaticKeypair: responderStatic,
		OurFeatures:   p2p.FeaturesCommunicationNode,
		PeerListProvider: func(ctx context.Context) ([]*pb.PeerInfo, error) {
			return nil, nil
		},
		OnPeerIdentity: onIdentity,
		Logf:           t.Logf,
	}

	serveCtx, serveCancel := context.WithCancel(context.Background())
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- p2p.Serve(serveCtx, listener, cfg)
	}()

	cleanup = func() {
		serveCancel()
		listener.Close()
		<-serveErrCh
	}
	return listener.Addr().String(), cleanup
}

// TestPushOneOverLoopbackAdvertisesRealIdentity is BRIEF8.md's most valuable test: it spins up a
// real go-tari-lib/p2p responder in-process (startLoopbackResponder above), dials it with this
// tool's own exact PushOne identity-exchange logic (the same function main.go's run() calls),
// and asserts the responder's OnPeerIdentity callback recorded OUR real pubkey, real
// COMMUNICATION_NODE features, and our real advertised address -- exactly what a real Tari seed
// node's own comms/peer manager would record from a real dial by this tool.
func TestPushOneOverLoopbackAdvertisesRealIdentity(t *testing.T) {
	type identityReport struct {
		remoteAddr net.Addr
		staticKey  []byte
		identity   *p2p.PeerInfo
	}
	identityCh := make(chan identityReport, 1)

	addr, cleanup := startLoopbackResponder(t, func(remoteAddr net.Addr, staticKey []byte, identity *p2p.PeerInfo) {
		identityCh <- identityReport{remoteAddr: remoteAddr, staticKey: staticKey, identity: identity}
	})
	defer cleanup()

	ourKey, err := p2p.GenerateRistrettoKeypair()
	if err != nil {
		t.Fatalf("generating our own keypair: %v", err)
	}
	ourAddresses, err := p2pidentity.ParseAdvertisedAddresses("/ip4/203.0.113.7/tcp/18189", "")
	if err != nil {
		t.Fatalf("ParseAdvertisedAddresses: %v", err)
	}

	cfg := PushConfig{
		StaticKeypair: ourKey,
		NetworkByte:   p2p.NetworkByteMainNet,
		Features:      p2p.FeaturesCommunicationNode,
		Addresses:     ourAddresses,
		DialTimeout:   10 * time.Second,
		Logf:          t.Logf,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := PushOne(ctx, addr, cfg)
	if result.Err != nil {
		t.Fatalf("PushOne: %v", result.Err)
	}
	if result.Target != addr {
		t.Errorf("result.Target = %q, want %q", result.Target, addr)
	}
	if len(result.PeerPubKey) != 32 {
		t.Errorf("result.PeerPubKey has %d bytes, want 32 (the responder's own static key)", len(result.PeerPubKey))
	}
	if result.Latency <= 0 {
		t.Errorf("result.Latency = %v, want > 0", result.Latency)
	}

	select {
	case report := <-identityCh:
		if string(report.staticKey) != string(ourKey.Public) {
			t.Errorf("responder recorded our static key = %x, want %x", report.staticKey, ourKey.Public)
		}
		if report.identity.Features != p2p.FeaturesCommunicationNode {
			t.Errorf("responder recorded our Features = %d, want %d (FeaturesCommunicationNode)", report.identity.Features, p2p.FeaturesCommunicationNode)
		}
		if len(report.identity.Addresses) != 1 {
			t.Fatalf("responder recorded %d address(es), want 1", len(report.identity.Addresses))
		}
		if string(report.identity.Addresses[0]) != string(ourAddresses[0]) {
			t.Errorf("responder recorded our address = %x, want %x", report.identity.Addresses[0], ourAddresses[0])
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for the responder's OnPeerIdentity to fire")
	}
}

// TestPushOneFailureAgainstUnreachableTarget confirms a target that never accepts a connection
// (nothing listening) is reported as a dial failure, with a non-nil Err and no PeerPubKey --
// this exercises the "diagnostic per-target output" path for the failure case, not just the
// happy path above.
func TestPushOneFailureAgainstUnreachableTarget(t *testing.T) {
	// A loopback listener we immediately close: its address is guaranteed unreachable
	// (connection refused) without depending on any real external network behavior.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	ourKey, err := p2p.GenerateRistrettoKeypair()
	if err != nil {
		t.Fatalf("generating our own keypair: %v", err)
	}
	ourAddresses, err := p2pidentity.ParseAdvertisedAddresses("/ip4/203.0.113.7/tcp/18189", "")
	if err != nil {
		t.Fatalf("ParseAdvertisedAddresses: %v", err)
	}

	cfg := PushConfig{
		StaticKeypair: ourKey,
		NetworkByte:   p2p.NetworkByteMainNet,
		Features:      p2p.FeaturesCommunicationNode,
		Addresses:     ourAddresses,
		DialTimeout:   5 * time.Second,
		Logf:          t.Logf,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result := PushOne(ctx, addr, cfg)
	if result.Err == nil {
		t.Fatalf("PushOne against an unreachable target succeeded, want an error")
	}
	if len(result.PeerPubKey) != 0 {
		t.Errorf("result.PeerPubKey = %x, want empty on failure", result.PeerPubKey)
	}
}

// TestPushAllRunsEveryTargetBoundedByConcurrency exercises PushAll's bounded-concurrency
// fan-out against several loopback responders, asserting every target's result comes back
// (in input order) and every one succeeds.
func TestPushAllRunsEveryTargetBoundedByConcurrency(t *testing.T) {
	const numResponders = 4

	var targets []string
	for i := 0; i < numResponders; i++ {
		addr, cleanup := startLoopbackResponder(t, nil)
		t.Cleanup(cleanup)
		targets = append(targets, addr)
	}

	ourKey, err := p2p.GenerateRistrettoKeypair()
	if err != nil {
		t.Fatalf("generating our own keypair: %v", err)
	}
	ourAddresses, err := p2pidentity.ParseAdvertisedAddresses("/ip4/203.0.113.7/tcp/18189", "")
	if err != nil {
		t.Fatalf("ParseAdvertisedAddresses: %v", err)
	}

	cfg := PushConfig{
		StaticKeypair: ourKey,
		NetworkByte:   p2p.NetworkByteMainNet,
		Features:      p2p.FeaturesCommunicationNode,
		Addresses:     ourAddresses,
		DialTimeout:   10 * time.Second,
		Concurrency:   2,
		Logf:          t.Logf,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	results := PushAll(ctx, targets, cfg)
	if len(results) != len(targets) {
		t.Fatalf("len(results) = %d, want %d", len(results), len(targets))
	}
	for i, r := range results {
		if r.Target != targets[i] {
			t.Errorf("results[%d].Target = %q, want %q (order must match input)", i, r.Target, targets[i])
		}
		if r.Err != nil {
			t.Errorf("results[%d] (%s) failed: %v", i, r.Target, r.Err)
		}
		if len(r.PeerPubKey) != 32 {
			t.Errorf("results[%d] (%s) PeerPubKey has %d bytes, want 32", i, r.Target, len(r.PeerPubKey))
		}
	}
}

// TestParseTargetsInline covers ParseTargets' inline comma-separated form.
func TestParseTargetsInline(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []string
		wantErr bool
	}{
		{
			name: "single target",
			raw:  "1.2.3.4:18189",
			want: []string{"1.2.3.4:18189"},
		},
		{
			name: "multiple, whitespace and dedup",
			raw:  "1.2.3.4:18189, 5.6.7.8:18189 ,1.2.3.4:18189",
			want: []string{"1.2.3.4:18189", "5.6.7.8:18189"},
		},
		{
			name: "onion target accepted",
			raw:  "abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuvwx.onion:18189",
			want: []string{"abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuvwx.onion:18189"},
		},
		{
			name:    "empty",
			raw:     "",
			wantErr: true,
		},
		{
			name:    "only whitespace",
			raw:     "   ",
			wantErr: true,
		},
		{
			name:    "missing port",
			raw:     "1.2.3.4",
			wantErr: true,
		},
		{
			name:    "one bad entry among good ones",
			raw:     "1.2.3.4:18189,not-a-target",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTargets(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseTargets(%q) = %v, want an error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTargets(%q): %v", tc.raw, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ParseTargets(%q) = %v, want %v", tc.raw, got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("ParseTargets(%q)[%d] = %q, want %q", tc.raw, i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestParseTargetsFromFile covers ParseTargets' file form: one (or comma-separated) target(s)
// per line, blank lines and # comments (including trailing partial-line comments) ignored.
func TestParseTargetsFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seeds.txt")
	content := "" +
		"# real mainnet seed nodes\n" +
		"1.2.3.4:18189\n" +
		"\n" +
		"5.6.7.8:18189 # some inline comment\n" +
		"9.9.9.9:18189, 10.10.10.10:18189\n" +
		"   \n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing fixture file: %v", err)
	}

	got, err := ParseTargets(path)
	if err != nil {
		t.Fatalf("ParseTargets(%q): %v", path, err)
	}
	want := []string{"1.2.3.4:18189", "5.6.7.8:18189", "9.9.9.9:18189", "10.10.10.10:18189"}
	if len(got) != len(want) {
		t.Fatalf("ParseTargets(file) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("ParseTargets(file)[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestParseTargetsFromEmptyFile covers a file that parses to zero targets (all blank/comment
// lines) -- must be a hard error, not a silent empty result.
func TestParseTargetsFromEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(path, []byte("# nothing here\n\n"), 0o600); err != nil {
		t.Fatalf("writing fixture file: %v", err)
	}

	if _, err := ParseTargets(path); err == nil {
		t.Fatalf("ParseTargets(%q) succeeded, want an error (zero targets)", path)
	}
}

// TestNetworkByteFor covers the -network flag's mapping to the real P2P wire network byte.
func TestNetworkByteFor(t *testing.T) {
	tests := []struct {
		network string
		want    byte
		wantErr bool
	}{
		{network: "mainnet", want: p2p.NetworkByteMainNet},
		{network: "testnet", want: p2p.NetworkByteEsmeralda},
		{network: "", wantErr: true},
		{network: "nextnet", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.network, func(t *testing.T) {
			got, err := networkByteFor(tc.network)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("networkByteFor(%q) = %v, want an error", tc.network, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("networkByteFor(%q): %v", tc.network, err)
			}
			if got != tc.want {
				t.Errorf("networkByteFor(%q) = 0x%02x, want 0x%02x", tc.network, got, tc.want)
			}
		})
	}
}
