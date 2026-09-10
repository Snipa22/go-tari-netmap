// Package p2pidentity holds the exact key-loading and advertised-address-encoding logic shared
// by every go-tari-netmap binary that needs to present a REAL, persistent Tari P2P identity to
// real peers — currently cmd/netmap-p2p-responder (the inbound side) and cmd/netmap-p2p-seed-push
// (the outbound side, see BRIEF8.md). Both binaries must derive their Session's
// IdentityOptions/ResponderConfig fields from the SAME identity key file and the SAME advertised
// addresses for a given deployment (e.g. CT129's mainnet identity.key), or a real peer ends up
// with two different, unconverging identities for what is supposed to be one logical responder.
// Factoring this logic out here (rather than duplicating it across cmd/ packages, which Go
// can't import from each other since they're both package main) is what makes that guarantee
// enforceable at compile time instead of by convention alone.
package p2pidentity

import (
	"fmt"
	"os"

	"github.com/flynn/noise"

	"github.com/Snipa22/go-tari-lib/p2p"
)

// KeyFileSize is the size, in bytes, of the file LoadOrGenerateKeypair reads/writes: the 32-byte
// private scalar followed by the 32-byte public point, i.e. exactly noise.DHKey's two fields
// concatenated. Storing both (rather than just the private key and re-deriving the public key on
// load) avoids needing to export Ristretto255 scalar-base-multiplication from package p2p purely
// for this convenience.
const KeyFileSize = 64

// LoadOrGenerateKeypair loads a keypair from path (see KeyFileSize) if path is non-empty and the
// file exists; otherwise it generates a fresh keypair and, if path is non-empty, saves it there
// (mode 0600) for reuse across restarts. An empty path always generates an ephemeral, unsaved
// keypair.
//
// Every caller that needs to present a REAL, converging identity to real Tari peers (as opposed
// to a disposable probe identity) MUST pass a non-empty, pre-existing path here — see this
// package's doc comment for why a fresh key each run defeats the entire point.
func LoadOrGenerateKeypair(path string) (noise.DHKey, error) {
	if path != "" {
		if raw, err := os.ReadFile(path); err == nil {
			if len(raw) != KeyFileSize {
				return noise.DHKey{}, fmt.Errorf("key file %s has %d bytes, want %d", path, len(raw), KeyFileSize)
			}
			return noise.DHKey{
				Private: append([]byte(nil), raw[:32]...),
				Public:  append([]byte(nil), raw[32:]...),
			}, nil
		} else if !os.IsNotExist(err) {
			return noise.DHKey{}, fmt.Errorf("reading key file %s: %w", path, err)
		}
	}

	keypair, err := p2p.GenerateRistrettoKeypair()
	if err != nil {
		return noise.DHKey{}, fmt.Errorf("generating a fresh keypair: %w", err)
	}

	if path != "" {
		raw := append(append([]byte(nil), keypair.Private...), keypair.Public...)
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			return noise.DHKey{}, fmt.Errorf("saving fresh keypair to %s: %w", path, err)
		}
	}
	return keypair, nil
}

// ParseAdvertisedAddresses validates and encodes a -public-tcp-addr/-onion3-addr flag pair into
// the raw binary rust-multiaddr wire encoding p2p.ResponderConfig.OurAddresses / p2p.
// IdentityOptions.Addresses expect (see go-tari-lib's p2p/multiaddr.go EncodeMultiaddrString doc
// comment for exactly why that, and not a UTF-8 string, is required). At least one of the two
// arguments MUST be non-empty — fails fast with a clear error otherwise, rather than silently
// advertising ourselves with zero addresses (a real Tari node's comms/dht/src/peer_validator.rs
// PeerHasNoAddresses/PeerHasNoUsableAddresses checks reject a peer with zero advertised
// addresses regardless of which side of the connection advertised them).
func ParseAdvertisedAddresses(publicTCPAddr, onion3Addr string) ([][]byte, error) {
	if publicTCPAddr == "" && onion3Addr == "" {
		return nil, fmt.Errorf("at least one of -public-tcp-addr or -onion3-addr must be set: a COMMUNICATION_NODE peer with no advertised addresses is rejected by real Tari nodes' peer validation and would advertise itself unreachably")
	}

	var out [][]byte
	if publicTCPAddr != "" {
		encoded, err := p2p.EncodeMultiaddrString(publicTCPAddr)
		if err != nil {
			return nil, fmt.Errorf("-public-tcp-addr %q: %w", publicTCPAddr, err)
		}
		out = append(out, encoded)
	}
	if onion3Addr != "" {
		encoded, err := p2p.EncodeMultiaddrString(onion3Addr)
		if err != nil {
			return nil, fmt.Errorf("-onion3-addr %q: %w", onion3Addr, err)
		}
		out = append(out, encoded)
	}
	return out, nil
}
