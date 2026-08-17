package collector

import (
	"encoding/binary"
	"testing"
)

// encodeTestBinaryMultiaddrIP4TCP builds a binary multiaddr byte sequence
// for an ip4/tcp address, matching the encoding parseBinaryMultiaddr
// expects: varint(4) + 4 raw IPv4 bytes + varint(6) + 2 big-endian port
// bytes.
func encodeTestBinaryMultiaddrIP4TCP(ip [4]byte, port uint16) []byte {
	buf := make([]byte, 0, 16)
	buf = appendVarint(buf, multiaddrProtoIP4)
	buf = append(buf, ip[:]...)
	buf = appendVarint(buf, multiaddrProtoTCP)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, port)
	buf = append(buf, portBytes...)
	return buf
}

// encodeTestBinaryMultiaddrIP6TCP is the ip6/tcp equivalent of
// encodeTestBinaryMultiaddrIP4TCP.
func encodeTestBinaryMultiaddrIP6TCP(ip [16]byte, port uint16) []byte {
	buf := make([]byte, 0, 32)
	buf = appendVarint(buf, multiaddrProtoIP6)
	buf = append(buf, ip[:]...)
	buf = appendVarint(buf, multiaddrProtoTCP)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, port)
	buf = append(buf, portBytes...)
	return buf
}

func appendVarint(buf []byte, v uint64) []byte {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, v)
	return append(buf, tmp[:n]...)
}

func TestParsePeerAddress(t *testing.T) {
	cases := []struct {
		name   string
		raw    []byte
		want   string
		wantOK bool
	}{
		{
			name:   "text multiaddr ip4/tcp",
			raw:    []byte("/ip4/1.2.3.4/tcp/18189"),
			want:   "1.2.3.4:18189",
			wantOK: true,
		},
		{
			name:   "text multiaddr ip6/tcp",
			raw:    []byte("/ip6/::1/tcp/18189"),
			want:   "[::1]:18189",
			wantOK: true,
		},
		{
			name:   "text multiaddr dns4/tcp",
			raw:    []byte("/dns4/example.com/tcp/443"),
			want:   "example.com:443",
			wantOK: true,
		},
		{
			name:   "binary multiaddr ip4/tcp",
			raw:    encodeTestBinaryMultiaddrIP4TCP([4]byte{10, 0, 0, 1}, 18189),
			want:   "10.0.0.1:18189",
			wantOK: true,
		},
		{
			name:   "binary multiaddr ip6/tcp",
			raw:    encodeTestBinaryMultiaddrIP6TCP([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 4200),
			want:   "[::1]:4200",
			wantOK: true,
		},
		{
			name:   "empty bytes",
			raw:    []byte{},
			wantOK: false,
		},
		{
			name:   "garbage bytes",
			raw:    []byte{0xff, 0xff, 0xff, 0xff, 0xff},
			wantOK: false,
		},
		{
			name:   "text multiaddr missing tcp segment",
			raw:    []byte("/ip4/1.2.3.4"),
			wantOK: false,
		},
		{
			name:   "binary multiaddr with unsupported protocol code",
			raw:    append(appendVarint(nil, 421), []byte{1, 2, 3}...), // /onion3-ish unsupported code
			wantOK: false,
		},
		{
			name:   "binary multiaddr truncated ip4 bytes",
			raw:    appendVarint(nil, multiaddrProtoIP4), // code present, no address bytes follow
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parsePeerAddress(tc.raw)
			if ok != tc.wantOK {
				t.Fatalf("parsePeerAddress(%v) ok = %v, want %v (got %q)", tc.raw, ok, tc.wantOK, got)
			}
			if ok && got != tc.want {
				t.Errorf("parsePeerAddress(%v) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestParsePeerAddressDoesNotPanicOnGarbage(t *testing.T) {
	// A broad sweep of adversarial inputs: must never panic, regardless
	// of return value.
	inputs := [][]byte{
		nil,
		{},
		{0x00},
		{0xff},
		{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, // malformed varint (all continuation bits)
		[]byte("not a multiaddr at all"),
		[]byte("/"),
		[]byte("/ip4"),
		{4, 1, 2}, // claims ip4 proto but only 2 bytes follow
	}
	for _, in := range inputs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("parsePeerAddress(%v) panicked: %v", in, r)
				}
			}()
			parsePeerAddress(in)
		}()
	}
}
