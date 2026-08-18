package api

import "testing"

// TestAddressToMultiaddr covers IPv4, IPv6, onion, and error cases,
// including the literal real-world examples given in
// SEED_CONFIG_SPEC.md: Alex's real dual-stack node (pubkey
// a841efcd8bc47d3db9e658f5da4d6858b6cbd387812d84f9f7a98e8cc871a85e,
// address 23.226.69.178:18189 and address
// wyow2dp6w2ff4u2kebklkmbzwlixyhjtza5bf3pt3oxnps5hcjn76iyd.onion:18141).
func TestAddressToMultiaddr(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		want    string
		wantErr bool
	}{
		{
			name: "ipv4",
			addr: "1.2.3.4:18189",
			want: "/ip4/1.2.3.4/tcp/18189",
		},
		{
			name: "ipv4 real example (Alex's node)",
			addr: "23.226.69.178:18189",
			want: "/ip4/23.226.69.178/tcp/18189",
		},
		{
			name: "ipv6 bracketed",
			addr: "[::1]:18189",
			want: "/ip6/::1/tcp/18189",
		},
		{
			name: "ipv6 bracketed, non-loopback",
			addr: "[2001:db8::1]:9000",
			want: "/ip6/2001:db8::1/tcp/9000",
		},
		{
			name: "onion real example (Alex's node)",
			addr: "wyow2dp6w2ff4u2kebklkmbzwlixyhjtza5bf3pt3oxnps5hcjn76iyd.onion:18141",
			want: "/onion3/wyow2dp6w2ff4u2kebklkmbzwlixyhjtza5bf3pt3oxnps5hcjn76iyd:18141",
		},
		{
			name: "onion uppercase suffix normalizes",
			addr: "abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuv.ONION:9001",
			want: "/onion3/abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuv:9001",
		},
		{
			name:    "missing port",
			addr:    "1.2.3.4",
			wantErr: true,
		},
		{
			name:    "empty",
			addr:    "",
			wantErr: true,
		},
		{
			name:    "non-numeric port",
			addr:    "1.2.3.4:notaport",
			wantErr: true,
		},
		{
			name:    "not an IP and not onion",
			addr:    "example.com:18189",
			wantErr: true,
		},
		{
			name:    "empty onion host",
			addr:    ".onion:18189",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := addressToMultiaddr(tc.addr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("addressToMultiaddr(%q) = %q, want error", tc.addr, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("addressToMultiaddr(%q) unexpected error: %v", tc.addr, err)
			}
			if got != tc.want {
				t.Errorf("addressToMultiaddr(%q) = %q, want %q", tc.addr, got, tc.want)
			}
		})
	}
}
