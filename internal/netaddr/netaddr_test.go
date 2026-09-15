package netaddr

import "testing"

func TestIPv4Host(t *testing.T) {
	cases := []struct {
		name    string
		address string
		wantIP  string
		wantOK  bool
	}{
		{name: "ipv4 host:port", address: "203.0.113.5:18189", wantIP: "203.0.113.5", wantOK: true},
		{name: "ipv6 host:port", address: "[2001:db8::1]:18189", wantOK: false},
		{name: "onion host:port", address: "abcdefghijklmnop.onion:18189", wantOK: false},
		{name: "malformed (no port)", address: "203.0.113.5", wantOK: false},
		{name: "empty string", address: "", wantOK: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ip, ok := IPv4Host(tc.address)
			if ok != tc.wantOK {
				t.Fatalf("IPv4Host(%q) ok = %v, want %v (ip=%q)", tc.address, ok, tc.wantOK, ip)
			}
			if ok && ip != tc.wantIP {
				t.Errorf("IPv4Host(%q) = %q, want %q", tc.address, ip, tc.wantIP)
			}
		})
	}
}
