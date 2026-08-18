package api

import "testing"

// TestValidateHostSyntax asserts validateHostSyntax accepts any
// syntactically valid IP (public or private — SSRF hardening is
// validateSubmittedHost's job, not this one's) and any syntactically
// plausible .onion address, while rejecting malformed hosts (including
// a stray leading slash on an otherwise onion-shaped string) and empty
// input.
func TestValidateHostSyntax(t *testing.T) {
	cases := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{"valid public IPv4", "1.1.1.1", false},
		{"valid private IPv4", "192.168.1.1", false},
		{"valid onion v3", "abcdefghijklmnopqrstuvwxyz234567abcdefghijklmnopqrstuvwx.onion", false},
		{"onion with leading slash", "/wyow2dp6w2ff4u2kebklkmbzwlixyhjtza5bf3pt3oxnps5hcjn76iyd.onion", true},
		{"empty string", "", true},
		{"garbage", "not a host!!", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateHostSyntax(tc.host)
			if tc.wantErr && err == nil {
				t.Errorf("validateHostSyntax(%q) = nil, want an error", tc.host)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validateHostSyntax(%q) = %v, want nil", tc.host, err)
			}
		})
	}
}
