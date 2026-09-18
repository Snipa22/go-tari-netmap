package main

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"testing"
)

func TestParseSeedNodes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "empty", raw: "", want: nil},
		{name: "single", raw: "seed:1", want: []string{"seed:1"}},
		{
			name: "multiple with whitespace and empty entries",
			raw:  " seed:1 , , seed:2,seed:3 ",
			want: []string{"seed:1", "seed:2", "seed:3"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseSeedNodes(tc.raw)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseSeedNodes(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// TestParseSocksProxyAddrs verifies parseSocksProxyAddrs' plural-preferred/singular-fallback
// precedence (see its doc comment and NETMAP_SOCKS_PROXY_ADDRS/NETMAP_SOCKS_PROXY_ADDR's
// construction site in main.go).
func TestParseSocksProxyAddrs(t *testing.T) {
	cases := []struct {
		name        string
		pluralRaw   string
		singularRaw string
		want        []string
	}{
		{name: "both empty/unset", pluralRaw: "", singularRaw: "", want: nil},
		{
			name:        "singular only (testnet's existing zero-config-change deployment)",
			pluralRaw:   "",
			singularRaw: "127.0.0.1:9050",
			want:        []string{"127.0.0.1:9050"},
		},
		{
			name:        "plural only",
			pluralRaw:   "127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102",
			singularRaw: "",
			want:        []string{"127.0.0.1:9100", "127.0.0.1:9101", "127.0.0.1:9102"},
		},
		{
			name:        "plural takes precedence when both are set",
			pluralRaw:   "127.0.0.1:9100,127.0.0.1:9101",
			singularRaw: "127.0.0.1:9050",
			want:        []string{"127.0.0.1:9100", "127.0.0.1:9101"},
		},
		{
			name:        "plural with whitespace and empty entries",
			pluralRaw:   " 127.0.0.1:9100 , , 127.0.0.1:9101 ",
			singularRaw: "",
			want:        []string{"127.0.0.1:9100", "127.0.0.1:9101"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseSocksProxyAddrs(tc.pluralRaw, tc.singularRaw)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseSocksProxyAddrs(%q, %q) = %v, want %v", tc.pluralRaw, tc.singularRaw, got, tc.want)
			}
		})
	}
}

// TestParseOwnedGRPCAddresses verifies parseOwnedGRPCAddresses' comma-separated
// "p2pAddress=grpcAddress" parsing, mirroring TestParseSeedNodes' style.
// TestParseCollectorKeys verifies parseCollectorKeys' comma-separated
// "collector_name:api_key" parsing, mirroring TestParseOwnedGRPCAddresses' style (see
// NETMAP_COLLECTOR_KEYS' construction site in main.go).
func TestParseCollectorKeys(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want map[string]string
	}{
		{name: "empty/unset returns nil (feature not configured)", raw: "", want: nil},
		{
			name: "single pair",
			raw:  "sydney:secret-key-1",
			want: map[string]string{"sydney": "secret-key-1"},
		},
		{
			name: "multiple pairs with whitespace",
			raw:  " sydney:secret-key-1 , london:secret-key-2 ",
			want: map[string]string{"sydney": "secret-key-1", "london": "secret-key-2"},
		},
		{
			name: "malformed entries (no ':', empty name/key) are skipped, not fatal",
			raw:  "no-colon,:missing-name,missing-key:,good:good-key",
			want: map[string]string{"good": "good-key"},
		},
		{
			name: "raw non-empty but every entry malformed still returns a non-nil (empty) map",
			raw:  "totally-malformed",
			want: map[string]string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseCollectorKeys(tc.raw)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseCollectorKeys(%q) = %#v, want %#v", tc.raw, got, tc.want)
			}
		})
	}
}

// TestParseCollectorKeysNeverLogsRawKeyMaterial asserts the malformed-entry log line (see
// parseCollectorKeys' doc comment / this repo's readiness-review follow-up Fix 7) never
// contains the raw, possibly-key-bearing entry text -- only its 1-based position. This
// redirects the standard logger's output for the duration of the test and restores it
// afterward.
func TestParseCollectorKeysNeverLogsRawKeyMaterial(t *testing.T) {
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })

	const secretLookingValue = "super-secret-api-key-do-not-leak"
	parseCollectorKeys("this-entry-has-no-colon-" + secretLookingValue)

	if strings.Contains(buf.String(), secretLookingValue) {
		t.Errorf("log output unexpectedly contains raw malformed-entry text (leaks potential key material): %s", buf.String())
	}
	if !strings.Contains(buf.String(), "entry #1") {
		t.Errorf("log output = %q, want it to identify the malformed entry by position (\"entry #1\")", buf.String())
	}
}

func TestParseOwnedGRPCAddresses(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want map[string]string
	}{
		{name: "empty/unset returns nil (feature not configured)", raw: "", want: nil},
		{
			name: "single pair",
			raw:  "23.226.69.178:18189=23.226.69.178:18102",
			want: map[string]string{"23.226.69.178:18189": "23.226.69.178:18102"},
		},
		{
			name: "multiple pairs with whitespace",
			raw:  " 23.226.69.178:18189=23.226.69.178:18102 , 10.0.0.5:18189=10.0.0.5:18102 ",
			want: map[string]string{
				"23.226.69.178:18189": "23.226.69.178:18102",
				"10.0.0.5:18189":      "10.0.0.5:18102",
			},
		},
		{
			name: "malformed entries (no '=', empty p2p/grpc side) are skipped, not fatal",
			raw:  "no-equals-sign,=missing-p2p-side,missing-grpc-side=,good:1=good:2",
			want: map[string]string{"good:1": "good:2"},
		},
		{
			name: "raw non-empty but every entry malformed still returns a non-nil (empty) map",
			raw:  "totally-malformed",
			want: map[string]string{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseOwnedGRPCAddresses(tc.raw)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseOwnedGRPCAddresses(%q) = %#v, want %#v", tc.raw, got, tc.want)
			}
		})
	}
}
