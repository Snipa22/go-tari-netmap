package api

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Snipa22/go-tari-netmap/internal/storage"
)

func TestScrubNodeP2PHidesAddress(t *testing.T) {
	n := storage.Node{
		ID:              uuid.New(),
		Address:         "1.2.3.4:18142",
		DiscoverySource: storage.DiscoverySourceP2P,
		Tags:            map[string]any{},
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
	}

	pn := ScrubNode(n, nil)

	if pn.Address != nil {
		t.Errorf("Address = %v, want nil for p2p_discovered node", *pn.Address)
	}
	if !pn.HasIPv4 {
		t.Error("HasIPv4 = false, want true")
	}
	if pn.HasIPv6 || pn.HasOnion {
		t.Errorf("HasIPv6 = %v, HasOnion = %v, want both false", pn.HasIPv6, pn.HasOnion)
	}
}

func TestScrubNodeRegistrySubmittedShowsAddress(t *testing.T) {
	n := storage.Node{
		ID:              uuid.New(),
		Address:         "5.6.7.8:18142",
		DiscoverySource: storage.DiscoverySourceRegistry,
		Tags:            map[string]any{},
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
	}

	pn := ScrubNode(n, nil)

	if pn.Address == nil || *pn.Address != "5.6.7.8:18142" {
		t.Errorf("Address = %v, want 5.6.7.8:18142", pn.Address)
	}
}

func TestScrubNodeBothShowsAddress(t *testing.T) {
	n := storage.Node{
		ID:              uuid.New(),
		Address:         "9.9.9.9:18142",
		DiscoverySource: storage.DiscoverySourceBoth,
		Tags:            map[string]any{},
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
	}

	pn := ScrubNode(n, nil)

	if pn.Address == nil || *pn.Address != "9.9.9.9:18142" {
		t.Errorf("Address = %v, want 9.9.9.9:18142", pn.Address)
	}
}

func TestScrubNodeOnionOnly(t *testing.T) {
	n := storage.Node{
		ID:              uuid.New(),
		Address:         "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd.onion:18142",
		DiscoverySource: storage.DiscoverySourceP2P,
		Tags:            map[string]any{},
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
	}

	pn := ScrubNode(n, nil)

	if !pn.HasOnion {
		t.Error("HasOnion = false, want true")
	}
	if pn.HasIPv4 || pn.HasIPv6 {
		t.Errorf("HasIPv4 = %v, HasIPv6 = %v, want both false", pn.HasIPv4, pn.HasIPv6)
	}
	if pn.Address != nil {
		t.Errorf("Address = %v, want nil", *pn.Address)
	}
}

func TestScrubNodeIPv6(t *testing.T) {
	n := storage.Node{
		ID:              uuid.New(),
		DiscoverySource: storage.DiscoverySourceP2P,
		Tags:            map[string]any{},
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
	}
	addrs := []storage.NodeAddress{
		{Address: "[2001:db8::1]:18142"},
	}

	pn := ScrubNode(n, addrs)

	if !pn.HasIPv6 {
		t.Error("HasIPv6 = false, want true")
	}
	if pn.HasIPv4 || pn.HasOnion {
		t.Errorf("HasIPv4 = %v, HasOnion = %v, want both false", pn.HasIPv4, pn.HasOnion)
	}
}

func TestScrubNodeMultipleAddresses(t *testing.T) {
	n := storage.Node{
		ID:              uuid.New(),
		DiscoverySource: storage.DiscoverySourceRegistry,
		Tags:            map[string]any{},
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
	}
	addrs := []storage.NodeAddress{
		{Address: "1.2.3.4:18142"},
		{Address: "[2001:db8::1]:18142"},
		{Address: "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd.onion:18142"},
	}

	pn := ScrubNode(n, addrs)

	if !pn.HasIPv4 || !pn.HasIPv6 || !pn.HasOnion {
		t.Errorf("HasIPv4 = %v, HasIPv6 = %v, HasOnion = %v, want all true", pn.HasIPv4, pn.HasIPv6, pn.HasOnion)
	}
	// n.Address is empty here (only addrs is populated), so Address
	// should fall back to the first addrs entry per primaryAddress.
	if pn.Address == nil || *pn.Address != "1.2.3.4:18142" {
		t.Errorf("Address = %v, want 1.2.3.4:18142", pn.Address)
	}
}

func TestScrubNodeMalformedAddressDoesNotPanic(t *testing.T) {
	n := storage.Node{
		ID:              uuid.New(),
		DiscoverySource: storage.DiscoverySourceP2P,
		Tags:            map[string]any{},
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
	}
	addrs := []storage.NodeAddress{
		{Address: "not-a-valid-address"},   // no port, SplitHostPort fails
		{Address: "1.2.3.4:18142"},         // still classified correctly
		{Address: "totally:bogus:address"}, // malformed, skipped gracefully
	}

	pn := ScrubNode(n, addrs)

	if !pn.HasIPv4 {
		t.Error("HasIPv4 = false, want true (valid entry should still be classified despite malformed siblings)")
	}
	if pn.HasIPv6 || pn.HasOnion {
		t.Errorf("HasIPv6 = %v, HasOnion = %v, want both false", pn.HasIPv6, pn.HasOnion)
	}
}

func TestScrubNodePlaceholderPubKey(t *testing.T) {
	n := storage.Node{
		ID:              uuid.New(),
		DiscoverySource: storage.DiscoverySourceP2P,
		PublicKey:       nil,
		Tags:            map[string]any{},
		FirstSeen:       time.Now(),
		LastSeen:        time.Now(),
	}

	pn := ScrubNode(n, nil)

	if len(pn.PublicKey) != 0 {
		t.Errorf("PublicKey = %x, want empty for placeholder node", pn.PublicKey)
	}
}

func TestClassifyAddress(t *testing.T) {
	cases := []struct {
		address string
		want    addrKind
	}{
		{"1.2.3.4:18142", addrKindIPv4},
		{"[2001:db8::1]:18142", addrKindIPv6},
		{"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd.onion:18142", addrKindOnion},
		{"ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCD.ONION:18142", addrKindOnion},
		{"not-a-valid-address", addrKindUnknown},
		{"", addrKindUnknown},
	}
	for _, tc := range cases {
		if got := classifyAddress(tc.address); got != tc.want {
			t.Errorf("classifyAddress(%q) = %v, want %v", tc.address, got, tc.want)
		}
	}
}
