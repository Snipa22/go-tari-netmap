package api

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// onionSuffix is the literal suffix stripped from an onion host before
// building an /onion3/ multiaddr segment.
const onionSuffix = ".onion"

// addressToMultiaddr converts a raw "host:port" address string — the
// same format stored in node_addresses/nodes.address throughout this
// codebase, including the bracketed-IPv6 form (`[::1]:18189`) already
// produced by internal/collector's multiaddr parsing (see
// decodeOnionAddr/parseTextMultiaddr there) — into the Tari-native
// multiaddr string used by a config.toml peer_seeds line:
//
//   - "1.2.3.4:18189"                    -> "/ip4/1.2.3.4/tcp/18189"
//   - "[::1]:18189"                      -> "/ip6/::1/tcp/18189"
//   - "xyz...56chars....onion:18141"     -> "/onion3/xyz...56chars...:18141"
//
// net.SplitHostPort does the host/port split (handling the bracketed
// IPv6 form transparently), and net.ParseIP + IP.To4() != nil is used to
// distinguish IPv4 from IPv6 reliably, rather than a naive dot-count
// heuristic. Anything that isn't a syntactically valid IPv4/IPv6 address
// or a ".onion" host returns a clear error — callers must not silently
// emit garbage into a generated TOML file for an address that fails to
// convert (see handleConfigPeerSeeds, which skips just that one address
// line instead of failing the whole response).
func addressToMultiaddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("api: address %q is not a valid host:port: %w", addr, err)
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return "", fmt.Errorf("api: address %q has an invalid port %q: %w", addr, port, err)
	}

	if strings.HasSuffix(strings.ToLower(host), onionSuffix) {
		onionHost := host[:len(host)-len(onionSuffix)]
		if onionHost == "" {
			return "", fmt.Errorf("api: address %q has an empty onion host", addr)
		}
		return fmt.Sprintf("/onion3/%s:%s", onionHost, port), nil
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return "", fmt.Errorf("api: address %q's host %q is neither a valid IP nor a .onion address", addr, host)
	}
	if ip.To4() != nil {
		return fmt.Sprintf("/ip4/%s/tcp/%s", ip.String(), port), nil
	}
	return fmt.Sprintf("/ip6/%s/tcp/%s", ip.String(), port), nil
}
