package main

import (
	"context"
	"fmt"
	"net"
	"strings"

	"golang.org/x/net/proxy"
)

// isOnionTarget reports whether target's host (as returned by net.SplitHostPort) ends in
// ".onion" (case-insensitive). Mirrors go-tari-lib/p2p/socks.go's own (unexported) isOnionAddr
// exactly -- that function can't be called from here since it's package-private to a different
// module, so this is a faithful, byte-for-byte-equivalent port, not a new invention.
func isOnionTarget(target string) bool {
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	return strings.HasSuffix(strings.ToLower(host), ".onion")
}

// dialTarget selects and performs the appropriate dial for target, mirroring go-tari-lib/p2p/
// socks.go's dialForProbe (also unexported/uncallable from here, see isOnionTarget's doc
// comment) so this tool honors the exact same `.onion` + SOCKS5 semantics
// ProbeOptions.SocksProxyAddr already documents elsewhere in this ecosystem:
//
//   - `.onion` target + socksProxyAddr set -> dial through a SOCKS5 proxy at socksProxyAddr
//     (golang.org/x/net/proxy.SOCKS5), honoring ctx via DialContext.
//   - `.onion` target + no proxy configured -> a clean, specific, actionable error. Never
//     attempts a raw TCP dial to a `.onion` hostname (it cannot resolve).
//   - non-`.onion` target -> always dials directly via net.Dialer.DialContext, regardless of
//     whether a SOCKS proxy is configured (the proxy is onion-specific only).
func dialTarget(ctx context.Context, target, socksProxyAddr string) (net.Conn, error) {
	if !isOnionTarget(target) {
		dialer := net.Dialer{}
		return dialer.DialContext(ctx, "tcp", target)
	}

	if socksProxyAddr == "" {
		return nil, fmt.Errorf("dialing onion target %q requires a SOCKS5 proxy (see -socks-proxy-addr); no proxy configured", target)
	}

	socksDialer, err := proxy.SOCKS5("tcp", socksProxyAddr, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("constructing SOCKS5 dialer for proxy %q: %w", socksProxyAddr, err)
	}

	if contextDialer, ok := socksDialer.(proxy.ContextDialer); ok {
		return contextDialer.DialContext(ctx, "tcp", target)
	}

	// Fallback for a golang.org/x/net version whose SOCKS5 dialer doesn't implement
	// proxy.ContextDialer: use the context-unaware Dial, but still honor ctx.Done() via a
	// wrapping goroutine/select so callers get cancellation. Mirrors dialForProbe's own
	// fallback exactly.
	type result struct {
		conn net.Conn
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		conn, err := socksDialer.Dial("tcp", target)
		resultCh <- result{conn, err}
	}()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("dialing %q via SOCKS5 proxy %q: %w", target, socksProxyAddr, ctx.Err())
	case res := <-resultCh:
		return res.conn, res.err
	}
}
