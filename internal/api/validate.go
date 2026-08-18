package api

import (
	"errors"
	"net"
	"regexp"
	"strings"
)

// onionHostPattern matches a syntactically plausible Tor onion address:
// a base32-ish label (a-z/2-7, case-insensitive) of onion-v2 length (16)
// or onion-v3 length (56), followed by the .onion suffix. This is
// deliberately not a strict/complete onion-address validator — it exists
// only to reject obvious garbage while still allowing legitimate onion
// submissions through, without this service doing any DNS/Tor
// resolution to check further.
var onionHostPattern = regexp.MustCompile(`(?i)^[a-z2-7]{16}\.onion$|^[a-z2-7]{56}\.onion$`)

// validateHostSyntax checks that host is either a syntactically valid IP
// address (any IP — private/reserved addresses are not rejected here,
// see validateSubmittedHost for that) or a plausible .onion address. It
// deliberately performs no DNS resolution — this is a syntactic check
// only.
func validateHostSyntax(host string) error {
	if net.ParseIP(host) != nil {
		return nil
	}

	if onionHostPattern.MatchString(strings.ToLower(host)) {
		return nil
	}

	return errors.New("host must be a valid IP address or .onion address")
}

// validateSubmittedHost rejects submitted hosts that are private/reserved
// IP addresses (basic SSRF hardening for POST /nodes: a submitted
// address is dialed by this service's own async health-check/probe
// machinery, so accepting e.g. 127.0.0.1 or an RFC1918 address would let
// a submitter make this service probe its own internal network) or that
// don't look like either a valid public IP or a plausible .onion
// address. It deliberately performs no DNS resolution — this is a
// syntactic check only.
func validateSubmittedHost(host string) error {
	if err := validateHostSyntax(host); err != nil {
		return err
	}

	if ip := net.ParseIP(host); ip != nil {
		if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
			ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
			return errors.New("private/reserved IP addresses are not allowed")
		}
	}

	return nil
}
