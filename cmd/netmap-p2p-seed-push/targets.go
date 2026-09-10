package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

// ParseTargets parses the -targets flag value into an ordered, deduplicated list of "host:port"
// dial targets. raw is either:
//
//   - a path to an existing file, one target per line (blank lines and lines starting with '#'
//     ignored; a '#' partway through a line starts a trailing comment, everything after it on
//     that line is ignored; multiple targets MAY also be comma-separated on a single line), or
//   - an inline comma-separated list of targets (mirroring cmd/netmap/main.go's parseSeedNodes,
//     the existing convention in this repo for a comma-separated env/flag value), if raw is not
//     an existing, readable file.
//
// Every non-empty candidate is validated as a syntactically-valid "host:port" pair (via
// net.SplitHostPort) -- this includes `.onion:port` addresses (net.SplitHostPort is a pure
// string-splitting operation, not a resolver, so it accepts a `.onion` host exactly like any
// other). A malformed entry is a hard error naming the offending entry, rather than being
// silently skipped: this tool is meant to be run against a hand-curated or extracted list, and a
// silently-dropped entry could easily go unnoticed.
//
// An empty (or entirely-comments/blank) result is also a hard error -- there is nothing for this
// tool to do without at least one target.
func ParseTargets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("-targets is empty: provide at least one host:port target (inline comma-separated, or a path to a file with one per line)")
	}

	var candidates []string
	if info, err := os.Stat(raw); err == nil && !info.IsDir() {
		content, err := os.ReadFile(raw)
		if err != nil {
			return nil, fmt.Errorf("reading -targets file %s: %w", raw, err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			if idx := strings.IndexByte(line, '#'); idx >= 0 {
				line = line[:idx]
			}
			for _, part := range strings.Split(line, ",") {
				part = strings.TrimSpace(part)
				if part != "" {
					candidates = append(candidates, part)
				}
			}
		}
	} else {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				candidates = append(candidates, part)
			}
		}
	}

	seen := make(map[string]bool, len(candidates))
	var targets []string
	for _, c := range candidates {
		if _, _, err := net.SplitHostPort(c); err != nil {
			return nil, fmt.Errorf("-targets entry %q is not a valid host:port: %w", c, err)
		}
		if seen[c] {
			continue
		}
		seen[c] = true
		targets = append(targets, c)
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("-targets %q produced zero targets after parsing (all blank/comment lines?)", raw)
	}
	return targets, nil
}
