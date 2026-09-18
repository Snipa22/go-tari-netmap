package remotestore

import "fmt"

// errRemoteStore is a small helper for building a "remotestore: ..." prefixed error, mirroring
// the storage package's own fmt.Errorf("storage: ...", ...) convention throughout this repo.
func errRemoteStore(format string, args ...any) error {
	return fmt.Errorf("remotestore: "+format, args...)
}

// errNotSupported is returned by every storage.Store method this package implements only to
// satisfy the interface, never because any real code path in cmd/netmap-p2p-responder or
// internal/collector's Poll/Discover functions actually calls it -- see this package's doc
// comment and unsupported.go for the exact list. These are central-API-only operations (the
// submission review queue, the full topology graph, network-height/top-peered aggregates,
// etc.) that make no sense for a single remote collector satellite to serve out of its own
// small local cache.
var errNotSupported = errRemoteStore("not supported by the remote-collector store (this is a central-API-only operation)")

// errClosed is returned by every write method once Close has been called.
var errClosed = errRemoteStore("store is closed")
