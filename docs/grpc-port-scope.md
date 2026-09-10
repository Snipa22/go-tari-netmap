# gRPC probing is scoped to owned nodes with a known gRPC address

## The finding

A Tari peer's real gRPC listen port is **never discoverable via Tari P2P peer discovery**, for
any peer -- arbitrary or our own.

- `tari_protos/network.proto`'s `Peer`/`Address` messages (vendored in
  `go-tari-grpc-lib/v3/tari_generated`) carry no gRPC-port field anywhere. `Address` is just:

  ```proto
  message Address {
    bytes address = 1;
    google.protobuf.Timestamp last_seen = 2;
    uint32 connection_attempts = 3;
    uint64 avg_latency = 4;
  }
  ```

  `address` decodes only to the peer's P2P/comms wire address (a multiaddr -- see
  `parsePeerAddress` in `internal/collector/grpc_client.go`), which is a different TCP port than
  the node's gRPC BaseNode service listens on.

- `go-tari-lib`'s P2P identity-exchange `p2p.PeerInfo` struct (`p2p/identity.go`) carries no
  gRPC-port field either -- `Addresses [][]byte` there is likewise just P2P multiaddrs, same
  encoding as above.

Put together: whether we learn about a peer via gRPC's `ListConnectedPeers` or via P2P identity
exchange, the address we get back is always the peer's P2P port (e.g. `18189`), never its gRPC
port (e.g. `18102`) -- and there is no other field, on either wire format, that carries the gRPC
port.

## Why this caused a real bug

`internal/collector/grpc_client.go`'s `grpcNodeClient.dialNode(addr)` used to dial whatever
`addr` string it was given -- which, for every seed/discovered node, is the P2P wire address.
Every such dial failed (`error reading server preface: EOF`), confirmed live via
`netmap_mainnet_collector_poll_result_total{probe_source="grpc"}` showing 100% failures across
the entire tracked node population.

## The fix: scope gRPC probing to owned nodes only

Since a node's real gRPC address can never be learned from the peer graph, gRPC probing is only
ever meaningful against an explicit, **operator-supplied** allowlist of nodes we own -- reusing
the existing "is this node ours" concept (`storage.Node.Tags["pool_owned"]`/`"owner"`, see
`isPoolOwned` in `internal/collector/collector.go` and `storage.NodeFilter.Owned`), not a second,
separate ownership concept.

`collector.NewGRPCClientWithAddressMap(addrMap map[string]string)` takes a map of P2P address ->
real gRPC address. `grpcNodeClient.dialNode`:

- If the map is `nil` (not configured at all): dials `addr` as-is, unconditionally -- the exact
  pre-existing behavior, preserved for backward compatibility with any test/caller that doesn't
  care about this feature (`NewGRPCClient()` is just `NewGRPCClientWithAddressMap(nil)`).
- If the map is non-nil and `addr` has an entry: dials the mapped gRPC address instead of `addr`.
- If the map is non-nil and `addr` has **no** entry: returns `ErrGRPCAddressUnknown` (wrapped, so
  callers can `errors.Is`) without attempting to dial the P2P address at all.

`Collector.pollOnceWithSource` specifically detects `ErrGRPCAddressUnknown` and skips recording a
health-check row (and the `OnPollResult` observer call) entirely, rather than recording a
`Reachable: false` row -- a skip is not the same signal as "we tried to reach this node over gRPC
and it failed". Recording it as a failure would keep polluting
`netmap_mainnet_collector_poll_result_total{probe_source="grpc"}` with noise for every non-owned
node, exactly the confusing signal that let the original P2P-address-dialing bug go undetected.
Only genuine dial/RPC failures against a **known** owned gRPC address count as
`probe_source="grpc"` failures going forward.

## Configuration: `NETMAP_OWNED_GRPC_ADDRESSES`

A comma-separated list of `p2pAddress=grpcAddress` pairs, mirroring the existing
`NETMAP_SEED_NODES` convention:

```
NETMAP_OWNED_GRPC_ADDRESSES=23.226.69.178:18189=23.226.69.178:18102,10.0.0.5:18189=10.0.0.5:18102
```

- Empty/unset (the default): gRPC probing is **not** scoped at all -- `grpcClient` dials whatever
  address it's given, as-is, matching the pre-existing zero-config behavior. This is the correct
  default for a deployment with no owned nodes configured yet, but it means gRPC probing will
  fail for every P2P-discovered address, per the finding above -- there is no way around that
  short of configuring this variable for at least the nodes you actually own and want gRPC health
  data for.
- Set: every P2P address NOT present as a key is treated as "no known gRPC address" -- gRPC is
  skipped for it entirely (no failure recorded), rather than dialed and failing.

See `cmd/netmap/main.go`'s wiring (`parseOwnedGRPCAddresses`) for the exact parsing rules.
