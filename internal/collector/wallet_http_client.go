package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// walletHTTPTipInfoTimeout bounds how long walletHTTPClient waits for a /get_tip_info
// response. Unlike grpc_client.go's 180s dialTimeout (sized specifically for real-world
// Tor-onion-circuit round trips, see that constant's doc comment), Tari's wallet-sync HTTP
// service is clearnet-only -- no Tor wrapping was found in the real Tari source (see this
// feature's design doc, Netmap-Wallet-HTTP-Port-Design-2026-09-19.md) -- so an ordinary,
// much shorter HTTP client timeout is appropriate here.
const walletHTTPTipInfoTimeout = 10 * time.Second

// walletHTTPMaxBodyBytes bounds how much of a /get_tip_info response body walletHTTPClient
// will ever read, defending against a misbehaving or hostile endpoint returning an
// unbounded body. 64KiB is generous for the small JSON shape this endpoint returns
// (metadata + a boolean) while still being a real, enforced cap -- see this method's own
// doc comment for the related "do not retain more than needed" minimal-retention rule.
const walletHTTPMaxBodyBytes = 64 * 1024

// errWalletHTTPUnexpectedStatus is wrapped into the error returned by walletHTTPClient.GetInfo
// when the endpoint responds with a non-200 status.
var errWalletHTTPUnexpectedStatus = errors.New("collector: wallet-http endpoint returned an unexpected HTTP status")

// walletHTTPClient is a NodeClient implementation that talks to a Tari base node's separate,
// optional wallet-sync HTTP service (minotari_node's [base_node.http_wallet_query_service],
// see this repo's Netmap-Wallet-HTTP-Port-Design-2026-09-19.md for the full feature
// background: an unauthenticated Axum HTTP/REST server distinct from the gRPC/P2P surface,
// letting third-party wallets sync against a public node).
//
// HARD SAFETY RULE, non-negotiable: GetInfo below calls ONLY GET /get_tip_info. This HTTP
// surface ALSO exposes /fetch_utxo, /get_header_by_height, /get_height_at_time,
// /get_utxos_mined_info, /get_utxos_deleted_info, /transactions, /sync_utxos_by_block,
// /get_utxos_by_block, /generate_kernel_merkle_proof, /get_mempool_fee_per_gram_stats, and
// POST /json_rpc (submit_transaction) -- several of which can return or accept real
// UTXO/transaction data. netmap has no legitimate reason to ever call any endpoint other
// than /get_tip_info, and doing so would make netmap an unintended relay for other people's
// wallet-sync traffic. Do not add calls to any other endpoint of this service anywhere in
// this package.
type walletHTTPClient struct {
	httpClient *http.Client
}

// NewWalletHTTPClient returns a NodeClient backed by real HTTP calls to a Tari base node's
// wallet-sync HTTP service -- see walletHTTPClient's doc comment for the hard, non-negotiable
// "GET /get_tip_info only" scope restriction.
func NewWalletHTTPClient() NodeClient {
	return &walletHTTPClient{httpClient: &http.Client{Timeout: walletHTTPTipInfoTimeout}}
}

// GetPeers is not supported: the wallet-sync HTTP service has no peer-discovery surface at
// all (it is a UTXO/transaction sync API, not a P2P endpoint) -- never actually called,
// since internal/collector never routes a walletHTTPClient through
// discoverWith/DiscoverOwned's GetPeers-calling paths (see Collector.WalletHTTPClient's doc
// comment: it is wired only into PollOnce/poll's GetInfo-only dispatch).
func (c *walletHTTPClient) GetPeers(ctx context.Context, addr string) ([]DiscoveredPeer, error) {
	return nil, errors.New("collector: wallet-http client does not support peer discovery")
}

// walletTipInfoResponse is the subset of minotari_node's GET /get_tip_info response shape
// this client decodes -- ONLY the two fields needed for a health check (see this package's
// hard safety-rule comment on walletHTTPClient and the design doc's "parses is_synced and
// metadata.best_block_height" scope). Any other field in the real response is simply never
// decoded here, not even transiently -- json.Decoder only ever populates fields present in
// this struct, so nothing else is ever held in memory.
type walletTipInfoResponse struct {
	Metadata *struct {
		BestBlockHeight uint64 `json:"best_block_height"`
	} `json:"metadata"`
	IsSynced bool `json:"is_synced"`
}

// GetInfo implements NodeClient by calling GET http://addr/get_tip_info ONLY -- see
// walletHTTPClient's doc comment for why no other endpoint of this HTTP surface may ever be
// called here. addr is the "host:port" to dial -- callers (see pollWalletHTTPOnce/
// walletHTTPTarget in collector.go) are responsible for resolving it to the node's own host
// paired with its configured WalletHTTPPort; this method does no address derivation of its
// own.
//
// The response body is read up to walletHTTPMaxBodyBytes and decoded directly into
// walletTipInfoResponse (which carries only is_synced/metadata.best_block_height) -- the raw
// bytes are never separately retained, logged, or cached beyond this single decode, matching
// the minimal-retention posture of the existing gRPC GetTipInfo probe (grpc_client.go's
// GetInfo) that this feature's brief requires.
func (c *walletHTTPClient) GetInfo(ctx context.Context, addr string) (NodeInfo, error) {
	url := "http://" + addr + "/get_tip_info"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("wallet-http get_tip_info %s: build request: %w", addr, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("wallet-http get_tip_info %s: %w", addr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return NodeInfo{}, fmt.Errorf("wallet-http get_tip_info %s: status %d: %w", addr, resp.StatusCode, errWalletHTTPUnexpectedStatus)
	}

	var body walletTipInfoResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, walletHTTPMaxBodyBytes)).Decode(&body); err != nil {
		return NodeInfo{}, fmt.Errorf("wallet-http get_tip_info %s: decode: %w", addr, err)
	}

	info := NodeInfo{Reachable: true}
	if body.Metadata != nil {
		height := int64(body.Metadata.BestBlockHeight)
		info.Height = &height
		// Mirrors grpc_client.go's GetInfo: this endpoint exposes no separate
		// "network tip vs our tip" split either, so ChainTipHeight is left equal to
		// Height rather than nil.
		info.ChainTipHeight = &height
	}
	// body.IsSynced is now wired through onto NodeInfo.IsSynced (see that field's doc
	// comment: only the wallet-http transport ever populates it) -- this codebase's
	// regular poll loop (pollWalletHTTPOnce) still doesn't persist it anywhere
	// (storage.HealthCheckInput has no is_synced column), but internal/api's
	// handleApproveWalletSubmission now reads it directly off this NodeInfo to build the
	// wallet-node registration flow's "permissions matrix".
	isSynced := body.IsSynced
	info.IsSynced = &isSynced

	return info, nil
}
