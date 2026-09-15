package geoip

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// fakeDoer is an in-memory HTTPDoer fixture: no real network access. It
// records every request body it receives (as decoded batchRequestItems)
// and returns a canned response built from lookupByIP, or an injected
// error/status code.
type fakeDoer struct {
	mu sync.Mutex

	// lookupByIP maps an IP to the (fake) ip-api.com result to return
	// for it. An IP with no entry here is reported with status "fail".
	lookupByIP map[string]batchResponseItem

	// requestBatches records the query IPs of every request this doer
	// has handled, in call order -- lets tests assert on batching
	// (chunk sizes/count) without depending on Client internals.
	requestBatches [][]string

	// err, if non-nil, is returned by Do instead of a real response
	// (simulates a transport-level failure).
	err error

	// statusCode, if non-zero and not 200, makes Do return a response
	// with that status code and an empty body (simulates an ip-api.com
	// outage/HTTP-level failure distinct from a per-IP "fail" status).
	statusCode int
}

func (f *fakeDoer) Do(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var items []batchRequestItem
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, err
	}

	ips := make([]string, len(items))
	for i, it := range items {
		ips[i] = it.Query
	}
	f.requestBatches = append(f.requestBatches, ips)

	if f.statusCode != 0 && f.statusCode != http.StatusOK {
		return &http.Response{
			StatusCode: f.statusCode,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}, nil
	}

	respItems := make([]batchResponseItem, len(items))
	for i, it := range items {
		if r, ok := f.lookupByIP[it.Query]; ok {
			r.Query = it.Query
			respItems[i] = r
		} else {
			respItems[i] = batchResponseItem{Query: it.Query, Status: "fail", Message: "no fixture"}
		}
		// Mirror ip-api.com's real batch behavior: a response item
		// only carries a "query" value at all if "query" was present
		// in that request item's "fields" string -- it is NOT echoed
		// back by default. Fixtures that always echo Query regardless
		// of the requested fields (as this one used to) would hide a
		// regression like requestFields dropping "query" (see
		// geoip.go's requestFields doc comment) instead of catching
		// it, since lookupBatch keys its result map by Query.
		if !fieldsRequested(it.Fields, "query") {
			respItems[i].Query = ""
		}
	}

	respBody, err := json.Marshal(respItems)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader(respBody)),
		Header:     make(http.Header),
	}, nil
}

// fieldsRequested reports whether field is present in a comma-separated
// "fields" request string, matching how ip-api.com itself parses that
// parameter -- used by fakeDoer to only echo back what the real API
// would for a given request, instead of unconditionally.
func fieldsRequested(fields, field string) bool {
	for _, f := range strings.Split(fields, ",") {
		if f == field {
			return true
		}
	}
	return false
}

// noWaitLimiter returns a rate.Limiter that never actually blocks Wait,
// so tests don't need to sit through ip-api.com's real 45/min cadence.
func noWaitLimiter() *rate.Limiter {
	return rate.NewLimiter(rate.Inf, MaxBatchSize*3)
}

func TestLookupSingleBatchSuccessAndFailure(t *testing.T) {
	doer := &fakeDoer{
		lookupByIP: map[string]batchResponseItem{
			"1.2.3.4": {Status: "success", Lat: 51.5, Lon: -0.12, City: "London", Country: "United Kingdom"},
		},
	}
	c := &Client{HTTPClient: doer, Limiter: noWaitLimiter()}

	results, err := c.Lookup(context.Background(), []string{"1.2.3.4", "10.0.0.1"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(doer.requestBatches) != 1 {
		t.Fatalf("expected exactly 1 batch HTTP call, got %d", len(doer.requestBatches))
	}

	got, ok := results["1.2.3.4"]
	if !ok {
		t.Fatalf("expected a result for 1.2.3.4")
	}
	if got.Failed {
		t.Errorf("1.2.3.4: Failed = true, want false")
	}
	if got.Latitude != 51.5 || got.Longitude != -0.12 || got.City != "London" || got.Country != "United Kingdom" {
		t.Errorf("1.2.3.4: unexpected result %+v", got)
	}

	// 10.0.0.1 has no fixture entry -- fakeDoer reports it as a "fail"
	// status, which Lookup must surface as Result.Failed == true, not
	// a missing key and not an error.
	got2, ok := results["10.0.0.1"]
	if !ok {
		t.Fatalf("expected a (failed) result for 10.0.0.1, got no entry at all")
	}
	if !got2.Failed {
		t.Errorf("10.0.0.1: Failed = false, want true")
	}
}

// TestLookupKeysDistinctIPsSeparately is a regression test for the bug
// where requestFields omitted "query": ip-api.com's batch endpoint only
// echoes a response item's "query" key back when it was explicitly
// requested (see requestFields's doc comment in geoip.go), so every
// batchResponseItem.Query decoded as "" and lookupBatch's
// out[item.Query] = ... keying silently merged every IP's result onto
// the single out[""] map entry -- the last IP processed in the batch
// "winning" and every other IP simply vanishing from the returned map.
//
// With requestFields correctly requesting "query" (and fakeDoer
// realistically only echoing it back when requested -- see
// fieldsRequested), this must resolve each IP to its own distinct,
// correct entry instead of collapsing them all into one.
func TestLookupKeysDistinctIPsSeparately(t *testing.T) {
	doer := &fakeDoer{
		lookupByIP: map[string]batchResponseItem{
			"1.2.3.4": {Status: "success", City: "London", Country: "United Kingdom"},
			"5.6.7.8": {Status: "success", City: "Paris", Country: "France"},
		},
	}
	c := &Client{HTTPClient: doer, Limiter: noWaitLimiter()}

	results, err := c.Lookup(context.Background(), []string{"1.2.3.4", "5.6.7.8"})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 distinct results, got %d (results collapsed by an empty Query key?): %+v", len(results), results)
	}
	got1, ok := results["1.2.3.4"]
	if !ok {
		t.Fatalf("expected a result keyed by 1.2.3.4")
	}
	if got1.City != "London" {
		t.Errorf("1.2.3.4: City = %q, want %q", got1.City, "London")
	}
	got2, ok := results["5.6.7.8"]
	if !ok {
		t.Fatalf("expected a result keyed by 5.6.7.8")
	}
	if got2.City != "Paris" {
		t.Errorf("5.6.7.8: City = %q, want %q", got2.City, "Paris")
	}
	if _, ok := results[""]; ok {
		t.Errorf("expected no result keyed by an empty Query, got one: %+v", results[""])
	}
}

func TestLookupChunksIntoMultipleBatches(t *testing.T) {
	doer := &fakeDoer{lookupByIP: map[string]batchResponseItem{}}
	c := &Client{HTTPClient: doer, Limiter: noWaitLimiter()}

	// 150 distinct IPs, one more than a single MaxBatchSize(=100) batch
	// can hold -- must be split into exactly 2 HTTP calls: 100 + 50.
	// Uniqueness doesn't matter for this test (only counts/sizes).
	ips := make([]string, 150)
	for i := range ips {
		ips[i] = ipFor(i)
	}

	if _, err := c.Lookup(context.Background(), ips); err != nil {
		t.Fatalf("Lookup: %v", err)
	}

	if len(doer.requestBatches) != 2 {
		t.Fatalf("expected exactly 2 batch HTTP calls for 150 ips, got %d", len(doer.requestBatches))
	}
	if len(doer.requestBatches[0]) != MaxBatchSize {
		t.Errorf("first batch size = %d, want %d", len(doer.requestBatches[0]), MaxBatchSize)
	}
	if len(doer.requestBatches[1]) != 50 {
		t.Errorf("second batch size = %d, want 50", len(doer.requestBatches[1]))
	}
}

// ipFor generates a syntactically-distinct-enough fake IPv4 string for
// index i, purely so TestLookupChunksIntoMultipleBatches has 150 unique
// query values -- these are never dialed for real (fakeDoer never makes
// a real network call).
func ipFor(i int) string {
	b := byte(i % 256)
	return "203.0." + string(rune('0'+i/256)) + "." + string(rune('0'+int(b)%10))
}

func TestLookupSurfacesTransportError(t *testing.T) {
	doer := &fakeDoer{err: errors.New("boom: connection refused")}
	c := &Client{HTTPClient: doer, Limiter: noWaitLimiter()}

	results, err := c.Lookup(context.Background(), []string{"1.2.3.4"})
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	if len(results) != 0 {
		t.Errorf("expected no results for a failed batch call, got %v", results)
	}
}

func TestLookupSurfacesUnexpectedStatusCode(t *testing.T) {
	doer := &fakeDoer{statusCode: http.StatusTooManyRequests}
	c := &Client{HTTPClient: doer, Limiter: noWaitLimiter()}

	_, err := c.Lookup(context.Background(), []string{"1.2.3.4"})
	if err == nil {
		t.Fatalf("expected an error for a non-200 response, got nil")
	}
}

// TestLookupRespectsRateLimiterContextCancellation exercises the actual
// rate-limiting path (unlike every other test above, which uses a
// non-blocking noWaitLimiter): a limiter with zero burst and a very
// long refill period can never grant a token before a short-deadline
// context expires, so Lookup must return the context's error rather
// than hanging or silently proceeding without waiting.
func TestLookupRespectsRateLimiterContextCancellation(t *testing.T) {
	doer := &fakeDoer{lookupByIP: map[string]batchResponseItem{}}
	// One token every hour, zero burst: the very first Wait call can
	// never succeed within a short deadline.
	c := &Client{HTTPClient: doer, Limiter: rate.NewLimiter(rate.Every(time.Hour), 0)}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := c.Lookup(ctx, []string{"1.2.3.4"})
	if err == nil {
		t.Fatalf("expected an error from a rate limiter that can never grant a token in time, got nil")
	}
	if len(doer.requestBatches) != 0 {
		t.Errorf("expected zero HTTP calls when the rate limiter never grants a token, got %d", len(doer.requestBatches))
	}
}

func TestLookupEmptyInput(t *testing.T) {
	doer := &fakeDoer{}
	c := &Client{HTTPClient: doer, Limiter: noWaitLimiter()}

	results, err := c.Lookup(context.Background(), nil)
	if err != nil {
		t.Fatalf("Lookup(nil): %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results for empty input, got %v", results)
	}
	if len(doer.requestBatches) != 0 {
		t.Errorf("expected zero HTTP calls for empty input, got %d", len(doer.requestBatches))
	}
}

// TestClientZeroValueUsable confirms a zero-value Client (no explicit
// HTTPClient/BatchURL/Limiter) doesn't panic when building its defaults
// -- httpClient()/batchURL()/rateLimiter() must all tolerate an
// unconfigured Client, even though this test doesn't actually perform a
// real network call (real network access is explicitly out of scope
// for this package's tests).
func TestClientZeroValueUsable(t *testing.T) {
	var c Client
	if c.httpClient() == nil {
		t.Errorf("httpClient() returned nil for a zero-value Client")
	}
	if c.batchURL() != DefaultBatchURL {
		t.Errorf("batchURL() = %q, want %q", c.batchURL(), DefaultBatchURL)
	}
	if c.rateLimiter() == nil {
		t.Errorf("rateLimiter() returned nil for a zero-value Client")
	}
}
