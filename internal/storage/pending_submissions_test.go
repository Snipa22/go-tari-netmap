package storage

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// strPtr is a small helper for building *string literals inline in test
// table data.
func strPtr(s string) *string { return &s }

// TestCreatePendingSubmissionInsertsNewRow verifies a first-time
// submission for an address creates a new pending row with the expected
// fields.
func TestCreatePendingSubmissionInsertsNewRow(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ps, err := store.CreatePendingSubmission(ctx, "1.2.3.4:18142", strPtr("my node"), strPtr("pool-x"))
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}
	if ps.Address != "1.2.3.4:18142" {
		t.Errorf("address = %q, want %q", ps.Address, "1.2.3.4:18142")
	}
	if ps.Status != SubmissionStatusPending {
		t.Errorf("status = %q, want %q", ps.Status, SubmissionStatusPending)
	}
	if ps.Label == nil || *ps.Label != "my node" {
		t.Errorf("label = %v, want %q", ps.Label, "my node")
	}
	if ps.OwnerTag == nil || *ps.OwnerTag != "pool-x" {
		t.Errorf("owner_tag = %v, want %q", ps.OwnerTag, "pool-x")
	}
	if ps.SubmittedAt.IsZero() {
		t.Errorf("expected submitted_at to be set")
	}
	if ps.ReviewedAt != nil {
		t.Errorf("expected reviewed_at to be nil, got %v", ps.ReviewedAt)
	}
	if ps.PromotedNodeID != nil {
		t.Errorf("expected promoted_node_id to be nil, got %v", ps.PromotedNodeID)
	}
}

// TestCreatePendingSubmissionUpdatesExistingPending verifies that
// re-submitting the same address while a pending submission for it still
// exists updates that row in place (new label/owner_tag, bumped
// submitted_at) instead of creating a second row — the partial unique
// index on (address) WHERE status = 'pending' would otherwise reject a
// second INSERT outright.
func TestCreatePendingSubmissionUpdatesExistingPending(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	first, err := store.CreatePendingSubmission(ctx, "1.2.3.4:18142", strPtr("first label"), nil)
	if err != nil {
		t.Fatalf("create first submission: %v", err)
	}
	firstSubmittedAt := first.SubmittedAt

	time.Sleep(10 * time.Millisecond)
	second, err := store.CreatePendingSubmission(ctx, "1.2.3.4:18142", strPtr("second label"), strPtr("new-owner"))
	if err != nil {
		t.Fatalf("create second submission (should update in place): %v", err)
	}

	if second.ID != first.ID {
		t.Errorf("second.ID = %v, want same as first.ID = %v (update in place)", second.ID, first.ID)
	}
	if second.Label == nil || *second.Label != "second label" {
		t.Errorf("label = %v, want %q", second.Label, "second label")
	}
	if second.OwnerTag == nil || *second.OwnerTag != "new-owner" {
		t.Errorf("owner_tag = %v, want %q", second.OwnerTag, "new-owner")
	}
	if !second.SubmittedAt.After(firstSubmittedAt) {
		t.Errorf("expected submitted_at to be bumped: got %v, want after %v", second.SubmittedAt, firstSubmittedAt)
	}

	// Exactly one row must exist for this address.
	all, err := store.ListPendingSubmissions(ctx, "pending")
	if err != nil {
		t.Fatalf("list pending submissions: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("len(all) = %d, want 1 (no duplicate rows)", len(all))
	}
}

// TestListPendingSubmissionsByStatus verifies ListPendingSubmissions
// filters by exact status, and defaults to "pending" when status is "".
func TestListPendingSubmissionsByStatus(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	pending, err := store.CreatePendingSubmission(ctx, "pending:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending: %v", err)
	}
	toApprove, err := store.CreatePendingSubmission(ctx, "approved:1", nil, nil)
	if err != nil {
		t.Fatalf("create to-approve: %v", err)
	}
	toReject, err := store.CreatePendingSubmission(ctx, "rejected:1", nil, nil)
	if err != nil {
		t.Fatalf("create to-reject: %v", err)
	}

	node, err := store.UpsertDiscoveredNode(ctx, "approved:1", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert node for approval: %v", err)
	}
	if err := store.ApprovePendingSubmission(ctx, toApprove.ID, node.ID); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if err := store.RejectPendingSubmission(ctx, toReject.ID, strPtr("spam")); err != nil {
		t.Fatalf("reject: %v", err)
	}

	// Default ("" -> "pending") should return only the still-pending one.
	defaulted, err := store.ListPendingSubmissions(ctx, "")
	if err != nil {
		t.Fatalf("list default: %v", err)
	}
	if len(defaulted) != 1 || defaulted[0].ID != pending.ID {
		t.Fatalf("defaulted = %+v, want just %v", defaulted, pending.ID)
	}

	approved, err := store.ListPendingSubmissions(ctx, "approved")
	if err != nil {
		t.Fatalf("list approved: %v", err)
	}
	if len(approved) != 1 || approved[0].ID != toApprove.ID {
		t.Fatalf("approved = %+v, want just %v", approved, toApprove.ID)
	}

	rejected, err := store.ListPendingSubmissions(ctx, "rejected")
	if err != nil {
		t.Fatalf("list rejected: %v", err)
	}
	if len(rejected) != 1 || rejected[0].ID != toReject.ID {
		t.Fatalf("rejected = %+v, want just %v", rejected, toReject.ID)
	}
}

// TestGetPendingSubmissionNotFound verifies GetPendingSubmission returns
// ErrNotFound for an unknown id.
func TestGetPendingSubmissionNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	_, err := store.GetPendingSubmission(ctx, uuid.New())
	if err != ErrNotFound {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// TestIsAddressPubliclyOptedIn verifies the true/false cases: registry_
// submitted and both count as opted-in, p2p_discovered does not, and an
// address with no node at all does not either.
func TestIsAddressPubliclyOptedIn(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.UpsertDiscoveredNode(ctx, "registry:1", DiscoverySourceRegistry, nil, nil); err != nil {
		t.Fatalf("upsert registry node: %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "p2p:1", DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert p2p node: %v", err)
	}
	// "both" via re-upsert with the other source.
	if _, err := store.UpsertDiscoveredNode(ctx, "both:1", DiscoverySourceP2P, nil, nil); err != nil {
		t.Fatalf("upsert both (1st): %v", err)
	}
	if _, err := store.UpsertDiscoveredNode(ctx, "both:1", DiscoverySourceRegistry, nil, nil); err != nil {
		t.Fatalf("upsert both (2nd): %v", err)
	}

	cases := []struct {
		address string
		want    bool
	}{
		{"registry:1", true},
		{"both:1", true},
		{"p2p:1", false},
		{"unknown:1", false},
	}
	for _, tc := range cases {
		got, err := store.IsAddressPubliclyOptedIn(ctx, tc.address)
		if err != nil {
			t.Fatalf("is address publicly opted in (%s): %v", tc.address, err)
		}
		if got != tc.want {
			t.Errorf("IsAddressPubliclyOptedIn(%q) = %v, want %v", tc.address, got, tc.want)
		}
	}
}

// TestApprovePendingSubmission verifies status flips to approved,
// promoted_node_id is set, reviewed_at is set, and re-approving an
// already-decided submission errors.
func TestApprovePendingSubmission(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ps, err := store.CreatePendingSubmission(ctx, "a:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}
	node, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}

	if err := store.ApprovePendingSubmission(ctx, ps.ID, node.ID); err != nil {
		t.Fatalf("approve: %v", err)
	}

	got, err := store.GetPendingSubmission(ctx, ps.ID)
	if err != nil {
		t.Fatalf("get pending submission: %v", err)
	}
	if got.Status != SubmissionStatusApproved {
		t.Errorf("status = %q, want %q", got.Status, SubmissionStatusApproved)
	}
	if got.PromotedNodeID == nil || *got.PromotedNodeID != node.ID {
		t.Errorf("promoted_node_id = %v, want %v", got.PromotedNodeID, node.ID)
	}
	if got.ReviewedAt == nil {
		t.Errorf("expected reviewed_at to be set")
	}

	// Re-approving must error: it's already decided.
	if err := store.ApprovePendingSubmission(ctx, ps.ID, node.ID); err == nil {
		t.Error("expected error re-approving an already-approved submission, got nil")
	}
}

// TestRejectPendingSubmission verifies status flips to rejected, reason
// is set, and re-rejecting an already-decided submission errors.
func TestRejectPendingSubmission(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	ps, err := store.CreatePendingSubmission(ctx, "a:1", nil, nil)
	if err != nil {
		t.Fatalf("create pending submission: %v", err)
	}

	reason := "does not look legitimate"
	if err := store.RejectPendingSubmission(ctx, ps.ID, &reason); err != nil {
		t.Fatalf("reject: %v", err)
	}

	got, err := store.GetPendingSubmission(ctx, ps.ID)
	if err != nil {
		t.Fatalf("get pending submission: %v", err)
	}
	if got.Status != SubmissionStatusRejected {
		t.Errorf("status = %q, want %q", got.Status, SubmissionStatusRejected)
	}
	if got.RejectionReason == nil || *got.RejectionReason != reason {
		t.Errorf("rejection_reason = %v, want %q", got.RejectionReason, reason)
	}
	if got.ReviewedAt == nil {
		t.Errorf("expected reviewed_at to be set")
	}

	// Re-rejecting must error: it's already decided.
	if err := store.RejectPendingSubmission(ctx, ps.ID, nil); err == nil {
		t.Error("expected error re-rejecting an already-rejected submission, got nil")
	}

	// Approving an already-rejected submission must also error (the
	// same "must currently be pending" guard applies both ways).
	node, err := store.UpsertDiscoveredNode(ctx, "a:1", DiscoverySourceRegistry, nil, nil)
	if err != nil {
		t.Fatalf("upsert node: %v", err)
	}
	if err := store.ApprovePendingSubmission(ctx, ps.ID, node.ID); err == nil {
		t.Error("expected error approving an already-rejected submission, got nil")
	}
}
