// notification_handler_test.go — COMPONENT tests for the Notification gRPC handler.
//
// ============================================================================
// WHAT "COMPONENT TEST" MEANS HERE (and why this is the right level)
// ============================================================================
//
// These tests exercise the handler END-TO-END through a REAL gRPC stack
// (bufconn: an in-process HTTP/2 transport — see pkg/testutil/grpc.go) but with
// the DOMAIN SERVICE replaced by a hand-written mock. That isolation is
// deliberate:
//
//   - We mock the NotificationService INTERFACE, not the repositories. The unit
//     under test is the handler: proto<->domain conversion, validation, identity
//     resolution from claims, error mapping, and the never-leak-secrets invariant.
//     Mocking the service (one interface) instead of the repos/notifier keeps the
//     test about the handler, not the domain's internal wiring. The domain's own
//     logic has its own -race unit tests.
//
//   - Using bufconn instead of calling handler methods directly means the proto
//     actually serializes over the wire: a field we forget to map shows up as a
//     zero value on the client side, and a status error round-trips through gRPC's
//     status machinery exactly as a real client would see it.
//
//   - We drive the REAL grpcutil.AuthUnaryInterceptor with a stub validator so the
//     handler reads claims via grpcutil's own private context key — the SAME path
//     production uses. We are not faking the context plumbing; an RPC that forgets
//     to resolve the caller would be a real, observable bug here.
//
// NO testcontainers, NO database, NO network sockets — this whole file runs in
// milliseconds and is safe to run on every change.
//
// ============================================================================
// THE FOUR THINGS EVERY RPC TEST ASSERTS (the task's contract)
// ============================================================================
//
//	(a) HAPPY PATH: proto<->domain conversion is correct (the mock records what
//	    domain input it received; we assert the response fields + the captured
//	    server-authoritative recipient id came from CLAIMS, not the request).
//	(b) VALIDATION: bad requests are rejected with codes.InvalidArgument BEFORE the
//	    domain is ever called (we assert the mock was NOT invoked).
//	(c) ERROR MAPPING: each domain sentinel maps to the right gRPC status code.
//	(d) NO LEAK: error messages never contain internal/SQL/secret/target text;
//	    a webhook target never appears on a Notification/DeliveryAttempt response.
package handler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/notification/internal/domain"
)

// ----------------------------------------------------------------------------
// Constants used across tests.
// ----------------------------------------------------------------------------

// testUserID is the user id the stub validator injects as the authenticated
// caller. Every RPC must scope to THIS id (resolved from claims), never to any
// value in the request body — that is the anti-IDOR property we assert.
const testUserID = "user-ada-42"

// ----------------------------------------------------------------------------
// Tiny helpers (kept local so the test file is self-contained).
// ----------------------------------------------------------------------------

// wrap simulates the domain wrapping a sentinel with a specific message
// (fmt.Errorf("...: %w", sentinel)) so we can verify errors.Is-based mapping
// survives wrapping — which is how the real domain raises ErrValidation etc.
func wrap(sentinel error, msg string) error { return fmt.Errorf("%s: %w", msg, sentinel) }

// stubValidator implements grpcutil.TokenValidator by returning fixed claims. We
// drive the REAL grpcutil.AuthUnaryInterceptor with it so the test exercises the
// genuine claims-injection path (the handler reads claims via grpcutil's own
// private context key, which only that interceptor can populate).
type stubValidator struct {
	claims *grpcutil.Claims
	err    error
}

func (s stubValidator) Validate(_ context.Context, _ string) (*grpcutil.Claims, error) {
	return s.claims, s.err
}

// ============================================================================
// MOCK domain.NotificationService — a hand-written test double
// ============================================================================
//
// WHY hand-written and not gomock/mockery: a hand-written mock with function
// fields is dependency-free, reads top-to-bottom, and makes each test set ONLY the
// behavior it needs. A nil field means "this test does not expect this method to
// be called"; calling it would nil-panic — which the recovery interceptor turns
// into codes.Internal, a signal the test then flags. To make "not called"
// assertions precise we also track per-method call counts and capture the inputs
// the handler forwarded (so happy-path conversion can be asserted exactly).
type mockService struct {
	reactFn       func(ctx context.Context, e domain.InboundEvent, p domain.NotificationPreferences) (domain.RoutingDecision, error)
	listFn        func(ctx context.Context, in domain.ListNotificationsInput) (domain.ListNotificationsOutput, error)
	getFn         func(ctx context.Context, recipientUserID, id string) (domain.GetNotificationOutput, error)
	markReadFn    func(ctx context.Context, in domain.MarkReadInput) (domain.MarkReadOutput, error)
	listDelivFn   func(ctx context.Context, in domain.ListDeliveryAttemptsInput) (domain.ListDeliveryAttemptsOutput, error)
	getPrefsFn    func(ctx context.Context, userID string) (domain.NotificationPreferences, error)
	updatePrefsFn func(ctx context.Context, in domain.UpdatePreferencesInput) (domain.NotificationPreferences, error)
	testChannelFn func(ctx context.Context, in domain.TestChannelInput) (domain.TestChannelOutput, error)

	// Call recorders — let validation tests assert the domain was NOT reached.
	listCalls        int
	getCalls         int
	markReadCalls    int
	listDelivCalls   int
	getPrefsCalls    int
	updatePrefsCalls int
	testChannelCalls int

	// Captured inputs for happy-path conversion + authority assertions.
	lastList        domain.ListNotificationsInput
	lastGetUserID   string
	lastGetID       string
	lastMarkRead    domain.MarkReadInput
	lastListDeliv   domain.ListDeliveryAttemptsInput
	lastGetPrefsUID string
	lastUpdatePrefs domain.UpdatePreferencesInput
	lastTestChannel domain.TestChannelInput
}

// The mock must satisfy domain.NotificationService.

func (m *mockService) ReactToEvent(ctx context.Context, e domain.InboundEvent, p domain.NotificationPreferences) (domain.RoutingDecision, error) {
	return m.reactFn(ctx, e, p)
}

func (m *mockService) ListNotifications(ctx context.Context, in domain.ListNotificationsInput) (domain.ListNotificationsOutput, error) {
	m.listCalls++
	m.lastList = in
	return m.listFn(ctx, in)
}

func (m *mockService) GetNotification(ctx context.Context, recipientUserID, id string) (domain.GetNotificationOutput, error) {
	m.getCalls++
	m.lastGetUserID = recipientUserID
	m.lastGetID = id
	return m.getFn(ctx, recipientUserID, id)
}

func (m *mockService) MarkRead(ctx context.Context, in domain.MarkReadInput) (domain.MarkReadOutput, error) {
	m.markReadCalls++
	m.lastMarkRead = in
	return m.markReadFn(ctx, in)
}

func (m *mockService) ListDeliveryAttempts(ctx context.Context, in domain.ListDeliveryAttemptsInput) (domain.ListDeliveryAttemptsOutput, error) {
	m.listDelivCalls++
	m.lastListDeliv = in
	return m.listDelivFn(ctx, in)
}

func (m *mockService) GetPreferences(ctx context.Context, userID string) (domain.NotificationPreferences, error) {
	m.getPrefsCalls++
	m.lastGetPrefsUID = userID
	return m.getPrefsFn(ctx, userID)
}

func (m *mockService) UpdatePreferences(ctx context.Context, in domain.UpdatePreferencesInput) (domain.NotificationPreferences, error) {
	m.updatePrefsCalls++
	m.lastUpdatePrefs = in
	return m.updatePrefsFn(ctx, in)
}

func (m *mockService) TestChannel(ctx context.Context, in domain.TestChannelInput) (domain.TestChannelOutput, error) {
	m.testChannelCalls++
	m.lastTestChannel = in
	return m.testChannelFn(ctx, in)
}

// Compile-time assertion that the mock fully satisfies the domain interface — if
// the interface gains a method, this fails fast instead of producing a confusing
// "does not implement" error deep in a test helper.
var _ domain.NotificationService = (*mockService)(nil)

// ============================================================================
// TEST HARNESS
// ============================================================================

// newClient wires the handler (backed by the given mock) onto a bufconn gRPC
// server WITH the real auth interceptor injecting testUserID claims, and returns a
// ready client. Because every RPC on this surface resolves the caller from claims,
// the interceptor is installed for all tests by default.
func newClient(t *testing.T, svc domain.NotificationService) notificationv1.NotificationServiceClient {
	t.Helper()
	return newClientWithClaims(t, svc, &grpcutil.Claims{UserID: testUserID, Email: "ada@forgepoint.dev"})
}

// newClientWithClaims is newClient but lets a test control (or omit) the injected
// claims — used by the "missing authentication" test (claims=nil).
func newClientWithClaims(t *testing.T, svc domain.NotificationService, claims *grpcutil.Claims) notificationv1.NotificationServiceClient {
	t.Helper()
	h := NewNotificationHandler(svc)
	opt := grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(stubValidator{claims: claims}))
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		notificationv1.RegisterNotificationServiceServer(s, h)
	}, opt)
	return notificationv1.NewNotificationServiceClient(conn)
}

// newClientNoAuth wires the handler with NO auth interceptor at all, so claims are
// absent from the context. Used to prove the handler FAILS CLOSED (Unauthenticated)
// rather than defaulting to some user when the interceptor is bypassed.
func newClientNoAuth(t *testing.T, svc domain.NotificationService) notificationv1.NotificationServiceClient {
	t.Helper()
	h := NewNotificationHandler(svc)
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		notificationv1.RegisterNotificationServiceServer(s, h)
	})
	return notificationv1.NewNotificationServiceClient(conn)
}

// authCtx attaches a Bearer header so the server-side AuthUnaryInterceptor extracts
// it and runs the (stub) validator. The token value is irrelevant — the stub
// ignores it — but it must be present, since the interceptor rejects a missing
// header before the validator runs.
func authCtx() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer test-token")
}

// requireCode asserts that err carries the expected gRPC status code.
func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected a gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != want {
		t.Fatalf("expected code %s, got %s (message: %q)", want, st.Code(), st.Message())
	}
}

// assertNoLeak fails if the message contains any substring that would indicate an
// internal/SQL/secret/target leak. This is the (d) guarantee, checked uniformly.
func assertNoLeak(t *testing.T, msg string) {
	t.Helper()
	lower := strings.ToLower(msg)
	for _, banned := range []string{
		"notification: ", // raw domain sentinel prefix (un-sanitized error text)
		"repository:",    // storage-layer sentinel text
		"sql",            // SQL fragments
		"pgx", "pq:",     // driver internals
		"hooks.slack.com/services", // a real slack webhook secret path
		"https://10.",              // a private-range webhook target
		"goroutine",                // stack-trace leak
		"connection string",
		"panic",
	} {
		if strings.Contains(lower, banned) {
			t.Fatalf("error message leaks internal detail (%q): %q", banned, msg)
		}
	}
}

// ============================================================================
// ListNotifications
// ============================================================================

func TestListNotifications_HappyPath_AuthorityFromClaims(t *testing.T) {
	created := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	mock := &mockService{
		listFn: func(_ context.Context, in domain.ListNotificationsInput) (domain.ListNotificationsOutput, error) {
			return domain.ListNotificationsOutput{
				Notifications: []domain.Notification{{
					ID:              "n-1",
					RecipientUserID: in.RecipientUserID,
					Title:           "Pipeline failed",
					Body:            "step train failed",
					Severity:        domain.SeverityError,
					Channels:        []domain.NotificationChannel{domain.ChannelInApp, domain.ChannelSlack},
					Read:            false,
					EventID:         "evt-9",
					EventType:       "fp.pipelines.failed",
					SourceService:   "pipeline",
					CreatedAt:       created,
				}},
				NextPageToken: "next-cursor",
				UnreadCount:   7,
			}, nil
		},
	}
	client := newClient(t, mock)

	resp, err := client.ListNotifications(authCtx(), &notificationv1.ListNotificationsRequest{
		UnreadOnly:      true,
		MinSeverity:     notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_WARNING,
		EventTypeFilter: "fp.pipelines.failed",
		Pagination:      &commonv1.PaginationRequest{PageSize: 50, PageToken: "cur"},
	})
	if err != nil {
		t.Fatalf("ListNotifications returned error: %v", err)
	}

	// (a) The recipient MUST be the claims user, never anything from the request.
	if mock.lastList.RecipientUserID != testUserID {
		t.Fatalf("recipient = %q, want claims user %q", mock.lastList.RecipientUserID, testUserID)
	}
	// Filters forwarded faithfully.
	if !mock.lastList.UnreadOnly || mock.lastList.MinSeverity != domain.SeverityWarning ||
		mock.lastList.EventTypeFilter != "fp.pipelines.failed" ||
		mock.lastList.PageSize != 50 || mock.lastList.PageToken != "cur" {
		t.Fatalf("filters not forwarded correctly: %+v", mock.lastList)
	}
	// Response conversion: enum, channels, timestamp, badge, cursor.
	if len(resp.GetNotifications()) != 1 {
		t.Fatalf("got %d notifications, want 1", len(resp.GetNotifications()))
	}
	n := resp.GetNotifications()[0]
	if n.GetId() != "n-1" || n.GetTitle() != "Pipeline failed" ||
		n.GetSeverity() != notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_ERROR ||
		n.GetEventType() != "fp.pipelines.failed" || n.GetSourceService() != "pipeline" {
		t.Fatalf("notification not converted correctly: %+v", n)
	}
	if len(n.GetChannels()) != 2 ||
		n.GetChannels()[0] != notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_IN_APP ||
		n.GetChannels()[1] != notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_SLACK {
		t.Fatalf("channels not converted correctly: %v", n.GetChannels())
	}
	if n.GetCreatedAt() == nil || !n.GetCreatedAt().AsTime().Equal(created) {
		t.Fatalf("created_at not converted: %v", n.GetCreatedAt())
	}
	// Unread notification → read_at must be UNSET (not zero-epoch).
	if n.GetReadAt() != nil {
		t.Fatalf("read_at should be nil for an unread notification, got %v", n.GetReadAt())
	}
	if resp.GetUnreadCount() != 7 || resp.GetPagination().GetNextPageToken() != "next-cursor" {
		t.Fatalf("badge/cursor not converted: count=%d token=%q", resp.GetUnreadCount(), resp.GetPagination().GetNextPageToken())
	}
}

func TestListNotifications_NegativePageSize_InvalidArgument(t *testing.T) {
	mock := &mockService{
		listFn: func(context.Context, domain.ListNotificationsInput) (domain.ListNotificationsOutput, error) {
			t.Fatal("domain must NOT be called for a negative page size")
			return domain.ListNotificationsOutput{}, nil
		},
	}
	client := newClient(t, mock)
	_, err := client.ListNotifications(authCtx(), &notificationv1.ListNotificationsRequest{
		Pagination: &commonv1.PaginationRequest{PageSize: -1},
	})
	requireCode(t, err, codes.InvalidArgument)
	if mock.listCalls != 0 {
		t.Fatalf("domain List called %d times; want 0", mock.listCalls)
	}
}

func TestListNotifications_BadSeverityEnum_InvalidArgument(t *testing.T) {
	mock := &mockService{
		listFn: func(context.Context, domain.ListNotificationsInput) (domain.ListNotificationsOutput, error) {
			t.Fatal("domain must NOT be called for an out-of-range severity")
			return domain.ListNotificationsOutput{}, nil
		},
	}
	client := newClient(t, mock)
	_, err := client.ListNotifications(authCtx(), &notificationv1.ListNotificationsRequest{
		MinSeverity: notificationv1.NotificationSeverity(99), // outside the closed enum
	})
	requireCode(t, err, codes.InvalidArgument)
	if mock.listCalls != 0 {
		t.Fatalf("domain List called %d times; want 0", mock.listCalls)
	}
}

func TestListNotifications_MissingAuth_Unauthenticated(t *testing.T) {
	mock := &mockService{
		listFn: func(context.Context, domain.ListNotificationsInput) (domain.ListNotificationsOutput, error) {
			t.Fatal("domain must NOT be called when the caller is unauthenticated")
			return domain.ListNotificationsOutput{}, nil
		},
	}
	// No auth interceptor installed → claims absent → handler must fail closed.
	client := newClientNoAuth(t, mock)
	_, err := client.ListNotifications(context.Background(), &notificationv1.ListNotificationsRequest{})
	requireCode(t, err, codes.Unauthenticated)
	if mock.listCalls != 0 {
		t.Fatalf("domain List called %d times; want 0", mock.listCalls)
	}
}

func TestListNotifications_DomainError_SanitizedInternal(t *testing.T) {
	mock := &mockService{
		listFn: func(context.Context, domain.ListNotificationsInput) (domain.ListNotificationsOutput, error) {
			// An unrecognized error wrapping storage text — must be sanitized.
			return domain.ListNotificationsOutput{}, errors.New("repository: pq: connection refused")
		},
	}
	client := newClient(t, mock)
	_, err := client.ListNotifications(authCtx(), &notificationv1.ListNotificationsRequest{})
	requireCode(t, err, codes.Internal)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

// ============================================================================
// GetNotification
// ============================================================================

func TestGetNotification_HappyPath_NoTargetLeak(t *testing.T) {
	attemptedAt := time.Date(2026, 6, 17, 11, 0, 0, 0, time.UTC)
	mock := &mockService{
		getFn: func(_ context.Context, recipientUserID, id string) (domain.GetNotificationOutput, error) {
			return domain.GetNotificationOutput{
				Notification: domain.Notification{
					ID:              id,
					RecipientUserID: recipientUserID,
					Title:           "Drift detected",
					Severity:        domain.SeverityCritical,
					Channels:        []domain.NotificationChannel{domain.ChannelInApp, domain.ChannelWebhook},
				},
				DeliveryAttempts: []domain.DeliveryAttempt{{
					NotificationID: id,
					Channel:        domain.ChannelWebhook,
					Status:         domain.StatusFailed,
					Attempt:        3,
					ResponseCode:   503,
					// ErrorMessage is operator-facing delivery detail (allowed), but it
					// must NOT carry the target URL/secret. We set a clean message.
					ErrorMessage: "circuit breaker open",
					AttemptedAt:  attemptedAt,
				}},
			}, nil
		},
	}
	client := newClient(t, mock)

	resp, err := client.GetNotification(authCtx(), &notificationv1.GetNotificationRequest{Id: "n-77"})
	if err != nil {
		t.Fatalf("GetNotification returned error: %v", err)
	}
	// Authority + id forwarding.
	if mock.lastGetUserID != testUserID || mock.lastGetID != "n-77" {
		t.Fatalf("forwarded (uid=%q,id=%q); want (%q,n-77)", mock.lastGetUserID, mock.lastGetID, testUserID)
	}
	if resp.GetNotification().GetSeverity() != notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_CRITICAL {
		t.Fatalf("severity not converted: %v", resp.GetNotification().GetSeverity())
	}
	// Delivery attempt conversion + the structural no-leak guarantee: the proto
	// DeliveryAttempt type simply has no target field, so we assert the carried
	// fields and trust the type to make a target impossible to echo.
	if len(resp.GetDeliveryAttempts()) != 1 {
		t.Fatalf("got %d attempts, want 1", len(resp.GetDeliveryAttempts()))
	}
	a := resp.GetDeliveryAttempts()[0]
	if a.GetChannel() != notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_WEBHOOK ||
		a.GetStatus() != notificationv1.DeliveryStatus_DELIVERY_STATUS_FAILED ||
		a.GetAttempt() != 3 || a.GetResponseCode() != 503 ||
		a.GetErrorMessage() != "circuit breaker open" {
		t.Fatalf("attempt not converted correctly: %+v", a)
	}
	if a.GetAttemptedAt() == nil || !a.GetAttemptedAt().AsTime().Equal(attemptedAt) {
		t.Fatalf("attempted_at not converted: %v", a.GetAttemptedAt())
	}
}

func TestGetNotification_EmptyID_InvalidArgument(t *testing.T) {
	mock := &mockService{
		getFn: func(context.Context, string, string) (domain.GetNotificationOutput, error) {
			t.Fatal("domain must NOT be called for an empty id")
			return domain.GetNotificationOutput{}, nil
		},
	}
	client := newClient(t, mock)
	_, err := client.GetNotification(authCtx(), &notificationv1.GetNotificationRequest{Id: ""})
	requireCode(t, err, codes.InvalidArgument)
	if mock.getCalls != 0 {
		t.Fatalf("domain Get called %d times; want 0", mock.getCalls)
	}
}

func TestGetNotification_NotFound_MapsNotFound_NoExistenceLeak(t *testing.T) {
	mock := &mockService{
		getFn: func(context.Context, string, string) (domain.GetNotificationOutput, error) {
			// The domain returns the SAME ErrNotFound for "missing" and "foreign".
			return domain.GetNotificationOutput{}, domain.ErrNotFound
		},
	}
	client := newClient(t, mock)
	_, err := client.GetNotification(authCtx(), &notificationv1.GetNotificationRequest{Id: "someone-elses-id"})
	requireCode(t, err, codes.NotFound)
	st, _ := status.FromError(err)
	// The message must be generic — it must not reveal whether the id exists.
	if strings.Contains(strings.ToLower(st.Message()), "foreign") ||
		strings.Contains(strings.ToLower(st.Message()), "another user") {
		t.Fatalf("NotFound message leaks existence/ownership: %q", st.Message())
	}
	assertNoLeak(t, st.Message())
}

// ============================================================================
// MarkRead
// ============================================================================

func TestMarkRead_HappyPath(t *testing.T) {
	mock := &mockService{
		markReadFn: func(_ context.Context, in domain.MarkReadInput) (domain.MarkReadOutput, error) {
			return domain.MarkReadOutput{MarkedCount: 2, UnreadCount: 5}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.MarkRead(authCtx(), &notificationv1.MarkReadRequest{Ids: []string{"a", "b"}})
	if err != nil {
		t.Fatalf("MarkRead returned error: %v", err)
	}
	if mock.lastMarkRead.RecipientUserID != testUserID {
		t.Fatalf("recipient = %q, want %q", mock.lastMarkRead.RecipientUserID, testUserID)
	}
	if len(mock.lastMarkRead.Ids) != 2 || mock.lastMarkRead.MarkAll {
		t.Fatalf("ids/mark_all not forwarded: %+v", mock.lastMarkRead)
	}
	if resp.GetMarkedCount() != 2 || resp.GetUnreadCount() != 5 {
		t.Fatalf("counts not converted: marked=%d unread=%d", resp.GetMarkedCount(), resp.GetUnreadCount())
	}
}

func TestMarkRead_MarkAll_IgnoresIds(t *testing.T) {
	mock := &mockService{
		markReadFn: func(_ context.Context, in domain.MarkReadInput) (domain.MarkReadOutput, error) {
			if !in.MarkAll {
				t.Errorf("expected MarkAll=true")
			}
			return domain.MarkReadOutput{MarkedCount: 10, UnreadCount: 0}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.MarkRead(authCtx(), &notificationv1.MarkReadRequest{MarkAll: true})
	if err != nil {
		t.Fatalf("MarkRead returned error: %v", err)
	}
	if resp.GetMarkedCount() != 10 || resp.GetUnreadCount() != 0 {
		t.Fatalf("counts not converted: %+v", resp)
	}
}

func TestMarkRead_Validation(t *testing.T) {
	// Build an oversized id batch (cap + 1) to trip the boundary check.
	tooMany := make([]string, maxMarkReadBatch+1)
	for i := range tooMany {
		tooMany[i] = "id"
	}
	cases := []struct {
		name string
		req  *notificationv1.MarkReadRequest
	}{
		{"neither ids nor mark_all", &notificationv1.MarkReadRequest{}},
		{"over batch cap", &notificationv1.MarkReadRequest{Ids: tooMany}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				markReadFn: func(context.Context, domain.MarkReadInput) (domain.MarkReadOutput, error) {
					t.Fatal("domain must NOT be called for invalid MarkRead input")
					return domain.MarkReadOutput{}, nil
				},
			}
			client := newClient(t, mock)
			_, err := client.MarkRead(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.markReadCalls != 0 {
				t.Fatalf("domain MarkRead called %d times; want 0", mock.markReadCalls)
			}
		})
	}
}

// ============================================================================
// ListDeliveryAttempts
// ============================================================================

func TestListDeliveryAttempts_HappyPath_ParallelIDs(t *testing.T) {
	mock := &mockService{
		listDelivFn: func(_ context.Context, in domain.ListDeliveryAttemptsInput) (domain.ListDeliveryAttemptsOutput, error) {
			return domain.ListDeliveryAttemptsOutput{
				Attempts: []domain.DeliveryAttempt{
					{NotificationID: "n-1", Channel: domain.ChannelWebhook, Status: domain.StatusDelivered, Attempt: 1, ResponseCode: 200},
					{NotificationID: "n-2", Channel: domain.ChannelSlack, Status: domain.StatusFailed, Attempt: 2, ResponseCode: 401},
				},
				NextPageToken: "page2",
			}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.ListDeliveryAttempts(authCtx(), &notificationv1.ListDeliveryAttemptsRequest{
		Channel: notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_WEBHOOK,
		Status:  notificationv1.DeliveryStatus_DELIVERY_STATUS_FAILED,
	})
	if err != nil {
		t.Fatalf("ListDeliveryAttempts returned error: %v", err)
	}
	// Authority + filter forwarding.
	if mock.lastListDeliv.RecipientUserID != testUserID ||
		mock.lastListDeliv.Channel != domain.ChannelWebhook ||
		mock.lastListDeliv.Status != domain.StatusFailed {
		t.Fatalf("filters/authority not forwarded: %+v", mock.lastListDeliv)
	}
	// The parallel notification_ids slice must align 1:1 with attempts.
	if len(resp.GetAttempts()) != 2 || len(resp.GetNotificationIds()) != 2 {
		t.Fatalf("attempts/ids length mismatch: %d / %d", len(resp.GetAttempts()), len(resp.GetNotificationIds()))
	}
	if resp.GetNotificationIds()[0] != "n-1" || resp.GetNotificationIds()[1] != "n-2" {
		t.Fatalf("notification_ids not aligned: %v", resp.GetNotificationIds())
	}
	if resp.GetPagination().GetNextPageToken() != "page2" {
		t.Fatalf("cursor not converted: %q", resp.GetPagination().GetNextPageToken())
	}
}

func TestListDeliveryAttempts_BadStatusEnum_InvalidArgument(t *testing.T) {
	mock := &mockService{
		listDelivFn: func(context.Context, domain.ListDeliveryAttemptsInput) (domain.ListDeliveryAttemptsOutput, error) {
			t.Fatal("domain must NOT be called for an out-of-range status")
			return domain.ListDeliveryAttemptsOutput{}, nil
		},
	}
	client := newClient(t, mock)
	_, err := client.ListDeliveryAttempts(authCtx(), &notificationv1.ListDeliveryAttemptsRequest{
		Status: notificationv1.DeliveryStatus(42),
	})
	requireCode(t, err, codes.InvalidArgument)
	if mock.listDelivCalls != 0 {
		t.Fatalf("domain ListDeliveryAttempts called %d times; want 0", mock.listDelivCalls)
	}
}

func TestListDeliveryAttempts_ForeignNotificationID_NotFound(t *testing.T) {
	mock := &mockService{
		listDelivFn: func(context.Context, domain.ListDeliveryAttemptsInput) (domain.ListDeliveryAttemptsOutput, error) {
			return domain.ListDeliveryAttemptsOutput{}, domain.ErrNotFound
		},
	}
	client := newClient(t, mock)
	_, err := client.ListDeliveryAttempts(authCtx(), &notificationv1.ListDeliveryAttemptsRequest{
		NotificationId: "foreign-id",
	})
	requireCode(t, err, codes.NotFound)
}

// ============================================================================
// GetPreferences
// ============================================================================

func TestGetPreferences_HappyPath_AuthorityFromClaims(t *testing.T) {
	updated := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	mock := &mockService{
		getPrefsFn: func(_ context.Context, userID string) (domain.NotificationPreferences, error) {
			return domain.NotificationPreferences{
				UserID: userID,
				Channels: []domain.ChannelPreference{
					{Channel: domain.ChannelInApp, Enabled: true},
					{Channel: domain.ChannelEmail, Enabled: true, MinSeverity: domain.SeverityError, Target: "ada@forgepoint.dev"},
				},
				MutedEventPatterns: []string{"fp.inference.*"},
				UpdatedAt:          updated,
			}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.GetPreferences(authCtx(), &notificationv1.GetPreferencesRequest{})
	if err != nil {
		t.Fatalf("GetPreferences returned error: %v", err)
	}
	if mock.lastGetPrefsUID != testUserID {
		t.Fatalf("prefs fetched for %q, want claims user %q", mock.lastGetPrefsUID, testUserID)
	}
	p := resp.GetPreferences()
	if p.GetUserId() != testUserID || len(p.GetChannels()) != 2 ||
		len(p.GetMutedEventPatterns()) != 1 || p.GetMutedEventPatterns()[0] != "fp.inference.*" {
		t.Fatalf("prefs not converted: %+v", p)
	}
	// The OWNER may see their own email channel's target (settings UI needs it).
	if p.GetChannels()[1].GetChannel() != notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL ||
		p.GetChannels()[1].GetMinSeverity() != notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_ERROR ||
		p.GetChannels()[1].GetTarget() != "ada@forgepoint.dev" {
		t.Fatalf("email channel pref not converted: %+v", p.GetChannels()[1])
	}
	if p.GetUpdatedAt() == nil || !p.GetUpdatedAt().AsTime().Equal(updated) {
		t.Fatalf("updated_at not converted: %v", p.GetUpdatedAt())
	}
}

// ============================================================================
// UpdatePreferences
// ============================================================================

func TestUpdatePreferences_HappyPath_OwnerPinnedToCaller(t *testing.T) {
	mock := &mockService{
		updatePrefsFn: func(_ context.Context, in domain.UpdatePreferencesInput) (domain.NotificationPreferences, error) {
			return domain.NotificationPreferences{
				UserID:   in.UserID,
				Channels: in.Channels,
			}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.UpdatePreferences(authCtx(), &notificationv1.UpdatePreferencesRequest{
		Channels: []*notificationv1.ChannelPreference{
			{
				Channel:     notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_SLACK,
				Enabled:     true,
				MinSeverity: notificationv1.NotificationSeverity_NOTIFICATION_SEVERITY_WARNING,
				Target:      "https://hooks.slack.com/services/T/B/x",
			},
		},
		MutedEventPatterns: []string{"fp.billing.*"},
		IdempotencyKey:     "idem-123",
	})
	if err != nil {
		t.Fatalf("UpdatePreferences returned error: %v", err)
	}
	// Anti mass-assignment: the OWNER is the claims user, full stop.
	if mock.lastUpdatePrefs.UserID != testUserID {
		t.Fatalf("update owner = %q, want claims user %q", mock.lastUpdatePrefs.UserID, testUserID)
	}
	if mock.lastUpdatePrefs.IdempotencyKey != "idem-123" {
		t.Fatalf("idempotency key not forwarded: %q", mock.lastUpdatePrefs.IdempotencyKey)
	}
	if len(mock.lastUpdatePrefs.Channels) != 1 ||
		mock.lastUpdatePrefs.Channels[0].Channel != domain.ChannelSlack ||
		mock.lastUpdatePrefs.Channels[0].Target != "https://hooks.slack.com/services/T/B/x" {
		t.Fatalf("channels not converted to domain: %+v", mock.lastUpdatePrefs.Channels)
	}
	if resp.GetPreferences().GetUserId() != testUserID {
		t.Fatalf("response owner = %q, want %q", resp.GetPreferences().GetUserId(), testUserID)
	}
}

func TestUpdatePreferences_SSRFRejected_InvalidArgument_NoLeak(t *testing.T) {
	mock := &mockService{
		updatePrefsFn: func(context.Context, domain.UpdatePreferencesInput) (domain.NotificationPreferences, error) {
			// The domain raises this (wrapped) for a target that fails the SSRF guard.
			return domain.NotificationPreferences{}, wrap(domain.ErrSSRFTargetRejected, "target IP is loopback/link-local/private/reserved")
		},
	}
	client := newClient(t, mock)
	_, err := client.UpdatePreferences(authCtx(), &notificationv1.UpdatePreferencesRequest{
		Channels: []*notificationv1.ChannelPreference{
			{Channel: notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_WEBHOOK, Enabled: true, Target: "https://10.0.0.1/hook"},
		},
	})
	requireCode(t, err, codes.InvalidArgument)
	st, _ := status.FromError(err)
	// The message names the violated RULE (safe), and must not echo the target URL.
	if !strings.Contains(st.Message(), "loopback") {
		t.Fatalf("expected the SSRF rule in the message, got %q", st.Message())
	}
	assertNoLeak(t, st.Message())
}

func TestUpdatePreferences_Validation(t *testing.T) {
	tooManyMutes := make([]string, maxMutePatterns+1)
	for i := range tooManyMutes {
		tooManyMutes[i] = "fp.x.*"
	}
	cases := []struct {
		name string
		req  *notificationv1.UpdatePreferencesRequest
	}{
		{"too many mute patterns", &notificationv1.UpdatePreferencesRequest{MutedEventPatterns: tooManyMutes}},
		{"bad channel enum", &notificationv1.UpdatePreferencesRequest{
			Channels: []*notificationv1.ChannelPreference{{Channel: notificationv1.NotificationChannel(77), Enabled: true}},
		}},
		{"bad min_severity enum", &notificationv1.UpdatePreferencesRequest{
			Channels: []*notificationv1.ChannelPreference{{
				Channel:     notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL,
				MinSeverity: notificationv1.NotificationSeverity(55),
			}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				updatePrefsFn: func(context.Context, domain.UpdatePreferencesInput) (domain.NotificationPreferences, error) {
					t.Fatal("domain must NOT be called for invalid UpdatePreferences input")
					return domain.NotificationPreferences{}, nil
				},
			}
			client := newClient(t, mock)
			_, err := client.UpdatePreferences(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.updatePrefsCalls != 0 {
				t.Fatalf("domain UpdatePreferences called %d times; want 0", mock.updatePrefsCalls)
			}
		})
	}
}

// ============================================================================
// TestChannel
// ============================================================================

func TestTestChannel_HappyPath(t *testing.T) {
	mock := &mockService{
		testChannelFn: func(_ context.Context, in domain.TestChannelInput) (domain.TestChannelOutput, error) {
			return domain.TestChannelOutput{Status: domain.StatusDelivered, ResponseCode: 200}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.TestChannel(authCtx(), &notificationv1.TestChannelRequest{
		Channel:        notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_SLACK,
		IdempotencyKey: "test-idem-1",
	})
	if err != nil {
		t.Fatalf("TestChannel returned error: %v", err)
	}
	if mock.lastTestChannel.UserID != testUserID || mock.lastTestChannel.Channel != domain.ChannelSlack ||
		mock.lastTestChannel.IdempotencyKey != "test-idem-1" {
		t.Fatalf("input not forwarded: %+v", mock.lastTestChannel)
	}
	if resp.GetStatus() != notificationv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED || resp.GetResponseCode() != 200 {
		t.Fatalf("outcome not converted: %+v", resp)
	}
}

// A delivery FAILURE is a successful RPC carrying a FAILED outcome — NOT a gRPC
// error. This is the proto contract: the settings UI shows "✗ <reason>" inline.
func TestTestChannel_DeliveryFailure_IsOkResponseNotError(t *testing.T) {
	mock := &mockService{
		testChannelFn: func(context.Context, domain.TestChannelInput) (domain.TestChannelOutput, error) {
			return domain.TestChannelOutput{Status: domain.StatusFailed, ResponseCode: 401, ErrorMessage: "401 from Slack"}, nil
		},
	}
	client := newClient(t, mock)
	resp, err := client.TestChannel(authCtx(), &notificationv1.TestChannelRequest{
		Channel: notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_SLACK,
	})
	if err != nil {
		t.Fatalf("a failed delivery must be an OK response, got gRPC error: %v", err)
	}
	if resp.GetStatus() != notificationv1.DeliveryStatus_DELIVERY_STATUS_FAILED ||
		resp.GetResponseCode() != 401 || resp.GetErrorMessage() != "401 from Slack" {
		t.Fatalf("failed outcome not converted: %+v", resp)
	}
}

func TestTestChannel_NotConfigured_MapsFailedPrecondition(t *testing.T) {
	mock := &mockService{
		testChannelFn: func(context.Context, domain.TestChannelInput) (domain.TestChannelOutput, error) {
			return domain.TestChannelOutput{}, domain.ErrChannelNotConfigured
		},
	}
	client := newClient(t, mock)
	_, err := client.TestChannel(authCtx(), &notificationv1.TestChannelRequest{
		Channel: notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL,
	})
	requireCode(t, err, codes.FailedPrecondition)
	st, _ := status.FromError(err)
	assertNoLeak(t, st.Message())
}

func TestTestChannel_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  *notificationv1.TestChannelRequest
	}{
		{"unspecified channel", &notificationv1.TestChannelRequest{Channel: notificationv1.NotificationChannel_NOTIFICATION_CHANNEL_UNSPECIFIED}},
		{"out-of-range channel", &notificationv1.TestChannelRequest{Channel: notificationv1.NotificationChannel(88)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockService{
				testChannelFn: func(context.Context, domain.TestChannelInput) (domain.TestChannelOutput, error) {
					t.Fatal("domain must NOT be called for an invalid channel")
					return domain.TestChannelOutput{}, nil
				},
			}
			client := newClient(t, mock)
			_, err := client.TestChannel(authCtx(), tc.req)
			requireCode(t, err, codes.InvalidArgument)
			if mock.testChannelCalls != 0 {
				t.Fatalf("domain TestChannel called %d times; want 0", mock.testChannelCalls)
			}
		})
	}
}

// ============================================================================
// NIL-SVC GUARD (the half-wired binary must behave like the scaffold)
// ============================================================================
//
// With svc==nil (the state main.go ships today), every RPC must return
// codes.Unimplemented — the SAME code the embedded base returns — instead of
// nil-derefing. We test one representative unary RPC; the guard is identical in
// all of them.
func TestNilService_ReturnsUnimplemented(t *testing.T) {
	// Wire the handler with a nil domain service, behind the real auth interceptor
	// so we know the Unimplemented comes from the guard, not from missing claims.
	h := NewNotificationHandler(nil)
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		notificationv1.RegisterNotificationServiceServer(s, h)
	}, grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(stubValidator{claims: &grpcutil.Claims{UserID: testUserID}})))
	client := notificationv1.NewNotificationServiceClient(conn)

	_, err := client.GetPreferences(authCtx(), &notificationv1.GetPreferencesRequest{})
	requireCode(t, err, codes.Unimplemented)
}
