// auth_handler_rpcs.go holds the per-RPC method bodies for AuthHandler.
//
// The scaffold (auth_handler.go) embeds UnimplementedAuthServiceServer and holds
// the domain.AuthService field. This file OVERRIDES each RPC with a real
// implementation. Once a method is defined here on *AuthHandler, Go's method set
// resolution prefers it over the embedded Unimplemented base — so every RPC
// below is now live, and only RPCs we have NOT written fall through to
// Unimplemented (there are none left for AuthService; all eight are here).
//
// ============================================================================
// THE FOUR-STEP HANDLER CONTRACT (every method follows it)
// ============================================================================
//
//  1. NIL-SVC GUARD. A binary may be wired with svc==nil during the scaffold /
//     pre-repo phase (main.go passes nil until Task 1.4 lands the Postgres
//     adapter). Calling a nil interface's method panics. So every method first
//     checks h.svc==nil and returns codes.Unimplemented — the SAME code the
//     embedded base would return, so a half-wired binary behaves identically to
//     the scaffold instead of crashing. (Streaming RPCs would return the status
//     error directly with no response value; AuthService has none today.)
//
//  2. VALIDATE THE REQUEST. Reject malformed input with codes.InvalidArgument
//     and a CLEAN message (no internal detail, no echo of secrets). This is the
//     handler's job, not the domain's: we fail fast at the transport boundary so
//     the domain only ever sees well-formed input. The domain ALSO validates
//     (defense in depth) but the handler gives the client a precise gRPC code.
//
//  3. CONVERT proto -> domain, pulling SERVER-AUTHORITATIVE identity from the
//     auth interceptor's claims (grpcutil.ClaimsFromContext) — NEVER from
//     client-supplied fields. The client cannot be trusted to say "I am user X";
//     the validated token says who they are.
//
//  4. CALL the domain service, then MAP its sentinel errors -> precise gRPC
//     status codes via toStatusError, and CONVERT the domain result -> proto.
//
// ============================================================================
// WHY ERROR MAPPING IS CENTRALIZED (toStatusError)
// ============================================================================
//
// gRPC clients branch on status.Code(err) — NotFound vs AlreadyExists vs
// Internal drive ret[r]y logic, user-facing messages, and alerting. A handler
// that returned bare domain errors would leak internal text ("repository: ...",
// SQL fragments, PII) to every caller and give them codes.Unknown, which no
// client can reason about. toStatusError is the single anti-corruption point
// that turns domain vocabulary into the gRPC status vocabulary and SANITIZES
// anything it doesn't recognize down to codes.Internal with a fixed generic
// message. See toStatusError below.
package handler

import (
	"context"
	"errors"
	"fmt"
	"time"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// PAGINATION CONSTANTS
// ============================================================================

const (
	// defaultPageSize is applied when the client omits page_size (sends 0).
	// 20 mirrors the proto's documented default for PaginationRequest.
	defaultPageSize = 20

	// maxPageSize caps page_size to prevent a client from requesting a
	// multi-thousand-row page that would pin memory and starve other callers.
	// 100 mirrors the proto's documented max. We CLAMP rather than reject an
	// over-large request: a client asking for "as much as possible" should get
	// the maximum we'll serve, not an error. (Rejecting would also be defensible;
	// clamping is friendlier and is what Google's AIP-158 recommends.)
	maxPageSize = 100
)

// ============================================================================
// Login
// ============================================================================
//
// Login is UNAUTHENTICATED by design — it is the call that MINTS the first
// token, so there are no claims in the context yet. That is why it does NOT
// call ClaimsFromContext: the email+password in the request ARE the identity
// assertion, verified by the domain against the bcrypt hash.
//
// SECURITY — anti-enumeration: the domain returns the single sentinel
// ErrInvalidCredentials for BOTH "no such email" and "wrong password", so the
// handler maps both to codes.Unauthenticated with one generic message. A
// different code or message for the two cases would let an attacker enumerate
// valid accounts by diffing responses. See domain/errors.go.
func (h *AuthHandler) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// VALIDATE. Empty email/password can never authenticate; reject before we
	// burn a bcrypt comparison (bcrypt is intentionally ~250ms — cheap DoS
	// vector if we let empty creds reach it).
	if req.GetEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "email is required")
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password is required")
	}

	// CALL DOMAIN. The domain hashes/compares and signs the JWT; the handler
	// never touches bcrypt or the JWT library — that logic is not transport.
	token, err := h.svc.Login(ctx, req.GetEmail(), req.GetPassword())
	if err != nil {
		// Map ErrInvalidCredentials -> Unauthenticated explicitly (it is NOT in
		// the generic table because it must produce a deliberately vague message).
		if errors.Is(err, domain.ErrInvalidCredentials) {
			return nil, status.Error(codes.Unauthenticated, "invalid email or password")
		}
		return nil, toStatusError(err)
	}

	// CONVERT domain -> proto. The domain returns only the raw JWT string (it
	// deliberately does not couple the handler to the JWT library's claim struct).
	// The proto LoginResponse also has expires_at and user; populating those
	// fully requires the domain to surface them, which it does not in this phase
	// (Login returns just the token). We return the token — the load-bearing
	// field — and leave the optional convenience fields unset. They are additive
	// and a later Login signature can fill them without a wire-breaking change.
	return &authv1.LoginResponse{
		AccessToken: token,
	}, nil
}

// ============================================================================
// AUTHORIZATION HELPER — in-handler admin gate (fail-closed)
// ============================================================================
//
// AUTHN vs AUTHZ — WHO POPULATES CLAIMS, WHO DECIDES ADMIN:
//
//	AUTHENTICATION (who are you) is now done by the shared grpcutil
//	AuthUnaryInterceptor, which main.go wires with authn.NewValidator: it
//	validates the Bearer token (JWT signature verify with FP_JWT_SECRET, or the
//	"fp_" API-key path) and injects grpcutil.Claims into the context. THAT is the
//	only thing that makes ClaimsFromContext below return a caller — without it (the
//	original bug) these RPCs were permanently Unauthenticated.
//
//	AUTHORIZATION (may you do this) is still enforced HERE. pkg/grpcutil's
//	interceptor authenticates but does NOT call CheckPermission, so if the handler
//	merely *assumed* admin from the presence of claims, any authenticated caller
//	could create users or reassign roles. requireAdmin closes that: it asks the
//	domain "may THIS caller perform 'admin' on <resource>?" via the same
//	CheckPermission a future platform-wide authz interceptor would call. Until such
//	an authz interceptor lands, the handler is the correct fail-closed home for the
//	admin decision.
//
// CONTRACT — requireAdmin(ctx, resource):
//
//	1. AUTHN: pull the caller's claims that the authentication interceptor injected.
//	   No claims => the call bypassed authn (or carried no/invalid token, which the
//	   interceptor would already have rejected) => fail closed with Unauthenticated.
//	2. AUTHZ: ask the domain "may THIS caller perform 'admin' on <resource>?"
//	   via the same CheckPermission the (future) interceptor would call.
//	     - err != nil  => "couldn't decide" (DB down) => Internal (fail closed),
//	       sanitized via toStatusError so no infra detail leaks.
//	     - allowed==false => authoritative DENY => PermissionDenied.
//	     - allowed==true  => return the claims so the caller can reuse them.
//
//	The action is always "admin" because these are administrative RPCs; "admin"
//	is the strongest action in the model (auth.proto: "full control including
//	granting the resource to others"). A role holding resource="*"/action="*"
//	(platform admin) matches via the domain's wildcard semantics.
func (h *AuthHandler) requireAdmin(ctx context.Context, resource string) (*grpcutil.Claims, error) {
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.UserID == "" {
		// No authenticated identity: the request did not pass the authentication
		// interceptor (or carried no token). Fail closed — we never run an authz
		// check for an anonymous caller.
		return nil, status.Error(codes.Unauthenticated, "missing authentication")
	}

	allowed, err := h.svc.CheckPermission(ctx, claims.UserID, resource, "admin")
	if err != nil {
		// Infrastructure failure ("couldn't decide") -> sanitized Internal. This is
		// the fail-closed posture: we never proceed when authz could not be evaluated.
		return nil, toStatusError(err)
	}
	if !allowed {
		// Authoritative deny. PermissionDenied (NOT Unauthenticated): the caller IS
		// authenticated, they simply lack the admin grant. We do not echo the
		// resource/action in a way that aids probing beyond the generic statement.
		return nil, status.Error(codes.PermissionDenied, "admin permission required")
	}
	return claims, nil
}

// ============================================================================
// CreateUser
// ============================================================================
//
// AUTHORIZATION: the proto says "Requires admin role." Because no authorization
// interceptor exists yet (see requireAdmin above), we enforce admin in-handler
// BEFORE doing any work: requireAdmin gates on CheckPermission(caller, "users",
// "admin"). This runs before validation so an unauthorized caller learns nothing
// about input shape — and never reaches the domain.
func (h *AuthHandler) CreateUser(ctx context.Context, req *authv1.CreateUserRequest) (*authv1.CreateUserResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// AUTHZ GATE (fail-closed). Provisioning users is an admin action; enforce it
	// here until the platform-wide authz interceptor lands.
	if _, err := h.requireAdmin(ctx, "users"); err != nil {
		return nil, err
	}

	// VALIDATE required fields. We do NOT validate password strength or email
	// RFC-format here beyond emptiness — the domain owns the business rules
	// (e.g. min length); the handler only guarantees the fields are present so
	// the domain isn't handed obviously-unusable zero values.
	if req.GetEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "email is required")
	}
	if req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "password is required")
	}
	if req.GetTeam() == "" {
		// Team drives multi-tenancy scoping; a user with no team can't be
		// correctly isolated, so it is required (see domain.CreateUserInput.Team).
		return nil, status.Error(codes.InvalidArgument, "team is required")
	}

	// CONVERT proto -> domain input. Note ID/CreatedAt/Role are NOT taken from
	// the client: ID + CreatedAt are generated by the DB, and the default role
	// is assigned by the domain. The client can only propose email/name/
	// password/team — all the rest is server-authoritative.
	out, err := h.svc.CreateUser(ctx, domain.CreateUserInput{
		Email:    req.GetEmail(),
		Name:     req.GetName(),
		Password: req.GetPassword(), // plaintext in, hashed by the domain — never stored or logged raw
		Team:     req.GetTeam(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	// CONVERT domain -> proto. userToProto NEVER copies PasswordHash — the
	// proto User has no password field, and that invariant is enforced in one
	// place (userToProto) so no handler can accidentally leak the hash.
	return &authv1.CreateUserResponse{
		User: userToProto(out),
	}, nil
}

// ============================================================================
// ListUsers
// ============================================================================
//
// PAGINATION is the interesting part: we normalize the client's page_size
// (default when 0, clamp when over the cap) and forward the opaque cursor.
// The domain returns a nextToken which we echo back so the client can request
// the following page. TotalCount is left at 0 (the domain's List does not
// compute it — see ListOptions; computing an exact total can require a full
// scan, which the proto's PaginationResponse explicitly allows omitting).
//
// AUTHORIZATION: the proto says "Requires admin role." Listing all users is an
// administrative, tenant-wide read; we gate it on CheckPermission(caller,
// "users", "admin") in-handler (no authz interceptor exists yet — see
// requireAdmin). The gate runs FIRST, fail-closed, before any other work.
func (h *AuthHandler) ListUsers(ctx context.Context, req *authv1.ListUsersRequest) (*authv1.ListUsersResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// AUTHZ GATE (fail-closed) before validation: an unauthorized caller must not
	// be able to distinguish a valid from an invalid pagination request.
	if _, err := h.requireAdmin(ctx, "users"); err != nil {
		return nil, err
	}

	// NOTE: the current domain.AuthService interface does not expose a ListUsers
	// method (it lands with the repository phase). Returning Unimplemented here
	// is the honest, correct answer until that method exists — NOT a silent
	// success or a panic. This keeps the RPC wired and forward-compatible: when
	// the domain gains List, this body is replaced with the real conversion.
	//
	// We still VALIDATE the pagination request so the contract (InvalidArgument
	// on a negative page_size) is enforced and tested today.
	if p := req.GetPagination(); p != nil {
		if p.GetPageSize() < 0 {
			return nil, status.Error(codes.InvalidArgument, "page_size must not be negative")
		}
	}

	return nil, status.Error(codes.Unimplemented, "ListUsers is not yet implemented")
}

// ============================================================================
// CreateAPIKey
// ============================================================================
//
// SERVER-AUTHORITATIVE OWNERSHIP — the security-critical part of this handler:
//
//	A client could send any user_id in CreateAPIKeyRequest. If we trusted it
//	blindly, ANY authenticated user could mint a key OWNED BY (and carrying the
//	role authority of) ANOTHER user — a privilege-escalation / impersonation
//	bug. So the owner is resolved server-side from the validated token claims,
//	NEVER taken verbatim from the request.
//
//	OWNER RESOLUTION (the rule, fail-closed):
//	  - user_id omitted, OR user_id == caller          -> SELF-SERVICE: owner is
//	    the caller (from claims). No extra check: minting your own key needs only
//	    that you are authenticated.
//	  - user_id names a DIFFERENT user                  -> CROSS-USER (admin only):
//	    we do NOT honor it on trust. We perform an explicit admin authorization
//	    check — CheckPermission(caller, "api-keys", "admin") — and only then set
//	    the owner to the requested user_id. Denied => PermissionDenied.
//
//	WHY THE EXPLICIT CHECK INSTEAD OF "the interceptor enforces it": there is no
//	authorization interceptor in the platform yet — pkg/grpcutil only
//	AUTHENTICATES (validate token -> inject claims) and never calls
//	CheckPermission. An earlier version of this handler honored req.user_id
//	verbatim and claimed "the interceptor gates cross-user creation"; since that
//	interceptor does not exist, that was an open impersonation hole. Until a
//	platform-wide authz interceptor lands, the handler enforces the admin gate
//	itself, fail-closed. (When that interceptor arrives, this in-handler check
//	can move there.)
func (h *AuthHandler) CreateAPIKey(ctx context.Context, req *authv1.CreateAPIKeyRequest) (*authv1.CreateAPIKeyResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// Pull the caller's identity from the interceptor-set claims. This RPC is
	// authenticated, so claims MUST be present; their absence means the call
	// bypassed the auth interceptor — fail closed with Unauthenticated.
	claims, ok := grpcutil.ClaimsFromContext(ctx)
	if !ok || claims == nil || claims.UserID == "" {
		return nil, status.Error(codes.Unauthenticated, "missing authentication")
	}

	// Resolve the OWNER (server-authoritative). Default: the caller (self-service).
	ownerID := claims.UserID
	if req.GetUserId() != "" && req.GetUserId() != claims.UserID {
		// CROSS-USER creation: minting a key on behalf of someone else is an
		// admin-only action. Enforce it explicitly here — do NOT trust the
		// client-supplied user_id. Only on an affirmative allow do we adopt it.
		allowed, err := h.svc.CheckPermission(ctx, claims.UserID, "api-keys", "admin")
		if err != nil {
			// "Couldn't decide" (e.g. DB down) -> sanitized Internal, fail-closed.
			return nil, toStatusError(err)
		}
		if !allowed {
			// Authoritative deny: the caller may not mint keys for other users.
			return nil, status.Error(codes.PermissionDenied, "admin permission required to create an API key for another user")
		}
		ownerID = req.GetUserId()
	}

	// CONVERT proto -> domain. expires_at is optional: a nil/zero proto timestamp
	// means "no expiry" (long-lived service key), represented as a nil *time.Time.
	var expiresAt *time.Time
	if ts := req.GetExpiresAt(); ts != nil {
		// Reject a malformed timestamp (e.g. out-of-range) at the boundary rather
		// than letting it become a confusing time.Time deep in the domain.
		if err := ts.CheckValid(); err != nil {
			return nil, status.Error(codes.InvalidArgument, "expires_at is not a valid timestamp")
		}
		t := ts.AsTime()
		expiresAt = &t
	}

	// CALL DOMAIN. It generates the random key, hashes it, stores the hash, and
	// returns BOTH the stored metadata AND the raw key (shown exactly once).
	apiKey, rawKey, err := h.svc.CreateAPIKey(ctx, ownerID, req.GetScopes(), expiresAt)
	if err != nil {
		return nil, toStatusError(err)
	}

	// CONVERT domain -> proto. The raw key goes in raw_key (one-time display);
	// the metadata (id, prefix, scopes, expiry) goes in api_key. apiKeyToProto
	// NEVER copies KeyHash — only the non-secret KeyPrefix is surfaced.
	return &authv1.CreateAPIKeyResponse{
		RawKey: rawKey,
		ApiKey: apiKeyToProto(apiKey),
	}, nil
}

// ============================================================================
// RevokeAPIKey
// ============================================================================
//
// The domain.AuthService interface does not (yet) expose a Revoke method — it
// arrives with the repository phase (APIKeyRepository.Revoke exists, but the
// service-level method to call it does not). We VALIDATE the request and return
// Unimplemented honestly until that method lands, keeping the RPC wired.
func (h *AuthHandler) RevokeAPIKey(ctx context.Context, req *authv1.RevokeAPIKeyRequest) (*authv1.RevokeAPIKeyResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// VALIDATE: revocation targets a specific key by its UUID (NOT the raw key —
	// see the proto note: using the id avoids any partial-match revocation).
	if req.GetKeyId() == "" {
		return nil, status.Error(codes.InvalidArgument, "key_id is required")
	}

	return nil, status.Error(codes.Unimplemented, "RevokeAPIKey is not yet implemented")
}

// ============================================================================
// ValidateToken
// ============================================================================
//
// This is the workhorse every OTHER service's auth interceptor calls. It is
// itself UNAUTHENTICATED in the usual sense — the token IN the request is the
// thing being authenticated, so we do not read claims from the context.
//
// SECURITY — opaque failure: the domain returns the single ErrInvalidToken for
// expired, forged, AND revoked tokens, so we map all of them to
// codes.Unauthenticated with one generic message. Telling the caller WHY a
// token failed (expired vs. bad signature vs. revoked) hands an attacker
// diagnostic feedback. See domain/errors.go ErrInvalidToken.
func (h *AuthHandler) ValidateToken(ctx context.Context, req *authv1.ValidateTokenRequest) (*authv1.ValidateTokenResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	if req.GetToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "token is required")
	}

	claims, err := h.svc.ValidateToken(ctx, req.GetToken())
	if err != nil {
		if errors.Is(err, domain.ErrInvalidToken) {
			return nil, status.Error(codes.Unauthenticated, "invalid token")
		}
		return nil, toStatusError(err)
	}

	return &authv1.ValidateTokenResponse{
		Claims: tokenClaimsToProto(claims),
	}, nil
}

// ============================================================================
// CheckPermission
// ============================================================================
//
// RBAC decision gate. The proto's CheckPermissionRequest carries only
// (user_id, resource, action) — no credential context — so this maps to the
// ROLE-only domain.CheckPermission (NOT CheckPermissionForClaims, which needs
// the full TokenClaims to honor API-key scopes; that path is the in-process
// interceptor, not this cross-service RPC). See the domain interface comments.
//
// CONTRACT NUANCE — (bool, error) -> proto:
//
//	The domain returns (allowed bool, err error) where err means "I could not
//	decide" (DB down) and allowed is the decision when err==nil. We map a
//	non-nil err to a gRPC status (fail-closed on the interceptor side) and a nil
//	err to a populated CheckPermissionResponse{allowed, reason}. A DENY is NOT
//	an error here — it's allowed=false with a 200-equivalent status, because the
//	question "is X allowed?" was answered successfully ("no"). Returning
//	PermissionDenied as the RPC status would conflate "the check itself failed"
//	with "the answer is no", which the caller cannot distinguish.
func (h *AuthHandler) CheckPermission(ctx context.Context, req *authv1.CheckPermissionRequest) (*authv1.CheckPermissionResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// VALIDATE: an authorization question over an empty coordinate ("may user X
	// do <nothing> on <nothing>?") is meaningless and a likely caller bug. The
	// domain also denies empty coordinates (defense in depth), but we reject at
	// the boundary so the client gets a precise InvalidArgument, not a silent
	// deny that masks the missing field.
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if req.GetResource() == "" {
		return nil, status.Error(codes.InvalidArgument, "resource is required")
	}
	if req.GetAction() == "" {
		return nil, status.Error(codes.InvalidArgument, "action is required")
	}

	allowed, err := h.svc.CheckPermission(ctx, req.GetUserId(), req.GetResource(), req.GetAction())
	if err != nil {
		// A non-nil error is an INFRASTRUCTURE failure ("couldn't decide"), not a
		// deny. Surface it as a sanitized status so the calling interceptor can
		// fail closed and operators see Internal, not PermissionDenied.
		return nil, toStatusError(err)
	}

	// CONVERT decision -> proto. The proto requires reason "Always populated";
	// the domain returns only a bool, so the handler synthesizes a stable,
	// non-sensitive reason string (it names role/resource/action — no PII, no
	// internal detail). This matches the AWS IAM evaluation-response shape.
	reason := fmt.Sprintf("denied: user %q lacks action %q on resource %q",
		req.GetUserId(), req.GetAction(), req.GetResource())
	if allowed {
		reason = fmt.Sprintf("allowed: user %q may perform action %q on resource %q",
			req.GetUserId(), req.GetAction(), req.GetResource())
	}

	return &authv1.CheckPermissionResponse{
		Allowed: allowed,
		Reason:  reason,
	}, nil
}

// ============================================================================
// AssignRole
// ============================================================================
//
// Changes a user's role. The proto says "Requires admin permission." Because no
// authorization interceptor exists yet (see requireAdmin), we enforce admin
// in-handler: CheckPermission(caller, "users", "admin"). Role assignment is a
// privilege-granting operation — the canonical thing that MUST be admin-gated
// (an attacker who could self-assign "admin" would own the platform), so the
// gate runs FIRST, fail-closed, before any validation or domain work.
//
// ERROR MAPPING is the teaching point: the domain distinguishes ErrUserNotFound
// (the target user is gone) from ErrRoleNotFound (the named role doesn't exist).
// Both are codes.NotFound but the message differs, so the caller knows WHICH
// thing was missing. toStatusError handles both via the table.
func (h *AuthHandler) AssignRole(ctx context.Context, req *authv1.AssignRoleRequest) (*authv1.AssignRoleResponse, error) {
	if h.svc == nil {
		return nil, errServiceNotWired
	}

	// AUTHZ GATE (fail-closed). Assigning roles changes a user's authority; it is
	// admin-only. Enforce here until the platform-wide authz interceptor lands.
	if _, err := h.requireAdmin(ctx, "users"); err != nil {
		return nil, err
	}

	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if req.GetRoleName() == "" {
		return nil, status.Error(codes.InvalidArgument, "role_name is required")
	}

	if err := h.svc.AssignRole(ctx, req.GetUserId(), req.GetRoleName()); err != nil {
		return nil, toStatusError(err)
	}

	// Success is implicit; the response is intentionally empty (see proto note —
	// a named empty message keeps room to add audit fields later).
	return &authv1.AssignRoleResponse{}, nil
}

// ============================================================================
// PROTO <-> DOMAIN CONVERTERS
// ============================================================================
//
// These are the ANTI-CORRUPTION LAYER (DDD term): the single place domain types
// become wire types. Centralizing them means the "never leak PasswordHash /
// KeyHash" invariant lives in ONE auditable spot, not scattered across handlers.

// userToProto converts a domain.User to the proto User.
//
// SECURITY INVARIANT: the proto User has no password field and this function
// copies neither PasswordHash nor the Active flag's internal meaning — only the
// fields a client is allowed to see. The domain Role is reduced to its Name
// (the proto User.role is a string; full permissions are not embedded — see the
// proto note on why role is denormalized to a name).
func userToProto(u domain.User) *authv1.User {
	return &authv1.User{
		Id:        u.ID,
		Email:     u.Email,
		Name:      u.Name,
		Team:      u.Team,
		Role:      u.Role.Name, // name only; never the permission list
		CreatedAt: timestamppb.New(u.CreatedAt),
	}
}

// apiKeyToProto converts a domain.APIKey to the proto APIKey.
//
// SECURITY INVARIANT: it copies KeyPrefix (safe to display) but NEVER KeyHash
// (the only key material in the DB). The raw key itself is never on the domain
// struct — it exists only transiently during CreateAPIKey and is returned via
// the separate raw_key response field.
func apiKeyToProto(k domain.APIKey) *authv1.APIKey {
	out := &authv1.APIKey{
		Id:        k.ID,
		KeyPrefix: k.KeyPrefix,
		UserId:    k.UserID,
		Scopes:    k.Scopes,
		CreatedAt: timestamppb.New(k.CreatedAt),
	}
	// expires_at is optional: a nil domain *time.Time means "no expiry", which we
	// represent as an unset proto timestamp (nil) rather than the zero epoch.
	if k.ExpiresAt != nil {
		out.ExpiresAt = timestamppb.New(*k.ExpiresAt)
	}
	return out
}

// tokenClaimsToProto converts domain.TokenClaims to the proto TokenClaims.
//
// This is what every other service's interceptor deserializes from
// ValidateTokenResponse. The TokenKind field is intentionally NOT surfaced on
// the proto (the proto TokenClaims has no such field) — downstream services key
// off scopes uniformly regardless of whether a JWT or API key produced them.
func tokenClaimsToProto(c domain.TokenClaims) *authv1.TokenClaims {
	return &authv1.TokenClaims{
		UserId: c.UserID,
		Email:  c.Email,
		Team:   c.Team,
		Role:   c.Role,
		Scopes: c.Scopes,
		Exp:    timestamppb.New(c.ExpiresAt),
		Iat:    timestamppb.New(c.IssuedAt),
	}
}

// ============================================================================
// ERROR MAPPING — domain sentinels -> gRPC status codes
// ============================================================================

// errServiceNotWired is the canonical response when h.svc is nil (a binary
// running before the domain service is wired). It mirrors what the embedded
// UnimplementedAuthServiceServer would return, so a half-wired binary behaves
// exactly like the scaffold instead of panicking on a nil-interface call.
var errServiceNotWired = status.Error(codes.Unimplemented, "auth service not wired")

// toStatusError is the single anti-corruption point between domain errors and
// the gRPC status vocabulary. It is intentionally a small, explicit table:
//
//	domain sentinel            -> gRPC code           why
//	-------------------------------------------------------------------------
//	ErrValidation              -> InvalidArgument     client sent bad input
//	ErrEmailAlreadyExists      -> AlreadyExists       unique-constraint conflict
//	ErrUserNotFound            -> NotFound            target user absent
//	ErrRoleNotFound            -> NotFound            named role absent
//	ErrInvalidToken            -> Unauthenticated     bad/expired/revoked token
//	ErrInvalidCredentials      -> Unauthenticated     login failed (anti-enum)
//	ErrWeakSecret              -> Internal            server misconfig, not client
//	(anything else)            -> Internal (generic)  SANITIZED — no leak
//
// WHY ErrWeakSecret -> Internal (not InvalidArgument): a too-short JWT secret is
// an OPERATOR misconfiguration of THIS service, not a client mistake. The client
// did nothing wrong and can't fix it, so we don't blame them with a 4xx-class
// code; we return Internal and rely on server-side alerting to surface it. We
// also do NOT echo the sentinel's text to the client (it would hint at the
// security misconfig); the generic Internal message is used.
//
// WHY the default is Internal with a FIXED message: any error we don't
// recognize might wrap SQL text, a connection string, a file path, or PII. The
// safe default is to log the real error server-side (done by the logging
// interceptor) and return a constant "internal error" to the client. errors.Is
// is used (not ==) so a wrapped sentinel (fmt.Errorf("...: %w", ErrUserNotFound))
// still maps correctly.
//
// KEEPING INTERNAL ERRORS OFF THE WIRE: centralize
// the mapping; whitelist the codes you intend to expose; sanitize everything
// else to Internal with a fixed string; never str-format the raw error into the
// status message on the default path.
func toStatusError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, domain.ErrValidation):
		// Wrapped ErrValidation carries a specific, NON-sensitive message
		// (e.g. "auth: validation failed: email must contain @"). It is safe and
		// useful to forward — it describes the client's own bad input, never
		// internal state. We forward err.Error() ONLY for this whitelisted case.
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, domain.ErrEmailAlreadyExists):
		return status.Error(codes.AlreadyExists, "a user with that email already exists")

	case errors.Is(err, domain.ErrUserNotFound):
		return status.Error(codes.NotFound, "user not found")

	case errors.Is(err, domain.ErrRoleNotFound):
		return status.Error(codes.NotFound, "role not found")

	case errors.Is(err, domain.ErrInvalidToken):
		return status.Error(codes.Unauthenticated, "invalid token")

	case errors.Is(err, domain.ErrInvalidCredentials):
		return status.Error(codes.Unauthenticated, "invalid email or password")

	default:
		// SANITIZE: do not expose err.Error() — it may wrap SQL, secrets, or PII.
		// The real error is logged by the logging interceptor server-side; the
		// client gets a constant, opaque message and codes.Internal.
		return status.Error(codes.Internal, "internal error")
	}
}
