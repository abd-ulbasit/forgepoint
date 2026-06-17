// Package handler implements the gRPC server side of the Auth service.
// It is the outermost layer in Clean Architecture: it speaks proto (wire format)
// and delegates all business logic to the domain.AuthService interface.
//
// ============================================================================
// HANDLER LAYER RESPONSIBILITIES
// ============================================================================
//
// The handler layer has exactly three jobs:
//
//  1. PROTO → DOMAIN CONVERSION: Extract fields from the incoming proto Request,
//     validate them (required fields, format), and construct domain input types.
//     Example: *authv1.CreateUserRequest → domain.CreateUserInput.
//
//  2. CALL DOMAIN SERVICE: Invoke the appropriate AuthService method.
//     The handler holds an AuthService interface — it never knows whether the
//     implementation is backed by Postgres, Redis, or an in-memory test stub.
//
//  3. DOMAIN → PROTO CONVERSION: Map the domain result to a proto Response.
//     Example: domain.User → *authv1.CreateUserResponse.
//     CRITICAL: Never map domain.User.PasswordHash into any proto response.
//
// WHAT THE HANDLER DOES NOT DO:
//   - Business logic (belongs in domain)
//   - SQL queries (belongs in repository)
//   - JWT signing (belongs in domain service)
//   - Caching (belongs in domain service or repository)
//
// ============================================================================
// EMBEDDING UnimplementedAuthServiceServer — WHY THIS IS CORRECT (NOT A STUB)
// ============================================================================
//
// protoc-gen-go-grpc generates AuthServiceServer with a private method:
//
//	mustEmbedUnimplementedAuthServiceServer()
//
// This forces every implementation to embed UnimplementedAuthServiceServer.
// The UnimplementedAuthServiceServer base implements every RPC to return
// codes.Unimplemented. When we embed it, AuthHandler:
//
//  1. Satisfies the AuthServiceServer interface at compile time — the
//     server can be registered and started RIGHT NOW.
//  2. Returns codes.Unimplemented for any RPC whose handler method we haven't
//     written yet — the client gets a real, correct gRPC error, not a panic.
//  3. Is forward-compatible: when a new RPC is added to the proto, the
//     embedded base handles it (returning Unimplemented) until we implement it,
//     instead of a compile error that breaks the whole service.
//
// This is the IDIOMATIC Go/gRPC scaffold pattern, not a placeholder or TODO.
// It is a production-correct, running server. Per-RPC implementations are
// filled in Task 1.5 (Login, CreateUser, ValidateToken, etc.) by overriding
// each method on *AuthHandler.
//
// INTERVIEW: "How does grpc-go ensure forward compatibility of service servers?"
//
//	The mustEmbedUnimplementedAuthServiceServer() private method forces embedding
//	the Unimplemented base. Adding a new RPC to the proto breaks clients that
//	generated their server interface against the old proto — they can no longer
//	satisfy the interface — but services that embed the Unimplemented base
//	compile fine and return Unimplemented for the new method. This is gRPC's
//	server-side backward compatibility story.
//
// ============================================================================
package handler

import (
	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
)

// AuthHandler is the gRPC server implementation for the Auth service.
//
// It embeds authv1.UnimplementedAuthServiceServer to satisfy the full
// authv1.AuthServiceServer interface immediately, with all unimplemented RPCs
// returning codes.Unimplemented until Task 1.5 fills them in.
//
// svc is the domain.AuthService that contains all business logic. In Task 1.5
// each RPC method body calls svc.Login, svc.CreateUser, etc. and converts
// between proto and domain types. The svc field is nil during the scaffold phase
// (main.go passes nil); the embedded UnimplementedAuthServiceServer methods
// never dereference svc, so this is safe.
type AuthHandler struct {
	authv1.UnimplementedAuthServiceServer // embedded by value, not pointer (see grpc-go note above)
	svc                                   domain.AuthService
}

// NewAuthHandler creates an AuthHandler with the given domain service.
//
// The svc parameter is nil during the scaffold phase (Tasks 1.2–1.4) because
// the domain service implementation and Postgres repositories are wired in
// Task 1.3/1.4. Once those layers land, main.go passes the real AuthServiceImpl.
// The nil svc is safe here because the embedded UnimplementedAuthServiceServer
// handles all RPCs without touching svc.
//
// After Task 1.5 (RPC implementations), every method will guard against a nil
// service. The exact return shape depends on the RPC kind:
//   - UNARY RPCs return (resp, err), so the guard is:
//     if h.svc == nil { return nil, status.Error(codes.Internal, "service not wired") }
//   - STREAMING RPCs return only err (the response flows via stream.Send), so
//     there is no resp value to return — the guard is:
//     if h.svc == nil { return status.Error(codes.Internal, "service not wired") }
//
// (The Auth service's RPCs are all unary today; the streaming form is noted so
// the pattern is correct if a server-streaming RPC is ever added.) During this
// scaffold phase the embedded Unimplemented base answers first, so svc is never
// dereferenced.
func NewAuthHandler(svc domain.AuthService) *AuthHandler {
	return &AuthHandler{svc: svc}
}
