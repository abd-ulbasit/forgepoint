// errors.go — sentinel errors owned by the domain layer.
//
// WHY domain (business) errors are distinct from the repository (storage)
// sentinels in ports.go:
//
//	ErrRepoEmailExists / ErrRepoNotFound (see ports.go) describe STORAGE
//	outcomes ("the unique index rejected this row", "no such row"). The errors
//	below re-express them as BUSINESS outcomes the handler can map to gRPC
//	status codes. The service is the single translation point: it catches a
//	storage sentinel and returns the matching business error.
//
// WHY a single generic ErrInvalidCredentials for login failures:
//
//	Login must NOT reveal whether the email exists. "Unknown email" and "wrong
//	password" return the SAME error — otherwise an attacker can enumerate valid
//	accounts by diffing the responses (user enumeration). One generic error
//	closes that side channel. See authService.Login.
package domain

import "errors"

var (
	// ErrWeakSecret is returned by GenerateToken and ValidateToken when the
	// HMAC signing secret is either empty or shorter than 32 bytes (256 bits).
	//
	// WHY 32 bytes is the floor:
	//   NIST SP 800-107 and RFC 7518 §3.2 both require HS256 keys to be at
	//   least as long as the hash output — 256 bits (32 bytes). A shorter secret
	//   reduces the effective key space: an 8-byte secret has only 2^64 possible
	//   values, trivially exhausted by an offline attacker who captures any
	//   signed token. With 32 random bytes the brute-force surface is 2^256 —
	//   computationally infeasible.
	//
	// WHY a DEDICATED sentinel (not ErrValidation / ErrInvalidToken):
	//   Operators need to detect misconfiguration at startup and surface a clear
	//   alert ("your JWT_SECRET is too short"), not a generic "bad token". A
	//   named sentinel lets callers errors.Is(err, ErrWeakSecret) and handle it
	//   separately from a legitimately invalid credential.
	//
	// INTERVIEW: "What key-length requirements do you enforce for HS256?"
	//   >= 32 bytes, enforced symmetrically in both mint and verify so a
	//   misconfigured verifier can't accept tokens the minter would reject.
	ErrWeakSecret = errors.New("auth: HMAC secret must be at least 32 bytes (256 bits)")

	// ErrEmailAlreadyExists is returned by CreateUser when the email is taken.
	// The handler maps this to codes.AlreadyExists. It is the business-level
	// translation of the storage sentinel ErrRepoEmailExists (ports.go).
	ErrEmailAlreadyExists = errors.New("auth: email already exists")

	// ErrInvalidCredentials is the SINGLE error Login returns for any
	// authentication failure — unknown email OR wrong password. Returning the
	// same error for both prevents user-enumeration. The handler maps this to
	// codes.Unauthenticated with a generic message.
	ErrInvalidCredentials = errors.New("auth: invalid credentials")

	// ErrInvalidToken is returned by ValidateToken when a credential is neither
	// a valid JWT nor a valid, active API key. The handler maps this to
	// codes.Unauthenticated. It deliberately does not say WHY (expired vs.
	// forged vs. revoked) to avoid handing an attacker diagnostic feedback.
	ErrInvalidToken = errors.New("auth: invalid token")

	// ErrUserNotFound is returned when an operation targets a user that does not
	// exist (e.g. AssignRole on a missing user). Maps to codes.NotFound.
	ErrUserNotFound = errors.New("auth: user not found")

	// ErrRoleNotFound is returned by AssignRole when the named role does not
	// exist. Maps to codes.NotFound.
	ErrRoleNotFound = errors.New("auth: role not found")

	// ErrValidation is returned for malformed input the service rejects before
	// touching any repository (empty email, empty password, etc.). Maps to
	// codes.InvalidArgument. Wrapped with %w so callers get a specific message
	// while still being able to errors.Is(err, ErrValidation).
	ErrValidation = errors.New("auth: validation failed")
)
