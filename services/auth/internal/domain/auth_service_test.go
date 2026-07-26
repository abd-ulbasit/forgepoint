// auth_service_test.go — TDD specification for the AuthService implementation.
//
// ============================================================================
// WHY THIS IS AN EXTERNAL TEST PACKAGE (domain_test, not domain)
// ============================================================================
//
// The repository interfaces (repository.UserRepository, ...) are defined in
// terms of domain types, so the repository package imports domain. Our mocks
// must satisfy those repository interfaces, which means the test must import
// repository. If the test lived in `package domain`, we'd have:
//
//	domain (test) -> repository -> domain          (import CYCLE, rejected)
//
// Go forbids that for in-package tests. Putting the test in the EXTERNAL test
// package `domain_test` breaks the cycle: domain_test imports both domain and
// repository, and neither of those imports domain_test. This is the idiomatic
// Go resolution (the std lib does this constantly, e.g. net/http_test). It also
// forces the test to exercise only the EXPORTED surface — exactly the contract
// real callers (handlers) depend on.
//
// These tests are written BEFORE auth_service_impl.go exists and assert REAL
// outcomes (bcrypt verifies, sha256 matches, JWT validates), not mock call
// counts. See per-test comments.
package domain_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/abd-ulbasit/forgepoint/services/auth/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/auth/internal/repository"
)

// ----------------------------------------------------------------------------
// Hand-written mocks (no codegen).
// Each mock is a struct with function fields; an unset field panics if called,
// surfacing accidental coupling (a test hitting a repo it didn't intend to).
// ----------------------------------------------------------------------------

type mockUserRepo struct {
	createFn     func(ctx context.Context, u domain.User) (domain.User, error)
	getByEmailFn func(ctx context.Context, email string) (domain.User, error)
	getByIDFn    func(ctx context.Context, id string) (domain.User, error)
}

func (m *mockUserRepo) Create(ctx context.Context, u domain.User) (domain.User, error) {
	return m.createFn(ctx, u)
}
func (m *mockUserRepo) GetByEmail(ctx context.Context, email string) (domain.User, error) {
	return m.getByEmailFn(ctx, email)
}
func (m *mockUserRepo) GetByID(ctx context.Context, id string) (domain.User, error) {
	return m.getByIDFn(ctx, id)
}
func (m *mockUserRepo) List(ctx context.Context, opts repository.ListOptions) ([]domain.User, string, error) {
	panic("List not implemented in this test")
}

// mockAPIKeyRepo holds in-memory state keyed by hash so a key "created" in one
// call can be "looked up" in another — letting us assert the real hash
// roundtrip rather than inspecting a single captured argument.
type mockAPIKeyRepo struct {
	byHash   map[string]domain.APIKey
	createFn func(ctx context.Context, k domain.APIKey) (domain.APIKey, error) // optional override
}

func newMockAPIKeyRepo() *mockAPIKeyRepo {
	return &mockAPIKeyRepo{byHash: make(map[string]domain.APIKey)}
}

func (m *mockAPIKeyRepo) Create(ctx context.Context, k domain.APIKey) (domain.APIKey, error) {
	if m.createFn != nil {
		return m.createFn(ctx, k)
	}
	k.ID = "key-" + k.KeyPrefix
	k.CreatedAt = time.Now()
	m.byHash[k.KeyHash] = k
	return k, nil
}
func (m *mockAPIKeyRepo) GetByKeyHash(ctx context.Context, keyHash string) (domain.APIKey, error) {
	k, ok := m.byHash[keyHash]
	if !ok {
		return domain.APIKey{}, repository.ErrNotFound
	}
	return k, nil
}
func (m *mockAPIKeyRepo) Revoke(ctx context.Context, keyID string, now time.Time) error {
	panic("Revoke not implemented in this test")
}
func (m *mockAPIKeyRepo) ListByUser(ctx context.Context, userID string) ([]domain.APIKey, error) {
	panic("ListByUser not implemented in this test")
}

type mockRoleRepo struct {
	getUserRolesFn func(ctx context.Context, userID string) ([]domain.Role, error)
	getByNameFn    func(ctx context.Context, name string) (domain.Role, error)
	assignFn       func(ctx context.Context, userID, roleID string) error
}

func (m *mockRoleRepo) Create(ctx context.Context, r domain.Role) (domain.Role, error) {
	panic("Create not implemented in this test")
}
func (m *mockRoleRepo) GetByName(ctx context.Context, name string) (domain.Role, error) {
	return m.getByNameFn(ctx, name)
}
func (m *mockRoleRepo) AssignToUser(ctx context.Context, userID, roleID string) error {
	return m.assignFn(ctx, userID, roleID)
}
func (m *mockRoleRepo) GetUserRoles(ctx context.Context, userID string) ([]domain.Role, error) {
	return m.getUserRolesFn(ctx, userID)
}

// Compile-time proof the mocks satisfy the repository interfaces. If a method
// signature drifts, this fails to build — catching interface skew at test time.
var (
	_ repository.UserRepository   = (*mockUserRepo)(nil)
	_ repository.APIKeyRepository = (*mockAPIKeyRepo)(nil)
	_ repository.RoleRepository   = (*mockRoleRepo)(nil)
)

// testSecret is the JWT HMAC secret for all service-level tests.
// It is >= 32 bytes to satisfy the minimum key-length requirement
// enforced by validateHMACSecret in jwt.go (Finding 1 hardening).
const testSecret = "unit-test-jwt-secret-32bytes-ok!"

// ============================================================================
// CreateUser
// ============================================================================

func TestCreateUser_Success_HashesPasswordAndNeverLeaksPlaintext(t *testing.T) {
	const plaintext = "correct horse battery staple"
	var stored domain.User

	userRepo := &mockUserRepo{
		createFn: func(ctx context.Context, u domain.User) (domain.User, error) {
			stored = u // capture exactly what the service tried to persist
			u.ID = "user-1"
			u.CreatedAt = time.Now()
			return u, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	got, err := svc.CreateUser(context.Background(), domain.CreateUserInput{
		Email:    "ada@forgepoint.dev",
		Name:     "Ada",
		Password: plaintext,
		Team:     "platform",
	})
	if err != nil {
		t.Fatalf("CreateUser: unexpected error: %v", err)
	}
	if got.ID == "" {
		t.Error("returned user has empty ID; expected DB-populated ID")
	}

	// SECURITY: the plaintext must not appear anywhere in the stored row.
	if stored.PasswordHash == plaintext {
		t.Fatal("PasswordHash equals the plaintext password — not hashed")
	}
	if strings.Contains(stored.PasswordHash, plaintext) {
		t.Fatal("plaintext password leaked into the stored hash")
	}
	if got.PasswordHash == plaintext {
		t.Fatal("returned user carries plaintext password")
	}

	// REAL behavior: the stored hash must actually verify against the password.
	if err := bcrypt.CompareHashAndPassword([]byte(stored.PasswordHash), []byte(plaintext)); err != nil {
		t.Fatalf("stored bcrypt hash does not verify the original password: %v", err)
	}
	// And it must reject a wrong password.
	if err := bcrypt.CompareHashAndPassword([]byte(stored.PasswordHash), []byte("wrong")); err == nil {
		t.Fatal("stored hash verified an incorrect password")
	}
	// Email should be normalized to lowercase for case-insensitive uniqueness.
	if stored.Email != "ada@forgepoint.dev" {
		t.Errorf("stored email = %q, want lowercased", stored.Email)
	}
	if !stored.Active {
		t.Error("new user should be Active by default")
	}
}

func TestCreateUser_NormalizesEmailCase(t *testing.T) {
	var stored domain.User
	userRepo := &mockUserRepo{
		createFn: func(ctx context.Context, u domain.User) (domain.User, error) {
			stored = u
			return u, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	if _, err := svc.CreateUser(context.Background(), domain.CreateUserInput{
		Email: "  Ada@ForgePoint.DEV ", Name: "Ada", Password: "password123", Team: "x",
	}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if stored.Email != "ada@forgepoint.dev" {
		t.Errorf("stored email = %q, want trimmed+lowercased", stored.Email)
	}
}

func TestCreateUser_DuplicateEmail_MapsToDomainError(t *testing.T) {
	userRepo := &mockUserRepo{
		createFn: func(ctx context.Context, u domain.User) (domain.User, error) {
			// Simulate the unique-index violation from Postgres.
			return domain.User{}, repository.ErrEmailAlreadyExists
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	_, err := svc.CreateUser(context.Background(), domain.CreateUserInput{
		Email: "dup@forgepoint.dev", Name: "Dup", Password: "password123", Team: "x",
	})
	if !errors.Is(err, domain.ErrEmailAlreadyExists) {
		t.Fatalf("error = %v, want ErrEmailAlreadyExists", err)
	}
}

func TestCreateUser_RejectsEmptyEmailAndPassword(t *testing.T) {
	// The repo's createFn fails the test if called — proving validation happens
	// BEFORE any storage attempt.
	userRepo := &mockUserRepo{
		createFn: func(ctx context.Context, u domain.User) (domain.User, error) {
			t.Fatal("Create should not be called for invalid input")
			return domain.User{}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	cases := []domain.CreateUserInput{
		{Email: "", Password: "password123", Team: "x"},
		{Email: "a@b.dev", Password: "", Team: "x"},
	}
	for _, in := range cases {
		if _, err := svc.CreateUser(context.Background(), in); !errors.Is(err, domain.ErrValidation) {
			t.Errorf("input %+v: error = %v, want ErrValidation", in, err)
		}
	}
}

// ============================================================================
// CreateAPIKey
// ============================================================================

func TestCreateAPIKey_ReturnsRawOnce_StoresHashAndPrefix(t *testing.T) {
	apiKeyRepo := newMockAPIKeyRepo()
	svc := domain.NewAuthService(&mockUserRepo{}, apiKeyRepo, &mockRoleRepo{}, []byte(testSecret), time.Hour)

	scopes := []string{"models:read", "experiments:write"}
	key, raw, err := svc.CreateAPIKey(context.Background(), "user-1", scopes, nil)
	if err != nil {
		t.Fatalf("CreateAPIKey: unexpected error: %v", err)
	}

	// The raw key is the only time we ever see the secret. It must look like a
	// Forgepoint key and carry real entropy.
	if !strings.HasPrefix(raw, "fp_") {
		t.Errorf("raw key %q does not start with fp_", raw)
	}
	if len(raw) < 16 {
		t.Errorf("raw key %q is suspiciously short (low entropy?)", raw)
	}

	// SECURITY: the raw key must NOT be stored. The stored record holds only the
	// hash and the prefix.
	if key.KeyHash == raw {
		t.Fatal("stored KeyHash equals the raw key — must be a hash")
	}
	if strings.Contains(key.KeyHash, raw) {
		t.Fatal("raw key leaked into stored hash")
	}

	// REAL behavior: SHA-256 of the returned raw key must equal the stored hash.
	sum := sha256.Sum256([]byte(raw))
	wantHash := hex.EncodeToString(sum[:])
	if key.KeyHash != wantHash {
		t.Fatalf("KeyHash = %q, want sha256(raw) = %q", key.KeyHash, wantHash)
	}

	// Prefix must be 8 chars and be a genuine prefix of the raw key (for lookup).
	if len(key.KeyPrefix) != 8 {
		t.Errorf("KeyPrefix = %q, want 8 chars", key.KeyPrefix)
	}
	if !strings.HasPrefix(raw, key.KeyPrefix) {
		t.Errorf("KeyPrefix %q is not a prefix of raw key %q", key.KeyPrefix, raw)
	}

	// Scopes and owner must be carried through to storage.
	if key.UserID != "user-1" {
		t.Errorf("UserID = %q, want user-1", key.UserID)
	}
	if len(key.Scopes) != len(scopes) {
		t.Errorf("Scopes = %v, want %v", key.Scopes, scopes)
	}

	// And the repo must actually hold it under that hash.
	if _, err := apiKeyRepo.GetByKeyHash(context.Background(), wantHash); err != nil {
		t.Fatalf("stored key not retrievable by its hash: %v", err)
	}
}

func TestCreateAPIKey_DistinctKeysAcrossCalls(t *testing.T) {
	// crypto/rand must produce different keys each call. A static/predictable
	// key (e.g. from math/rand seeded once) would be a critical vulnerability.
	apiKeyRepo := newMockAPIKeyRepo()
	svc := domain.NewAuthService(&mockUserRepo{}, apiKeyRepo, &mockRoleRepo{}, []byte(testSecret), time.Hour)

	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		_, raw, err := svc.CreateAPIKey(context.Background(), "user-1", nil, nil)
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		if seen[raw] {
			t.Fatalf("duplicate raw key generated: %q", raw)
		}
		seen[raw] = true
	}
}

func TestCreateAPIKey_StoresExpiry(t *testing.T) {
	apiKeyRepo := newMockAPIKeyRepo()
	svc := domain.NewAuthService(&mockUserRepo{}, apiKeyRepo, &mockRoleRepo{}, []byte(testSecret), time.Hour)

	exp := time.Now().Add(24 * time.Hour)
	key, _, err := svc.CreateAPIKey(context.Background(), "user-1", nil, &exp)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if key.ExpiresAt == nil || !key.ExpiresAt.Equal(exp) {
		t.Errorf("ExpiresAt = %v, want %v", key.ExpiresAt, exp)
	}
}

// ============================================================================
// ValidateToken — API key path
// ============================================================================

func TestValidateToken_APIKey_Valid(t *testing.T) {
	apiKeyRepo := newMockAPIKeyRepo()
	svc := domain.NewAuthService(&mockUserRepo{}, apiKeyRepo, &mockRoleRepo{}, []byte(testSecret), time.Hour)

	// Create a key, then validate the raw value it returned.
	_, raw, err := svc.CreateAPIKey(context.Background(), "user-7", []string{"models:read"}, nil)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	claims, err := svc.ValidateToken(context.Background(), raw)
	if err != nil {
		t.Fatalf("ValidateToken(api key): unexpected error: %v", err)
	}
	if claims.TokenKind != domain.TokenKindAPIKey {
		t.Errorf("TokenKind = %q, want api_key", claims.TokenKind)
	}
	if claims.UserID != "user-7" {
		t.Errorf("UserID = %q, want user-7", claims.UserID)
	}
	if len(claims.Scopes) != 1 || claims.Scopes[0] != "models:read" {
		t.Errorf("Scopes = %v, want [models:read]", claims.Scopes)
	}
}

func TestValidateToken_APIKey_UnknownRejected(t *testing.T) {
	apiKeyRepo := newMockAPIKeyRepo() // empty store
	svc := domain.NewAuthService(&mockUserRepo{}, apiKeyRepo, &mockRoleRepo{}, []byte(testSecret), time.Hour)

	// A well-formed-looking but never-issued key must be rejected.
	_, err := svc.ValidateToken(context.Background(), "fp_totally-made-up-key")
	if !errors.Is(err, domain.ErrInvalidToken) {
		t.Fatalf("error = %v, want ErrInvalidToken", err)
	}
}

func TestValidateToken_APIKey_RevokedRejected(t *testing.T) {
	apiKeyRepo := newMockAPIKeyRepo()
	svc := domain.NewAuthService(&mockUserRepo{}, apiKeyRepo, &mockRoleRepo{}, []byte(testSecret), time.Hour)

	_, raw, err := svc.CreateAPIKey(context.Background(), "user-7", nil, nil)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	// Revoke the stored key directly in the mock store.
	sum := sha256.Sum256([]byte(raw))
	h := hex.EncodeToString(sum[:])
	k := apiKeyRepo.byHash[h]
	now := time.Now()
	k.RevokedAt = &now
	apiKeyRepo.byHash[h] = k

	if _, err := svc.ValidateToken(context.Background(), raw); !errors.Is(err, domain.ErrInvalidToken) {
		t.Fatalf("revoked key: error = %v, want ErrInvalidToken", err)
	}
}

func TestValidateToken_APIKey_ExpiredRejected(t *testing.T) {
	apiKeyRepo := newMockAPIKeyRepo()
	svc := domain.NewAuthService(&mockUserRepo{}, apiKeyRepo, &mockRoleRepo{}, []byte(testSecret), time.Hour)

	past := time.Now().Add(-time.Hour)
	_, raw, err := svc.CreateAPIKey(context.Background(), "user-7", nil, &past)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if _, err := svc.ValidateToken(context.Background(), raw); !errors.Is(err, domain.ErrInvalidToken) {
		t.Fatalf("expired key: error = %v, want ErrInvalidToken", err)
	}
}

func TestValidateToken_EmptyRejected(t *testing.T) {
	svc := domain.NewAuthService(&mockUserRepo{}, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)
	if _, err := svc.ValidateToken(context.Background(), ""); !errors.Is(err, domain.ErrInvalidToken) {
		t.Fatalf("empty token: error = %v, want ErrInvalidToken", err)
	}
}

func TestValidateToken_JWT_PreferredOverAPIKeyPath(t *testing.T) {
	// A real JWT must validate via the JWT branch, returning TokenKindJWT (the
	// API-key path would set TokenKindAPIKey).
	svc := domain.NewAuthService(&mockUserRepo{}, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	jwtStr, err := domain.GenerateToken(domain.TokenClaims{UserID: "user-9", Role: "engineer"}, []byte(testSecret), time.Hour)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	claims, err := svc.ValidateToken(context.Background(), jwtStr)
	if err != nil {
		t.Fatalf("ValidateToken(jwt): %v", err)
	}
	if claims.TokenKind != domain.TokenKindJWT {
		t.Errorf("TokenKind = %q, want jwt", claims.TokenKind)
	}
	if claims.UserID != "user-9" {
		t.Errorf("UserID = %q, want user-9", claims.UserID)
	}
}

// ============================================================================
// CheckPermission — RBAC with wildcards
// ============================================================================

func TestCheckPermission(t *testing.T) {
	tests := []struct {
		name        string
		permissions []domain.Permission
		resource    string
		action      string
		want        bool
	}{
		{
			name:        "exact match allows",
			permissions: []domain.Permission{{Resource: "models", Action: "write"}},
			resource:    "models", action: "write", want: true,
		},
		{
			name:        "resource mismatch denies",
			permissions: []domain.Permission{{Resource: "models", Action: "write"}},
			resource:    "experiments", action: "write", want: false,
		},
		{
			name:        "action mismatch denies",
			permissions: []domain.Permission{{Resource: "models", Action: "read"}},
			resource:    "models", action: "write", want: false,
		},
		{
			name:        "no permissions denies",
			permissions: nil,
			resource:    "models", action: "read", want: false,
		},
		{
			name:        "action wildcard allows any action on resource",
			permissions: []domain.Permission{{Resource: "models", Action: "*"}},
			resource:    "models", action: "delete", want: true,
		},
		{
			name:        "resource wildcard allows action on any resource",
			permissions: []domain.Permission{{Resource: "*", Action: "read"}},
			resource:    "billing", action: "read", want: true,
		},
		{
			name:        "admin star-star allows everything",
			permissions: []domain.Permission{{Resource: "*", Action: "*"}},
			resource:    "anything", action: "whatever", want: true,
		},
		{
			name:        "resource wildcard does not widen action",
			permissions: []domain.Permission{{Resource: "*", Action: "read"}},
			resource:    "billing", action: "write", want: false,
		},
		{
			name: "match found among multiple permissions",
			permissions: []domain.Permission{
				{Resource: "models", Action: "read"},
				{Resource: "experiments", Action: "write"},
			},
			resource: "experiments", action: "write", want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roleRepo := &mockRoleRepo{
				getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
					return []domain.Role{{Name: "test", Permissions: tt.permissions}}, nil
				},
			}
			// CheckPermission now loads the user to gate on Active (see the
			// suspended-user regression test). Supply an active user so this
			// table focuses purely on the role-matching wildcard semantics.
			userRepo := &mockUserRepo{
				getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
					return domain.User{ID: id, Active: true}, nil
				},
			}
			svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

			got, err := svc.CheckPermission(context.Background(), "user-1", tt.resource, tt.action)
			if err != nil {
				t.Fatalf("CheckPermission: unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("CheckPermission(%q,%q) = %v, want %v", tt.resource, tt.action, got, tt.want)
			}
		})
	}
}

// ============================================================================
// SECURITY REGRESSION: empty resource/action wildcard-match gap (Finding 2)
// ============================================================================
//
// WHY these tests exist:
//
//	permissionMatches uses a simple string comparison: perm.Resource == resource.
//	An empty string ("") compares equal to another empty string, so a
//	Permission{Resource:"",Action:""} stored in the DB (a corrupted or zero-value
//	row) would match a CheckPermission call with resource="" action="" — granting
//	access to a "resource" that is undefined.
//
//	More critically, if the caller accidentally passes empty resource or action
//	(e.g. a handler that forgot to extract a field from the proto request), the
//	empty string would match any permission that ALSO had an empty field, silently
//	granting more than intended.
//
//	The fix has two parts:
//	  1. CheckPermission (and the claims path) rejects the request up front if
//	     resource or action is empty — an empty coordinate is not a valid
//	     authorization request; denying it immediately is the safest stance.
//	  2. permissionMatches treats an empty stored Resource or Action as
//	     NON-matching: a wildcard must be the explicit "*" string, never "".
//
// WHY THE EMPTY STRING IS NOT A WILDCARD:
//
//	A wildcard is an explicit capability grant ("this role can do anything").
//	An empty field is a misconfiguration or programming error. Treating "" as a
//	wildcard silently promotes garbage data into a security grant. The principle
//	of least surprise — and least privilege — demands that ambiguous data denies.

// TestCheckPermission_EmptyResourceDenied verifies that a request with an empty
// resource string is always denied, even when the user is active and their role
// would otherwise grant everything.
func TestCheckPermission_EmptyResourceDenied(t *testing.T) {
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	roleRepo := &mockRoleRepo{
		getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
			// Admin grant (*:*) — the scope is intentionally maximal so any
			// denial below is attributable to the empty-resource guard, not the
			// role evaluation.
			return []domain.Role{{Name: "admin", Permissions: []domain.Permission{{Resource: "*", Action: "*"}}}}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	allowed, err := svc.CheckPermission(context.Background(), "user-1", "" /*resource*/, "write")
	if err != nil {
		t.Fatalf("CheckPermission(empty resource): unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("CheckPermission with empty resource was ALLOWED; empty resource must always deny")
	}
}

// TestCheckPermission_EmptyActionDenied is the symmetric case: empty action
// must also deny even with a full admin role.
func TestCheckPermission_EmptyActionDenied(t *testing.T) {
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	roleRepo := &mockRoleRepo{
		getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
			return []domain.Role{{Name: "admin", Permissions: []domain.Permission{{Resource: "*", Action: "*"}}}}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	allowed, err := svc.CheckPermission(context.Background(), "user-1", "models", "" /*action*/)
	if err != nil {
		t.Fatalf("CheckPermission(empty action): unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("CheckPermission with empty action was ALLOWED; empty action must always deny")
	}
}

// TestCheckPermission_ZeroValuePermissionDoesNotGrant verifies that a stored
// Permission{Resource:"",Action:""} (a corrupted/zero-value DB row) does NOT
// grant access when the request has valid non-empty resource and action.
//
// This tests the permissionMatches guard: empty stored fields must not be
// treated as wildcards — only the explicit "*" is a wildcard.
func TestCheckPermission_ZeroValuePermissionDoesNotGrant(t *testing.T) {
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	roleRepo := &mockRoleRepo{
		getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
			// A corrupted role with zero-value permissions. Without the fix,
			// permissionMatches returns true for the empty-string comparison.
			return []domain.Role{{
				Name:        "corrupted",
				Permissions: []domain.Permission{{Resource: "", Action: ""}},
			}}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	allowed, err := svc.CheckPermission(context.Background(), "user-1", "models", "write")
	if err != nil {
		t.Fatalf("CheckPermission(zero-value perm): unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("Permission{Resource:'',Action:''} granted access; zero-value stored permission must NOT match any request")
	}
}

// TestCheckPermission_EmptyScopeDoesNotGrant verifies that a scope string ":" or
// "" in an API key's scope list does NOT grant anything.
//
// scopeMatches calls permissionMatches with the parsed parts — after the fix,
// both halves being empty means the stored permission is invalid (non-matching).
func TestCheckPermissionForClaims_EmptyScopeDoesNotGrant(t *testing.T) {
	userRepo, roleRepo := activeAdminDeps()
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	cases := []struct {
		name   string
		scopes []string
	}{
		{"colon-only scope", []string{":"}},
		{"empty resource in scope", []string{":write"}},
		{"empty action in scope", []string{"models:"}},
		{"both empty in scope", []string{":", ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := domain.TokenClaims{
				UserID:    "user-1",
				TokenKind: domain.TokenKindAPIKey,
				Scopes:    tc.scopes,
			}
			// Request a valid (non-empty) resource/action — only the malformed
			// scope should cause denial, not the request itself.
			allowed, err := svc.CheckPermissionForClaims(context.Background(), claims, "models", "write")
			if err != nil {
				t.Fatalf("CheckPermissionForClaims(%v): unexpected error: %v", tc.scopes, err)
			}
			if allowed {
				t.Fatalf("scope %v granted access to models:write; malformed/empty scope parts must not match", tc.scopes)
			}
		})
	}
}

// TestCheckPermission_ExplicitWildcardStillWorks confirms that the empty-field
// guard does NOT break legitimate "*" wildcard grants. This is the non-regression
// side: existing behavior that must be preserved.
func TestCheckPermission_ExplicitWildcardStillWorks(t *testing.T) {
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	cases := []struct {
		name        string
		permissions []domain.Permission
		resource    string
		action      string
		want        bool
	}{
		{
			name:        "explicit *:* still grants everything",
			permissions: []domain.Permission{{Resource: "*", Action: "*"}},
			resource:    "models", action: "delete", want: true,
		},
		{
			name:        "explicit models:* still grants any action on models",
			permissions: []domain.Permission{{Resource: "models", Action: "*"}},
			resource:    "models", action: "purge", want: true,
		},
		{
			name:        "explicit *:read still grants read on any resource",
			permissions: []domain.Permission{{Resource: "*", Action: "read"}},
			resource:    "billing", action: "read", want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			roleRepo := &mockRoleRepo{
				getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
					return []domain.Role{{Name: "test", Permissions: tc.permissions}}, nil
				},
			}
			svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)
			got, err := svc.CheckPermission(context.Background(), "user-1", tc.resource, tc.action)
			if err != nil {
				t.Fatalf("CheckPermission: %v", err)
			}
			if got != tc.want {
				t.Errorf("CheckPermission(%q,%q) = %v, want %v", tc.resource, tc.action, got, tc.want)
			}
		})
	}
}

// REGRESSION (suspended-user authorization bypass): a deactivated account must
// be denied EVERY permission even when its role would otherwise allow — its
// outstanding JWT/API key stays cryptographically valid, so authorization (not
// just Login) has to re-check Active. Here the role is full admin (*:*) yet the
// user is inactive: the answer must be a clean deny, not an error.
func TestCheckPermission_InactiveUserDenied(t *testing.T) {
	roleRepo := &mockRoleRepo{
		getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
			return []domain.Role{{Name: "admin", Permissions: []domain.Permission{{Resource: "*", Action: "*"}}}}, nil
		},
	}
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: false}, nil // suspended / soft-deleted
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	allowed, err := svc.CheckPermission(context.Background(), "user-1", "models", "write")
	if err != nil {
		t.Fatalf("CheckPermission: unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("inactive user with admin role was ALLOWED; suspension must revoke all authorization")
	}
}

// A user the repo can't find is a clean deny (no active subject), NOT an error.
func TestCheckPermission_UnknownUserDenied(t *testing.T) {
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{}, repository.ErrNotFound
		},
	}
	// roleRepo must NOT be consulted once the user lookup fails — leaving
	// getUserRolesFn nil makes the mock panic if it's wrongly called.
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	allowed, err := svc.CheckPermission(context.Background(), "ghost", "models", "read")
	if err != nil {
		t.Fatalf("missing user should be a clean deny, got error: %v", err)
	}
	if allowed {
		t.Fatal("missing user was ALLOWED")
	}
}

// FAIL-CLOSED: a genuine user-lookup infrastructure error must propagate as an
// error (so the interceptor denies AND can surface Internal), not collapse into
// a (false, nil) "decision".
func TestCheckPermission_UserLookupErrorPropagates(t *testing.T) {
	boom := errors.New("db down")
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{}, boom
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	allowed, err := svc.CheckPermission(context.Background(), "user-1", "models", "read")
	if err == nil {
		t.Fatal("expected user-lookup error to propagate, got nil")
	}
	if allowed {
		t.Fatal("allowed should be false when the check failed")
	}
}

func TestCheckPermission_RepoErrorPropagates(t *testing.T) {
	// An infrastructure failure must surface as an error (fail-closed at the
	// interceptor), distinct from a clean (false, nil) deny decision.
	boom := errors.New("db down")
	roleRepo := &mockRoleRepo{
		getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
			return nil, boom
		},
	}
	// The user lookup (the new Active gate) must succeed so we reach — and
	// surface the error from — the role lookup specifically.
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	allowed, err := svc.CheckPermission(context.Background(), "user-1", "models", "read")
	if err == nil {
		t.Fatal("expected error to propagate, got nil")
	}
	if allowed {
		t.Fatal("allowed should be false when the check failed")
	}
}

// ============================================================================
// CheckPermissionForClaims — scope enforcement (role ∩ scope)
// ============================================================================

// activeAdminDeps builds repos where the user is active and the role grants
// EVERYTHING (*:*). With these, any denial must come from the SCOPE gate, not
// from the role or account-status gates — isolating the behavior under test.
func activeAdminDeps() (*mockUserRepo, *mockRoleRepo) {
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	roleRepo := &mockRoleRepo{
		getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
			return []domain.Role{{Name: "admin", Permissions: []domain.Permission{{Resource: "*", Action: "*"}}}}, nil
		},
	}
	return userRepo, roleRepo
}

// REGRESSION (scope authorization bypass): the headline finding. A key minted
// with ["models:read"] must be DENIED models:write even though the owner's role
// is full admin. Before the fix, scopes were stored and surfaced in TokenClaims
// but never enforced, so the key inherited the role's full power.
func TestCheckPermissionForClaims_NarrowScopeDeniesBeyondScope(t *testing.T) {
	userRepo, roleRepo := activeAdminDeps()
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	claims := domain.TokenClaims{
		UserID:    "user-1",
		TokenKind: domain.TokenKindAPIKey,
		Scopes:    []string{"models:read"},
	}

	// In scope: models:read → ALLOWED (role allows AND scope covers).
	allowed, err := svc.CheckPermissionForClaims(context.Background(), claims, "models", "read")
	if err != nil {
		t.Fatalf("CheckPermissionForClaims(models:read): %v", err)
	}
	if !allowed {
		t.Fatal("models:read should be allowed for a [models:read] key with an admin owner")
	}

	// Out of scope: models:write → DENIED despite the admin role. THIS is the fix.
	allowed, err = svc.CheckPermissionForClaims(context.Background(), claims, "models", "write")
	if err != nil {
		t.Fatalf("CheckPermissionForClaims(models:write): %v", err)
	}
	if allowed {
		t.Fatal("models:write was ALLOWED for a [models:read] key; key scopes are not enforced (authorization bypass)")
	}

	// Different resource entirely → also denied (scope is models:* only via read).
	allowed, err = svc.CheckPermissionForClaims(context.Background(), claims, "billing", "read")
	if err != nil {
		t.Fatalf("CheckPermissionForClaims(billing:read): %v", err)
	}
	if allowed {
		t.Fatal("billing:read was ALLOWED for a [models:read] key")
	}
}

// The effective grant is role ∩ scope: a broad scope cannot widen a narrow role.
// Here the KEY scope is *:* but the ROLE only allows models:read — the role gate
// must still deny models:write. (Guards against accidentally treating scope as
// the sole authority.)
func TestCheckPermissionForClaims_ScopeCannotExceedRole(t *testing.T) {
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	roleRepo := &mockRoleRepo{
		getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
			return []domain.Role{{Name: "viewer", Permissions: []domain.Permission{{Resource: "models", Action: "read"}}}}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	claims := domain.TokenClaims{UserID: "user-1", TokenKind: domain.TokenKindAPIKey, Scopes: []string{"*:*"}}

	allowed, err := svc.CheckPermissionForClaims(context.Background(), claims, "models", "write")
	if err != nil {
		t.Fatalf("CheckPermissionForClaims: %v", err)
	}
	if allowed {
		t.Fatal("a *:* scope widened a models:read role; effective grant must be role ∩ scope")
	}
}

// Scope wildcards must work and reuse the same semantics as role matching:
// "models:*" covers any action on models; "*:read" covers read on any resource.
func TestCheckPermissionForClaims_ScopeWildcards(t *testing.T) {
	userRepo, roleRepo := activeAdminDeps() // admin role so only the scope gate decides

	cases := []struct {
		name     string
		scopes   []string
		resource string
		action   string
		want     bool
	}{
		{"resource wildcard allows any action on models", []string{"models:*"}, "models", "delete", true},
		{"resource wildcard does not cross resources", []string{"models:*"}, "billing", "read", false},
		{"action wildcard allows read anywhere", []string{"*:read"}, "billing", "read", true},
		{"action wildcard does not widen action", []string{"*:read"}, "billing", "write", false},
		{"full wildcard allows everything", []string{"*:*"}, "anything", "whatever", true},
		{"first matching scope among several wins", []string{"experiments:write", "models:read"}, "models", "read", true},
		{"malformed scope matches nothing", []string{"models-read"}, "models", "read", false},
		{"empty scopes deny", nil, "models", "read", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)
			claims := domain.TokenClaims{UserID: "user-1", TokenKind: domain.TokenKindAPIKey, Scopes: tc.scopes}
			got, err := svc.CheckPermissionForClaims(context.Background(), claims, tc.resource, tc.action)
			if err != nil {
				t.Fatalf("CheckPermissionForClaims: %v", err)
			}
			if got != tc.want {
				t.Errorf("scopes=%v (%s:%s) = %v, want %v", tc.scopes, tc.resource, tc.action, got, tc.want)
			}
		})
	}
}

// A JWT (human session) is NOT scope-narrowed: it carries the user's full role
// authority. With an admin role and no scopes, a JWT must be allowed everything
// — the scope gate applies ONLY to API keys.
func TestCheckPermissionForClaims_JWTSkipsScopeGate(t *testing.T) {
	userRepo, roleRepo := activeAdminDeps()
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	claims := domain.TokenClaims{UserID: "user-1", TokenKind: domain.TokenKindJWT} // no scopes
	allowed, err := svc.CheckPermissionForClaims(context.Background(), claims, "models", "write")
	if err != nil {
		t.Fatalf("CheckPermissionForClaims(jwt): %v", err)
	}
	if !allowed {
		t.Fatal("a JWT with an admin role was denied; JWTs are not scope-narrowed")
	}
}

// Account-status and fail-closed contracts apply to the claims path too, since
// it delegates gates 1 and 2 to CheckPermission. An inactive user is denied
// even with an in-scope request and an admin role.
func TestCheckPermissionForClaims_InactiveUserDenied(t *testing.T) {
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: false}, nil
		},
	}
	roleRepo := &mockRoleRepo{
		getUserRolesFn: func(ctx context.Context, userID string) ([]domain.Role, error) {
			return []domain.Role{{Name: "admin", Permissions: []domain.Permission{{Resource: "*", Action: "*"}}}}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	claims := domain.TokenClaims{UserID: "user-1", TokenKind: domain.TokenKindAPIKey, Scopes: []string{"models:read"}}
	allowed, err := svc.CheckPermissionForClaims(context.Background(), claims, "models", "read")
	if err != nil {
		t.Fatalf("CheckPermissionForClaims: %v", err)
	}
	if allowed {
		t.Fatal("inactive user's API key was ALLOWED; suspension must revoke authorization on the claims path too")
	}
}

// ============================================================================
// Login
// ============================================================================

// makeUserWithPassword builds a User whose PasswordHash is a real bcrypt hash of
// password — so Login's bcrypt.CompareHashAndPassword exercises real crypto.
func makeUserWithPassword(t *testing.T, password string) domain.User {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt.GenerateFromPassword: %v", err)
	}
	return domain.User{
		ID:           "user-1",
		Email:        "ada@forgepoint.dev",
		Name:         "Ada",
		Team:         "platform",
		Role:         domain.Role{Name: "engineer"},
		PasswordHash: string(hash),
		Active:       true,
	}
}

func TestLogin_Success_ReturnsVerifiableJWT(t *testing.T) {
	const password = "s3cret-passw0rd"
	user := makeUserWithPassword(t, password)
	userRepo := &mockUserRepo{
		getByEmailFn: func(ctx context.Context, email string) (domain.User, error) {
			return user, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	tok, err := svc.Login(context.Background(), "ada@forgepoint.dev", password)
	if err != nil {
		t.Fatalf("Login: unexpected error: %v", err)
	}
	if tok == "" {
		t.Fatal("Login returned empty token")
	}

	// REAL behavior: the returned JWT must validate with the service's secret
	// and carry the user's identity claims.
	claims, err := domain.ValidateToken(tok, []byte(testSecret))
	if err != nil {
		t.Fatalf("issued token failed validation: %v", err)
	}
	if claims.UserID != user.ID {
		t.Errorf("token UserID = %q, want %q", claims.UserID, user.ID)
	}
	if claims.Email != user.Email {
		t.Errorf("token Email = %q, want %q", claims.Email, user.Email)
	}
	if claims.Role != "engineer" {
		t.Errorf("token Role = %q, want engineer", claims.Role)
	}
	if claims.Team != "platform" {
		t.Errorf("token Team = %q, want platform", claims.Team)
	}
}

func TestLogin_WrongPassword_GenericError(t *testing.T) {
	user := makeUserWithPassword(t, "the-right-password")
	userRepo := &mockUserRepo{
		getByEmailFn: func(ctx context.Context, email string) (domain.User, error) {
			return user, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	_, err := svc.Login(context.Background(), "ada@forgepoint.dev", "the-WRONG-password")
	if !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatalf("error = %v, want ErrInvalidCredentials", err)
	}
}

// THE anti-enumeration test: unknown email and wrong password must return the
// SAME error. If they differed, an attacker could enumerate valid accounts.
func TestLogin_UnknownEmail_SameErrorAsWrongPassword(t *testing.T) {
	// Case A: unknown email → repo returns ErrNotFound.
	unknownRepo := &mockUserRepo{
		getByEmailFn: func(ctx context.Context, email string) (domain.User, error) {
			return domain.User{}, repository.ErrNotFound
		},
	}
	svcUnknown := domain.NewAuthService(unknownRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)
	_, errUnknown := svcUnknown.Login(context.Background(), "nobody@forgepoint.dev", "whatever")

	// Case B: known email, wrong password.
	user := makeUserWithPassword(t, "right")
	knownRepo := &mockUserRepo{
		getByEmailFn: func(ctx context.Context, email string) (domain.User, error) {
			return user, nil
		},
	}
	svcKnown := domain.NewAuthService(knownRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)
	_, errWrong := svcKnown.Login(context.Background(), "ada@forgepoint.dev", "wrong")

	if !errors.Is(errUnknown, domain.ErrInvalidCredentials) {
		t.Errorf("unknown-email error = %v, want ErrInvalidCredentials", errUnknown)
	}
	if !errors.Is(errWrong, domain.ErrInvalidCredentials) {
		t.Errorf("wrong-password error = %v, want ErrInvalidCredentials", errWrong)
	}
	// Identical error VALUE — no distinguishing signal leaks to the attacker.
	if errUnknown.Error() != errWrong.Error() {
		t.Errorf("error messages differ (enumeration risk): unknown=%q wrong=%q",
			errUnknown.Error(), errWrong.Error())
	}
}

func TestLogin_InactiveUserRejected(t *testing.T) {
	user := makeUserWithPassword(t, "right")
	user.Active = false // suspended / soft-deleted
	userRepo := &mockUserRepo{
		getByEmailFn: func(ctx context.Context, email string) (domain.User, error) {
			return user, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), &mockRoleRepo{}, []byte(testSecret), time.Hour)

	if _, err := svc.Login(context.Background(), "ada@forgepoint.dev", "right"); !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatalf("inactive user: error = %v, want ErrInvalidCredentials", err)
	}
}

// ============================================================================
// AssignRole
// ============================================================================

func TestAssignRole_Success(t *testing.T) {
	var assignedUser, assignedRoleID string
	roleRepo := &mockRoleRepo{
		getByNameFn: func(ctx context.Context, name string) (domain.Role, error) {
			return domain.Role{ID: "role-admin", Name: name}, nil
		},
		assignFn: func(ctx context.Context, userID, roleID string) error {
			assignedUser, assignedRoleID = userID, roleID
			return nil
		},
	}
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	if err := svc.AssignRole(context.Background(), "user-1", "admin"); err != nil {
		t.Fatalf("AssignRole: unexpected error: %v", err)
	}
	// REAL behavior: the role NAME was resolved to an ID before assignment.
	if assignedUser != "user-1" {
		t.Errorf("assigned userID = %q, want user-1", assignedUser)
	}
	if assignedRoleID != "role-admin" {
		t.Errorf("assigned roleID = %q, want role-admin (resolved from name)", assignedRoleID)
	}
}

func TestAssignRole_UnknownRole(t *testing.T) {
	roleRepo := &mockRoleRepo{
		getByNameFn: func(ctx context.Context, name string) (domain.Role, error) {
			return domain.Role{}, repository.ErrNotFound
		},
	}
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{ID: id, Active: true}, nil
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	if err := svc.AssignRole(context.Background(), "user-1", "nonexistent"); !errors.Is(err, domain.ErrRoleNotFound) {
		t.Fatalf("error = %v, want ErrRoleNotFound", err)
	}
}

func TestAssignRole_UnknownUser(t *testing.T) {
	roleRepo := &mockRoleRepo{
		getByNameFn: func(ctx context.Context, name string) (domain.Role, error) {
			return domain.Role{ID: "role-admin", Name: name}, nil
		},
	}
	userRepo := &mockUserRepo{
		getByIDFn: func(ctx context.Context, id string) (domain.User, error) {
			return domain.User{}, repository.ErrNotFound
		},
	}
	svc := domain.NewAuthService(userRepo, newMockAPIKeyRepo(), roleRepo, []byte(testSecret), time.Hour)

	if err := svc.AssignRole(context.Background(), "ghost", "admin"); !errors.Is(err, domain.ErrUserNotFound) {
		t.Fatalf("error = %v, want ErrUserNotFound", err)
	}
}
