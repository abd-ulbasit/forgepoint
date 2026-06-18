// Package token persists the JWT issued by `fp login` and reloads it for every
// subsequent command so the user authenticates once per session.
//
// ============================================================================
// WHY A TOKEN STORE — AND WHY 0600
// ============================================================================
//
// A bearer JWT is a CREDENTIAL: anyone holding it can act as the user until it
// expires. So the on-disk store is treated like an SSH private key or a
// ~/.aws/credentials file:
//
//   - File mode 0600 (owner read/write only). Other local users cannot read it.
//   - Stored under ~/.forgepoint/ alongside the config, not in the repo or CWD.
//   - We persist the expiry timestamp next to the token so the CLI can give a
//     precise "token expired — run 'fp login'" message WITHOUT a server round
//     trip (a cheap, offline pre-check; the server is still the source of truth).
//
// FORMAT: a tiny JSON document. We could store the raw JWT as a bare string,
// but JSON lets us co-locate expires_at (and later: which server/profile the
// token was minted against) without a second file or fragile parsing.
//
// REAL-WORLD COMPARISON: `gh` stores its token in a keyring or hosts.yml;
// `kubectl` stores credentials in ~/.kube/config; `aws` in ~/.aws/credentials.
// We follow the same "dotfile dir under $HOME, owner-only perms" convention.
//
// SECURITY NOTE (interview): a file is less secure than an OS keychain. The
// honest tradeoff for a portfolio CLI is simplicity + cross-platform behavior
// over keychain integration. Production hardening would shell out to the macOS
// Keychain / libsecret / Windows Credential Manager (e.g. via the `keyring`
// pattern `docker login` uses).
// ============================================================================
package token

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// fileMode is owner read/write only. Group and world get nothing. This is the
// crux of treating the token as a secret on disk.
const fileMode = 0o600

// dirMode is owner read/write/execute only on the ~/.forgepoint directory.
const dirMode = 0o700

// ErrNoToken is returned when no token has been stored yet. Commands surface
// this as "run 'fp login' first".
var ErrNoToken = errors.New("no stored credentials")

// ErrExpired is returned when a token exists but its recorded expiry is in the
// past. This is an OFFLINE check — the server still revalidates on every call.
var ErrExpired = errors.New("stored token has expired")

// Stored is the on-disk shape. JSON keeps it forward-compatible: new fields
// (e.g. a "server" or "user_email" field) can be added without breaking older
// files, because unknown fields are ignored on read.
type Stored struct {
	AccessToken string    `json:"access_token"`
	ExpiresAt   time.Time `json:"expires_at"`
	Email       string    `json:"email,omitempty"`
}

// Store reads and writes the credential file. It is parameterized by path so
// tests can point it at a temp dir (and so the binary can honor $FP_HOME).
type Store struct {
	path string
}

// New builds a Store rooted at the given ~/.forgepoint directory. The token
// lives at <dir>/token.
func New(dir string) *Store {
	return &Store{path: filepath.Join(dir, "token")}
}

// Path exposes the credential file path (useful for `fp login` to tell the
// user where the token landed, and for tests).
func (s *Store) Path() string { return s.path }

// Save writes the token atomically-ish with 0600 perms, creating the parent
// directory if needed.
//
// WHY write-temp-then-rename: a crash mid-write must never leave a truncated
// (and thus unusable) credential file. We write to a sibling temp file, then
// os.Rename — which is atomic on the same filesystem — so the final file is
// either the old contents or the complete new contents, never a half-write.
func (s *Store) Save(st Stored) error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return fmt.Errorf("token: create config dir: %w", err)
	}

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("token: marshal: %w", err)
	}

	// Create the temp file with 0600 from the start (O_CREATE honors the mode),
	// so there is never a window where the credential is world-readable.
	tmp, err := os.CreateTemp(dir, ".token-*")
	if err != nil {
		return fmt.Errorf("token: create temp: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup if anything below fails before the rename succeeds.
	defer os.Remove(tmpName)

	if err := tmp.Chmod(fileMode); err != nil {
		tmp.Close()
		return fmt.Errorf("token: chmod temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("token: write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("token: close temp: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("token: rename into place: %w", err)
	}
	return nil
}

// Load reads the stored token. It returns ErrNoToken if the file is absent so
// callers can print the "run 'fp login' first" guidance. It does NOT check
// expiry — use LoadValid for that.
func (s *Store) Load() (Stored, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Stored{}, ErrNoToken
		}
		return Stored{}, fmt.Errorf("token: read: %w", err)
	}
	var st Stored
	if err := json.Unmarshal(data, &st); err != nil {
		// A corrupt file is functionally "no token" — tell the user to re-login.
		return Stored{}, fmt.Errorf("token: parse %s: %w (re-run 'fp login')", s.path, err)
	}
	if st.AccessToken == "" {
		return Stored{}, ErrNoToken
	}
	return st, nil
}

// LoadValid loads the token AND fails fast (ErrExpired) if its recorded expiry
// is in the past. A zero ExpiresAt is treated as "no client-side expiry known"
// and passes — the server remains the authority on validity.
//
// This offline pre-check exists purely for a better UX: it turns a confusing
// server-side Unauthenticated into a clear local "token expired, re-login"
// before we even dial.
func (s *Store) LoadValid(now time.Time) (Stored, error) {
	st, err := s.Load()
	if err != nil {
		return Stored{}, err
	}
	if !st.ExpiresAt.IsZero() && !st.ExpiresAt.After(now) {
		return Stored{}, ErrExpired
	}
	return st, nil
}

// Clear deletes the credential file (used by a future `fp logout`; also handy
// in tests). Absence is not an error.
func (s *Store) Clear() error {
	err := os.Remove(s.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("token: clear: %w", err)
	}
	return nil
}
