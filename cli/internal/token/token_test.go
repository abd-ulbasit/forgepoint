package token

import (
	"errors"
	"os"
	"runtime"
	"testing"
	"time"
)

// TestSaveLoadRoundTrip verifies a token survives a write/read cycle intact.
func TestSaveLoadRoundTrip(t *testing.T) {
	s := New(t.TempDir())
	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	in := Stored{AccessToken: "jwt-abc", ExpiresAt: exp, Email: "a@b.co"}

	if err := s.Save(in); err != nil {
		t.Fatalf("Save: %v", err)
	}
	out, err := s.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if out.AccessToken != in.AccessToken {
		t.Errorf("token = %q, want %q", out.AccessToken, in.AccessToken)
	}
	if !out.ExpiresAt.Equal(exp) {
		t.Errorf("expiry = %v, want %v", out.ExpiresAt, exp)
	}
	if out.Email != in.Email {
		t.Errorf("email = %q, want %q", out.Email, in.Email)
	}
}

// TestSaveUses0600 is the security-critical test: the credential file must be
// owner-read/write only. A regression here would leak the token to other local
// users. (Skipped on Windows where Unix perm bits don't apply.)
func TestSaveUses0600(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file mode not meaningful on Windows")
	}
	s := New(t.TempDir())
	if err := s.Save(Stored{AccessToken: "secret"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	info, err := os.Stat(s.Path())
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("token file mode = %o, want 0600", perm)
	}
}

// TestLoadMissingReturnsErrNoToken drives the "run fp login first" path.
func TestLoadMissingReturnsErrNoToken(t *testing.T) {
	s := New(t.TempDir())
	if _, err := s.Load(); !errors.Is(err, ErrNoToken) {
		t.Errorf("Load on empty dir = %v, want ErrNoToken", err)
	}
}

// TestLoadValidExpiry checks the offline expiry pre-check both ways.
func TestLoadValidExpiry(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)

	t.Run("expired token rejected", func(t *testing.T) {
		s := New(t.TempDir())
		if err := s.Save(Stored{AccessToken: "x", ExpiresAt: now.Add(-time.Minute)}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.LoadValid(now); !errors.Is(err, ErrExpired) {
			t.Errorf("LoadValid = %v, want ErrExpired", err)
		}
	})

	t.Run("valid token accepted", func(t *testing.T) {
		s := New(t.TempDir())
		if err := s.Save(Stored{AccessToken: "x", ExpiresAt: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.LoadValid(now); err != nil {
			t.Errorf("LoadValid = %v, want nil", err)
		}
	})

	t.Run("zero expiry passes (server is authority)", func(t *testing.T) {
		s := New(t.TempDir())
		if err := s.Save(Stored{AccessToken: "x"}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.LoadValid(now); err != nil {
			t.Errorf("LoadValid with zero expiry = %v, want nil", err)
		}
	})
}

// TestLoadCorruptFile ensures a garbage file is a clear error, not a panic, and
// not silently treated as a valid empty token.
func TestLoadCorruptFile(t *testing.T) {
	s := New(t.TempDir())
	if err := os.WriteFile(s.Path(), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Error("expected error loading corrupt token, got nil")
	}
}

// TestEmptyTokenTreatedAsNoToken: a file present but with an empty access_token
// is functionally "not logged in".
func TestEmptyTokenTreatedAsNoToken(t *testing.T) {
	s := New(t.TempDir())
	if err := os.WriteFile(s.Path(), []byte(`{"access_token":""}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); !errors.Is(err, ErrNoToken) {
		t.Errorf("Load empty-token file = %v, want ErrNoToken", err)
	}
}

// TestClear removes the file and tolerates absence.
func TestClear(t *testing.T) {
	s := New(t.TempDir())
	if err := s.Save(Stored{AccessToken: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := s.Load(); !errors.Is(err, ErrNoToken) {
		t.Errorf("after Clear, Load = %v, want ErrNoToken", err)
	}
	// Clearing again (file already gone) is not an error.
	if err := s.Clear(); err != nil {
		t.Errorf("Clear on absent file = %v, want nil", err)
	}
}

// TestSaveCreatesDirWith0700 verifies the ~/.forgepoint dir is created with
// owner-only perms when missing.
func TestSaveCreatesDirWith0700(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file mode not meaningful on Windows")
	}
	base := t.TempDir()
	// Point the store at a not-yet-existing subdir.
	s := New(base + "/sub/.forgepoint")
	if err := s.Save(Stored{AccessToken: "x"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	info, err := os.Stat(base + "/sub/.forgepoint")
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("dir mode = %o, want 0700", perm)
	}
}
