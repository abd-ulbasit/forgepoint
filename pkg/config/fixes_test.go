package config_test

import (
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/config"
)

// These tests cover the loader gaps found in review: time.Duration support,
// non-string slice rejection, the non-struct guard, and integer overflow.

type durationConfig struct {
	Timeout time.Duration `env:"TIMEOUT" default:"30s"`
}

func TestLoad_Duration(t *testing.T) {
	t.Setenv("APP_TIMEOUT", "1m30s")

	cfg, err := config.Load[durationConfig]("APP")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Timeout != 90*time.Second {
		t.Fatalf("Timeout = %v, want 90s", cfg.Timeout)
	}
}

func TestLoad_DurationDefault(t *testing.T) {
	// APP_TIMEOUT unset → falls back to the default tag "30s".
	cfg, err := config.Load[durationConfig]("APP")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Timeout != 30*time.Second {
		t.Fatalf("Timeout = %v, want 30s", cfg.Timeout)
	}
}

func TestLoad_DurationInvalid(t *testing.T) {
	t.Setenv("APP_TIMEOUT", "not-a-duration")

	if _, err := config.Load[durationConfig]("APP"); err == nil {
		t.Fatal("expected error for invalid duration, got nil")
	}
}

func TestLoad_NonStructTypeIsError(t *testing.T) {
	// Previously panicked in loadStruct via NumField; must be a clean error now.
	if _, err := config.Load[int]("APP"); err == nil {
		t.Fatal("expected error loading into non-struct type, got nil")
	}
}

type stringSliceConfig struct {
	Scopes []string `env:"SCOPES"`
}

func TestLoad_StringSlice(t *testing.T) {
	t.Setenv("APP_SCOPES", "read, write , admin")

	cfg, err := config.Load[stringSliceConfig]("APP")
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	want := []string{"read", "write", "admin"}
	if len(cfg.Scopes) != len(want) {
		t.Fatalf("Scopes = %v, want %v", cfg.Scopes, want)
	}
	for i := range want {
		if cfg.Scopes[i] != want[i] {
			t.Fatalf("Scopes[%d] = %q, want %q", i, cfg.Scopes[i], want[i])
		}
	}
}

type intSliceConfig struct {
	Nums []int `env:"NUMS"`
}

func TestLoad_NonStringSliceIsError(t *testing.T) {
	t.Setenv("APP_NUMS", "1,2,3")

	// Used to silently set nothing; must now fail loudly.
	if _, err := config.Load[intSliceConfig]("APP"); err == nil {
		t.Fatal("expected error for non-string slice, got nil")
	}
}

type smallIntConfig struct {
	N int8 `env:"N"`
}

func TestLoad_IntOverflowIsError(t *testing.T) {
	t.Setenv("APP_N", "70000") // overflows int8

	if _, err := config.Load[smallIntConfig]("APP"); err == nil {
		t.Fatal("expected overflow error for int8, got nil")
	}
}
