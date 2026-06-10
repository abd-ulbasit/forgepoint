package config_test

import (
	"testing"

	"github.com/abd-ulbasit/forgepoint/pkg/config"
)

// ============================================================================
// CONFIG LOADER TESTS
// ============================================================================
//
// Tests follow the pattern: set env vars → call Load → assert result.
// Each test uses t.Setenv which automatically cleans up after the test
// (available since Go 1.17), so tests don't leak env state.
//
// WHY test env-based config:
//   The config loader is the foundation of every service. If it silently
//   ignores a required field or misparses a value, the service starts with
//   wrong settings — a class of bugs that's hard to debug in production
//   because the service "works" but behaves incorrectly.
// ============================================================================

// TestConfig is a sample config struct for testing.
// Each field demonstrates a different config feature.
type TestConfig struct {
	Port        int    `env:"PORT" default:"8080"`
	Host        string `env:"HOST" default:"localhost"`
	DatabaseURL string `env:"DATABASE_URL" required:"true"`
	Debug       bool   `env:"DEBUG" default:"false"`
	LogLevel    string `env:"LOG_LEVEL" default:"info"`
	Workers     int    `env:"WORKERS"`
}

func TestLoad_WithAllEnvVars(t *testing.T) {
	// Arrange: set all env vars with the FP_ prefix
	t.Setenv("FP_PORT", "9090")
	t.Setenv("FP_HOST", "0.0.0.0")
	t.Setenv("FP_DATABASE_URL", "postgres://localhost:5432/fp")
	t.Setenv("FP_DEBUG", "true")
	t.Setenv("FP_LOG_LEVEL", "debug")
	t.Setenv("FP_WORKERS", "4")

	// Act
	cfg, err := config.Load[TestConfig]("FP")

	// Assert
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != 9090 {
		t.Errorf("Port = %d, want 9090", cfg.Port)
	}
	if cfg.Host != "0.0.0.0" {
		t.Errorf("Host = %q, want %q", cfg.Host, "0.0.0.0")
	}
	if cfg.DatabaseURL != "postgres://localhost:5432/fp" {
		t.Errorf("DatabaseURL = %q, want postgres://localhost:5432/fp", cfg.DatabaseURL)
	}
	if !cfg.Debug {
		t.Error("Debug = false, want true")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "debug")
	}
	if cfg.Workers != 4 {
		t.Errorf("Workers = %d, want 4", cfg.Workers)
	}
}

func TestLoad_UsesDefaults(t *testing.T) {
	// Arrange: only set the required field, let others use defaults
	t.Setenv("FP_DATABASE_URL", "postgres://localhost:5432/fp")

	// Act
	cfg, err := config.Load[TestConfig]("FP")

	// Assert
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want default 8080", cfg.Port)
	}
	if cfg.Host != "localhost" {
		t.Errorf("Host = %q, want default %q", cfg.Host, "localhost")
	}
	if cfg.Debug {
		t.Error("Debug = true, want default false")
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want default %q", cfg.LogLevel, "info")
	}
	// Workers has no default and isn't required → zero value
	if cfg.Workers != 0 {
		t.Errorf("Workers = %d, want zero value 0", cfg.Workers)
	}
}

func TestLoad_RequiredFieldMissing(t *testing.T) {
	// Arrange: do NOT set DATABASE_URL which is required
	// (clear it in case it's set in the environment)
	t.Setenv("FP_DATABASE_URL", "")

	// Act
	_, err := config.Load[TestConfig]("FP")

	// Assert: should return an error about the missing required field
	if err == nil {
		t.Fatal("expected error for missing required field, got nil")
	}
}

func TestLoad_InvalidIntValue(t *testing.T) {
	// Arrange: set PORT to a non-integer value
	t.Setenv("FP_PORT", "not-a-number")
	t.Setenv("FP_DATABASE_URL", "postgres://localhost/fp")

	// Act
	_, err := config.Load[TestConfig]("FP")

	// Assert
	if err == nil {
		t.Fatal("expected error for invalid int value, got nil")
	}
}

func TestLoad_InvalidBoolValue(t *testing.T) {
	// Arrange: set DEBUG to an invalid bool value
	t.Setenv("FP_DEBUG", "not-a-bool")
	t.Setenv("FP_DATABASE_URL", "postgres://localhost/fp")

	// Act
	_, err := config.Load[TestConfig]("FP")

	// Assert
	if err == nil {
		t.Fatal("expected error for invalid bool value, got nil")
	}
}

// ============================================================================
// BaseConfig Tests — the standard config struct every service embeds
// ============================================================================

func TestLoad_BaseConfig_Defaults(t *testing.T) {
	// Act: load BaseConfig with no env vars set
	cfg, err := config.Load[config.BaseConfig]("FP")

	// Assert: all defaults should apply, no required fields in BaseConfig
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want default 8080", cfg.Port)
	}
	if cfg.GRPCPort != 9090 {
		t.Errorf("GRPCPort = %d, want default 9090", cfg.GRPCPort)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want default %q", cfg.LogLevel, "info")
	}
}

func TestLoad_DifferentPrefix(t *testing.T) {
	// Arrange: use a different prefix to verify prefix handling
	t.Setenv("AUTH_DATABASE_URL", "postgres://auth-db:5432/auth")

	// Act
	cfg, err := config.Load[TestConfig]("AUTH")

	// Assert
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DatabaseURL != "postgres://auth-db:5432/auth" {
		t.Errorf("DatabaseURL = %q, want postgres://auth-db:5432/auth", cfg.DatabaseURL)
	}
}

// NestedConfig tests embedded struct support.
type NestedConfig struct {
	config.BaseConfig
	JWTSecret string `env:"JWT_SECRET" required:"true"`
	MaxUsers  int    `env:"MAX_USERS" default:"1000"`
}

func TestLoad_NestedConfig(t *testing.T) {
	// Arrange
	t.Setenv("FP_PORT", "3000")
	t.Setenv("FP_JWT_SECRET", "super-secret-key")

	// Act
	cfg, err := config.Load[NestedConfig]("FP")

	// Assert
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Embedded BaseConfig field
	if cfg.Port != 3000 {
		t.Errorf("Port = %d, want 3000", cfg.Port)
	}
	// Own field
	if cfg.JWTSecret != "super-secret-key" {
		t.Errorf("JWTSecret = %q, want %q", cfg.JWTSecret, "super-secret-key")
	}
	// Default
	if cfg.MaxUsers != 1000 {
		t.Errorf("MaxUsers = %d, want default 1000", cfg.MaxUsers)
	}
}
