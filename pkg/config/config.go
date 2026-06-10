// Package config provides a generic, reflection-based configuration loader
// that reads values from environment variables with struct tag directives.
//
// ============================================================================
// ENV-BASED CONFIGURATION LOADER
// ============================================================================
//
// WHY env vars over config files:
//
//	This follows the 12-Factor App methodology (factor III: "Store config in
//	the environment"). Env vars are:
//	- Language-agnostic (same mechanism for Go, Python, Rust services)
//	- Easy to change between deploys without code changes
//	- Hard to accidentally check into source control (unlike config.yaml)
//	- Native to every deployment platform: K8s ConfigMaps/Secrets map to env
//	  vars, Docker supports --env, systemd has Environment=, etc.
//
// HOW IT WORKS:
//  1. Define a config struct with `env`, `default`, and `required` tags:
//     type Config struct {
//     Port int    `env:"PORT" default:"8080"`
//     DB   string `env:"DATABASE_URL" required:"true"`
//     }
//  2. Call Load[Config]("FP") → reads FP_PORT, FP_DATABASE_URL
//  3. For each field: look up PREFIX_ENVNAME, fallback to default, error if
//     required and empty
//  4. Type conversion: string→int, string→bool, string→float64, etc.
//
// ALTERNATIVES:
//
//	┌─────────────────┬──────────────────────────┬──────────────────────────┐
//	│ Approach         │ Pros                     │ Cons                     │
//	├─────────────────┼──────────────────────────┼──────────────────────────┤
//	│ ✅ Env vars     │ 12-Factor, K8s native,  │ No nesting, no comments, │
//	│ (struct tags)    │ simple, testable         │ flat key space           │
//	├─────────────────┼──────────────────────────┼──────────────────────────┤
//	│ Viper + YAML    │ Rich: multiple sources,  │ Heavy dependency, magic  │
//	│                  │ nested config, watchers  │ key resolution, implicit │
//	├─────────────────┼──────────────────────────┼──────────────────────────┤
//	│ envconfig (lib) │ Popular, well-tested     │ External dep for simple  │
//	│                  │                          │ functionality            │
//	├─────────────────┼──────────────────────────┼──────────────────────────┤
//	│ Kong CLI        │ CLI flags + env, help    │ Overkill for services    │
//	│                  │ text auto-gen            │ that only read env vars  │
//	└─────────────────┴──────────────────────────┴──────────────────────────┘
//
//	We roll our own (zero dependencies) because:
//	1. The implementation is ~100 lines — not worth an external dependency
//	2. We need exactly: env var lookup, prefix, defaults, required validation
//	3. No config file watching needed (K8s restarts pods on ConfigMap change)
//
// HOW UBER/NETFLIX DO IT:
//   - Uber: fx.Module config with env struct tags (similar to this)
//   - Netflix: Archaius (Java) — dynamic config with polling (overkill for Go)
//   - Kubernetes: ConfigMap → env vars is the standard pattern
//   - AWS Lambda: Pure env vars, no config files
//
// USAGE:
//
//	type ServiceConfig struct {
//	    config.BaseConfig
//	    JWTSecret string `env:"JWT_SECRET" required:"true"`
//	}
//
//	cfg, err := config.Load[ServiceConfig]("FP")
//
// ============================================================================
package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// BaseConfig contains configuration fields common to every Forgepoint service.
// Services embed this struct and add their own fields:
//
//	type AuthConfig struct {
//	    config.BaseConfig
//	    JWTSecret string `env:"JWT_SECRET" required:"true"`
//	}
//
// WHY these specific fields:
//   - Port: HTTP health check / metrics endpoint
//   - GRPCPort: Main service API (gRPC is always on a separate port from HTTP
//     because gRPC uses HTTP/2 and health checks use HTTP/1.1)
//   - LogLevel: Controls slog verbosity (debug/info/warn/error)
//   - OTelEndpoint: OpenTelemetry collector for traces/metrics
//   - NATSUrl: Async event bus connection
//   - DatabaseURL: Per-service PostgreSQL database
type BaseConfig struct {
	Port         int    `env:"PORT" default:"8080"`
	GRPCPort     int    `env:"GRPC_PORT" default:"9090"`
	LogLevel     string `env:"LOG_LEVEL" default:"info"`
	OTelEndpoint string `env:"OTEL_ENDPOINT" default:"localhost:4317"`
	NATSUrl      string `env:"NATS_URL" default:"nats://localhost:4222"`
	DatabaseURL  string `env:"DATABASE_URL"`
}

// Load reads environment variables into a struct of type T.
//
// The prefix parameter is prepended to each env tag value with an underscore
// separator. For example, Load[Config]("FP") reads FP_PORT for a field
// tagged `env:"PORT"`.
//
// Struct tag reference:
//   - env:"NAME"         — env var name (joined with prefix as PREFIX_NAME)
//   - default:"value"    — fallback if env var is empty/unset
//   - required:"true"    — error if both env var and default are empty
//
// Returns a pointer to the populated struct, or an error if:
//   - A required field has no value
//   - A value cannot be parsed into the field's type (e.g., "abc" for int)
//
// Supports types: string, all signed/unsigned int widths, float32/float64, bool,
// time.Duration ("30s", "1h30m"), and []string (comma-separated).
// Supports embedded structs (like BaseConfig).
func Load[T any](prefix string) (*T, error) {
	var cfg T
	v := reflect.ValueOf(&cfg).Elem()

	// Guard: T must be a struct. Without this, calling Load[int]("FP") would
	// reach loadStruct → t.NumField() and PANIC on a non-struct kind. We turn
	// that programmer error into a clear, recoverable error instead of a crash.
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("config: Load requires a struct type, got %s", v.Kind())
	}

	if err := loadStruct(v, prefix); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// loadStruct processes a struct value, handling embedded structs recursively.
//
// WHY recursion for embedded structs:
//
//	Go's reflect package treats embedded (anonymous) fields as regular fields
//	with the embedded type. To support:
//	  type AuthConfig struct {
//	      BaseConfig           // ← embedded, fields should be flattened
//	      JWTSecret string     // ← own field
//	  }
//	We detect struct-typed fields and recurse into them, so BaseConfig.Port
//	is treated the same as AuthConfig.JWTSecret — both read from env vars.
func loadStruct(v reflect.Value, prefix string) error {
	t := v.Type()

	for i := range t.NumField() {
		field := t.Field(i)
		fieldVal := v.Field(i)

		// Recurse into embedded structs (anonymous fields).
		// This flattens the env var namespace — embedded fields are treated
		// as if they were defined directly on the outer struct.
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			if err := loadStruct(fieldVal, prefix); err != nil {
				return err
			}
			continue
		}

		// Skip fields without env tag — they're not configurable from env.
		envName := field.Tag.Get("env")
		if envName == "" {
			continue
		}

		// Build full env var name: PREFIX_ENVNAME
		// e.g., prefix="FP", envName="PORT" → "FP_PORT"
		fullEnvName := prefix + "_" + envName

		// Look up the env var. os.Getenv returns "" for both unset and
		// explicitly empty vars — we treat both the same (fall through to
		// default or required check).
		value := os.Getenv(fullEnvName)

		// Fall back to default if env var is empty.
		if value == "" {
			value = field.Tag.Get("default")
		}

		// Check required constraint.
		if value == "" && field.Tag.Get("required") == "true" {
			return fmt.Errorf("config: required field %s (env: %s) is not set",
				field.Name, fullEnvName)
		}

		// Skip if still empty (not required, no default → keep zero value).
		if value == "" {
			continue
		}

		// Parse and set the field value based on its type.
		if err := setField(fieldVal, value, field.Name, fullEnvName); err != nil {
			return err
		}
	}

	return nil
}

// setField parses a string value and sets it on a reflect.Value.
//
// WHY we handle each type explicitly instead of using fmt.Sscanf:
//   - strconv functions return typed errors ("cannot parse X as int")
//   - We can provide better error messages with field name and env var name
//   - fmt.Sscanf is slower and less precise for type conversion
func setField(fieldVal reflect.Value, value, fieldName, envName string) error {
	// time.Duration is an int64 underneath, so the Kind switch below would treat
	// it as a plain integer and reject "30s". We MUST special-case it before the
	// switch, comparing the concrete type (not the Kind), so durations parse via
	// time.ParseDuration ("30s", "1h30m", "500ms"). This is the single most
	// common config type a microservice needs beyond the primitives.
	if fieldVal.Type() == reflect.TypeFor[time.Duration]() {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("config: field %s (env: %s): cannot parse %q as duration: %w",
				fieldName, envName, value, err)
		}
		fieldVal.SetInt(int64(parsed))
		return nil
	}

	switch fieldVal.Kind() {
	case reflect.String:
		fieldVal.SetString(value)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// BitSize from the field's type so we reject overflow (e.g. "70000"
		// into an int16) at parse time with a clear error instead of silently
		// wrapping. SetInt itself does not check overflow, so ParseInt's
		// bitSize is our guard.
		parsed, err := strconv.ParseInt(value, 10, fieldVal.Type().Bits())
		if err != nil {
			return fmt.Errorf("config: field %s (env: %s): cannot parse %q as int: %w",
				fieldName, envName, value, err)
		}
		fieldVal.SetInt(parsed)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		parsed, err := strconv.ParseUint(value, 10, fieldVal.Type().Bits())
		if err != nil {
			return fmt.Errorf("config: field %s (env: %s): cannot parse %q as uint: %w",
				fieldName, envName, value, err)
		}
		fieldVal.SetUint(parsed)

	case reflect.Float32, reflect.Float64:
		parsed, err := strconv.ParseFloat(value, fieldVal.Type().Bits())
		if err != nil {
			return fmt.Errorf("config: field %s (env: %s): cannot parse %q as float: %w",
				fieldName, envName, value, err)
		}
		fieldVal.SetFloat(parsed)

	case reflect.Bool:
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("config: field %s (env: %s): cannot parse %q as bool: %w",
				fieldName, envName, value, err)
		}
		fieldVal.SetBool(parsed)

	case reflect.Slice:
		// Support comma-separated lists: "scope1,scope2,scope3".
		// Only string slices are supported. A non-string slice ([]int, etc.)
		// previously fell through and SILENTLY set nothing — a config that
		// looks applied but isn't. We now fail loudly so the misconfiguration
		// surfaces at startup, not at 3am in production.
		if fieldVal.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("config: field %s (env: %s): unsupported slice element type %s (only []string)",
				fieldName, envName, fieldVal.Type().Elem().Kind())
		}
		parts := strings.Split(value, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		fieldVal.Set(reflect.ValueOf(parts))

	default:
		return fmt.Errorf("config: field %s (env: %s): unsupported type %s",
			fieldName, envName, fieldVal.Kind())
	}

	return nil
}
