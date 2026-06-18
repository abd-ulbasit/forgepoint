// Package config resolves, per service, the gRPC address the CLI should dial,
// following a strict precedence chain, and locates the ~/.forgepoint home dir.
//
// ============================================================================
// CONFIG PRECEDENCE — flag > env > config-file > default
// ============================================================================
//
// The CLI runs on a developer laptop and talks to services that are usually
// reached via `kubectl port-forward`. The same `fp` binary must work whether
// the user is hitting localhost, a shared dev cluster, or a teammate's tunnel.
// So every service address is resolvable from four sources, highest priority
// first:
//
//  1. FLAG        --addr (global) or --<svc>-addr   ← most explicit, wins
//  2. ENV         FP_<SVC>_ADDR (e.g. FP_AUTH_ADDR=localhost:9090)
//  3. CONFIG FILE ~/.forgepoint/config   (key = value, addr.<svc> = host:port)
//  4. DEFAULT     localhost:<conventional per-service port>
//
// WHY THIS ORDER (interview framing): the rule of thumb across mature CLIs
// (kubectl, gh, aws, docker) is "the more immediate and explicit the source,
// the higher it ranks." A flag is typed for THIS invocation, so it must beat a
// persisted file. Env sits between: it's per-shell-session (more ephemeral than
// a file) but less explicit than a flag. The file is the durable baseline, and
// a compiled-in default guarantees the CLI works with zero configuration.
//
// WHY A HAND-ROLLED RESOLVER instead of pkg/config: pkg/config is an env-only,
// reflection-based loader for SERVERS (12-factor: config from the environment).
// A CLI's needs are different — it must merge FLAGS and a user dotfile on top
// of env, with per-service keys. Reusing the server loader would force flags
// and files into env vars first, losing the clean precedence. Different problem,
// different (still zero-dependency, stdlib-only) tool.
//
// ============================================================================
// THE PORT-FORWARD MODEL (documented default behavior)
// ============================================================================
//
// All Forgepoint services bind gRPC on container port 9090 (BaseConfig default).
// In-cluster they are distinguished by service name; from a laptop you forward
// each to a DISTINCT localhost port, e.g.:
//
//	kubectl port-forward -n fp-system svc/fp-auth     9090:9090
//	kubectl port-forward -n fp-system svc/fp-registry 9091:9090
//	kubectl port-forward -n fp-system svc/fp-pipeline-orchestrator 9092:9090
//	kubectl port-forward -n fp-system svc/fp-experiment-tracker    9093:9090
//	kubectl port-forward -n fp-system svc/fp-model-monitor         9094:9090
//	kubectl port-forward -n fp-system svc/fp-billing               9095:9090
//
// Those local ports are the compiled-in DEFAULTS below. If you forward to
// different ports, override per service via flag/env/file. A single
// --addr/FP_ADDR overrides ALL services at once (handy when you front them with
// one gateway).
// ============================================================================
package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Service is a typed key for each backend the CLI can talk to. Using a small
// enum-like string type (rather than bare strings everywhere) keeps the env-var
// names, file keys, and default ports in one table and prevents typos.
type Service string

const (
	ServiceAuth       Service = "auth"
	ServiceRegistry   Service = "registry"
	ServicePipeline   Service = "pipeline"
	ServiceExperiment Service = "experiment"
	ServiceMonitor    Service = "monitor"
	ServiceBilling    Service = "billing"
)

// defaultAddrs are the compiled-in localhost defaults — the lowest-priority
// source. They assume the port-forward layout documented in the package header.
var defaultAddrs = map[Service]string{
	ServiceAuth:       "localhost:9090",
	ServiceRegistry:   "localhost:9091",
	ServicePipeline:   "localhost:9092",
	ServiceExperiment: "localhost:9093",
	ServiceMonitor:    "localhost:9094",
	ServiceBilling:    "localhost:9095",
}

// envVar returns the per-service env var name, e.g. ServiceAuth → FP_AUTH_ADDR.
func envVar(s Service) string {
	return "FP_" + strings.ToUpper(string(s)) + "_ADDR"
}

// Resolver answers "what address should I dial for service X?" by applying the
// precedence chain. It is constructed once (in main) from the parsed global
// flag, the process environment, and the parsed config file, then consulted by
// every command.
//
// It is intentionally a plain struct with no I/O of its own beyond the file
// already read into fileAddrs — this makes the precedence logic a pure function
// of its inputs and therefore trivially unit-testable.
type Resolver struct {
	// globalAddr is the value of --addr / FP_ADDR: a single override applied to
	// EVERY service. Empty means "not set".
	globalAddr string

	// perServiceFlag holds --<svc>-addr flag values keyed by service. Populated
	// only for services whose flag the user actually set.
	perServiceFlag map[Service]string

	// env is the lookup function for environment variables. Injectable so tests
	// don't have to mutate the real process environment.
	env func(string) string

	// fileAddrs holds addresses parsed from ~/.forgepoint/config (addr.<svc>).
	fileAddrs map[Service]string

	// fileGlobal holds a file-level `addr = host:port` (applies to all services,
	// ranking between per-service file entries and defaults).
	fileGlobal string
}

// Options carries everything main() has gathered to build a Resolver.
type Options struct {
	GlobalAddr     string             // --addr / FP_ADDR
	PerServiceFlag map[Service]string // --auth-addr, --registry-addr, ...
	Env            func(string) string
	FilePath       string // path to ~/.forgepoint/config ("" to skip)
}

// NewResolver builds a Resolver, reading the config file if present. A missing
// file is NOT an error — config files are optional; defaults cover everything.
func NewResolver(opts Options) (*Resolver, error) {
	envFn := opts.Env
	if envFn == nil {
		envFn = os.Getenv
	}
	r := &Resolver{
		globalAddr:     opts.GlobalAddr,
		perServiceFlag: opts.PerServiceFlag,
		env:            envFn,
		fileAddrs:      map[Service]string{},
	}
	if opts.FilePath != "" {
		if err := r.loadFile(opts.FilePath); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// SetGlobalFlag records the parsed --addr value. main calls this AFTER flag
// parsing (the resolver is built before flags are known, from env + file, then
// the flag layer is folded in on top). Empty string means the flag was unset.
func (r *Resolver) SetGlobalFlag(addr string) {
	r.globalAddr = addr
}

// SetPerServiceFlag records parsed --<svc>-addr values. Only non-empty entries
// take effect, so passing every flag (even unset ones as "") is safe.
func (r *Resolver) SetPerServiceFlag(flags map[Service]string) {
	if r.perServiceFlag == nil {
		r.perServiceFlag = map[Service]string{}
	}
	for svc, v := range flags {
		if v != "" {
			r.perServiceFlag[svc] = v
		}
	}
}

// Addr applies the precedence chain for one service and returns the address to
// dial. This is the heart of the package and the thing the unit tests pin down.
//
// Order (first non-empty wins):
//
//  1. --<svc>-addr flag           (per-service flag, most explicit)
//  2. --addr / FP_ADDR flag       (global flag override)
//  3. FP_<SVC>_ADDR env var       (per-service env)
//  4. addr.<svc> in config file   (per-service file entry)
//  5. addr in config file         (global file entry)
//  6. compiled-in default         (localhost:<port>)
func (r *Resolver) Addr(s Service) string {
	// 1. per-service flag
	if v, ok := r.perServiceFlag[s]; ok && v != "" {
		return v
	}
	// 2. global flag (--addr). A global flag is "for this invocation", so it
	//    outranks any persisted env/file value.
	if r.globalAddr != "" {
		return r.globalAddr
	}
	// 3. per-service env
	if v := r.env(envVar(s)); v != "" {
		return v
	}
	// A global env override (FP_ADDR) sits just below per-service env: it's
	// still session-scoped but less specific than naming the service.
	if v := r.env("FP_ADDR"); v != "" {
		return v
	}
	// 4. per-service file entry
	if v, ok := r.fileAddrs[s]; ok && v != "" {
		return v
	}
	// 5. global file entry
	if r.fileGlobal != "" {
		return r.fileGlobal
	}
	// 6. compiled-in default
	return defaultAddrs[s]
}

// loadFile parses the ~/.forgepoint/config dotfile. The format is deliberately
// the simplest thing that works: `key = value`, one per line, `#` comments,
// blank lines ignored. Recognized keys:
//
//	addr            = host:port   (global, all services)
//	addr.auth       = host:port
//	addr.registry   = host:port
//	addr.pipeline   = host:port
//	addr.experiment = host:port
//	addr.monitor    = host:port
//	addr.billing    = host:port
//
// WHY not TOML/YAML: those need a dependency, and the CLI charter is stdlib
// only. A flat key=value file covers every current need and is self-documenting.
// Unknown keys are ignored (forward-compatible), not errors.
func (r *Resolver) loadFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // optional file — absence is fine
		}
		return fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("config: %s:%d: expected key = value, got %q", path, lineNo, line)
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if val == "" {
			continue
		}
		switch {
		case key == "addr":
			r.fileGlobal = val
		case strings.HasPrefix(key, "addr."):
			svc := Service(strings.TrimPrefix(key, "addr."))
			if _, known := defaultAddrs[svc]; known {
				r.fileAddrs[svc] = val
			}
			// Unknown service keys are ignored on purpose (forward-compat).
		default:
			// Unknown top-level keys ignored (forward-compat).
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	return nil
}

// Home returns the ~/.forgepoint directory, honoring an explicit override env
// var FP_HOME (useful for tests and for users with a non-standard $HOME).
// Falling back to os.UserHomeDir keeps it cross-platform.
func Home() (string, error) {
	if h := os.Getenv("FP_HOME"); h != "" {
		return h, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("config: locate home dir: %w", err)
	}
	return filepath.Join(home, ".forgepoint"), nil
}

// ConfigFilePath returns the conventional path to the config dotfile inside a
// given home dir.
func ConfigFilePath(home string) string {
	return filepath.Join(home, "config")
}
