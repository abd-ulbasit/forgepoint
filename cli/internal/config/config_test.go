package config

import (
	"os"
	"path/filepath"
	"testing"
)

// envMap turns a map into an os.Getenv-shaped lookup func so tests inject a
// controlled environment without mutating the real process env (which would
// make tests order-dependent and unsafe for -race parallelism).
func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

// TestResolverPrecedence is the heart of the config tests: it pins down the
// flag > env > file > default ordering for a single service, one layer at a
// time, so a regression in the chain is caught precisely.
func TestResolverPrecedence(t *testing.T) {
	const svc = ServiceAuth
	defaultAddr := defaultAddrs[svc]

	tests := []struct {
		name      string
		globalFlg string
		svcFlag   map[Service]string
		env       map[string]string
		fileLines string
		want      string
	}{
		{
			name: "default wins when nothing set",
			want: defaultAddr,
		},
		{
			name:      "file beats default",
			fileLines: "addr.auth = file:1\n",
			want:      "file:1",
		},
		{
			name:      "global file entry beats default but loses to per-service file",
			fileLines: "addr = global:9\naddr.auth = svc:1\n",
			want:      "svc:1",
		},
		{
			name:      "global file entry applies when no per-service entry",
			fileLines: "addr = global:9\n",
			want:      "global:9",
		},
		{
			name:      "env beats file",
			env:       map[string]string{"FP_AUTH_ADDR": "env:2"},
			fileLines: "addr.auth = file:1\n",
			want:      "env:2",
		},
		{
			name:      "per-service env beats global env",
			env:       map[string]string{"FP_AUTH_ADDR": "env:2", "FP_ADDR": "genv:3"},
			fileLines: "addr.auth = file:1\n",
			want:      "env:2",
		},
		{
			name:      "global env beats file when no per-service env",
			env:       map[string]string{"FP_ADDR": "genv:3"},
			fileLines: "addr.auth = file:1\n",
			want:      "genv:3",
		},
		{
			name:      "global flag beats env and file",
			globalFlg: "flag:4",
			env:       map[string]string{"FP_AUTH_ADDR": "env:2"},
			fileLines: "addr.auth = file:1\n",
			want:      "flag:4",
		},
		{
			name:      "per-service flag beats global flag",
			globalFlg: "flag:4",
			svcFlag:   map[Service]string{ServiceAuth: "svcflag:5"},
			env:       map[string]string{"FP_AUTH_ADDR": "env:2"},
			want:      "svcflag:5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			filePath := ""
			if tc.fileLines != "" {
				filePath = filepath.Join(dir, "config")
				if err := os.WriteFile(filePath, []byte(tc.fileLines), 0o600); err != nil {
					t.Fatalf("write config: %v", err)
				}
			}

			r, err := NewResolver(Options{
				Env:      envMap(tc.env),
				FilePath: filePath,
			})
			if err != nil {
				t.Fatalf("NewResolver: %v", err)
			}
			r.SetGlobalFlag(tc.globalFlg)
			r.SetPerServiceFlag(tc.svcFlag)

			if got := r.Addr(svc); got != tc.want {
				t.Errorf("Addr(%s) = %q, want %q", svc, got, tc.want)
			}
		})
	}
}

// TestResolverPerServiceIsolation verifies that overriding ONE service via env
// does not bleed into another service's resolution.
func TestResolverPerServiceIsolation(t *testing.T) {
	r, err := NewResolver(Options{
		Env: envMap(map[string]string{"FP_AUTH_ADDR": "only-auth:1"}),
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	if got := r.Addr(ServiceAuth); got != "only-auth:1" {
		t.Errorf("auth addr = %q, want only-auth:1", got)
	}
	if got := r.Addr(ServiceBilling); got != defaultAddrs[ServiceBilling] {
		t.Errorf("billing addr = %q, want default %q", got, defaultAddrs[ServiceBilling])
	}
}

// TestResolverGlobalFlagAffectsAllServices confirms --addr overrides every
// service at once (the "one gateway in front of everything" workflow).
func TestResolverGlobalFlagAffectsAllServices(t *testing.T) {
	r, err := NewResolver(Options{Env: envMap(nil)})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	r.SetGlobalFlag("gateway:443")
	for _, svc := range []Service{ServiceAuth, ServiceRegistry, ServicePipeline, ServiceExperiment, ServiceMonitor, ServiceBilling} {
		if got := r.Addr(svc); got != "gateway:443" {
			t.Errorf("Addr(%s) = %q, want gateway:443", svc, got)
		}
	}
}

// TestLoadFileErrors checks that a malformed config line is reported (not
// silently ignored) while unknown keys are tolerated for forward-compat.
func TestLoadFileErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")

	// Malformed: a line with no '='.
	if err := os.WriteFile(path, []byte("this is not valid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewResolver(Options{Env: envMap(nil), FilePath: path}); err == nil {
		t.Error("expected error for malformed config line, got nil")
	}

	// Unknown keys + comments + blanks are fine.
	good := "# a comment\n\nunknown_key = whatever\naddr.registry = reg:7\n"
	if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := NewResolver(Options{Env: envMap(nil), FilePath: path})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := r.Addr(ServiceRegistry); got != "reg:7" {
		t.Errorf("registry addr = %q, want reg:7", got)
	}
}

// TestMissingFileIsNotAnError confirms the config file is optional.
func TestMissingFileIsNotAnError(t *testing.T) {
	r, err := NewResolver(Options{
		Env:      envMap(nil),
		FilePath: filepath.Join(t.TempDir(), "does-not-exist"),
	})
	if err != nil {
		t.Fatalf("missing file should not error, got %v", err)
	}
	if got := r.Addr(ServiceAuth); got != defaultAddrs[ServiceAuth] {
		t.Errorf("addr = %q, want default", got)
	}
}

// TestUnknownServiceKeyIgnored ensures addr.<unknown> in the file is ignored.
func TestUnknownServiceKeyIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte("addr.ghost = nope:1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := NewResolver(Options{Env: envMap(nil), FilePath: path})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	// Nothing should have changed; auth still defaults.
	if got := r.Addr(ServiceAuth); got != defaultAddrs[ServiceAuth] {
		t.Errorf("auth addr = %q, want default", got)
	}
}

// TestHomeHonorsFPHome verifies the FP_HOME override path.
func TestHomeHonorsFPHome(t *testing.T) {
	t.Setenv("FP_HOME", "/tmp/fp-test-home")
	got, err := Home()
	if err != nil {
		t.Fatalf("Home: %v", err)
	}
	if got != "/tmp/fp-test-home" {
		t.Errorf("Home() = %q, want /tmp/fp-test-home", got)
	}
}
