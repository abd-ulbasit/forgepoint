// Package commands implements the `fp` CLI's command tree: the top-level
// dispatcher, global flag parsing, and every subcommand. It deliberately uses
// ONLY the stdlib `flag` package plus a hand-rolled subcommand dispatcher — no
// cobra/urfave — to keep the dependency surface to grpc + generated code + pkg.
//
// ============================================================================
// WHY HAND-ROLLED DISPATCH OVER COBRA
// ============================================================================
//
// cobra is excellent, but it's a heavy dependency for a portfolio CLI whose
// charter is "stdlib plumbing only." The stdlib `flag` package already gives us
// per-command FlagSets; all we add is a two-level dispatcher (command →
// subcommand) and a usage printer. The result is ~the same ergonomics for our
// command count, with zero third-party code to audit — and it makes the
// dispatch mechanism itself something you can read and explain in an interview.
//
// COMMAND TREE:
//
//	fp [global flags] <command> <subcommand> [flags] [args]
//
//	fp login        --email <e> [--password <p>]
//	fp models       list | get <id|--name n> | register --name n [...]
//	fp pipelines    list | trigger <pipeline-id> | status <execution-id>
//	                | watch <execution-id>
//	fp runs         list [--experiment id] | get <run-id>
//	fp monitors     list | drift [--model n] [--severity s]
//	fp usage        --team <t> [--since ...] [--until ...]
//	fp version
//
// GLOBAL FLAGS (parsed before the command):
//
//	--addr <host:port>   override the address for ALL services
//	--json               machine-readable JSON output
//	--timeout <dur>      per-RPC timeout (default 15s)
//	--tls                dial with TLS (prod); default is insecure (dev tunnel)
//	--<svc>-addr         per-service address override (e.g. --auth-addr)
//
// PRECEDENCE for addresses (flag > env > config-file > default) is implemented
// in the config package; this file just feeds it the parsed flags.
// ============================================================================
package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/abd-ulbasit/forgepoint/cli/internal/clientfactory"
	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"
	"github.com/abd-ulbasit/forgepoint/cli/internal/output"
	"github.com/abd-ulbasit/forgepoint/cli/internal/token"

	"google.golang.org/grpc"
)

// version is the CLI version, overridable at build time via
// -ldflags "-X .../commands.version=v1.2.3".
var version = "dev"

// Dialer abstracts opening a gRPC connection. The real implementation is
// clientfactory.Dial; tests inject a fake that returns a *grpc.ClientConn wired
// to a bufconn server (or a stub). Keeping this as a field on App is what lets
// the command tests run without a real network.
type Dialer func(addr, token string, useTLS bool) (*grpc.ClientConn, error)

// App holds everything a command needs to run: where to write output, how to
// resolve service addresses, how to load the token, and how to dial. It is the
// CLI's dependency-injection container — assembled once in main, threaded into
// every command. Tests build an App with fakes for total control.
type App struct {
	Stdout io.Writer
	Stderr io.Writer
	Stdin  io.Reader

	Resolver *config.Resolver
	Tokens   *token.Store
	Dial     Dialer

	// global flag values, parsed once in Run.
	jsonOut bool
	timeout time.Duration
	useTLS  bool

	// now is injectable so token-expiry checks are deterministic in tests.
	now func() time.Time
}

// NewApp builds an App with production defaults. main calls this, then Run.
func NewApp(stdout, stderr io.Writer, stdin io.Reader, resolver *config.Resolver, tokens *token.Store) *App {
	return &App{
		Stdout:   stdout,
		Stderr:   stderr,
		Stdin:    stdin,
		Resolver: resolver,
		Tokens:   tokens,
		Dial:     clientfactory.Dial,
		now:      time.Now,
	}
}

// Run parses global flags, dispatches to the named command, and returns an
// error whose exit code (via cmderr) main turns into the process status. It
// never calls os.Exit itself — exactly one place (main) owns that, which keeps
// every command path unit-testable.
func (a *App) Run(args []string) error {
	// ---- Global flags ----------------------------------------------------
	// We parse the GLOBAL flags up to the first non-flag token (the command),
	// then hand the REMAINING args to the command's own FlagSet. flag.Parse
	// stops at the first non-flag arg, which is precisely the command name, so
	// global flags must precede the command: `fp --json models list`.
	gfs := flag.NewFlagSet("fp", flag.ContinueOnError)
	gfs.SetOutput(a.Stderr)
	var (
		addr    = gfs.String("addr", "", "override gRPC address for ALL services (host:port)")
		jsonOut = gfs.Bool("json", false, "output machine-readable JSON")
		timeout = gfs.Duration("timeout", 15*time.Second, "per-RPC timeout")
		useTLS  = gfs.Bool("tls", false, "dial services over TLS (default: insecure, for dev port-forward)")
		// Per-service address overrides. These let a user point one service at a
		// different tunnel without touching the rest.
		authAddr = gfs.String("auth-addr", "", "override auth service address")
		regAddr  = gfs.String("registry-addr", "", "override registry service address")
		pipeAddr = gfs.String("pipeline-addr", "", "override pipeline service address")
		expAddr  = gfs.String("experiment-addr", "", "override experiment service address")
		monAddr  = gfs.String("monitor-addr", "", "override monitor service address")
		billAddr = gfs.String("billing-addr", "", "override billing service address")
	)
	gfs.Usage = func() { a.printUsage(a.Stderr) }

	if err := gfs.Parse(args); err != nil {
		// flag.ErrHelp: the user passed -h or --help. flag already printed usage
		// to gfs.Output (our Stderr), and by convention (kubectl, gh, aws) explicit
		// help must exit 0, not 2. Return nil so main calls os.Exit(0).
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		// Any other parse error (unknown flag, bad value) is a usage mistake.
		return cmderr.New(cmderr.ExitUsage, "")
	}

	a.jsonOut = *jsonOut
	a.timeout = *timeout
	a.useTLS = *useTLS

	// Fold the global --addr and per-service flags into the resolver so the
	// precedence chain (flag > env > file > default) accounts for them.
	a.Resolver.SetGlobalFlag(*addr)
	a.Resolver.SetPerServiceFlag(map[config.Service]string{
		config.ServiceAuth:       *authAddr,
		config.ServiceRegistry:   *regAddr,
		config.ServicePipeline:   *pipeAddr,
		config.ServiceExperiment: *expAddr,
		config.ServiceMonitor:    *monAddr,
		config.ServiceBilling:    *billAddr,
	})

	rest := gfs.Args()
	if len(rest) == 0 {
		a.printUsage(a.Stderr)
		return cmderr.New(cmderr.ExitUsage, "")
	}

	cmd, cmdArgs := rest[0], rest[1:]

	// ---- Two-level dispatch ---------------------------------------------
	// Each case routes to a command handler that owns its own FlagSet and (if
	// it has subcommands) its own inner switch. This flat map of verbs is the
	// whole "router" — easy to read, easy to extend.
	switch cmd {
	case "login":
		return a.cmdLogin(cmdArgs)
	case "models":
		return a.cmdModels(cmdArgs)
	case "pipelines":
		return a.cmdPipelines(cmdArgs)
	case "runs":
		return a.cmdRuns(cmdArgs)
	case "monitors":
		return a.cmdMonitors(cmdArgs)
	case "usage":
		return a.cmdUsage(cmdArgs)
	case "version":
		return a.cmdVersion(cmdArgs)
	case "help":
		// "fp help" (without a leading dash) prints help and exits 0.
		// The -h/--help variants are handled above by flag.ErrHelp and never reach
		// here, so only the bare word "help" needs to stay in the switch.
		a.printUsage(a.Stdout)
		return nil
	default:
		a.printUsage(a.Stderr)
		return cmderr.Usage("unknown command %q", cmd)
	}
}

// printer builds an output.Printer honoring the global --json flag.
func (a *App) printer() *output.Printer {
	return output.New(a.Stdout, a.jsonOut)
}

// ctx returns a context with the global per-RPC timeout applied, plus its
// cancel func (callers must defer cancel). A timeout keeps the CLI from hanging
// forever when a service is wedged; for streaming commands (watch) the caller
// uses a non-timeout context and relies on Ctrl-C instead.
func (a *App) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, a.timeout)
}

// loadToken loads a valid (non-expired) token or returns a CLIError telling the
// user to log in. Every authenticated command begins with this.
func (a *App) requireToken() (string, error) {
	st, err := a.Tokens.LoadValid(a.now())
	if err != nil {
		switch err {
		case token.ErrNoToken:
			return "", cmderr.New(cmderr.ExitAuth, "not logged in — run 'fp login' first")
		case token.ErrExpired:
			return "", cmderr.New(cmderr.ExitAuth, "session expired — run 'fp login' again")
		default:
			return "", cmderr.New(cmderr.ExitAuth, "could not read credentials: %v", err)
		}
	}
	return st.AccessToken, nil
}

// dialService resolves the address for a service, dials it with the (already
// loaded) token attached, and returns the connection. The caller closes it.
func (a *App) dialService(svc config.Service, tok string) (*grpc.ClientConn, error) {
	addr := a.Resolver.Addr(svc)
	conn, err := a.Dial(addr, tok, a.useTLS)
	if err != nil {
		return nil, cmderr.New(cmderr.ExitUnavailable, "cannot connect to %s at %s: %v", svc, addr, err)
	}
	return conn, nil
}

// printUsage writes the top-level help.
func (a *App) printUsage(w io.Writer) {
	fmt.Fprint(w, `fp — Forgepoint platform CLI

USAGE:
  fp [global flags] <command> <subcommand> [flags] [args]

COMMANDS:
  login        Authenticate and store a token (fp login --email you@co)
  models       Manage models      (list | get <id> | register --name n)
  pipelines    Pipeline runs      (list | trigger <id> | status <exec> | watch <exec>)
  runs         Experiment runs    (list [--experiment id] | get <run-id>)
  monitors     Model monitoring   (list | drift [--model n] [--severity s])
  usage        Billing usage      (usage --team <t>)
  version      Print CLI version
  help         Show this help

GLOBAL FLAGS:
  --addr host:port     Override address for ALL services
  --<svc>-addr h:p     Override one service (auth|registry|pipeline|experiment|monitor|billing)
  --json               Machine-readable JSON output
  --timeout 15s        Per-RPC timeout
  --tls                Dial over TLS (default: insecure, for kubectl port-forward)

CONFIG PRECEDENCE (address resolution, highest first):
  --<svc>-addr / --addr flag  >  FP_<SVC>_ADDR env  >  ~/.forgepoint/config  >  built-in default

Run 'fp <command>' with no subcommand for command-specific help.
`)
}
