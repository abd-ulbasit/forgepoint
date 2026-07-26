package commands

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"

	"github.com/abd-ulbasit/forgepoint/cli/internal/clientfactory"
	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"
	"github.com/abd-ulbasit/forgepoint/cli/internal/token"
)

// cmdLogin implements `fp login` — the ONLY command that does not require a
// pre-existing token (it mints one). It calls AuthService.Login and persists
// the returned JWT to ~/.forgepoint/token at mode 0600.
//
// ============================================================================
// THE LOGIN FLOW
// ============================================================================
//
//	fp login --email you@co
//	  └─▶ prompt for password (stdin)
//	      └─▶ dial auth service (NO token attached — there isn't one yet)
//	          └─▶ AuthService.Login(email, password)
//	              └─▶ LoginResponse{ access_token, expires_at, user }
//	                  └─▶ token.Store.Save(...)  (file mode 0600)
//
// Every SUBSEQUENT command loads that token and forwards it as
// "authorization: Bearer <token>" metadata. Login is the bootstrap that breaks
// the chicken-and-egg: you can't authenticate a call without a token, so the
// one call that issues the token must be reachable unauthenticated. The server
// allows Login through its auth interceptor for exactly this reason.
//
// ============================================================================
// PASSWORD ENTRY — THE ECHO CAVEAT
// ============================================================================
//
// Ideally a password prompt reads with terminal echo DISABLED, so the secret
// never appears on screen or in scrollback (this is what `golang.org/x/term`'s
// ReadPassword does, and what ssh/sudo do). This CLI's dependency charter is
// "stdlib only for plumbing," and the no-echo dependency is not cleanly
// resolvable in the build at the time of writing, so we read the password from
// stdin via the stdlib bufio.Reader. CAVEAT: the typed password IS echoed to
// the terminal. Mitigations a user can choose:
//
//   - Pass --password explicitly (visible in shell history — also imperfect).
//   - Pipe it:  printf '%s' "$PW" | fp login --email you@co
//
// PRODUCTION HARDENING: swap readPassword's body for term.ReadPassword(int(
// os.Stdin.Fd())) once x/term is a sanctioned dependency. The call site does
// not change — only the helper.
// ============================================================================
func (a *App) cmdLogin(args []string) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	email := fs.String("email", "", "your account email (required)")
	password := fs.String("password", "", "your password (omit to be prompted; CAVEAT: prompt echoes)")
	fs.Usage = func() {
		fmt.Fprint(a.Stderr, "Usage: fp login --email <email> [--password <password>]\n\n"+
			"Authenticates against the auth service and stores a JWT at ~/.forgepoint/token (mode 0600).\n")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}

	if *email == "" {
		fs.Usage()
		return cmderr.Usage("login: --email is required")
	}

	pw := *password
	if pw == "" {
		var err error
		pw, err = a.readPassword(fmt.Sprintf("Password for %s: ", *email))
		if err != nil {
			return cmderr.New(cmderr.ExitGeneric, "login: read password: %v", err)
		}
	}
	if pw == "" {
		return cmderr.Usage("login: password must not be empty")
	}

	// Dial auth WITHOUT a token — Login is the unauthenticated bootstrap call.
	addr := a.Resolver.Addr(config.ServiceAuth)
	conn, err := a.Dial(addr, "", a.useTLS)
	if err != nil {
		return cmderr.New(cmderr.ExitUnavailable, "login: cannot connect to auth at %s: %v", addr, err)
	}
	defer conn.Close()

	client := clientfactory.NewAuth(conn)

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := client.Login(ctx, &authv1.LoginRequest{Email: *email, Password: pw})
	if err != nil {
		// Map gRPC codes: Unauthenticated here means BAD CREDENTIALS (not an
		// expired session), so give a login-specific message rather than the
		// generic "run fp login" (the user is already running it).
		return loginError(err)
	}

	// Persist the credential. expires_at lets later commands pre-check expiry
	// offline; we tolerate a nil timestamp (zero time = "unknown, never expire
	// client-side"; the server still enforces real expiry).
	st := token.Stored{
		AccessToken: resp.GetAccessToken(),
		Email:       *email,
	}
	if ts := resp.GetExpiresAt(); ts != nil {
		st.ExpiresAt = ts.AsTime()
	}
	if err := a.Tokens.Save(st); err != nil {
		return cmderr.New(cmderr.ExitGeneric, "login: save token: %v", err)
	}

	name := *email
	if u := resp.GetUser(); u != nil && u.GetName() != "" {
		name = u.GetName()
	}
	return a.printer().Line("Logged in as %s. Token stored at %s.", name, a.Tokens.Path())
}

// loginError converts a Login RPC failure into a login-specific CLIError so the
// message fits the context (entering credentials), unlike the generic
// session-expired wording in cmderr.FromGRPC.
func loginError(err error) error {
	mapped := cmderr.FromGRPC("login", err)
	// Re-word the auth case for the login context.
	if cmderr.ExitCode(mapped) == cmderr.ExitAuth {
		return cmderr.New(cmderr.ExitAuth, "login failed: invalid email or password")
	}
	return mapped
}

// readPassword reads a line from stdin after writing a prompt. See the package
// echo-caveat note above for why this is plain (echoing) stdin and how to
// upgrade it to no-echo.
func (a *App) readPassword(prompt string) (string, error) {
	fmt.Fprint(a.Stderr, prompt)
	r := bufio.NewReader(a.Stdin)
	line, err := r.ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}
