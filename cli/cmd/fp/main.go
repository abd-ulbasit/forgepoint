// Command fp is the Forgepoint platform CLI: a thin gRPC client that lets a
// developer authenticate once and then drive the platform's services (models,
// pipelines, experiment runs, monitors, billing) from the terminal.
//
// ============================================================================
// WHY main() IS TINY
// ============================================================================
//
// main has exactly three jobs and nothing else:
//
//  1. WIRING — locate ~/.forgepoint, build the address Resolver (env + config
//     file) and the token Store, assemble them into a commands.App.
//  2. DISPATCH — hand argv to App.Run, which parses global flags and routes to
//     the right command.
//  3. EXIT — translate the returned error into a process exit code via cmderr.
//
// All logic lives in the internal packages so it is unit-testable WITHOUT a
// process boundary. main is the ONLY place that calls os.Exit — keeping exit
// out of library code means every command path returns errors and can be
// asserted in tests. This is the standard "errors flow up, os.Exit at the top"
// Go CLI structure.
// ============================================================================
package main

import (
	"fmt"
	"os"

	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/commands"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"
	"github.com/abd-ulbasit/forgepoint/cli/internal/token"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run does the wiring and returns the process exit code. Splitting it out from
// main (which only calls os.Exit) means run itself is callable from a test if
// we ever want a full end-to-end exercise of the binary.
func run(args []string) int {
	// Locate ~/.forgepoint (honors $FP_HOME override). A failure here means we
	// can't find $HOME at all — rare, but report it cleanly rather than panic.
	home, err := config.Home()
	if err != nil {
		fmt.Fprintln(os.Stderr, "fp:", err)
		return cmderr.ExitGeneric
	}

	// Build the address resolver from env + the optional config file. Flags are
	// folded in later (inside App.Run) once they're parsed, preserving the
	// flag > env > file > default precedence.
	resolver, err := config.NewResolver(config.Options{
		Env:      os.Getenv,
		FilePath: config.ConfigFilePath(home),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "fp:", err)
		return cmderr.ExitGeneric
	}

	tokens := token.New(home)

	app := commands.NewApp(os.Stdout, os.Stderr, os.Stdin, resolver, tokens)

	if err := app.Run(args); err != nil {
		// A CLIError carries its own message+code. We print the message (unless
		// it's empty — flag.Parse already printed its own usage in that case)
		// and exit with the mapped code so shell scripts can branch on it.
		if msg := err.Error(); msg != "" {
			fmt.Fprintln(os.Stderr, "fp:", msg)
		}
		return cmderr.ExitCode(err)
	}
	return cmderr.ExitOK
}
