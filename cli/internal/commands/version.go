package commands

import (
	"errors"
	"flag"
	"runtime"

	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
)

// cmdVersion prints the CLI version (set via -ldflags at build time) plus the
// Go runtime it was built with. It needs no token and no network — it's the one
// command that must always work, even with nothing configured, which makes it a
// handy smoke test for "is the binary intact?".
func (a *App) cmdVersion(args []string) error {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}

	p := a.printer()
	if p.JSON() {
		return p.Value(map[string]string{
			"version":   version,
			"goVersion": runtime.Version(),
			"platform":  runtime.GOOS + "/" + runtime.GOARCH,
		})
	}
	return p.KeyVals([][2]string{
		{"fp version", version},
		{"go version", runtime.Version()},
		{"platform", runtime.GOOS + "/" + runtime.GOARCH},
	})
}
