package commands

import (
	"context"
	"errors"
	"flag"
	"strings"

	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"

	"github.com/abd-ulbasit/forgepoint/cli/internal/clientfactory"
	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"
)

const (
	driftSeverityPrefix = "DRIFT_SEVERITY_"
	monitorStatePrefix  = "MONITOR_STATE_"
	driftTypePrefix     = "DRIFT_TYPE_"
)

// cmdMonitors dispatches `fp monitors <subcommand>` against the Model Monitor
// (streaming drift) service: list | drift.
func (a *App) cmdMonitors(args []string) error {
	if len(args) == 0 {
		return cmderr.Usage("monitors: expected a subcommand: list | drift")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "list":
		return a.monitorsList(rest)
	case "drift":
		return a.monitorsDrift(rest)
	default:
		return cmderr.Usage("monitors: unknown subcommand %q (want list | drift)", sub)
	}
}

func (a *App) monitorsList(args []string) error {
	fs := flag.NewFlagSet("monitors list", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	limit := fs.Int("limit", 50, "max results")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServiceMonitor, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewMonitor(conn).ListMonitors(ctx, &monitorv1.ListMonitorsRequest{
		Pagination: pageReq(*limit),
	})
	if err != nil {
		return cmderr.FromGRPC("list monitors", err)
	}

	rows := make([][]string, 0, len(resp.GetEntries()))
	for _, e := range resp.GetEntries() {
		m := e.GetMonitor()
		h := e.GetHealth()
		rows = append(rows, []string{
			m.GetId(),
			orDash(m.GetModelName()),
			shortEnum(m.GetState().String(), monitorStatePrefix),
			shortEnum(h.GetOverallSeverity().String(), driftSeverityPrefix),
			boolYesNo(m.GetAutoRetrain()),
			fmtTime(h.GetLastEventAt()),
		})
	}
	return a.printer().Table(
		[]string{"ID", "MODEL", "STATE", "SEVERITY", "AUTO_RETRAIN", "LAST_EVENT"},
		rows,
	)
}

func (a *App) monitorsDrift(args []string) error {
	fs := flag.NewFlagSet("monitors drift", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	model := fs.String("model", "", "filter by model name")
	severity := fs.String("severity", "", "minimum severity: ok | warning | critical")
	limit := fs.Int("limit", 50, "max results")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}

	minSev, err := parseSeverity(*severity)
	if err != nil {
		return err
	}

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServiceMonitor, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewMonitor(conn).ListDriftReports(ctx, &monitorv1.ListDriftReportsRequest{
		ModelName:   *model,
		MinSeverity: minSev,
		Pagination:  pageReq(*limit),
	})
	if err != nil {
		return cmderr.FromGRPC("list drift reports", err)
	}

	rows := make([][]string, 0, len(resp.GetReports()))
	for _, r := range resp.GetReports() {
		rows = append(rows, []string{
			r.GetId(),
			orDash(r.GetModelName()),
			orDash(r.GetModelVersion()),
			shortEnum(r.GetDriftType().String(), driftTypePrefix),
			shortEnum(r.GetSeverity().String(), driftSeverityPrefix),
			fmtTime(r.GetWindowEnd()),
		})
	}
	return a.printer().Table(
		[]string{"ID", "MODEL", "VERSION", "DRIFT_TYPE", "SEVERITY", "WINDOW_END"},
		rows,
	)
}

// parseSeverity converts a user-friendly severity flag into the proto enum.
// Empty means "no filter" (UNSPECIFIED). We accept lowercase words rather than
// the verbose enum names so users type `--severity critical`, not
// `DRIFT_SEVERITY_CRITICAL`.
func parseSeverity(s string) (monitorv1.DriftSeverity, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return monitorv1.DriftSeverity_DRIFT_SEVERITY_UNSPECIFIED, nil
	case "ok":
		return monitorv1.DriftSeverity_DRIFT_SEVERITY_OK, nil
	case "warning", "warn":
		return monitorv1.DriftSeverity_DRIFT_SEVERITY_WARNING, nil
	case "critical", "crit":
		return monitorv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL, nil
	default:
		return 0, cmderr.Usage("invalid --severity %q (want ok | warning | critical)", s)
	}
}

// boolYesNo formats a bool as a compact table cell.
func boolYesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
