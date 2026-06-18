package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"

	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"

	"github.com/abd-ulbasit/forgepoint/cli/internal/clientfactory"
	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"
)

const runStatusPrefix = "RUN_STATUS_"

// cmdRuns dispatches `fp runs <subcommand>` against the Experiment Tracker
// (event-driven) service: list | get. "runs" is the user-facing noun for the
// experiment runs the tracker records.
func (a *App) cmdRuns(args []string) error {
	if len(args) == 0 {
		return cmderr.Usage("runs: expected a subcommand: list | get")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "list":
		return a.runsList(rest)
	case "get":
		return a.runsGet(rest)
	default:
		return cmderr.Usage("runs: unknown subcommand %q (want list | get)", sub)
	}
}

func (a *App) runsList(args []string) error {
	fs := flag.NewFlagSet("runs list", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	experiment := fs.String("experiment", "", "filter by experiment id")
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
	conn, err := a.dialService(config.ServiceExperiment, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewExperiment(conn).ListRuns(ctx, &experimentv1.ListRunsRequest{
		ExperimentId: *experiment,
		Pagination:   pageReq(*limit),
	})
	if err != nil {
		return cmderr.FromGRPC("list runs", err)
	}

	rows := make([][]string, 0, len(resp.GetRuns()))
	for _, r := range resp.GetRuns() {
		rows = append(rows, []string{
			r.GetId(),
			orDash(r.GetDisplayName()),
			shortEnum(r.GetStatus().String(), runStatusPrefix),
			orDash(r.GetExperimentId()),
			fmtTime(r.GetStartedAt()),
			fmtTime(r.GetEndedAt()),
		})
	}
	return a.printer().Table(
		[]string{"ID", "NAME", "STATUS", "EXPERIMENT", "STARTED", "ENDED"},
		rows,
	)
}

func (a *App) runsGet(args []string) error {
	fs := flag.NewFlagSet("runs get", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}
	if fs.NArg() < 1 {
		return cmderr.Usage("runs get: provide a <run-id>")
	}
	runID := fs.Arg(0)

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServiceExperiment, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewExperiment(conn).GetRun(ctx, &experimentv1.GetRunRequest{Id: runID})
	if err != nil {
		return cmderr.FromGRPC("get run", err)
	}
	run := resp.GetRun()
	if run == nil {
		return cmderr.New(cmderr.ExitNotFound, "get run: empty response")
	}

	p := a.printer()
	if p.JSON() {
		return p.Value(run)
	}

	// Detail view, plus params and final metrics as small sub-tables.
	if err := p.KeyVals([][2]string{
		{"ID", run.GetId()},
		{"Name", orDash(run.GetDisplayName())},
		{"Status", shortEnum(run.GetStatus().String(), runStatusPrefix)},
		{"Experiment", orDash(run.GetExperimentId())},
		{"ModelVersion", orDash(run.GetModelVersionId())},
		{"Owner", orDash(run.GetOwnerId())},
		{"StartedAt", fmtTime(run.GetStartedAt())},
		{"EndedAt", fmtTime(run.GetEndedAt())},
	}); err != nil {
		return err
	}

	if params := run.GetParams(); len(params) > 0 {
		fmt.Fprintln(a.Stdout, "\nParams:")
		rows := make([][]string, 0, len(params))
		for _, pm := range params {
			rows = append(rows, []string{orDash(pm.GetKey()), orDash(pm.GetValue())})
		}
		if err := p.Table([]string{"KEY", "VALUE"}, rows); err != nil {
			return err
		}
	}
	if metrics := run.GetFinalMetrics(); len(metrics) > 0 {
		fmt.Fprintln(a.Stdout, "\nFinal metrics:")
		rows := make([][]string, 0, len(metrics))
		for _, mp := range metrics {
			rows = append(rows, []string{orDash(mp.GetKey()), fmt.Sprintf("%g", mp.GetValue())})
		}
		if err := p.Table([]string{"METRIC", "VALUE"}, rows); err != nil {
			return err
		}
	}
	return nil
}
