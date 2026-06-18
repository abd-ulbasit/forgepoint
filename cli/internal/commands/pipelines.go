package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"

	"github.com/abd-ulbasit/forgepoint/cli/internal/clientfactory"
	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"
)

// execStatusPrefix is the verbose prefix proto puts on every ExecutionStatus
// enum value; shortEnum strips it for display.
const execStatusPrefix = "EXECUTION_STATUS_"
const pipelineTypePrefix = "PIPELINE_TYPE_"

// cmdPipelines dispatches `fp pipelines <subcommand>` against the Pipeline
// Orchestrator (saga + DAG) service: list | trigger | status | watch.
func (a *App) cmdPipelines(args []string) error {
	if len(args) == 0 {
		return cmderr.Usage("pipelines: expected a subcommand: list | trigger | status | watch")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "list":
		return a.pipelinesList(rest)
	case "trigger":
		return a.pipelinesTrigger(rest)
	case "status":
		return a.pipelinesStatus(rest)
	case "watch":
		return a.pipelinesWatch(rest)
	default:
		return cmderr.Usage("pipelines: unknown subcommand %q (want list | trigger | status | watch)", sub)
	}
}

func (a *App) pipelinesList(args []string) error {
	fs := flag.NewFlagSet("pipelines list", flag.ContinueOnError)
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
	conn, err := a.dialService(config.ServicePipeline, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewPipeline(conn).ListPipelines(ctx, &pipelinev1.ListPipelinesRequest{
		Pagination: pageReq(*limit),
	})
	if err != nil {
		return cmderr.FromGRPC("list pipelines", err)
	}

	rows := make([][]string, 0, len(resp.GetPipelines()))
	for _, p := range resp.GetPipelines() {
		rows = append(rows, []string{
			p.GetId(),
			orDash(p.GetName()),
			shortEnum(p.GetType().String(), pipelineTypePrefix),
			orDash(p.GetTeam()),
			fmtTime(p.GetCreatedAt()),
		})
	}
	return a.printer().Table([]string{"ID", "NAME", "TYPE", "TEAM", "CREATED"}, rows)
}

func (a *App) pipelinesTrigger(args []string) error {
	fs := flag.NewFlagSet("pipelines trigger", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	idem := fs.String("idempotency-key", "", "optional idempotency key for safe retries")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}
	if fs.NArg() < 1 {
		return cmderr.Usage("pipelines trigger: provide a <pipeline-id>")
	}
	pipelineID := fs.Arg(0)

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServicePipeline, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewPipeline(conn).TriggerExecution(ctx, &pipelinev1.TriggerExecutionRequest{
		PipelineId:     pipelineID,
		IdempotencyKey: *idem,
	})
	if err != nil {
		return cmderr.FromGRPC("trigger execution", err)
	}
	ex := resp.GetExecution()
	if a.printer().JSON() {
		return a.printer().Value(ex)
	}
	return a.printer().Line("Triggered execution %s (status=%s). Watch it: fp pipelines watch %s",
		ex.GetId(), shortEnum(ex.GetStatus().String(), execStatusPrefix), ex.GetId())
}

func (a *App) pipelinesStatus(args []string) error {
	fs := flag.NewFlagSet("pipelines status", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}
	if fs.NArg() < 1 {
		return cmderr.Usage("pipelines status: provide an <execution-id>")
	}
	execID := fs.Arg(0)

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServicePipeline, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewPipeline(conn).GetExecution(ctx, &pipelinev1.GetExecutionRequest{
		ExecutionId: execID,
	})
	if err != nil {
		return cmderr.FromGRPC("get execution", err)
	}
	return a.renderExecution(resp.GetExecution())
}

// pipelinesWatch consumes the WatchExecution SERVER STREAM and prints each
// update live until the execution reaches a terminal state or the user hits
// Ctrl-C. This is the one command that needs streaming + signal handling.
//
// ============================================================================
// CONSUMING A gRPC SERVER STREAM (interview-critical)
// ============================================================================
//
// WatchExecution returns a stream client. We loop calling stream.Recv():
//   - Recv() blocks until the next WatchExecutionResponse arrives.
//   - It returns (nil, io.EOF) when the SERVER closes the stream cleanly —
//     that's our signal the execution finished and the server is done sending.
//   - Any other error is a real failure (or a cancellation we triggered).
//
// We DO NOT apply the global per-RPC timeout here: a watch can legitimately run
// for minutes (a saga deploying canaries). Instead we bind the stream to a
// context cancelled by SIGINT, so Ctrl-C tears the stream down promptly.
//
// The stream interceptor (clientfactory.streamTokenInterceptor) attaches the
// Bearer token to the stream's opening headers, so the server authenticates the
// watch exactly like a unary call. Forgetting the STREAM interceptor (and only
// installing the unary one) is a common bug — the watch would be rejected
// Unauthenticated while every other command works.
// ============================================================================
func (a *App) pipelinesWatch(args []string) error {
	fs := flag.NewFlagSet("pipelines watch", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}
	if fs.NArg() < 1 {
		return cmderr.Usage("pipelines watch: provide an <execution-id>")
	}
	execID := fs.Arg(0)

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServicePipeline, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Ctrl-C → cancel the stream context. signal.NotifyContext wires SIGINT to
	// ctx cancellation for us; stop() restores default signal handling on exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	stream, err := clientfactory.NewPipeline(conn).WatchExecution(ctx, &pipelinev1.WatchExecutionRequest{
		ExecutionId:         execID,
		IncludeCurrentState: true, // get an immediate snapshot, then deltas
	})
	if err != nil {
		return cmderr.FromGRPC("watch execution", err)
	}

	p := a.printer()
	for {
		update, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Server closed the stream: the execution reached a terminal state.
			return p.Line("watch: stream closed by server (execution finished)")
		}
		if err != nil {
			if ctx.Err() != nil {
				// We cancelled via Ctrl-C — not an error condition for the user.
				return p.Line("watch: interrupted")
			}
			return cmderr.FromGRPC("watch execution", err)
		}

		if p.JSON() {
			if err := p.Value(update); err != nil {
				return err
			}
		} else {
			a.printWatchUpdate(update)
		}

		// Stop watching once the execution is terminal even if the server keeps
		// the stream open briefly — gives a snappy exit and a clean exit code.
		if isTerminal(update.GetExecution().GetStatus()) {
			return nil
		}
	}
}

// printWatchUpdate renders one streamed update as a compact line. We show the
// sequence number (monotonic, so a user can spot gaps), the overall status, and
// the step that changed (if any).
func (a *App) printWatchUpdate(u *pipelinev1.WatchExecutionResponse) {
	ex := u.GetExecution()
	status := shortEnum(ex.GetStatus().String(), execStatusPrefix)
	changed := ""
	if s := u.GetChangedStep(); s != nil {
		changed = fmt.Sprintf("  step=%s", s.GetStepId())
	}
	fmt.Fprintf(a.Stdout, "[seq %d] %s status=%s current=%s%s\n",
		u.GetSequence(), fmtTime(u.GetEmittedAt()), status, orDash(ex.GetCurrentStep()), changed)
}

// renderExecution prints a single execution as a detail view (or JSON).
func (a *App) renderExecution(ex *pipelinev1.Execution) error {
	if ex == nil {
		return cmderr.New(cmderr.ExitNotFound, "execution not found")
	}
	p := a.printer()
	if p.JSON() {
		return p.Value(ex)
	}
	pairs := [][2]string{
		{"ID", ex.GetId()},
		{"Pipeline", ex.GetPipelineId()},
		{"Status", shortEnum(ex.GetStatus().String(), execStatusPrefix)},
		{"CurrentStep", orDash(ex.GetCurrentStep())},
		{"TriggeredBy", orDash(ex.GetTriggeredBy())},
		{"StartedAt", fmtTime(ex.GetStartedAt())},
		{"CompletedAt", fmtTime(ex.GetCompletedAt())},
		{"Error", orDash(ex.GetError())},
	}
	if err := p.KeyVals(pairs); err != nil {
		return err
	}
	// Show step breakdown as a small table under the detail view.
	if steps := ex.GetStepExecutions(); len(steps) > 0 {
		fmt.Fprintln(a.Stdout)
		rows := make([][]string, 0, len(steps))
		for _, s := range steps {
			rows = append(rows, []string{
				orDash(s.GetStepId()),
				shortEnum(s.GetStatus().String(), "STEP_STATUS_"),
				fmtTime(s.GetStartedAt()),
				fmtTime(s.GetCompletedAt()),
				orDash(s.GetError()),
			})
		}
		return a.printer().Table([]string{"STEP", "STATUS", "STARTED", "COMPLETED", "ERROR"}, rows)
	}
	return nil
}

// isTerminal reports whether an execution status is final (no further updates
// expected). Mirrors the server's saga state machine terminal states.
func isTerminal(s pipelinev1.ExecutionStatus) bool {
	switch s {
	case pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPLETED,
		pipelinev1.ExecutionStatus_EXECUTION_STATUS_FAILED,
		pipelinev1.ExecutionStatus_EXECUTION_STATUS_CANCELLED:
		return true
	default:
		return false
	}
}
