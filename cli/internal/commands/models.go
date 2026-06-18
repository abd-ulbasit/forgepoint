package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"

	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"

	"github.com/abd-ulbasit/forgepoint/cli/internal/clientfactory"
	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"
)

// cmdModels dispatches `fp models <subcommand>` against the Model Registry
// (CQRS) service. Subcommands: list, get, register.
//
// Each subcommand follows the same skeleton every authenticated command uses:
//  1. parse its own FlagSet
//  2. requireToken()         — fail early with "run fp login" if absent
//  3. dialService(...)       — token auto-forwarded as Bearer metadata
//  4. call ONE RPC
//  5. render via the --json-aware Printer
//
// The repetition is intentional and readable; a CLI is a flat list of these.
func (a *App) cmdModels(args []string) error {
	if len(args) == 0 {
		return cmderr.Usage("models: expected a subcommand: list | get | register")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "list":
		return a.modelsList(rest)
	case "get":
		return a.modelsGet(rest)
	case "register":
		return a.modelsRegister(rest)
	default:
		return cmderr.Usage("models: unknown subcommand %q (want list | get | register)", sub)
	}
}

func (a *App) modelsList(args []string) error {
	fs := flag.NewFlagSet("models list", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	framework := fs.String("framework", "", "filter by framework (e.g. pytorch)")
	taskType := fs.String("task-type", "", "filter by task type (e.g. classification)")
	includeArchived := fs.Bool("archived", false, "include archived models")
	pageSize := fs.Int("limit", 50, "max results")
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
	conn, err := a.dialService(config.ServiceRegistry, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewRegistry(conn).ListModels(ctx, &registryv1.ListModelsRequest{
		FrameworkFilter: *framework,
		TaskTypeFilter:  *taskType,
		IncludeArchived: *includeArchived,
		Pagination:      pageReq(*pageSize),
	})
	if err != nil {
		return cmderr.FromGRPC("list models", err)
	}

	rows := make([][]string, 0, len(resp.GetModels()))
	for _, m := range resp.GetModels() {
		rows = append(rows, []string{
			m.GetId(),
			orDash(m.GetName()),
			orDash(m.GetFramework()),
			orDash(m.GetTaskType()),
			orDash(m.GetProductionVersion()),
			orDash(m.GetLatestVersion()),
			fmtTime(m.GetUpdatedAt()),
		})
	}
	return a.printer().Table(
		[]string{"ID", "NAME", "FRAMEWORK", "TASK", "PROD_VER", "LATEST_VER", "UPDATED"},
		rows,
	)
}

func (a *App) modelsGet(args []string) error {
	fs := flag.NewFlagSet("models get", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	byName := fs.String("name", "", "look up by name instead of ID")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}

	req := &registryv1.GetModelRequest{}
	switch {
	case *byName != "":
		req.Name = *byName
	case fs.NArg() >= 1:
		req.Id = fs.Arg(0)
	default:
		return cmderr.Usage("models get: provide a model <id> or --name <name>")
	}

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServiceRegistry, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewRegistry(conn).GetModel(ctx, req)
	if err != nil {
		return cmderr.FromGRPC("get model", err)
	}
	m := resp.GetModel()
	if m == nil {
		return cmderr.New(cmderr.ExitNotFound, "get model: empty response")
	}

	// Detail view: ordered key/value lines (or a JSON object under --json).
	tags := ""
	for k, v := range m.GetTags() {
		if tags != "" {
			tags += ", "
		}
		tags += fmt.Sprintf("%s=%s", k, v)
	}
	return a.printer().KeyVals([][2]string{
		{"ID", m.GetId()},
		{"Name", m.GetName()},
		{"Description", orDash(m.GetDescription())},
		{"Owner", orDash(m.GetOwnerId())},
		{"Team", orDash(m.GetTeam())},
		{"Framework", orDash(m.GetFramework())},
		{"TaskType", orDash(m.GetTaskType())},
		{"Tags", orDash(tags)},
		{"ProductionVersion", orDash(m.GetProductionVersion())},
		{"LatestVersion", orDash(m.GetLatestVersion())},
		{"CreatedAt", fmtTime(m.GetCreatedAt())},
		{"UpdatedAt", fmtTime(m.GetUpdatedAt())},
	})
}

func (a *App) modelsRegister(args []string) error {
	fs := flag.NewFlagSet("models register", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	name := fs.String("name", "", "model name (required)")
	desc := fs.String("description", "", "model description")
	framework := fs.String("framework", "", "ML framework (e.g. pytorch, sklearn)")
	taskType := fs.String("task-type", "", "task type (e.g. classification)")
	idem := fs.String("idempotency-key", "", "optional idempotency key for safe retries")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}
	if *name == "" {
		return cmderr.Usage("models register: --name is required")
	}

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServiceRegistry, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewRegistry(conn).RegisterModel(ctx, &registryv1.RegisterModelRequest{
		Name:           *name,
		Description:    *desc,
		Framework:      *framework,
		TaskType:       *taskType,
		IdempotencyKey: *idem,
	})
	if err != nil {
		return cmderr.FromGRPC("register model", err)
	}
	m := resp.GetModel()
	if a.printer().JSON() {
		return a.printer().Value(m)
	}
	return a.printer().Line("Registered model %q (id=%s).", m.GetName(), m.GetId())
}
