package commands

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"

	"github.com/abd-ulbasit/forgepoint/cli/internal/clientfactory"
	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// cmdUsage calls the Billing (outbox) service's GetUsage and prints the per-team
// usage summary. Unlike the other groups this is a single-RPC command, so it has
// no subcommands — `fp usage --team X`.
func (a *App) cmdUsage(args []string) error {
	fs := flag.NewFlagSet("usage", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	team := fs.String("team", "", "team to report usage for (required)")
	sinceStr := fs.String("since", "", "period start (RFC3339, e.g. 2026-06-01T00:00:00Z)")
	untilStr := fs.String("until", "", "period end (RFC3339)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return cmderr.New(cmderr.ExitUsage, "")
	}
	if *team == "" {
		return cmderr.Usage("usage: --team is required")
	}

	since, err := parseOptionalTime("since", *sinceStr)
	if err != nil {
		return err
	}
	until, err := parseOptionalTime("until", *untilStr)
	if err != nil {
		return err
	}

	tok, err := a.requireToken()
	if err != nil {
		return err
	}
	conn, err := a.dialService(config.ServiceBilling, tok)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := a.ctx(context.Background())
	defer cancel()

	resp, err := clientfactory.NewBilling(conn).GetUsage(ctx, &billingv1.GetUsageRequest{
		Team:        *team,
		PeriodStart: since,
		PeriodEnd:   until,
	})
	if err != nil {
		return cmderr.FromGRPC("get usage", err)
	}

	p := a.printer()
	if p.JSON() {
		return p.Value(resp)
	}

	// One row per (summary, meter) so a team's breakdown is scannable.
	rows := make([][]string, 0)
	for _, sum := range resp.GetSummaries() {
		period := fmt.Sprintf("%s..%s", fmtTime(sum.GetPeriodStart()), fmtTime(sum.GetPeriodEnd()))
		for meter, mu := range sum.GetByMeter() {
			rows = append(rows, []string{
				sum.GetTeam(),
				period,
				meter,
				fmt.Sprintf("%d", mu.GetTotalQuantity()),
				fmtMoney(mu.GetTotalCost()),
			})
		}
	}
	if err := p.Table([]string{"TEAM", "PERIOD", "METER", "QUANTITY", "COST"}, rows); err != nil {
		return err
	}
	if gt := resp.GetGrandTotal(); gt != nil {
		return p.Line("\nGrand total: %s", fmtMoney(gt))
	}
	return nil
}

// fmtMoney renders the platform Money type (integer micros + currency) as a
// human-readable decimal. Money is stored as amount_micros (millionths of a
// currency unit) to avoid floating-point rounding in financial math — a
// standard ledger technique (Stripe uses integer minor units for the same
// reason). We divide back to a decimal only for DISPLAY.
func fmtMoney(m *billingv1.Money) string {
	if m == nil {
		return dash
	}
	major := float64(m.GetAmountMicros()) / 1_000_000.0
	cur := m.GetCurrencyCode()
	if cur == "" {
		cur = "USD"
	}
	return fmt.Sprintf("%.4f %s", major, cur)
}

// parseOptionalTime parses an RFC3339 timestamp flag, returning nil when empty
// (so the server applies its default period). A bad value is a usage error.
func parseOptionalTime(name, v string) (*timestamppb.Timestamp, error) {
	if v == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return nil, cmderr.Usage("usage: --%s must be RFC3339 (e.g. 2026-06-01T00:00:00Z): %v", name, err)
	}
	return timestamppb.New(t), nil
}
