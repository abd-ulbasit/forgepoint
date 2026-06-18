// Package output renders command results either as a human-friendly aligned
// table or as machine-readable JSON, selected by the global --json flag.
//
// ============================================================================
// WHY TWO OUTPUT MODES
// ============================================================================
//
// A good CLI serves two audiences from the same command:
//
//   - HUMANS at a terminal want a compact, column-aligned table they can scan.
//   - SCRIPTS / CI want stable, parseable JSON they can pipe into `jq`.
//
// `--json` flips the mode. This mirrors `kubectl -o json`, `gh --json`, and
// `aws --output json`. Keeping the rendering in one place means every command
// gets both modes for free and they all align/format identically.
//
// IMPLEMENTATION NOTE: tables use the stdlib text/tabwriter — zero dependencies,
// and exactly the right tool: it pads columns so headers and rows line up
// regardless of cell width. JSON uses encoding/json with indentation for
// readability; it's still valid, jq-parseable JSON.
// ============================================================================
package output

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
)

// Printer writes results to a destination (os.Stdout in production, a bytes
// buffer in tests) in the chosen mode. Constructing it with the writer injected
// is what makes output assertions in unit tests trivial.
type Printer struct {
	w    io.Writer
	json bool
}

// New builds a Printer. When jsonMode is true, every call renders JSON.
func New(w io.Writer, jsonMode bool) *Printer {
	return &Printer{w: w, json: jsonMode}
}

// JSON reports whether the printer is in JSON mode (lets a command skip building
// a table when it's going to emit JSON anyway).
func (p *Printer) JSON() bool { return p.json }

// Table renders headers + rows as an aligned table, OR — if --json is set —
// emits the rows as a JSON array of objects keyed by the (lowercased) headers.
//
// WHY map headers→cells for the JSON path: it produces self-describing output
// ({"id":"...","name":"..."}) without each command having to define a parallel
// struct purely for serialization. For richer/nested JSON a command can call
// Value directly instead.
func (p *Printer) Table(headers []string, rows [][]string) error {
	if p.json {
		objs := make([]map[string]string, 0, len(rows))
		for _, row := range rows {
			obj := make(map[string]string, len(headers))
			for i, h := range headers {
				if i < len(row) {
					obj[h] = row[i]
				}
			}
			objs = append(objs, obj)
		}
		return p.Value(objs)
	}

	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	// Header row.
	for i, h := range headers {
		if i > 0 {
			fmt.Fprint(tw, "\t")
		}
		fmt.Fprint(tw, h)
	}
	fmt.Fprintln(tw)
	// Data rows.
	for _, row := range rows {
		for i := range headers {
			if i > 0 {
				fmt.Fprint(tw, "\t")
			}
			if i < len(row) {
				fmt.Fprint(tw, row[i])
			}
		}
		fmt.Fprintln(tw)
	}
	return tw.Flush()
}

// Value renders an arbitrary value as indented JSON. Used by `get`-style
// commands that show a single rich object, and by Table's JSON branch.
//
// In NON-json mode, Value falls back to a simple key/value dump so a command
// can call Value unconditionally; but in practice commands render a table or a
// keyed list for the human path and reserve Value for --json. Here we keep it
// strict: Value always emits JSON, because callers choose it deliberately.
func (p *Printer) Value(v any) error {
	enc := json.NewEncoder(p.w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("output: encode json: %w", err)
	}
	return nil
}

// KeyVals prints a single object as aligned "Key: value" lines (human mode) or
// as a JSON object (--json). Ideal for `fp models get <id>` style detail views.
//
// pairs is an ordered slice of [key, value] so the human output preserves a
// meaningful field order (a map would randomize it).
func (p *Printer) KeyVals(pairs [][2]string) error {
	if p.json {
		obj := make(map[string]string, len(pairs))
		for _, kv := range pairs {
			obj[kv[0]] = kv[1]
		}
		return p.Value(obj)
	}
	tw := tabwriter.NewWriter(p.w, 0, 4, 2, ' ', 0)
	for _, kv := range pairs {
		fmt.Fprintf(tw, "%s:\t%s\n", kv[0], kv[1])
	}
	return tw.Flush()
}

// Line prints a plain status line (human mode) or a {"message": ...} object
// (--json), so even one-shot confirmations stay script-friendly.
func (p *Printer) Line(format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if p.json {
		return p.Value(map[string]string{"message": msg})
	}
	_, err := fmt.Fprintln(p.w, msg)
	return err
}
