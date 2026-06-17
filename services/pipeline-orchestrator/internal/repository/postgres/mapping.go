// mapping.go — the ROW ⇄ DOMAIN translation boundary.
//
// ============================================================================
// THIS IS WHERE THE WIRE/STORAGE SHAPE MEETS THE DOMAIN SHAPE
// ============================================================================
//
// The domain models step graphs, configs, outputs, and inputs as map[string]any
// (and StepDefinition slices). Postgres stores them as JSONB. The domain also
// models its enums as Go int types (PipelineType, StepType, ExecutionStatus,
// StepStatus) whose integer values we persist as SMALLINT. These helpers are the
// SINGLE place that marshals/unmarshals across that boundary, so neither the SQL
// nor the domain leaks into the other. Keeping the (un)marshaling here — not
// inline in each query — means one auditable conversion and one place to handle
// the null/empty-JSON edge cases.
//
// WHY json.Marshal for the maps rather than pgx's automatic map encoding: we
// want EXPLICIT control of the JSONB bytes (and the nil → SQL NULL decision) so
// a nil map round-trips as NULL, not as the JSON literal "null" or "{}". A round-
// trip that changes nil into a non-nil empty map would subtly break the engine's
// "is there an output yet?" checks. We test that round-trip explicitly.
// ============================================================================
package postgres

import (
	"encoding/json"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// timeFromNanos converts a stored nanosecond count back to a time.Duration.
// Trivial, but isolating it keeps the unit at the conversion boundary obvious
// (the JSONB stores Timeout as an int64 ns count; the domain wants a Duration).
func timeFromNanos(ns int64) time.Duration { return time.Duration(ns) }

// ----------------------------------------------------------------------------
// JSONB ⇄ map[string]any
// ----------------------------------------------------------------------------

// marshalMap encodes a domain map[string]any to JSONB bytes for storage. A nil
// or empty map encodes to a SQL NULL (returned as nil []byte) rather than "{}":
// "the step has produced no output yet" and "the step produced an empty object"
// are semantically the same here, and NULL is the cleaner durable representation
// (it also keeps the IS NULL recovery predicates meaningful). Returns the bytes
// and any marshal error (a non-JSON-encodable value in config — caller surfaces).
func marshalMap(m map[string]any) ([]byte, error) {
	if len(m) == 0 {
		return nil, nil // → SQL NULL
	}
	return json.Marshal(m)
}

// unmarshalMap decodes JSONB bytes back to a domain map. NULL/empty bytes decode
// to a nil map (the inverse of marshalMap), so the round-trip is faithful: what
// went in as nil comes back as nil, preserving the engine's emptiness checks.
func unmarshalMap(b []byte) (map[string]any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// ----------------------------------------------------------------------------
// JSONB ⇄ []domain.StepDefinition  (the template's step graph)
// ----------------------------------------------------------------------------
//
// We persist the StepDefinition slice as a single JSONB document. The fields are
// re-described in a local wire struct (stepDefRow) with explicit json tags so the
// stored shape is STABLE and decoupled from the Go field names — renaming a
// domain field later won't silently break already-stored rows, and the JSON keys
// are snake_case (the platform's stored convention). Timeout is a time.Duration
// (an int64 nanosecond count) which JSON encodes as a number; that is exactly
// what we want to round-trip losslessly.

type stepDefRow struct {
	ID                 string          `json:"id"`
	Name               string          `json:"name"`
	Type               int             `json:"type"`
	DependsOn          []string        `json:"depends_on,omitempty"`
	CompensationStepID string          `json:"compensation_step_id,omitempty"`
	Config             map[string]any  `json:"config,omitempty"`
	Timeout            int64           `json:"timeout_ns,omitempty"` // time.Duration as ns
	MaxRetries         int             `json:"max_retries,omitempty"`
}

// marshalSteps encodes the template's step graph to JSONB. The graph is NOT NULL
// in the schema (the service rejects empty pipelines), but we encode an empty
// slice as "[]" defensively rather than NULL so a read always gets a valid array.
func marshalSteps(steps []domain.StepDefinition) ([]byte, error) {
	rows := make([]stepDefRow, len(steps))
	for i, s := range steps {
		rows[i] = stepDefRow{
			ID:                 s.ID,
			Name:               s.Name,
			Type:               int(s.Type),
			DependsOn:          s.DependsOn,
			CompensationStepID: s.CompensationStepID,
			Config:             s.Config,
			Timeout:            int64(s.Timeout),
			MaxRetries:         s.MaxRetries,
		}
	}
	return json.Marshal(rows)
}

// unmarshalSteps decodes the stored step graph back to domain types, restoring
// the enum (int → StepType) and the duration (ns int64 → time.Duration). This is
// the inverse of marshalSteps; together they guarantee a template survives a
// store/load round-trip byte-for-byte at the domain level.
func unmarshalSteps(b []byte) ([]domain.StepDefinition, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var rows []stepDefRow
	if err := json.Unmarshal(b, &rows); err != nil {
		return nil, err
	}
	out := make([]domain.StepDefinition, len(rows))
	for i, r := range rows {
		out[i] = domain.StepDefinition{
			ID:                 r.ID,
			Name:               r.Name,
			Type:               domain.StepType(r.Type),
			DependsOn:          r.DependsOn,
			CompensationStepID: r.CompensationStepID,
			Config:             r.Config,
			Timeout:            timeFromNanos(r.Timeout),
			MaxRetries:         r.MaxRetries,
		}
	}
	return out, nil
}
