// mapping.go — the ROW ⇄ DOMAIN translation boundary.
//
// ============================================================================
// THIS IS WHERE THE STORAGE SHAPE MEETS THE DOMAIN SHAPE
// ============================================================================
//
// The domain models tags as map[string]string, final metrics as
// []domain.MetricPoint, and artifacts as map[string]any. Postgres stores these
// as JSONB. These helpers are the SINGLE place that marshals/unmarshals across
// that boundary, so neither the SQL nor the domain leaks into the other. Keeping
// the (un)marshaling here — not inline in each query — means one auditable
// conversion and one place to handle the NULL/empty-JSON edge cases.
//
// WHY explicit json.Marshal rather than leaning on pgx's automatic map encoding:
// we want EXPLICIT control of the JSONB bytes (and the nil → SQL value decision)
// so a round-trip is faithful — a nil artifacts map comes back nil (not "{}"),
// an empty tags map comes back as an empty map (not nil), and the stored JSON key
// names are STABLE (decoupled from Go field names via the wire structs below) so
// renaming a domain field later can't silently break already-stored rows.
// ============================================================================
package postgres

import (
	"encoding/json"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/experiment-tracker/internal/domain"
)

// ----------------------------------------------------------------------------
// JSONB ⇄ tags (map[string]string)
// ----------------------------------------------------------------------------

// marshalTags encodes the experiment's tag map to JSONB bytes. A nil/empty map
// encodes to the JSON object "{}" (NOT SQL NULL): the column is NOT NULL DEFAULT
// '{}', and tags are conceptually "always a map, possibly empty" — so the read
// path never has to special-case NULL and an absent-tags experiment round-trips
// as an empty (non-nil semantics) map. We return []byte("{}") explicitly rather
// than relying on the column default so an explicit replace-with-empty (clearing
// all tags via the update mask) actually writes "{}".
func marshalTags(tags map[string]string) ([]byte, error) {
	if len(tags) == 0 {
		return []byte("{}"), nil
	}
	return json.Marshal(tags)
}

// unmarshalTags decodes stored JSONB back to a tag map. "{}"/NULL/empty decode to
// an empty (allocated) map so callers never deref nil — the experiment always has
// a tags map. WHY allocate even for empty: the domain's UpdateExperiment replaces
// tags wholesale; returning a non-nil empty map keeps "no tags" and "tags=[]"
// indistinguishable and avoids a surprising nil at a call site that ranges it.
func unmarshalTags(b []byte) (map[string]string, error) {
	out := make(map[string]string)
	if len(b) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// JSONB ⇄ artifacts (map[string]any)
// ----------------------------------------------------------------------------

// marshalArtifacts encodes the run's free-form artifacts to JSONB. A nil/empty
// map encodes to SQL NULL (returned as nil []byte) rather than "{}": "no
// artifacts set" and "an empty artifacts object" are the same here, and NULL is
// the honest durable representation (the artifacts column is nullable, the domain
// carries nil when unset). This keeps the round-trip faithful — what went in nil
// comes back nil.
func marshalArtifacts(m map[string]any) ([]byte, error) {
	if len(m) == 0 {
		return nil, nil // → SQL NULL
	}
	return json.Marshal(m)
}

// unmarshalArtifacts decodes JSONB artifacts back to a domain map. NULL/empty
// bytes decode to a nil map (the inverse of marshalArtifacts), preserving the
// domain's "nil = none" convention.
func unmarshalArtifacts(b []byte) (map[string]any, error) {
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
// JSONB ⇄ final metrics ([]domain.MetricPoint)
// ----------------------------------------------------------------------------
//
// FinalMetrics is the DENORMALIZED CQRS read model stored on the run. We persist
// it as a JSONB array of metricPointRow (explicit snake_case json tags) so the
// stored shape is STABLE and decoupled from the Go field names. Storing the
// finals inline on the run (vs re-deriving from run_metrics on every read) is the
// whole point of the projection: ListRuns/leaderboards render the headline number
// with zero extra reads.

// metricPointRow is the stable wire shape of a MetricPoint inside the
// final_metrics JSONB. ts is an RFC3339 timestamp (time.Time JSON-encodes as
// such), preserving the server-stamped instant losslessly.
type metricPointRow struct {
	Key   string    `json:"key"`
	Value float64   `json:"value"`
	Step  int64     `json:"step"`
	TS    time.Time `json:"ts"`
}

// marshalFinalMetrics encodes the denormalized headline metrics to a JSONB array.
// We always encode a (possibly empty) ARRAY — never NULL — so the column's
// NOT NULL DEFAULT '[]' invariant holds and a read always gets a valid array
// (empty while the run is RUNNING, populated at FinishRun).
func marshalFinalMetrics(points []domain.MetricPoint) ([]byte, error) {
	rows := make([]metricPointRow, len(points))
	for i, p := range points {
		rows[i] = metricPointRow{Key: p.Key, Value: p.Value, Step: p.Step, TS: p.Timestamp}
	}
	return json.Marshal(rows) // empty slice → "[]", never "null"
}

// unmarshalFinalMetrics decodes the stored finals JSONB back to domain points,
// restoring the timestamp. The inverse of marshalFinalMetrics; together they
// guarantee the projection survives a store/load round-trip at the domain level.
func unmarshalFinalMetrics(b []byte) ([]domain.MetricPoint, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var rows []metricPointRow
	if err := json.Unmarshal(b, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]domain.MetricPoint, len(rows))
	for i, r := range rows {
		out[i] = domain.MetricPoint{Key: r.Key, Value: r.Value, Step: r.Step, Timestamp: r.TS}
	}
	return out, nil
}
