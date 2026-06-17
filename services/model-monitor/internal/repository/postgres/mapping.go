// mapping.go — the ONE place domain types ⇄ Postgres rows are translated.
//
// ============================================================================
// WHY A DEDICATED MAPPING FILE (single translation point)
// ============================================================================
//
// The domain must stay pure (no SQL/JSON tags on its structs), and the SQL must
// stay declarative (no domain logic). The translation between them — column order,
// enum int casts, JSONB (de)serialization of the threshold/metric slices — lives
// here so neither side leaks into the other and the column↔field contract is
// written exactly once per entity. If a domain field is added, this file is the one
// place the persistence shape changes.
//
// JSONB encoding choice: thresholds ([]ThresholdConfig) and a report's metrics
// ([]DriftMetric) are always read/written WHOLE (never queried by an inner field),
// so we marshal the whole slice to a private DTO with explicit json tags. WHY a DTO
// and not json-tag the domain struct directly: the domain is framework-free (no
// json tags), and a DTO pins the on-disk JSON shape independently of the Go field
// names — renaming a domain field can't silently corrupt already-stored JSONB.
// ============================================================================
package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// rowScanner is the minimal surface both pgx.Row (single-row QueryRow result) and
// pgx.Rows (iterated Query result) share: a Scan method. Abstracting it lets one
// scan helper serve BOTH single-row reads and list iteration, so the column order
// and JSONB decoding for an entity are written exactly once.
type rowScanner interface {
	Scan(dest ...any) error
}

// ----------------------------------------------------------------------------
// THRESHOLDS — []domain.ThresholdConfig ⇄ JSONB
// ----------------------------------------------------------------------------

// thresholdDTO is the on-disk JSON shape of one ThresholdConfig. Enums are stored as
// their int value (matching the SMALLINT columns elsewhere) so the JSON and the
// typed columns speak the same dialect.
type thresholdDTO struct {
	DriftType int     `json:"drift_type"`
	Method    int     `json:"method"`
	Warn      float64 `json:"warn"`
	Critical  float64 `json:"critical"`
}

// encodeThresholds marshals the threshold slice to JSONB bytes. An empty/nil slice
// becomes "[]" (not "null") so the column default and the round-trip agree and a
// reader never has to special-case null.
func encodeThresholds(ts []domain.ThresholdConfig) ([]byte, error) {
	dtos := make([]thresholdDTO, 0, len(ts))
	for _, t := range ts {
		dtos = append(dtos, thresholdDTO{
			DriftType: int(t.DriftType),
			Method:    int(t.Method),
			Warn:      t.WarnScore,
			Critical:  t.CriticalScore,
		})
	}
	b, err := json.Marshal(dtos)
	if err != nil {
		return nil, fmt.Errorf("encode thresholds: %w", err)
	}
	return b, nil
}

// decodeThresholds inverts encodeThresholds. Empty bytes / "null" / "[]" all yield a
// nil slice so the reconstructed Monitor matches a freshly-built one with no
// thresholds (ThresholdFor returns false either way).
func decodeThresholds(b []byte) ([]domain.ThresholdConfig, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var dtos []thresholdDTO
	if err := json.Unmarshal(b, &dtos); err != nil {
		return nil, fmt.Errorf("decode thresholds: %w", err)
	}
	if len(dtos) == 0 {
		return nil, nil
	}
	out := make([]domain.ThresholdConfig, 0, len(dtos))
	for _, d := range dtos {
		out = append(out, domain.ThresholdConfig{
			DriftType:     domain.DriftType(d.DriftType),
			Method:        domain.DriftMethod(d.Method),
			WarnScore:     d.Warn,
			CriticalScore: d.Critical,
		})
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// METRICS — []domain.DriftMetric ⇄ JSONB
// ----------------------------------------------------------------------------

// metricDTO is the on-disk JSON shape of one DriftMetric (a report's per-feature /
// per-output breakdown). Score/baseline/current are the actionable numbers an
// engineer reads ("income PSI was 0.04, now 0.41"); severity is this metric's rung.
type metricDTO struct {
	Name          string  `json:"name"`
	Method        int     `json:"method"`
	Score         float64 `json:"score"`
	BaselineValue float64 `json:"baseline_value"`
	CurrentValue  float64 `json:"current_value"`
	Severity      int     `json:"severity"`
}

// encodeMetrics marshals the metric slice to JSONB bytes ("[]" for empty, never
// "null").
func encodeMetrics(ms []domain.DriftMetric) ([]byte, error) {
	dtos := make([]metricDTO, 0, len(ms))
	for _, m := range ms {
		dtos = append(dtos, metricDTO{
			Name:          m.Name,
			Method:        int(m.Method),
			Score:         m.Score,
			BaselineValue: m.BaselineValue,
			CurrentValue:  m.CurrentValue,
			Severity:      int(m.Severity),
		})
	}
	b, err := json.Marshal(dtos)
	if err != nil {
		return nil, fmt.Errorf("encode metrics: %w", err)
	}
	return b, nil
}

// decodeMetrics inverts encodeMetrics. Empty/[]/null → nil slice.
func decodeMetrics(b []byte) ([]domain.DriftMetric, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var dtos []metricDTO
	if err := json.Unmarshal(b, &dtos); err != nil {
		return nil, fmt.Errorf("decode metrics: %w", err)
	}
	if len(dtos) == 0 {
		return nil, nil
	}
	out := make([]domain.DriftMetric, 0, len(dtos))
	for _, d := range dtos {
		out = append(out, domain.DriftMetric{
			Name:          d.Name,
			Method:        domain.DriftMethod(d.Method),
			Score:         d.Score,
			BaselineValue: d.BaselineValue,
			CurrentValue:  d.CurrentValue,
			Severity:      domain.DriftSeverity(d.Severity),
		})
	}
	return out, nil
}
