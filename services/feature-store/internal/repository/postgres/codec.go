// codec.go — the JSONB (de)serialization seam between domain types and Postgres.
//
// ============================================================================
// WHY A HAND-WRITTEN CODEC (not encoding/json on the domain structs directly)
// ============================================================================
//
// The domain's FeatureValue is a TAGGED UNION (a Kind discriminator + one
// meaningful field per kind). Letting encoding/json marshal it raw would:
//   - serialize the enum as an integer whose meaning is implicit (brittle if the
//     iota order ever changes), and
//   - emit every zero field (Int, Double, Str, Bool, Time, List, Struct) for every
//     value — noisy and ambiguous (is Double:0 "the value 0.0" or "unset"?).
//
// So we define an explicit on-the-wire shape (jsonValue) with a STRING type tag and
// exactly one carried field, and convert domain<->wire here. This keeps the stored
// JSON self-describing, stable across enum reordering, and round-trip-exact —
// which the integration tests assert (write a value, read it back, it must be ==
// what went in, including []float64 and time.Time).
//
// This codec is the ONE place that knows the JSONB layout; both the EventLog
// (feature_events.feature_values / view_def) and the OfflineViewStore
// (offline_view.feature_values) use it, so the encoding is defined once.
package postgres

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// ----------------------------------------------------------------------------
// FeatureValue <-> JSON
// ----------------------------------------------------------------------------

// valueTypeTag is the STABLE string name we persist for a FeatureValueType. We map
// to strings (not the raw iota int) so the stored payload survives a reordering of
// the domain enum — the on-disk contract is the string, decoupled from the in-memory
// integer. This mirrors the enum's String() method but is kept here as the
// AUTHORITATIVE persistence mapping (String() is for logs; this is for storage).
func valueTypeTag(t domain.FeatureValueType) string {
	switch t {
	case domain.FeatureTypeInt64:
		return "INT64"
	case domain.FeatureTypeDouble:
		return "DOUBLE"
	case domain.FeatureTypeString:
		return "STRING"
	case domain.FeatureTypeBool:
		return "BOOL"
	case domain.FeatureTypeTimestamp:
		return "TIMESTAMP"
	case domain.FeatureTypeDoubleList:
		return "DOUBLE_LIST"
	case domain.FeatureTypeStruct:
		return "STRUCT"
	default:
		return "UNSPECIFIED"
	}
}

// valueTypeFromTag is the inverse of valueTypeTag — used when decoding a stored
// payload back into a domain FeatureValue. An unknown tag decodes to Unspecified,
// which the domain treats as invalid; we never silently coerce.
func valueTypeFromTag(tag string) domain.FeatureValueType {
	switch tag {
	case "INT64":
		return domain.FeatureTypeInt64
	case "DOUBLE":
		return domain.FeatureTypeDouble
	case "STRING":
		return domain.FeatureTypeString
	case "BOOL":
		return domain.FeatureTypeBool
	case "TIMESTAMP":
		return domain.FeatureTypeTimestamp
	case "DOUBLE_LIST":
		return domain.FeatureTypeDoubleList
	case "STRUCT":
		return domain.FeatureTypeStruct
	default:
		return domain.FeatureTypeUnspecified
	}
}

// jsonValue is the on-the-wire shape of a single FeatureValue. `omitempty` on every
// carried field means only the field meaningful for the Type is emitted — the JSON
// stays compact and unambiguous. Type is always present (the discriminator).
//
// NOTE on int64: JSON numbers are float64 in JS land, but Go's encoding/json
// preserves int64 round-trips when the target is int64, so storing Int as a JSON
// number is safe up to 2^53; beyond that we'd risk precision loss. Feature INT64s
// are realistically small (ids, counts); if exact full-range int64 became a
// requirement we'd switch to a string-encoded number. Documented as a known bound.
type jsonValue struct {
	Type   string         `json:"type"`
	Int    int64          `json:"int,omitempty"`
	Double float64        `json:"double,omitempty"`
	Str    string         `json:"str,omitempty"`
	Bool   bool           `json:"bool,omitempty"`
	Time   *time.Time     `json:"time,omitempty"`
	List   []float64      `json:"list,omitempty"`
	Struct map[string]any `json:"struct,omitempty"`
}

// toJSONValue converts a domain FeatureValue to its wire form, carrying ONLY the
// field that matches the Kind. WHY switch on Kind rather than copy all fields: it
// guarantees a value tagged DOUBLE never accidentally persists a stale Str, keeping
// the stored payload a faithful tagged union.
func toJSONValue(v domain.FeatureValue) jsonValue {
	jv := jsonValue{Type: valueTypeTag(v.Kind)}
	switch v.Kind {
	case domain.FeatureTypeInt64:
		jv.Int = v.Int
	case domain.FeatureTypeDouble:
		jv.Double = v.Double
	case domain.FeatureTypeString:
		jv.Str = v.Str
	case domain.FeatureTypeBool:
		jv.Bool = v.Bool
	case domain.FeatureTypeTimestamp:
		// Pointer so the zero time is distinguishable from "absent"; omitempty drops a
		// nil pointer. We store UTC to keep the JSON timezone-stable.
		t := v.Time.UTC()
		jv.Time = &t
	case domain.FeatureTypeDoubleList:
		jv.List = v.List
	case domain.FeatureTypeStruct:
		jv.Struct = v.Struct
	}
	return jv
}

// fromJSONValue is the inverse: reconstruct a domain FeatureValue from the wire form.
func fromJSONValue(jv jsonValue) domain.FeatureValue {
	v := domain.FeatureValue{Kind: valueTypeFromTag(jv.Type)}
	switch v.Kind {
	case domain.FeatureTypeInt64:
		v.Int = jv.Int
	case domain.FeatureTypeDouble:
		v.Double = jv.Double
	case domain.FeatureTypeString:
		v.Str = jv.Str
	case domain.FeatureTypeBool:
		v.Bool = jv.Bool
	case domain.FeatureTypeTimestamp:
		if jv.Time != nil {
			v.Time = *jv.Time
		}
	case domain.FeatureTypeDoubleList:
		v.List = jv.List
	case domain.FeatureTypeStruct:
		v.Struct = jv.Struct
	}
	return v
}

// marshalValues encodes a name->FeatureValue map to JSONB bytes. Returns nil for an
// empty/nil map so the column stores SQL NULL (definition/deletion events carry no
// values) rather than an empty-object literal — keeps "no values" unambiguous.
func marshalValues(values map[string]domain.FeatureValue) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}
	wire := make(map[string]jsonValue, len(values))
	for name, v := range values {
		wire[name] = toJSONValue(v)
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal feature values: %w", err)
	}
	return b, nil
}

// unmarshalValues decodes JSONB bytes back into a name->FeatureValue map. nil/empty
// bytes decode to a nil map (the round-trip inverse of marshalValues' NULL case).
func unmarshalValues(b []byte) (map[string]domain.FeatureValue, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var wire map[string]jsonValue
	if err := json.Unmarshal(b, &wire); err != nil {
		return nil, fmt.Errorf("unmarshal feature values: %w", err)
	}
	out := make(map[string]domain.FeatureValue, len(wire))
	for name, jv := range wire {
		out[name] = fromJSONValue(jv)
	}
	return out, nil
}

// ----------------------------------------------------------------------------
// ViewDefinition <-> JSON (the schema snapshot carried by ViewDefined events)
// ----------------------------------------------------------------------------

// jsonViewDef is the wire form of a ViewDefinition. FeatureSpec.ValueType and the
// entity are flattened to explicit JSON so the stored schema is human-readable and
// type-tags are stable strings (same rationale as jsonValue). We keep this separate
// from the domain struct so a domain refactor never silently changes the on-disk
// format without touching this codec (the place tests pin).
type jsonViewDef struct {
	ID            string     `json:"id"`
	Name          string     `json:"name"`
	Description   string     `json:"description,omitempty"`
	Entity        jsonEntity `json:"entity"`
	Features      []jsonSpec `json:"features"`
	SchemaVersion int64      `json:"schema_version"`
	OwnerUserID   string     `json:"owner_user_id"`
	OwnerTeam     string     `json:"owner_team"`
}

type jsonEntity struct {
	Name        string `json:"name"`
	JoinKey     string `json:"join_key"`
	Description string `json:"description,omitempty"`
}

type jsonSpec struct {
	Name        string `json:"name"`
	ValueType   string `json:"value_type"`
	Description string `json:"description,omitempty"`
	Dimension   int    `json:"dimension,omitempty"`
}

func marshalViewDef(d *domain.ViewDefinition) ([]byte, error) {
	if d == nil {
		return nil, nil
	}
	wire := jsonViewDef{
		ID:            d.ID,
		Name:          d.Name,
		Description:   d.Description,
		Entity:        jsonEntity{Name: d.Entity.Name, JoinKey: d.Entity.JoinKey, Description: d.Entity.Description},
		SchemaVersion: d.SchemaVersion,
		OwnerUserID:   d.OwnerUserID,
		OwnerTeam:     d.OwnerTeam,
	}
	wire.Features = make([]jsonSpec, 0, len(d.Features))
	for _, f := range d.Features {
		wire.Features = append(wire.Features, jsonSpec{
			Name:        f.Name,
			ValueType:   valueTypeTag(f.ValueType),
			Description: f.Description,
			Dimension:   f.Dimension,
		})
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal view def: %w", err)
	}
	return b, nil
}

func unmarshalViewDef(b []byte) (*domain.ViewDefinition, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var wire jsonViewDef
	if err := json.Unmarshal(b, &wire); err != nil {
		return nil, fmt.Errorf("unmarshal view def: %w", err)
	}
	d := &domain.ViewDefinition{
		ID:            wire.ID,
		Name:          wire.Name,
		Description:   wire.Description,
		Entity:        domain.Entity{Name: wire.Entity.Name, JoinKey: wire.Entity.JoinKey, Description: wire.Entity.Description},
		SchemaVersion: wire.SchemaVersion,
		OwnerUserID:   wire.OwnerUserID,
		OwnerTeam:     wire.OwnerTeam,
	}
	d.Features = make([]domain.FeatureSpec, 0, len(wire.Features))
	for _, f := range wire.Features {
		d.Features = append(d.Features, domain.FeatureSpec{
			Name:        f.Name,
			ValueType:   valueTypeFromTag(f.ValueType),
			Description: f.Description,
			Dimension:   f.Dimension,
		})
	}
	return d, nil
}

// marshalVector encodes a FeatureVector's value map for the offline_view table. It
// reuses marshalValues but never returns nil — an offline row always has a values
// column (NOT NULL), even if the vector is empty, so we emit "{}" for an empty map.
func marshalVector(values map[string]domain.FeatureValue) ([]byte, error) {
	b, err := marshalValues(values)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return []byte("{}"), nil
	}
	return b, nil
}
