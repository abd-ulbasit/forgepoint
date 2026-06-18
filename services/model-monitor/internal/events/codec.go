package events

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// WIRE CODEC — encode (produce) + dual-format decode (consume)
// ============================================================================
//
// See the package doc's "WIRE-FORMAT NOTE": the platform is now UNIFORM on
// canonical proto-JSON for proto events. natsutil.Publisher protojson.Marshal's
// every proto.Message (and reserves encoding/json for plain Go structs), so every
// producer that hands a generated events.v1 message to Publish emits the SAME
// canonical shape. The events Model Monitor consumes — InferenceCompleted
// (inference-gateway), ModelPromoted (registry), FeaturesWritten (feature-store) —
// are ALL canonical protojson on the wire today. The decoder below still accepts
// encoding/json as a FALLBACK, but that is defensive compat tolerance (a
// pre-dialect-fix producer during a rolling deploy, or a future plain-struct
// producer), NOT a description of current producer behavior — protojson-first is
// the path that fires for every real producer.
//
// The encoder (for what WE produce) uses protojson, the canonical form, because
// OUR consumer that actually decodes the payload — the pipeline-orchestrator
// retrain subscriber — uses protojson (unmarshalCanonical). Notification consumes
// our event OPAQUELY (matches on EventEnvelope.type, never decodes the payload), so
// it is indifferent to the encoding; experiment-tracker reads it with protojson
// too. Producing canonical proto-JSON is therefore correct for every real consumer
// AND round-trips through our own dual-format decoder in tests.
//
// WHY protojson for OUR payloads specifically (not raw-proto-via-json.Marshal):
// ModelDriftDetected carries a google.protobuf.Struct (retrain_context) and
// google.protobuf.Timestamp fields. encoding/json on those well-known types
// produces a shape protojson.Unmarshal CANNOT read (a Struct's internal
// representation, a Timestamp as {seconds,nanos} instead of an RFC-3339 string).
// The orchestrator decodes with protojson, so we MUST encode with protojson — a
// raw-proto json.Marshal here would silently break the loop-closing consumer.

// marshalCanonical encodes an events.v1 payload as canonical proto-JSON (the wire
// form our consumers protojson.Unmarshal). Returns the raw bytes to splice into
// EventEnvelope.Data via a json.RawMessage (json.Marshal of a json.RawMessage is
// the identity, so natsutil.Publisher embeds these exact bytes unchanged).
//
// Options: defaults (omit zero-valued scalars, RFC-3339 timestamps, camelCase
// names). EmitUnpopulated is OFF — a consumer treats an absent field as its zero
// value (the proto3 contract), and we don't bloat the wire with explicit zeros.
func marshalCanonical(msg proto.Message) (json.RawMessage, error) {
	b, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("events: protojson marshal %T: %w", msg, err)
	}
	return json.RawMessage(b), nil
}

// decodeCanonical decodes an EventEnvelope.Data payload into the given events.v1
// message. Canonical proto-JSON is the form EVERY producer emits today (the
// platform is uniform on protojson — see the header note), so branch 1 always
// succeeds for current traffic; branch 2 is a defensive compat fallback.
//
//  1. Canonical proto-JSON (protojson) — the wire form ALL of today's producers
//     emit (registry, inference-gateway, feature-store et al. all hand raw proto
//     to a Publisher that protojson.Marshal's it). Tried FIRST because it is the
//     well-known-type-correct form and the path that fires in practice.
//     DiscardUnknown is set so a producer that adds a field this consumer's
//     generated types predate does not break decoding (forward compatibility — the
//     same posture the orchestrator's unmarshalCanonical takes).
//
//  2. encoding/json struct form — a FALLBACK for compat tolerance, NOT current
//     producer behavior. It would fire only for an OLDER producer build still
//     json.Marshal-ing raw proto during a rolling deploy (pre-dialect-fix), or a
//     future producer that publishes a plain (non-proto) struct by design. The
//     generated structs carry snake_case json tags, so a plain json.Unmarshal
//     faithfully reconstructs scalar/enum/map fields and the well-known types via
//     their Go-struct JSON. For today's protojson traffic this branch is never
//     reached.
//
// WHY try protojson first (not encoding/json first): protojson is STRICTER and
// well-known-type-aware, so for the canonical payload every producer sends it
// decodes correctly, whereas a plain json.Unmarshal of canonical proto-JSON would
// mis-handle a Timestamp string / Struct. For a stray encoding/json payload (the
// compat case), protojson fails fast (e.g. a Timestamp object where it expects a
// string) and we fall through. The two forms are distinguishable enough that
// "prefer canonical, fall back" decodes each correctly without ambiguity for the
// fields our handlers read.
//
// Returns an error ONLY when BOTH decoders fail — a genuinely unparseable payload
// (a poison message). Callers wrap that as non-retryable.
func decodeCanonical(data json.RawMessage, msg proto.Message) error {
	// 1) canonical proto-JSON (preferred).
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, msg); err == nil {
		return nil
	}
	// 2) encoding/json struct form — defensive compat fallback only (a pre-dialect-fix
	//    producer mid-rolling-deploy, or a future plain-struct producer); never hit by
	//    today's protojson-uniform producers. proto.Reset first so a partial protojson
	//    decode above can't leave stale fields populated underneath the json decode.
	proto.Reset(msg)
	if err := json.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("events: decode %T (tried protojson then encoding/json): %w", msg, err)
	}
	return nil
}
