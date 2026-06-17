// conversions.go holds the PURE proto↔domain mapping functions for the Feature
// Store handler. They are split out of featurestore_handler.go so the RPC method
// bodies read as orchestration (validate → convert → call → convert) while the
// fiddly, line-heavy wire mapping lives here, named and individually testable.
//
// ============================================================================
// THE ANTI-CORRUPTION BOUNDARY LIVES IN THIS FILE
// ============================================================================
//
// The domain speaks its own vocabulary: a tagged-union domain.FeatureValue, a
// domain.FeatureValueType enum, Go time.Time, plain strings. The wire speaks
// proto: google.protobuf.Value (a oneof), the generated FeatureValueType enum,
// *timestamppb.Timestamp. Every crossing of that boundary happens HERE, in one
// place, so:
//
//   - the domain never imports protobuf (zero-framework-imports rule), and
//   - a wire-format quirk (e.g. proto encoding every number as a float64) is
//     handled once, not smeared across six RPC methods.
//
// THE INT64 PRECISION TRAP (interview-critical):
//
//	google.protobuf.Value has only a `number_value` (double). A JSON/Value
//	number cannot losslessly carry every int64 (doubles have 53 bits of mantissa;
//	int64 has 63). So when the SCHEMA declares INT64 we must NOT round-trip the
//	value through a float — we convert the proto number to int64 and store it in
//	the domain's Int field, and on the way OUT we emit it back as a number_value
//	(the wire's only option) but the SERVER-SIDE truth is the exact int64. This is
//	the same hazard JSON-based APIs hit with large ids; we contain it at this edge.
package handler

import (
	"fmt"
	"time"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
	featurestorev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/featurestore/v1"
	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// rfc3339 is the canonical text encoding for TIMESTAMP-typed feature values on
// the wire: google.protobuf.Value has no native timestamp kind, so a TIMESTAMP
// domain value is formatted as an RFC3339 string when projected back to the wire.
const rfc3339 = time.RFC3339Nano

// ----------------------------------------------------------------------------
// ENUM MAPPING — proto FeatureValueType ↔ domain FeatureValueType
// ----------------------------------------------------------------------------
//
// Two parallel enums (one proto-generated, one pure-domain) intentionally exist
// so the domain stays proto-free. They have identical ordering by design, but we
// map EXPLICITLY (a switch, not an int cast) so a future reordering of either
// enum can't silently corrupt the mapping — the compiler/tests would catch a
// missing case, an int cast would not.

func protoToDomainValueType(t featurestorev1.FeatureValueType) domain.FeatureValueType {
	switch t {
	case featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_INT64:
		return domain.FeatureTypeInt64
	case featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE:
		return domain.FeatureTypeDouble
	case featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_STRING:
		return domain.FeatureTypeString
	case featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_BOOL:
		return domain.FeatureTypeBool
	case featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_TIMESTAMP:
		return domain.FeatureTypeTimestamp
	case featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE_LIST:
		return domain.FeatureTypeDoubleList
	case featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_STRUCT:
		return domain.FeatureTypeStruct
	default:
		// UNSPECIFIED and any unknown future enum value collapse to the domain's
		// zero value (Unspecified), which DefineFeatureView rejects as a validation
		// error. Failing closed (unknown ⇒ invalid) is the safe default.
		return domain.FeatureTypeUnspecified
	}
}

func domainToProtoValueType(t domain.FeatureValueType) featurestorev1.FeatureValueType {
	switch t {
	case domain.FeatureTypeInt64:
		return featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_INT64
	case domain.FeatureTypeDouble:
		return featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE
	case domain.FeatureTypeString:
		return featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_STRING
	case domain.FeatureTypeBool:
		return featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_BOOL
	case domain.FeatureTypeTimestamp:
		return featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_TIMESTAMP
	case domain.FeatureTypeDoubleList:
		return featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_DOUBLE_LIST
	case domain.FeatureTypeStruct:
		return featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_STRUCT
	default:
		return featurestorev1.FeatureValueType_FEATURE_VALUE_TYPE_UNSPECIFIED
	}
}

// ----------------------------------------------------------------------------
// ENTITY / FEATURESPEC MAPPING
// ----------------------------------------------------------------------------

// protoToDomainEntity maps the wire Entity to the domain Entity. A nil proto
// Entity (the field omitted on the request) becomes a zero-value domain.Entity,
// which DefineFeatureView's validation then rejects (empty name) — we don't
// special-case nil here; we let validation own that decision in one place.
func protoToDomainEntity(p *featurestorev1.Entity) domain.Entity {
	if p == nil {
		return domain.Entity{}
	}
	return domain.Entity{
		Name:        p.GetName(),
		JoinKey:     p.GetJoinKey(),
		Description: p.GetDescription(),
	}
}

func domainToProtoEntity(e domain.Entity) *featurestorev1.Entity {
	return &featurestorev1.Entity{
		Name:        e.Name,
		JoinKey:     e.JoinKey,
		Description: e.Description,
	}
}

func protoToDomainFeatureSpecs(specs []*featurestorev1.FeatureSpec) []domain.FeatureSpec {
	out := make([]domain.FeatureSpec, 0, len(specs))
	for _, s := range specs {
		if s == nil {
			continue
		}
		out = append(out, domain.FeatureSpec{
			Name:        s.GetName(),
			ValueType:   protoToDomainValueType(s.GetValueType()),
			Description: s.GetDescription(),
			Dimension:   int(s.GetDimension()),
		})
	}
	return out
}

func domainToProtoFeatureSpecs(specs []domain.FeatureSpec) []*featurestorev1.FeatureSpec {
	out := make([]*featurestorev1.FeatureSpec, 0, len(specs))
	for _, s := range specs {
		out = append(out, &featurestorev1.FeatureSpec{
			Name:        s.Name,
			ValueType:   domainToProtoValueType(s.ValueType),
			Description: s.Description,
			Dimension:   int32(s.Dimension),
		})
	}
	return out
}

// ----------------------------------------------------------------------------
// FEATUREVIEW MAPPING (domain → proto only — the client never sends a FeatureView)
// ----------------------------------------------------------------------------

// domainToProtoFeatureView maps a domain.FeatureView to the wire FeatureView,
// including the SERVER-AUTHORITATIVE fields (id/owner/team/version/timestamps)
// the domain stamped. A ZERO time becomes a nil proto Timestamp (not an epoch-0
// timestamp) so an unset DeletedAt serializes as "absent" — the wire signal for
// "live view" — rather than "deleted at 1970".
func domainToProtoFeatureView(v domain.FeatureView) *featurestorev1.FeatureView {
	return &featurestorev1.FeatureView{
		Id:            v.ID,
		Name:          v.Name,
		Description:   v.Description,
		Entity:        domainToProtoEntity(v.Entity),
		Features:      domainToProtoFeatureSpecs(v.Features),
		SchemaVersion: v.SchemaVersion,
		OwnerUserId:   v.OwnerUserID,
		OwnerTeam:     v.OwnerTeam,
		CreatedAt:     timeToProto(v.CreatedAt),
		UpdatedAt:     timeToProto(v.UpdatedAt),
		DeletedAt:     timeToProto(v.DeletedAt), // zero ⇒ nil ⇒ "not deleted"
	}
}

// ----------------------------------------------------------------------------
// FEATUREVALUE MAPPING — the tagged union ↔ google.protobuf.Value
// ----------------------------------------------------------------------------
//
// This is the trickiest crossing because one wire type (Value, a oneof over
// null/number/string/bool/struct/list) must carry SEVEN domain kinds. The READ
// direction (domain → wire) is SCHEMA-DRIVEN and TOTAL: the service already typed
// the value against the schema, so each domain kind has one correct encoding (see
// domainToProtoFeatureValue). The WRITE direction (wire → domain) is by intrinsic
// wire kind because the handler lacks the schema — that conversion lives in
// protoValueByWireKind (below, in the write-path section) and the service does the
// authoritative schema check. Keeping the two directions separate is deliberate:
// the asymmetry (typed on read, provisional on write) is the whole reason the
// service, not the handler, owns schema validation.

// domainToProtoFeatureValue maps a domain FeatureValue back to a wire Value. It
// is total (every domain kind has a wire encoding) and never errors: the value
// already passed validation on the way in / was produced by the trusted fold.
func domainToProtoFeatureValue(fv domain.FeatureValue) *structpb.Value {
	switch fv.Kind {
	case domain.FeatureTypeInt64:
		// The wire's only numeric carrier is a double; the server-side truth is the
		// exact Int field. For ids beyond 2^53 a richer encoding would be needed,
		// but the domain stores the exact int64 either way (see file header).
		return structpb.NewNumberValue(float64(fv.Int))
	case domain.FeatureTypeDouble:
		return structpb.NewNumberValue(fv.Double)
	case domain.FeatureTypeString:
		return structpb.NewStringValue(fv.Str)
	case domain.FeatureTypeBool:
		return structpb.NewBoolValue(fv.Bool)
	case domain.FeatureTypeTimestamp:
		return structpb.NewStringValue(fv.Time.UTC().Format(rfc3339))
	case domain.FeatureTypeDoubleList:
		vals := make([]*structpb.Value, 0, len(fv.List))
		for _, f := range fv.List {
			vals = append(vals, structpb.NewNumberValue(f))
		}
		return structpb.NewListValue(&structpb.ListValue{Values: vals})
	case domain.FeatureTypeStruct:
		// NewStruct can fail only on unrepresentable values (e.g. NaN keys); the
		// map came from a previously-accepted Value, so on the rare error we emit a
		// null rather than dropping the field silently or panicking.
		s, err := structpb.NewStruct(fv.Struct)
		if err != nil {
			return structpb.NewNullValue()
		}
		return structpb.NewStructValue(s)
	default:
		return structpb.NewNullValue()
	}
}

// domainToProtoFeatureVector maps a read-model FeatureVector (with provenance)
// to the wire FeatureVector.
func domainToProtoFeatureVector(v domain.FeatureVector) *featurestorev1.FeatureVector {
	values := make(map[string]*structpb.Value, len(v.Values))
	for name, fv := range v.Values {
		values[name] = domainToProtoFeatureValue(fv)
	}
	return &featurestorev1.FeatureVector{
		EntityId:      v.EntityID,
		Values:        values,
		AsOfVersion:   v.AsOfVersion,
		EventTime:     timeToProto(v.EventTime),
		SchemaVersion: v.SchemaVersion,
	}
}

func domainToProtoFeatureVectors(vs []domain.FeatureVector) []*featurestorev1.FeatureVector {
	out := make([]*featurestorev1.FeatureVector, 0, len(vs))
	for _, v := range vs {
		out = append(out, domainToProtoFeatureVector(v))
	}
	return out
}

// ----------------------------------------------------------------------------
// WRITE-PATH ROW MAPPING — wire FeatureValues → domain FeatureRow (SCHEMA-DRIVEN)
// ----------------------------------------------------------------------------
//
// WHY this conversion is now SCHEMA-DRIVEN (and what changed / why):
//
//	google.protobuf.Value is a lossy carrier — it has only number/string/bool/
//	list/struct/null kinds. Two of the domain's seven feature types have NO native
//	wire kind: INT64 rides inside a `number_value` (a double) and TIMESTAMP rides
//	inside a `string_value` (RFC3339 text). So a wire number is AMBIGUOUS (DOUBLE
//	or INT64?) and a wire string is AMBIGUOUS (STRING or TIMESTAMP?). Only the
//	declared SCHEMA disambiguates them.
//
//	An earlier version mapped purely by intrinsic wire kind (number→Double,
//	string→String) and asserted "the service reconciles INT64/TIMESTAMP during
//	validation." That reconciliation never existed: the domain's validateRow does a
//	STRICT equality check (val.Kind == spec.ValueType) with no narrowing. The net
//	effect was that EVERY write to an INT64 or TIMESTAMP feature was rejected with
//	ErrSchemaViolation — those two types were unwritable end-to-end. The handler is
//	the only layer that holds the raw wire Value, so the disambiguation MUST happen
//	here, against the schema.
//
//	The cost we accept (the tradeoff the previous design tried to avoid): WriteFeatures
//	now reads the FeatureView's schema before converting. That is one extra read and
//	a small TOCTOU window vs a concurrent schema evolution — but schema evolution is
//	ADDITIVE (DefineFeatureView only adds features / bumps the version; it never
//	re-types or removes one), so a value valid against the schema we read stays valid.
//	The service ALSO re-validates authoritatively (defense in depth), so a racing
//	change at worst yields a clean ErrSchemaViolation, never a corrupt write.
//
//	READ vs WRITE symmetry restored: reads were already schema-driven (the service
//	emits typed FeatureValues, so an INT64 comes back exactly via domainToProtoFeatureValue);
//	writes are now schema-driven too. INT64 → Int field, TIMESTAMP → Time field, and
//	the domain's equality check passes for all seven types.

// protoRowsToDomain converts the wire rows to domain rows using the view's schema
// (specs) to resolve the ambiguous wire kinds (number→INT64/DOUBLE, string→
// TIMESTAMP/STRING). A value for a feature NOT in the schema is converted by its
// intrinsic wire kind and left for the service's authoritative check to reject as
// an unknown feature (ErrSchemaViolation) — we don't duplicate that rejection here,
// keeping "is this feature in the schema" a single decision the service owns.
func protoRowsToDomain(rows []*featurestorev1.FeatureValues, specs map[string]domain.FeatureValueType) ([]domain.FeatureRow, error) {
	out := make([]domain.FeatureRow, 0, len(rows))
	for i, r := range rows {
		if r == nil {
			return nil, fmt.Errorf("feature row %d is nil", i)
		}
		if r.GetEntityId() == "" {
			return nil, fmt.Errorf("feature row %d: entity_id is required", i)
		}
		values := make(map[string]domain.FeatureValue, len(r.GetValues()))
		for name, v := range r.GetValues() {
			// declared is the schema-declared type for this feature, or Unspecified if
			// the feature is not in the schema (left for the service to reject).
			declared := specs[name]
			fv, err := protoValueToDomain(name, v, declared)
			if err != nil {
				return nil, fmt.Errorf("feature row %d: %w", i, err)
			}
			values[name] = fv
		}
		out = append(out, domain.FeatureRow{
			EntityID:  r.GetEntityId(),
			Values:    values,
			EventTime: protoToTime(r.GetEventTime()), // zero ⇒ service defaults to ingest time
		})
	}
	return out, nil
}

// featureSpecTypes builds the name→declared-type lookup the write path uses to
// disambiguate wire kinds. Pulled from the view the handler fetched before
// converting (see WriteFeatures).
func featureSpecTypes(view domain.FeatureView) map[string]domain.FeatureValueType {
	m := make(map[string]domain.FeatureValueType, len(view.Features))
	for _, f := range view.Features {
		m[f.Name] = f.ValueType
	}
	return m
}

// protoValueToDomain maps a single wire Value to a domain FeatureValue, using the
// DECLARED type to disambiguate the two lossy wire encodings:
//
//   - a wire number is INT64 when the schema declares INT64 (we convert to int64,
//     rejecting a non-integral double so 1.5 can't masquerade as an int), otherwise
//     it is a DOUBLE.
//   - a wire string is TIMESTAMP when the schema declares TIMESTAMP (we parse it as
//     RFC3339, the encoding domainToProtoFeatureValue emits on the way out), otherwise
//     it is a STRING.
//
// For every other declared type — and for a feature with no declared type (not in
// the schema) — we fall back to the intrinsic wire kind; the service's validateRow
// then rejects any genuine mismatch. A null/empty Value is an error (a feature row
// must carry a real value for each named feature).
func protoValueToDomain(name string, v *structpb.Value, declared domain.FeatureValueType) (domain.FeatureValue, error) {
	if v == nil {
		return domain.FeatureValue{}, fmt.Errorf("feature %q: missing value", name)
	}
	switch k := v.GetKind().(type) {
	case *structpb.Value_NumberValue:
		if declared == domain.FeatureTypeInt64 {
			// INT64 rides inside a double on the wire. Reject a non-integral value
			// rather than silently truncating: 1.5 written to an INT64 feature is a
			// caller bug, not a value to floor. (float64 carries integers exactly up to
			// 2^53; ids beyond that need a string-encoded int — out of scope here.)
			n := k.NumberValue
			if n != float64(int64(n)) {
				return domain.FeatureValue{}, fmt.Errorf("feature %q: value %v is not an integer for INT64", name, n)
			}
			return domain.FeatureValue{Kind: domain.FeatureTypeInt64, Int: int64(n)}, nil
		}
		// DOUBLE (or an unknown feature, left for the service to reject).
		return domain.FeatureValue{Kind: domain.FeatureTypeDouble, Double: k.NumberValue}, nil
	case *structpb.Value_StringValue:
		if declared == domain.FeatureTypeTimestamp {
			// TIMESTAMP rides as an RFC3339 string (the same format the read path emits
			// via domainToProtoFeatureValue). Parse it here so the domain stores a real
			// time.Time and validateRow's Kind check passes.
			t, err := time.Parse(rfc3339, k.StringValue)
			if err != nil {
				return domain.FeatureValue{}, fmt.Errorf("feature %q: value %q is not RFC3339 for TIMESTAMP: %w", name, k.StringValue, err)
			}
			return domain.FeatureValue{Kind: domain.FeatureTypeTimestamp, Time: t}, nil
		}
		return domain.FeatureValue{Kind: domain.FeatureTypeString, Str: k.StringValue}, nil
	case *structpb.Value_BoolValue:
		return domain.FeatureValue{Kind: domain.FeatureTypeBool, Bool: k.BoolValue}, nil
	case *structpb.Value_ListValue:
		if k.ListValue == nil {
			return domain.FeatureValue{}, fmt.Errorf("feature %q: empty list value", name)
		}
		nums := make([]float64, 0, len(k.ListValue.GetValues()))
		for idx, el := range k.ListValue.GetValues() {
			ne, ok := el.GetKind().(*structpb.Value_NumberValue)
			if !ok {
				return domain.FeatureValue{}, fmt.Errorf("feature %q: list element %d is not a number", name, idx)
			}
			nums = append(nums, ne.NumberValue)
		}
		return domain.FeatureValue{Kind: domain.FeatureTypeDoubleList, List: nums}, nil
	case *structpb.Value_StructValue:
		if k.StructValue == nil {
			return domain.FeatureValue{}, fmt.Errorf("feature %q: empty struct value", name)
		}
		return domain.FeatureValue{Kind: domain.FeatureTypeStruct, Struct: k.StructValue.AsMap()}, nil
	case *structpb.Value_NullValue:
		return domain.FeatureValue{}, fmt.Errorf("feature %q: null value not allowed", name)
	default:
		return domain.FeatureValue{}, fmt.Errorf("feature %q: unsupported value kind", name)
	}
}

// ----------------------------------------------------------------------------
// PAGINATION MAPPING — common.v1 ↔ domain.ListOptions
// ----------------------------------------------------------------------------

// paginationToListOptions maps the shared common.v1.PaginationRequest to the
// domain's ListOptions. A nil pagination (field omitted) yields the zero
// ListOptions, which the service treats as DefaultPageSize / first page. Page
// size is CLAMPED (not rejected) — a too-large page is harmless to bound.
func paginationToListOptions(p *commonv1.PaginationRequest) domain.ListOptions {
	if p == nil {
		return domain.ListOptions{}
	}
	return domain.ListOptions{
		PageSize:  int(p.GetPageSize()),
		PageToken: p.GetPageToken(),
	}
}

// newPaginationResponse builds the shared response pagination wrapper. TotalCount
// is -1 ("unknown/expensive to compute") — the platform convention for a count
// the read model does not cheaply materialize; clients treat -1 as "unknown".
func newPaginationResponse(nextToken string) *commonv1.PaginationResponse {
	return &commonv1.PaginationResponse{
		NextPageToken: nextToken,
		TotalCount:    -1,
	}
}

// ----------------------------------------------------------------------------
// REBUILD TARGET MAPPING
// ----------------------------------------------------------------------------

func protoToDomainRebuildTarget(t featurestorev1.RebuildTarget) domain.RebuildTarget {
	switch t {
	case featurestorev1.RebuildTarget_REBUILD_TARGET_ONLINE:
		return domain.RebuildTargetOnline
	case featurestorev1.RebuildTarget_REBUILD_TARGET_OFFLINE:
		return domain.RebuildTargetOffline
	default:
		// UNSPECIFIED and ALL both mean "rebuild everything" — the proto doc
		// explicitly collapses _UNSPECIFIED into ALL, so we do the same here.
		return domain.RebuildTargetAll
	}
}

// ----------------------------------------------------------------------------
// TIMESTAMP HELPERS
// ----------------------------------------------------------------------------

// timeToProto converts a Go time to a proto Timestamp, mapping the ZERO time to
// nil. A nil Timestamp is the wire's "field absent"; an epoch-0 Timestamp would
// be a real (wrong) instant. Optional fields (deleted_at) and unset times depend
// on this distinction.
func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// protoToTime converts a proto Timestamp to a Go time, mapping nil/invalid to
// the zero time. The service treats a zero as.Of as ErrAsOfRequired and a zero
// event_time as "default to ingest time", so collapsing nil → zero here keeps
// those decisions in the domain rather than guessing at the boundary.
func protoToTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil || !ts.IsValid() {
		return time.Time{}
	}
	return ts.AsTime()
}
