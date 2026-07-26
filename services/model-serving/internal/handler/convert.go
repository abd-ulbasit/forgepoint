// convert.go holds the pure proto↔domain conversion helpers for the serving
// handler. Keeping them in one file (separate from the RPC bodies) makes the
// anti-corruption boundary explicit and each conversion independently testable.
//
// ============================================================================
// THE CONVERSION SEAM — WHY THESE FUNCTIONS EXIST AT ALL
// ============================================================================
//
// The domain must never import generated proto (Clean Architecture's dependency
// rule). So the wire types (servingv1.TensorData, servingv1.ModelStatus, …) and
// the domain types (domain.Tensor, domain.ModelStatus, …) are DISTINCT, even when
// they look identical. These helpers are the ONLY place the two vocabularies
// meet. Every enum is mapped through an EXPLICIT switch (not a numeric cast) so:
//   - an unknown/garbage proto enum is REJECTED rather than silently passed
//     through as a meaningless domain value (the cast `domain.DataType(x)` would
//     happily produce an out-of-range value), and
//   - if the proto enum and the domain enum ever drift in numbering, the switch
//     breaks at compile/test time instead of silently mis-mapping.
//
// WHY NOT JUST CAST THE ENUM INT: because the two enums are
// independent types whose numeric values are a coincidence of today's proto; a
// switch is the contract, a cast is a latent bug.
// ============================================================================
package handler

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	servingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/serving/v1"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

// ----------------------------------------------------------------------------
// TENSORS
// ----------------------------------------------------------------------------

// protoTensorsToDomain converts the proto input-tensor map to domain tensors.
// It rejects a nil map entry and an UNSPECIFIED/garbage dtype at the boundary
// (a wire-level malformation), returning a plain error the caller turns into
// InvalidArgument. It does NOT do the byte-layout / cap checks — those are the
// domain's authoritative job (domain.Tensor.ValidateLayout + service caps).
func protoTensorsToDomain(in map[string]*servingv1.TensorData) (map[string]domain.Tensor, error) {
	out := make(map[string]domain.Tensor, len(in))
	for name, td := range in {
		if td == nil {
			return nil, fmt.Errorf("input tensor %q is nil", name)
		}
		dt, err := protoDataTypeToDomain(td.GetDtype())
		if err != nil {
			return nil, fmt.Errorf("input tensor %q: %v", name, err)
		}
		out[name] = domain.Tensor{
			Shape: td.GetShape(),
			Data:  td.GetData(),
			DType: dt,
		}
	}
	return out, nil
}

// domainTensorsToProto converts domain output tensors back to proto. Output
// tensors are server-produced (from the engine), so no validation is needed —
// the domain guarantees their well-formedness. A nil input map yields a nil
// proto map (proto3 treats an empty/nil map identically on the wire).
func domainTensorsToProto(in map[string]domain.Tensor) map[string]*servingv1.TensorData {
	if in == nil {
		return nil
	}
	out := make(map[string]*servingv1.TensorData, len(in))
	for name, t := range in {
		out[name] = &servingv1.TensorData{
			Shape: t.Shape,
			Data:  t.Data,
			Dtype: domainDataTypeToProto(t.DType),
		}
	}
	return out
}

// ----------------------------------------------------------------------------
// ENUMS — explicit switches both ways (no numeric casts)
// ----------------------------------------------------------------------------

// protoDataTypeToDomain maps a wire DataType to the domain DataType, rejecting
// UNSPECIFIED (the proto forces clients to be explicit) and any unknown value.
func protoDataTypeToDomain(d servingv1.DataType) (domain.DataType, error) {
	switch d {
	case servingv1.DataType_DATA_TYPE_FLOAT32:
		return domain.DataTypeFloat32, nil
	case servingv1.DataType_DATA_TYPE_FLOAT64:
		return domain.DataTypeFloat64, nil
	case servingv1.DataType_DATA_TYPE_INT32:
		return domain.DataTypeInt32, nil
	case servingv1.DataType_DATA_TYPE_INT64:
		return domain.DataTypeInt64, nil
	case servingv1.DataType_DATA_TYPE_BOOL:
		return domain.DataTypeBool, nil
	case servingv1.DataType_DATA_TYPE_STRING:
		return domain.DataTypeString, nil
	case servingv1.DataType_DATA_TYPE_UNSPECIFIED:
		return domain.DataTypeUnspecified, fmt.Errorf("dtype is unspecified")
	default:
		return domain.DataTypeUnspecified, fmt.Errorf("dtype %d is not a valid data type", int32(d))
	}
}

// domainDataTypeToProto maps a domain DataType to the wire DataType. The domain
// only ever holds valid dtypes (it validated on the way in), so UNSPECIFIED maps
// to the proto UNSPECIFIED zero value (it will not appear for real outputs).
func domainDataTypeToProto(d domain.DataType) servingv1.DataType {
	switch d {
	case domain.DataTypeFloat32:
		return servingv1.DataType_DATA_TYPE_FLOAT32
	case domain.DataTypeFloat64:
		return servingv1.DataType_DATA_TYPE_FLOAT64
	case domain.DataTypeInt32:
		return servingv1.DataType_DATA_TYPE_INT32
	case domain.DataTypeInt64:
		return servingv1.DataType_DATA_TYPE_INT64
	case domain.DataTypeBool:
		return servingv1.DataType_DATA_TYPE_BOOL
	case domain.DataTypeString:
		return servingv1.DataType_DATA_TYPE_STRING
	default:
		return servingv1.DataType_DATA_TYPE_UNSPECIFIED
	}
}

// protoStateToDomain maps a wire ModelState to the domain ModelState. Used for
// the ListLoadedModels state_filter. UNSPECIFIED is VALID here (it means "no
// filter"); only an out-of-range value is rejected.
func protoStateToDomain(s servingv1.ModelState) (domain.ModelState, error) {
	switch s {
	case servingv1.ModelState_MODEL_STATE_UNSPECIFIED:
		return domain.StateUnspecified, nil
	case servingv1.ModelState_MODEL_STATE_DOWNLOADING:
		return domain.StateDownloading, nil
	case servingv1.ModelState_MODEL_STATE_LOADING:
		return domain.StateLoading, nil
	case servingv1.ModelState_MODEL_STATE_READY:
		return domain.StateReady, nil
	case servingv1.ModelState_MODEL_STATE_FAILED:
		return domain.StateFailed, nil
	case servingv1.ModelState_MODEL_STATE_UNLOADED:
		return domain.StateUnloaded, nil
	default:
		return domain.StateUnspecified, fmt.Errorf("state_filter %d is not a valid model state", int32(s))
	}
}

// stateToProto maps a domain ModelState to the wire ModelState (always valid).
func stateToProto(s domain.ModelState) servingv1.ModelState {
	switch s {
	case domain.StateDownloading:
		return servingv1.ModelState_MODEL_STATE_DOWNLOADING
	case domain.StateLoading:
		return servingv1.ModelState_MODEL_STATE_LOADING
	case domain.StateReady:
		return servingv1.ModelState_MODEL_STATE_READY
	case domain.StateFailed:
		return servingv1.ModelState_MODEL_STATE_FAILED
	case domain.StateUnloaded:
		return servingv1.ModelState_MODEL_STATE_UNLOADED
	default:
		return servingv1.ModelState_MODEL_STATE_UNSPECIFIED
	}
}

// verdictToProto maps the domain HealthVerdict to the wire HealthStatus.
func verdictToProto(v domain.HealthVerdict) servingv1.HealthStatus {
	switch v {
	case domain.HealthServing:
		return servingv1.HealthStatus_HEALTH_STATUS_SERVING
	case domain.HealthNotServing:
		return servingv1.HealthStatus_HEALTH_STATUS_NOT_SERVING
	default:
		return servingv1.HealthStatus_HEALTH_STATUS_UNSPECIFIED
	}
}

// ----------------------------------------------------------------------------
// STATUS / INFO / SPEC
// ----------------------------------------------------------------------------

// statusToProto maps a domain ModelStatus (the lifecycle projection) to the wire
// ModelStatus. A zero UpdatedAt yields a nil timestamp (never an epoch-zero
// timestamp, which would mislead an operator).
func statusToProto(s domain.ModelStatus) *servingv1.ModelStatus {
	return &servingv1.ModelStatus{
		ModelName: s.Ref.Name,
		Version:   s.Ref.Version,
		State:     stateToProto(s.State),
		Message:   s.Message,
		UpdatedAt: timeToProto(s.UpdatedAt),
	}
}

// loadedModelToProtoInfo maps a domain LoadedModel to the wire ModelInfo (the
// static identity + I/O schema view returned by GetModelInfo). All fields are
// server-authoritative (derived from the loaded artifact).
func loadedModelToProtoInfo(m domain.LoadedModel) *servingv1.ModelInfo {
	return &servingv1.ModelInfo{
		Name:           m.Ref.Name,
		Version:        m.Ref.Version,
		InputSchema:    tensorSpecsToProto(m.InputSchema),
		OutputSchema:   tensorSpecsToProto(m.OutputSchema),
		LoadedAt:       timeToProto(m.LoadedAt),
		ArtifactDigest: m.ArtifactDigest,
	}
}

// tensorSpecsToProto maps a domain TensorSpec slice to the wire TensorSpec slice.
func tensorSpecsToProto(specs []domain.TensorSpec) []*servingv1.TensorSpec {
	if len(specs) == 0 {
		return nil
	}
	out := make([]*servingv1.TensorSpec, 0, len(specs))
	for _, s := range specs {
		out = append(out, &servingv1.TensorSpec{
			Name:  s.Name,
			Shape: s.Shape,
			Dtype: domainDataTypeToProto(s.DType),
		})
	}
	return out
}

// timeToProto converts a time.Time to a proto Timestamp, mapping the zero time
// to a nil timestamp (proto3 absent) rather than a 1970 epoch value — so an
// operator never sees a misleading "1970-01-01" loaded_at on a never-loaded model.
func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
