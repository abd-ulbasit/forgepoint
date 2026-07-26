package httpx

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// JSON RESPONSE + gRPC-STATUS -> HTTP-STATUS MAPPING
// ============================================================================
//
// protojson is used (not encoding/json) to serialize proto messages because it
// honors the proto3 JSON mapping the contract specifies: enums become their
// string names, well-known types (Timestamp -> RFC3339 string, Struct -> raw
// JSON object) serialize correctly, and field names use proto's lowerCamelCase.
// Plain encoding/json on a generated struct would leak protobuf internals
// (state/sizeCache fields, wrong enum ints). EmitUnpopulated keeps zero-valued
// fields present so the SPA sees a stable shape instead of keys vanishing.
// ============================================================================

// marshaler is configured once; protojson.MarshalOptions is safe to reuse.
var marshaler = protojson.MarshalOptions{
	EmitUnpopulated: true,  // stable JSON shape for the SPA (no disappearing keys)
	UseProtoNames:   false, // lowerCamelCase JSON names (JS-idiomatic)
}

// WriteProto serializes a single proto message to the response as JSON.
func WriteProto(w http.ResponseWriter, status int, msg proto.Message) {
	b, err := marshaler.Marshal(msg)
	if err != nil {
		// A marshal failure is an internal bug, not a client error.
		WriteError(w, http.StatusInternalServerError, "failed to encode response")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// WriteJSON serializes an arbitrary Go value (used for composed/aggregated
// payloads like the dashboard, which are NOT a single proto message).
func WriteJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		WriteError(w, http.StatusInternalServerError, "failed to encode response")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// errorBody is the SANITIZED error envelope the SPA receives. It carries a
// stable HTTP-ish code and a human message ONLY — never a stack trace, a gRPC
// internal detail, or a wrapped Go error chain that could leak infra topology.
type errorBody struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// WriteError emits a sanitized JSON error with the given HTTP status.
func WriteError(w http.ResponseWriter, status int, message string) {
	var body errorBody
	body.Error.Code = status
	body.Error.Message = message
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// WriteGRPCError maps a downstream gRPC error onto the correct HTTP status and a
// SANITIZED body, then logs the real error server-side (where it is safe).
//
// THE MAPPING (the task's required table). gRPC's status codes are richer than
// HTTP's; we collapse them to the closest HTTP semantics the browser/SPA expects:
//
//	Unauthenticated   -> 401  (no/!invalid identity; SPA should re-login)
//	PermissionDenied  -> 403  (authenticated but not allowed)
//	NotFound          -> 404
//	InvalidArgument   -> 400  (also FailedPrecondition/OutOfRange below)
//	AlreadyExists     -> 409
//	... everything else -> 500 with a GENERIC message (no internal detail)
//
// WHY sanitize 500s: a downstream "connection refused to
// fp-registry.fp-system..." or a SQL error must never reach the browser — it
// leaks topology and aids attackers. The SPA gets "internal error"; the operator
// gets the real status.Message() in the structured log, correlated by trace id.
func WriteGRPCError(w http.ResponseWriter, logger *slog.Logger, err error) {
	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status at all (e.g. a context error before the call). Treat
		// as internal; do not echo the raw error to the client.
		logger.Error("non-grpc downstream error", slog.String("error", err.Error()))
		WriteError(w, http.StatusInternalServerError, "internal error")
		return
	}

	switch st.Code() {
	case codes.OK:
		// Defensive: an OK status reaching the error path is a caller bug.
		WriteError(w, http.StatusInternalServerError, "internal error")
	case codes.Unauthenticated:
		WriteError(w, http.StatusUnauthorized, "unauthenticated")
	case codes.PermissionDenied:
		WriteError(w, http.StatusForbidden, "permission denied")
	case codes.NotFound:
		WriteError(w, http.StatusNotFound, "not found")
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		// These are client-fault codes; the message is the service's validation
		// text, which is safe to surface (it's about the request, not infra).
		WriteError(w, http.StatusBadRequest, st.Message())
	case codes.AlreadyExists:
		WriteError(w, http.StatusConflict, "already exists")
	case codes.Unavailable, codes.DeadlineExceeded:
		// Downstream down/slow — a retriable 503 is more honest than 500.
		logger.Warn("downstream unavailable", slog.String("code", st.Code().String()))
		WriteError(w, http.StatusServiceUnavailable, "service unavailable")
	default:
		// Internal/Unknown/DataLoss/etc. Log the real detail; tell the SPA nothing.
		logger.Error("downstream internal error",
			slog.String("code", st.Code().String()),
			slog.String("detail", st.Message()),
		)
		WriteError(w, http.StatusInternalServerError, "internal error")
	}
}

// HTTPStatusForGRPC exposes the code mapping without writing — used by the
// dashboard aggregator to decide a tile's degraded status, and by tests.
func HTTPStatusForGRPC(err error) int {
	if err == nil {
		return http.StatusOK
	}
	st, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError
	}
	switch st.Code() {
	case codes.OK:
		return http.StatusOK
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.Unavailable, codes.DeadlineExceeded:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
