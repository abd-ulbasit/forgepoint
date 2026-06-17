// errors.go — sentinel errors owned by the Feature Store domain layer.
//
// ============================================================================
// WHY SENTINEL ERRORS (errors.Is-friendly) AND TWO VOCABULARIES
// ============================================================================
//
// The handler maps these business errors to gRPC status codes (the only place
// gRPC is allowed to appear). The domain never imports grpc/codes — it speaks
// its own vocabulary and lets the adapter translate. This is the same split the
// Auth service uses: STORAGE sentinels (in ports.go: ErrEventLogNotFound) describe
// what the persistence port saw ("no such view"); the BUSINESS sentinels here
// re-express those as outcomes the caller cares about. The service impl is the
// single translation point.
//
// Sentinels (not bespoke error strings) so callers can branch with
// errors.Is(err, ErrViewNotFound). Where a specific human message helps, the
// service wraps with %w: fmt.Errorf("%w: feature %q not in schema", ErrSchemaViolation, name)
// — the caller still matches the sentinel, and the human gets detail.
package domain

import "errors"

var (
	// ErrValidation is returned for malformed COMMAND input the service rejects
	// before touching any port (empty view name, empty feature schema, a
	// zero-value FeatureValueType, a non-positive dimension). The handler maps it
	// to codes.InvalidArgument. Wrapped with %w so callers get a specific message
	// while still matching errors.Is(err, ErrValidation).
	//
	// WHY a single bucket for all input-shape errors: the caller's reaction is the
	// same for all of them ("fix your request") — they need ONE code
	// (InvalidArgument), not a taxonomy. We reserve distinct sentinels only where
	// the caller would react DIFFERENTLY (not-found vs already-exists vs conflict).
	ErrValidation = errors.New("featurestore: validation failed")

	// ErrViewNotFound is returned when a read/write targets a FeatureView that does
	// not exist (or, on the read side, exists for another team — see the security
	// note in featurestore_service.go: we deliberately return NotFound, NOT
	// PermissionDenied, for a cross-team id so a caller cannot probe which ids
	// exist in other teams). Maps to codes.NotFound.
	ErrViewNotFound = errors.New("featurestore: feature view not found")

	// ErrViewDeleted is returned when a WRITE (WriteFeatures) targets a view that
	// has been soft-retired (a terminal FeatureViewDeleted event is the latest
	// definition event). The log/history stays queryable for reproducibility, but
	// new appends are refused: a retired view must not accept new feature rows.
	// Maps to codes.FailedPrecondition (the request is well-formed; the resource
	// is in the wrong STATE for it). Reads against a deleted view's HISTORY still
	// succeed — only mutation is blocked.
	ErrViewDeleted = errors.New("featurestore: feature view is deleted")

	// ErrSchemaViolation is returned by WriteFeatures when a payload does not match
	// the view's declared schema: an unknown feature name, a value whose kind does
	// not match the declared FeatureValueType, or a DOUBLE_LIST whose length does
	// not match the declared dimension. This is the contract that prevents
	// train/serve skew and silently-corrupt features — the single worst, hardest-
	// to-debug failure mode in ML. A violation fails the WHOLE batch (we never
	// append a partially-valid set of rows). Maps to codes.InvalidArgument.
	ErrSchemaViolation = errors.New("featurestore: schema violation")

	// ErrViewNameConflict is returned by DefineFeatureView when a DIFFERENT entity
	// is supplied for an existing view name within the same team. WHY: a view's
	// Entity (its join key) is its identity — silently re-keying "user_features"
	// from `user` to `merchant` would corrupt every historical read. Schema
	// evolution (adding features) is allowed; changing the entity is not. Maps to
	// codes.FailedPrecondition.
	ErrViewNameConflict = errors.New("featurestore: feature view entity conflict")

	// ErrAsOfRequired is returned by GetHistoricalFeatures when the point-in-time
	// cutoff is missing/zero. A historical read with no as_of would mean "now",
	// which is non-reproducible (the answer changes over time) — defeating the
	// entire reason the offline read exists. We refuse it rather than silently
	// defaulting to now. Maps to codes.InvalidArgument.
	ErrAsOfRequired = errors.New("featurestore: as_of timestamp is required for historical reads")

	// ErrBatchTooLarge is returned when a WriteFeatures batch, or a
	// GetOnline/GetHistorical entity_ids batch, exceeds MaxBatchSize. WHY REJECT
	// (not truncate): silently dropping feature rows would corrupt counts and
	// point-in-time history, and silently dropping requested entities would return
	// a wrong-looking-complete result. The cap must fail LOUDLY. Maps to
	// codes.InvalidArgument. (Page sizes, by contrast, are CLAMPED not rejected —
	// see MaxPageSize — because a too-large PAGE is harmless to bound silently.)
	ErrBatchTooLarge = errors.New("featurestore: batch exceeds maximum size")
)
