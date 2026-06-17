// models.go — the PURE domain types of the Model Registry.
//
// ============================================================================
// CLEAN ARCHITECTURE: THE DOMAIN CORE
// ============================================================================
//
// These are the business types the registry reasons about: the Model aggregate
// root and its immutable ModelVersion children, plus the two enums that drive
// the lifecycle (ModelStage) and artifact readiness (VersionStatus).
//
// HARD RULE (verified in CI): this file imports ONLY the standard library and
// github.com/google/uuid. No proto, no gRPC, no SQL, no NATS. The handler maps
// these types to/from registryv1.* proto messages at the wire boundary; the
// Postgres/Redis adapters map them to/from rows/keys; the event layer maps them
// to eventsv1.* payloads. The domain itself never sees any of those vocabularies.
//
// WHY mirror the proto enums as native Go enums rather than reuse the generated
// registryv1.ModelStage:
//
//	The proto enum is a WIRE type. If the domain depended on it, the domain would
//	import gen/go (a framework dependency) and the "domain has zero framework
//	imports" rule would break — and the domain's notion of a lifecycle would be
//	pinned to whatever the .proto happens to say. We define the lifecycle HERE,
//	in business terms, and the handler maps domain.ModelStage <-> registryv1.ModelStage
//	(and the event layer maps domain.ModelStage <-> eventsv1.ModelStage). This is
//	the same "decouple the domain/event schema from the API schema" discipline
//	the proto header preaches, applied at the Go layer.
//
// ============================================================================
package domain

import (
	"time"
)

// ============================================================================
// ModelStage — the lifecycle state machine of a *version* (not the model).
// ============================================================================
//
// WHY stage lives on the version, not the model (interview framing):
//
//	"Production" is a property of a specific set of weights, not of the model
//	name. The model "fraud-detector" always exists; what changes over time is
//	WHICH version serves production traffic. Putting the stage on the version
//	lets exactly one version per model occupy PRODUCTION while older versions
//	sit ARCHIVED and candidates wait in STAGING.
//
// THE TRANSITION GRAPH (enforced by ModelStage.CanTransitionTo below):
//
//		DEV ──► STAGING ──► PRODUCTION ──► ARCHIVED
//		 │         │            │
//		 └─────────┴────────────┴────────► ARCHIVED   (abandon at any non-terminal stage)
//
//	  Promoting to PRODUCTION additionally triggers the SINGLE-PRODUCTION
//	  invariant (handled in the service, not the enum): the prior PRODUCTION
//	  version of the same model is atomically demoted to ARCHIVED.
//
// WHY a typed int enum rather than a string:
//
//	A closed set validated at the boundary prevents "prod" vs "production"
//	typos from fragmenting the read projection's stage index, and gives us
//	exhaustive switch checks. The String() method below keeps it loggable.
type ModelStage int

const (
	// StageUnspecified is the zero value. The service NEVER persists a version in
	// this state — a freshly created version starts at StageDev explicitly. It
	// exists only so the zero value is a detectable "unset", mirroring the proto's
	// required MODEL_STAGE_UNSPECIFIED=0 and used as the "any stage" filter value
	// in ListVersions.
	StageUnspecified ModelStage = iota // 0
	// StageDev is where every CreateVersion lands. Artifact may still be uploading;
	// not eligible for serving.
	StageDev // 1
	// StageStaging is a validated candidate undergoing pre-production checks
	// (shadow traffic, canary eval). Promotable to PRODUCTION.
	StageStaging // 2
	// StageProduction is the version currently serving production traffic.
	// INVARIANT: at most one PRODUCTION version per model (enforced by PromoteVersion).
	StageProduction // 3
	// StageArchived is a retired version, kept for lineage/audit/rollback but not
	// served and hidden from default lists. TERMINAL.
	StageArchived // 4
)

// String renders the stage for logs and errors. The numbers above match the
// proto enum values byte-for-byte (UNSPECIFIED=0..ARCHIVED=4) so the boundary
// mapping is a simple, auditable correspondence.
func (s ModelStage) String() string {
	switch s {
	case StageDev:
		return "DEV"
	case StageStaging:
		return "STAGING"
	case StageProduction:
		return "PRODUCTION"
	case StageArchived:
		return "ARCHIVED"
	default:
		return "UNSPECIFIED"
	}
}

// IsValid reports whether s is one of the four real lifecycle stages (not the
// zero/unspecified value). Used to reject a target_stage the handler couldn't
// map from the wire.
func (s ModelStage) IsValid() bool {
	return s >= StageDev && s <= StageArchived
}

// CanTransitionTo encodes the legal edges of the stage state machine in ONE
// place. Centralizing the rule here (rather than scattering if-checks across the
// service) is the whole point of modeling the lifecycle as a state machine: there
// is a single authoritative transition validator, easy to read in an interview
// and impossible to contradict from another call site.
//
// THE RULES:
//   - DEV        → STAGING | ARCHIVED
//   - STAGING    → PRODUCTION | ARCHIVED   (and back to DEV is NOT allowed — a
//     candidate that fails review is archived, not demoted to DEV; this keeps the
//     graph a DAG and the lineage honest)
//   - PRODUCTION → ARCHIVED                (a prod version only ever retires; it is
//     replaced by promoting a NEW version, which demotes this one)
//   - ARCHIVED   → (nothing; terminal)
//   - X          → X is rejected as a no-op transition (the service treats an
//     already-in-target promote as idempotent BEFORE reaching here; a literal
//     self-edge is not a valid state-machine move).
//
// WHY forbid DEV → PRODUCTION directly (skipping STAGING):
//
//	Jumping straight to production bypasses the pre-prod validation gate. The
//	server rejecting it is a guardrail against accidental (or malicious) promotion
//	of an unvetted candidate — the same reason the proto marks stage server-
//	authoritative. An interviewer probes "what stops someone shipping straight to
//	prod?" → this method.
func (s ModelStage) CanTransitionTo(target ModelStage) bool {
	switch s {
	case StageDev:
		return target == StageStaging || target == StageArchived
	case StageStaging:
		return target == StageProduction || target == StageArchived
	case StageProduction:
		return target == StageArchived
	default:
		// StageArchived (terminal) and StageUnspecified have no legal outgoing edges.
		return false
	}
}

// ============================================================================
// VersionStatus — artifact readiness, ORTHOGONAL to lifecycle stage.
// ============================================================================
//
// WHY separate from ModelStage (interview-critical distinction):
//
//	Stage answers "where in the human/policy lifecycle is this version?".
//	Status answers "are the bytes physically present and verified?". A version can
//	be STAGING (stage) yet PENDING_UPLOAD (status) because the weights are still
//	uploading to MinIO. Conflating them would make "promoted but artifact still
//	arriving" unrepresentable. Serving must check BOTH: Stage==PRODUCTION AND
//	Status==READY. Promotion to a serving stage requires Status==READY (enforced
//	in the service).
type VersionStatus int

const (
	// StatusUnspecified is the zero value (unset). Never persisted.
	StatusUnspecified VersionStatus = iota // 0
	// StatusPendingUpload is where CreateVersion lands a version: the row exists,
	// the artifact upload has not yet been confirmed.
	StatusPendingUpload // 1
	// StatusReady means the artifact is present in object storage and verified
	// (server-measured digest matched). ONLY a READY version may be promoted to a
	// serving stage. The PENDING_UPLOAD→READY edge (MarkVersionReady) is what the
	// event layer turns into events.ModelVersionReady.
	StatusReady // 2
	// StatusFailed means the upload/verification failed (missing object, checksum
	// mismatch, size mismatch). Not promotable. No ModelVersionReady is emitted.
	StatusFailed // 3
)

// String renders the status for logs and errors. Values match the proto enum
// (UNSPECIFIED=0..FAILED=3).
func (s VersionStatus) String() string {
	switch s {
	case StatusPendingUpload:
		return "PENDING_UPLOAD"
	case StatusReady:
		return "READY"
	case StatusFailed:
		return "FAILED"
	default:
		return "UNSPECIFIED"
	}
}

// ============================================================================
// Model — the aggregate root: the named, versioned thing.
// ============================================================================
//
// AGGREGATE NOTE (DDD framing): Model is the aggregate ROOT; ModelVersion is a
// child entity within the aggregate. All version state transitions go THROUGH the
// model's invariants (notably single-production) — which is why PromoteVersion is
// a service operation that loads the model's version set and mutates it as a unit,
// not a free-standing "set this version's stage" mutation. The aggregate boundary
// is the consistency boundary: a transaction never spans two models.
//
// SERVER-AUTHORITATIVE FIELDS (anti mass-assignment): ID, OwnerID, Team,
// CreatedAt, UpdatedAt, ArchivedAt are populated by the service, NEVER copied from
// a client request. OwnerID/Team come from the caller's validated auth claims (see
// the Actor type below), so a caller can neither forge ownership nor register into
// another team's namespace. The handler builds RegisterModelInput from the request
// for the client-owned fields only, and the Actor separately from the claims.
type Model struct {
	// ID is the server-generated UUIDv4 primary key. Immutable.
	ID string
	// Name is the unique, human-friendly handle within a team namespace
	// ("fraud-detector"). Client-supplied at registration; immutable afterward (a
	// rename is modeled as a new model so serving/gateway handles don't shift).
	Name string
	// Description is free text. Mutable via UpdateModel.
	Description string
	// OwnerID is the registrant's user id. SERVER-AUTHORITATIVE (from auth claims).
	OwnerID string
	// Team is the owning namespace. SERVER-AUTHORITATIVE (from auth claims).
	Team string
	// Framework ("pytorch","onnx",...) and TaskType ("classification","llm",...)
	// are client-supplied descriptive metadata.
	Framework string
	TaskType  string
	// Tags are arbitrary key/value labels for search/organization. A map (not a
	// slice of pairs) because order is irrelevant and keys must dedupe — exactly
	// the semantics tags want.
	Tags map[string]string
	// ProductionVersion and LatestVersion are READ-MODEL conveniences: the version
	// LABEL of the current PRODUCTION version ("" if none) and the most recently
	// created version regardless of stage ("" if none). They are denormalized onto
	// the Redis projection so the hot query ("prod version of X") is a single read.
	// On the WRITE path (RegisterModel) they are empty — there are no versions yet.
	ProductionVersion string
	LatestVersion     string
	// CreatedAt / UpdatedAt are server timestamps. ArchivedAt is set on soft-delete
	// (DeleteModel/ArchiveModel) and is the zero time while the model is active.
	CreatedAt  time.Time
	UpdatedAt  time.Time
	ArchivedAt time.Time
}

// IsArchived reports whether the model has been soft-deleted. A zero ArchivedAt
// means active. We model "archived" as a timestamp rather than a bool so we keep
// WHEN it happened (audit) for free — the same soft-delete posture as Stripe.
func (m Model) IsArchived() bool { return !m.ArchivedAt.IsZero() }

// ============================================================================
// ModelVersion — an immutable, point-in-time snapshot of a model's artifact.
// ============================================================================
//
// WHY versions are immutable except Stage/Status:
//
//	The artifact (weights) and the metrics captured at training time must never
//	change — that is the entire value of a registry (reproducibility, rollback).
//	The ONLY mutable aspects are the lifecycle Stage (via PromoteVersion) and the
//	Status (PENDING_UPLOAD→READY/FAILED, via MarkVersionReady). Everything else is
//	write-once.
//
// SERVER-AUTHORITATIVE: ID, ModelID, Stage, Status, ArtifactPath, ArtifactDigest,
// SizeBytes, CreatedBy, CreatedAt. A client cannot hand us a version that is
// already PRODUCTION, claim an arbitrary digest (which would defeat content
// integrity), or assert a size (which would let a tenant under-report storage to
// dodge billing). Digest/size are MEASURED by the storage layer on upload and set
// via MarkVersionReady — never accepted on CreateVersion.
type ModelVersion struct {
	// ID is the server-generated UUIDv4 of the version row.
	ID string
	// ModelID binds this version to its owning Model (the aggregate root). Set
	// server-side from the create target, not a forgeable body field.
	ModelID string
	// Version is the human label ("1", "1.2.0", a git SHA). Unique within a model.
	// Auto-assigned monotonically by the service unless the client pins one.
	Version string
	// Description is the changelog/notes for this version. Client-supplied at create.
	Description string
	// Metrics is a free-form bag of training/eval numbers ({"accuracy":0.97,...}).
	// Modeled as map[string]float64 in the domain because metrics are numeric by
	// nature and that is a pure, stdlib-only type; the handler converts to/from the
	// proto's google.protobuf.Struct and the repo stores it as JSONB.
	Metrics map[string]float64
	// ArtifactPath / ArtifactDigest / SizeBytes are SERVER-MEASURED storage facts,
	// empty/zero until MarkVersionReady populates them after upload verification.
	ArtifactPath   string
	ArtifactDigest string
	SizeBytes      int64
	// Stage and Status are the two orthogonal lifecycle/readiness fields (see the
	// enum docs). Stage starts at StageDev; Status starts at StatusPendingUpload.
	Stage  ModelStage
	Status VersionStatus
	// CreatedBy is the creator's user id (from auth claims). CreatedAt is server time.
	CreatedBy string
	CreatedAt time.Time
}

// IsServable is the single predicate serving/gateway logic would consult: a
// version is servable iff it is the PRODUCTION stage AND its artifact is READY.
// Encoding the "check BOTH" rule once, here, keeps callers from re-deriving (and
// occasionally getting wrong) the orthogonal stage-vs-status relationship.
func (v ModelVersion) IsServable() bool {
	return v.Stage == StageProduction && v.Status == StatusReady
}

// ============================================================================
// Actor — the server-authoritative identity of the caller.
// ============================================================================
//
// WHY a dedicated type rather than passing (ownerID, team) strings around:
//
//	These two fields are the SECURITY-CRITICAL, server-derived inputs to every
//	command. Bundling them in one type that the handler builds ONLY from validated
//	TokenClaims (never from the request body) makes the trust boundary explicit:
//	if a value is in an Actor, it came from auth, full stop. It also means a future
//	field (e.g. a request-id for audit) is added in one place. The service stamps
//	Actor.UserID into OwnerID/CreatedBy and Actor.Team into Team — the caller's
//	request never gets a vote on those.
type Actor struct {
	// UserID is the authenticated caller's user id (becomes Model.OwnerID /
	// ModelVersion.CreatedBy / the *_by fields on emitted events).
	UserID string
	// Team is the authenticated caller's team namespace (becomes Model.Team and
	// scopes all reads). A caller can never widen this from the request body.
	Team string
}
