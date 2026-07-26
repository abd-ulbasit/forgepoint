// registry_service_impl.go — the concrete RegistryService implementation.
//
// ============================================================================
// THIS FILE IS THE CQRS PATTERN MADE CONCRETE
// ============================================================================
//
// registryService lives in the domain package and depends ONLY on:
//   - the ports it consumes (WriteStore, ReadStore, ProjectionEmitter, Clock,
//     IDGenerator) — all defined in ports.go, all in this same package, and
//   - the standard library + github.com/google/uuid (the single permitted
//     external dep, used only by the production IDGenerator below).
//
// It imports NO gRPC, NO NATS, NO database driver, NO proto. The handler injects
// the Postgres-backed WriteStore, the Redis-backed ReadStore, and the NATS-backed
// emitter at wire time (later phases); tests inject in-memory fakes. This is
// dependency inversion: the business logic dictates the port contracts; the
// infrastructure conforms.
//
// THE COMMAND SHAPE (every write method follows this skeleton):
//
//  1. VALIDATE input (cheap, before any I/O).
//  2. IDEMPOTENCY: if a key is set, look it up; on hit, RETURN the original
//     record WITHOUT re-writing or re-emitting (the Stripe contract).
//  3. STAMP server-authoritative fields (id/owner/team/timestamps/stage/status)
//     from the Actor + Clock + IDGenerator — NEVER from the input.
//  4. WRITE to the WriteStore (the source of truth), translating storage
//     sentinels (ErrWriteConflict/ErrRecordNotFound) to business errors.
//  5. EMIT a ProjectionEvent so the read model + external consumers learn of the
//     change. The emit happens AFTER the commit; an emit failure is surfaced but
//     does NOT undo the committed write (see the emit-posture note in ports.go).
//
// THE QUERY SHAPE (every read method):
//  1. VALIDATE + CLAMP page size to [1, MaxPageSize] (the DoS guard).
//  2. READ the eventually-consistent ReadStore, scoped to the Actor's team.
//  3. TRANSLATE a projection miss (ErrRecordNotFound) to the business not-found.
//     Queries NEVER touch the WriteStore — that separation is the read/write split.
//
// ============================================================================
package domain

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// registryService is the production RegistryService. Unexported: callers receive
// it only through the RegistryService interface returned by NewRegistryService,
// enforcing program-to-the-interface and hiding the concrete fields.
type registryService struct {
	write   WriteStore
	read    ReadStore
	emitter ProjectionEmitter
	clock   Clock
	ids     IDGenerator
}

// Compile-time proof *registryService satisfies the interface. If a method
// signature drifts, the package fails to BUILD here rather than at a call site —
// the cheapest possible place to catch interface skew.
var _ RegistryService = (*registryService)(nil)

// NewRegistryService wires the ports and returns the RegistryService interface.
//
// WHY return the interface, not *registryService: callers depend on the
// abstraction (the constructor half of dependency inversion). The handler in
// main.go passes the real adapters; tests pass fakes. The two stores are SEPARATE
// parameters precisely because CQRS keeps them separate — the wiring site is where
// "Postgres for writes, Redis for reads" becomes literal.
func NewRegistryService(write WriteStore, read ReadStore, emitter ProjectionEmitter, clock Clock, ids IDGenerator) RegistryService {
	return &registryService{write: write, read: read, emitter: emitter, clock: clock, ids: ids}
}

// ============================================================================
// uuidGenerator — the production IDGenerator (hand-rolled UUIDv4 from crypto/rand)
// ============================================================================
//
// WHY HAND-ROLL THE UUID INSTEAD OF IMPORTING github.com/google/uuid HERE:
//
//	The domain's purity rule is "stdlib + github.com/google/uuid only", but the
//	uuid package transitively imports database/sql/driver (it implements
//	driver.Valuer so a UUID can be used directly as a SQL parameter). That would
//	leak a database-flavored import into the domain's transitive set — exactly what
//	the canonical Auth service avoids (its domain mints ids with crypto/rand too).
//	Generating a v4 UUID from crypto/rand is ~6 lines of stdlib and keeps the
//	domain's transitive dependency graph free of any storage/driver vocabulary. The
//	uuid dependency remains available to the OUTER layers (the Postgres adapter),
//	where a driver.Valuer is genuinely useful.
//
// WHY v4 (random) rather than a sequence: ids must be unguessable and collision-
// free across many service instances with no central coordinator — 122 random bits
// give that. Centralizing minting in this port means a client can NEVER supply an
// id (the server always generates it) — the structural defense behind the proto's
// "id is server-authoritative" guarantee.
type uuidGenerator struct{}

// NewID returns a fresh RFC-4122 version-4 UUID string. It reads 16 cryptographically
// random bytes, sets the version (4) and variant (RFC 4122) bits, and hex-formats
// them as 8-4-4-4-12. crypto/rand.Read fills the buffer or panics on the
// (effectively impossible) failure to read the OS CSPRNG — a panic is correct here
// because a process that cannot generate random ids must not continue serving.
func (uuidGenerator) NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means the OS entropy source is broken; there is no
		// safe way to continue (we'd risk colliding or guessable ids).
		panic("registry: crypto/rand failed generating an id: " + err.Error())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4 (random)
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10xx (RFC 4122)
	var dst [36]byte
	hex.Encode(dst[0:8], b[0:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:36], b[10:16])
	return string(dst[:])
}

// NewUUIDGenerator returns the production IDGenerator. Tests inject a deterministic
// sequence instead so they can assert on exact ids.
func NewUUIDGenerator() IDGenerator { return uuidGenerator{} }

// ============================================================================
// COMMANDS
// ============================================================================

// RegisterModel — create the model identity (no versions yet).
func (s *registryService) RegisterModel(ctx context.Context, actor Actor, in RegisterModelInput) (Model, error) {
	// 1. VALIDATE.
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Model{}, fmt.Errorf("%w: model name is required", ErrValidation)
	}
	if actor.Team == "" || actor.UserID == "" {
		// Defense in depth: the handler derives these from auth claims, but the
		// service refuses to stamp an empty owner/team (which would create an
		// unscoped, unowned model — a tenancy hole).
		return Model{}, fmt.Errorf("%w: actor identity is required", ErrValidation)
	}

	// 2. IDEMPOTENCY: a retried create with the same key returns the original,
	//    short-circuiting BEFORE any write or emit. Scoped by team so keys can't
	//    collide across tenants.
	if in.IdempotencyKey != "" {
		if existing, err := s.write.LookupModelByIdempotencyKey(ctx, actor.Team, in.IdempotencyKey); err == nil {
			return existing, nil
		} else if !errors.Is(err, ErrRecordNotFound) {
			return Model{}, fmt.Errorf("idempotency lookup: %w", err)
		}
	}

	// 3. STAMP server-authoritative fields. owner/team from the ACTOR (auth claims),
	//    id from the GENERATOR, timestamps from the CLOCK. The input contributes
	//    ONLY the descriptive fields — the mass-assignment guard in action.
	now := s.clock.Now()
	m := Model{
		ID:          s.ids.NewID(),
		Name:        name,
		Description: in.Description,
		OwnerID:     actor.UserID,
		Team:        actor.Team,
		Framework:   in.Framework,
		TaskType:    in.TaskType,
		Tags:        cloneTags(in.Tags),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// 4. WRITE to the source of truth, translating the storage conflict sentinel.
	created, err := s.write.CreateModel(ctx, m, in.IdempotencyKey)
	if err != nil {
		if errors.Is(err, ErrWriteConflict) {
			return Model{}, ErrModelNameTaken
		}
		return Model{}, fmt.Errorf("create model: %w", err)
	}

	// 5. EMIT the projection fact (→ fp.models.registered).
	s.emit(ctx, ProjectionEvent{
		Kind:       EventModelRegistered,
		Model:      created,
		Actor:      actor,
		OccurredAt: now,
	})
	return created, nil
}

// UpdateModel — mutate the small mutable surface (description/tags) with explicit
// apply flags so an omitted field never clears stored data. Emits NO lifecycle
// event (not a published platform fact) but refreshes the projection via an
// internal update; here we simply persist (the projection refresh is the adapter's
// job and is not a domain event).
func (s *registryService) UpdateModel(ctx context.Context, actor Actor, in UpdateModelInput) (Model, error) {
	if strings.TrimSpace(in.ModelID) == "" {
		return Model{}, fmt.Errorf("%w: model id is required", ErrValidation)
	}
	m, err := s.loadOwnedModel(ctx, actor, in.ModelID)
	if err != nil {
		return Model{}, err
	}
	if m.IsArchived() {
		return Model{}, ErrModelArchived
	}

	// Apply ONLY the fields whose flag is set (update-mask-style partial update).
	if in.UpdateDescription {
		m.Description = in.Description
	}
	if in.ReplaceTags {
		m.Tags = cloneTags(in.Tags) // wholesale replace; empty map clears all tags
	}
	m.UpdatedAt = s.clock.Now()

	updated, err := s.write.UpdateModel(ctx, m)
	if err != nil {
		return Model{}, fmt.Errorf("update model: %w", err)
	}
	// No EventModel* here on purpose — description/tag edits are not in the event
	// contract. The read projection is refreshed out of band by the adapter.
	return updated, nil
}

// CreateVersion — cut a new immutable version of an existing model.
func (s *registryService) CreateVersion(ctx context.Context, actor Actor, in CreateVersionInput) (ModelVersion, error) {
	if strings.TrimSpace(in.ModelID) == "" {
		return ModelVersion{}, fmt.Errorf("%w: model id is required", ErrValidation)
	}

	// Idempotency first (scoped per model).
	if in.IdempotencyKey != "" {
		if existing, err := s.write.LookupVersionByIdempotencyKey(ctx, in.ModelID, in.IdempotencyKey); err == nil {
			return existing, nil
		} else if !errors.Is(err, ErrRecordNotFound) {
			return ModelVersion{}, fmt.Errorf("idempotency lookup: %w", err)
		}
	}

	// Validate the TARGET model on the WRITE side (the truth): it must exist, be in
	// the caller's team, and not be archived. Reading the truth (not the lagging
	// projection) here is essential — we must not cut a version against stale state.
	m, err := s.loadOwnedModel(ctx, actor, in.ModelID)
	if err != nil {
		return ModelVersion{}, err
	}
	if m.IsArchived() {
		return ModelVersion{}, ErrModelArchived
	}

	// Decide whether the LABEL is client-PINNED or SERVER-AUTO-assigned. This split is
	// the crux of the collision fix: a pinned label that collides is the caller's
	// problem (→ ErrVersionExists, no retry); an AUTO label that collides is the
	// SERVER's problem (a stale count or a concurrent unpinned create) and must be
	// transparently retried so an unpinned caller — who chose NOTHING — never gets a
	// confusing ALREADY_EXISTS for a value they didn't pick.
	pinned := strings.TrimSpace(in.Version)
	autoAssign := pinned == ""

	// Seed the candidate label. WHY count+1 rather than max-label+1: labels can be
	// arbitrary strings (a git SHA, a semver), so there is no numeric "max"; the
	// monotonic auto-label is the version COUNT plus one, stable and human-obvious
	// ("1","2","3"...). A client that pins meaningful labels opts out of this.
	candidate := pinned
	if autoAssign {
		n, err := s.write.CountVersions(ctx, in.ModelID)
		if err != nil {
			return ModelVersion{}, fmt.Errorf("count versions: %w", err)
		}
		candidate = strconv.Itoa(n + 1)
	}

	now := s.clock.Now()

	// COLLISION-SAFE INSERT LOOP. We try the candidate label; on a unique-index
	// conflict we either fail (pinned) or bump-and-retry (auto). The loop is BOUNDED
	// (maxAutoLabelAttempts) so a pathological storm can't spin forever — it instead
	// surfaces a real error the caller can act on, rather than hanging.
	//
	// WHY this lives in the service as a retry rather than a single store insert:
	//   count+1 then a separate insert is a TOCTOU window — two unpinned creates can
	//   both read count=N, both compute "N+1", and one loses the race on the
	//   (model_id, version) unique index. Retrying the LOSER with N+2 (then N+3...)
	//   converges every concurrent unpinned create onto a distinct free label, so
	//   none of them ever sees ErrVersionExists. The same loop also rescues the
	//   PINNED-then-AUTO collision (a client pinned "2", later an unpinned create at
	//   count==1 computes "2" → conflict → retry yields "3").
	//
	//   The production-grade alternative is a per-model monotonic counter column
	//   bumped INSIDE the insert tx (or INSERT ... ON CONFLICT ... RETURNING), which
	//   removes the round-trips entirely; documented as the preferred adapter-level
	//   optimization. This service-level loop gives the identical OBSERVABLE contract
	//   ("an unpinned caller never sees ErrVersionExists") against any WriteStore,
	//   including stores without such a column — so the guarantee is the domain's,
	//   not delegated to the adapter.
	const maxAutoLabelAttempts = 100
	for attempt := 0; ; attempt++ {
		// STAMP server-authoritative fields. A new version ALWAYS starts at Stage=DEV /
		// Status=PENDING_UPLOAD — the client cannot create straight into a serving
		// stage or claim the artifact is ready (the proto's security posture). A fresh
		// id is minted per attempt: the previous attempt's row was never created, so
		// reusing its id is fine, but minting anew keeps each attempt independent.
		v := ModelVersion{
			ID:          s.ids.NewID(),
			ModelID:     in.ModelID,
			Version:     candidate,
			Description: in.Description,
			Metrics:     cloneMetrics(in.Metrics),
			Stage:       StageDev,
			Status:      StatusPendingUpload,
			CreatedBy:   actor.UserID,
			CreatedAt:   now,
			// ArtifactPath/Digest/SizeBytes intentionally left zero — server-measured
			// later by MarkVersionReady, never set on create.
		}

		created, err := s.write.CreateVersion(ctx, v, in.IdempotencyKey)
		if err == nil {
			// EMIT (→ fp.models.version.created). NOTE: this fires at ROW CREATION; it
			// is NOT a signal the artifact is usable — consumers needing a usable
			// artifact wait for EventVersionReady instead.
			s.emit(ctx, ProjectionEvent{
				Kind:       EventVersionCreated,
				Model:      m,
				Version:    created,
				Actor:      actor,
				OccurredAt: now,
			})
			return created, nil
		}
		if !errors.Is(err, ErrWriteConflict) {
			return ModelVersion{}, fmt.Errorf("create version: %w", err)
		}

		// CONFLICT. If the caller PINNED this label, it's a genuine AlreadyExists —
		// they asked for a label that is taken; surface it so they can choose another.
		if !autoAssign {
			return ModelVersion{}, ErrVersionExists
		}
		// AUTO label lost a race (or the seed count was stale). Bump and retry, up to
		// the bound. We bump the NUMERIC label by one each time so we keep walking
		// forward to the next free slot.
		if attempt >= maxAutoLabelAttempts {
			// Extremely unlikely (would require 100 consecutive label collisions);
			// fail with a wrapped conflict rather than spin. Not ErrVersionExists —
			// the caller pinned nothing, so AlreadyExists would be a lie.
			return ModelVersion{}, fmt.Errorf("auto-assign version label: %w after %d attempts", ErrWriteConflict, attempt)
		}
		next, convErr := strconv.Atoi(candidate)
		if convErr != nil {
			// Should be impossible — an auto label is always strconv.Itoa output, so
			// it always parses. Guard anyway so a future change can't loop forever.
			return ModelVersion{}, fmt.Errorf("auto-assign version label: non-numeric candidate %q: %w", candidate, ErrWriteConflict)
		}
		candidate = strconv.Itoa(next + 1)
	}
}

// MarkVersionReady — drive PENDING_UPLOAD → READY (Success) or → FAILED (!Success).
//
// SECURITY: artifact path/digest/size are taken from the input but the input is
// populated by the SERVER-side upload-confirmation caller that re-measured the
// object — never from a raw client body (see MarkVersionReadyInput's doc). The
// service records them only on the READY edge.
func (s *registryService) MarkVersionReady(ctx context.Context, actor Actor, in MarkVersionReadyInput) (ModelVersion, error) {
	if strings.TrimSpace(in.VersionID) == "" {
		return ModelVersion{}, fmt.Errorf("%w: version id is required", ErrValidation)
	}

	// IDEMPOTENCY by KEY (the proto's exactly-once contract). Checked FIRST, before
	// any state read, so a retry with the SAME key short-circuits to the version this
	// key already produced — even if a same-key retry now asserts a DIFFERENT outcome
	// (e.g. the first attempt set FAILED and a buggy retry claims Success). Returning
	// the recorded entity's CURRENT state — never re-running the transition — is what
	// stops a duplicate webhook from double-firing ModelVersionReady or double-metering
	// storage. Scoped by the actor's TEAM so keys can't collide across tenants.
	if done, v, err := lookupCommandIdempotency(ctx, s.write, actor.Team, CommandMarkVersionReady, in.IdempotencyKey, s.write.GetVersion); err != nil {
		return ModelVersion{}, err
	} else if done {
		return v, nil
	}

	v, err := s.write.GetVersion(ctx, in.VersionID)
	if err != nil {
		if errors.Is(err, ErrRecordNotFound) {
			return ModelVersion{}, ErrVersionNotFound
		}
		return ModelVersion{}, fmt.Errorf("get version: %w", err)
	}

	// TENANCY GATE. GetVersion(by id) above does NOT filter by
	// team — it is the generic write-store lookup. A version id is a global handle, so
	// without this gate a JWT from ANY team could confirm/flip a version owned by
	// another team (a cross-tenant mutation / privilege-escalation hole). We therefore
	// re-apply the SAME ownership check every model-targeting command uses
	// (loadOwnedModel): load the parent model from the truth and require it belongs to
	// the caller's team. A mismatch is mapped to ErrVersionNotFound — INDISTINGUISHABLE
	// from "the version doesn't exist" — so a caller cannot use this endpoint as an
	// oracle to probe whether another team's version id exists. (loadOwnedModel returns
	// ErrModelNotFound for both the missing-model and wrong-team cases; we translate it
	// to the version-shaped not-found because the caller is operating on a version.)
	if _, err := s.loadOwnedModel(ctx, actor, v.ModelID); err != nil {
		if errors.Is(err, ErrModelNotFound) {
			return ModelVersion{}, ErrVersionNotFound
		}
		return ModelVersion{}, err
	}

	// IDEMPOTENCY by terminal status: a version already in its target terminal
	// status is a no-op returning current state (a duplicate webhook delivery must
	// not double-fire ModelVersionReady or double-meter storage). This is status-
	// based idempotency, complementing the optional key.
	if v.Status == StatusReady && in.Success {
		return v, nil
	}
	if v.Status == StatusFailed && !in.Success {
		return v, nil
	}
	// Any OTHER transition out of a terminal status is illegal (e.g. flipping a
	// READY version to FAILED, or re-confirming a FAILED one as READY without a new
	// upload). Only PENDING_UPLOAD is a legal source for a fresh confirmation.
	if v.Status != StatusPendingUpload {
		return ModelVersion{}, fmt.Errorf("%w: cannot confirm a version in status %s", ErrInvalidStatusTransition, v.Status)
	}

	now := s.clock.Now()
	if in.Success {
		v.Status = StatusReady
		v.ArtifactPath = in.ArtifactPath
		v.ArtifactDigest = in.ArtifactDigest
		v.SizeBytes = in.SizeBytes
	} else {
		v.Status = StatusFailed
	}

	updated, err := s.write.UpdateVersionStatus(ctx, v)
	if err != nil {
		return ModelVersion{}, fmt.Errorf("update version status: %w", err)
	}

	// Record the idempotency key AFTER the mutation persisted (in production this is
	// one row in the same tx). A later same-key retry now hits the ledger and returns
	// `updated` verbatim — no second status flip, no second event. We record for BOTH
	// outcomes (READY and FAILED) because both are terminal facts the caller may retry.
	if err := s.recordCommandIdempotency(ctx, actor.Team, CommandMarkVersionReady, in.IdempotencyKey, updated.ID); err != nil {
		return ModelVersion{}, err
	}

	// EMIT only on the READY edge (→ fp.models.version.ready). A →FAILED outcome
	// announces nothing servable, so NO event — consumers only react to a real,
	// usable artifact.
	if in.Success {
		// We carry the owning model so the event payload can include model_name
		// (consumers key routes/loads by name). Load the truth for it.
		m, mErr := s.write.GetModel(ctx, updated.ModelID)
		if mErr != nil {
			// The version exists but its model vanished — should be impossible (FK),
			// but fail loudly rather than emit a half-populated event.
			return updated, fmt.Errorf("load owning model for ready event: %w", mErr)
		}
		s.emit(ctx, ProjectionEvent{
			Kind:       EventVersionReady,
			Model:      m,
			Version:    updated,
			Actor:      actor,
			OccurredAt: now,
		})
	}
	return updated, nil
}

// PromoteVersion — the stage state machine + the SINGLE-PRODUCTION invariant.
//
// This is the centerpiece. Read the steps as the answer to "walk me
// through promoting a model version to production."
func (s *registryService) PromoteVersion(ctx context.Context, actor Actor, in PromoteVersionInput) (PromoteResult, error) {
	if strings.TrimSpace(in.VersionID) == "" {
		return PromoteResult{}, fmt.Errorf("%w: version id is required", ErrValidation)
	}
	if !in.TargetStage.IsValid() {
		return PromoteResult{}, fmt.Errorf("%w: target stage is invalid", ErrValidation)
	}

	// IDEMPOTENCY by KEY (checked FIRST, before any state read). A retried promote
	// bearing a key we've already honored returns the recorded version's CURRENT
	// state without re-running the swap — so the side effects the proto calls out
	// (serving reload, billing re-meter) fire exactly once. Crucially this also
	// dedupes the dangerous case state-idempotency MISSES: a same-key retry whose
	// TargetStage now differs (a retried promote-to-STAGING racing a
	// promote-to-PRODUCTION) is collapsed to the first outcome, not executed twice.
	if done, v, err := lookupCommandIdempotency(ctx, s.write, actor.Team, CommandPromoteVersion, in.IdempotencyKey, s.write.GetVersion); err != nil {
		return PromoteResult{}, err
	} else if done {
		return PromoteResult{Promoted: v}, nil
	}

	v, err := s.write.GetVersion(ctx, in.VersionID)
	if err != nil {
		if errors.Is(err, ErrRecordNotFound) {
			return PromoteResult{}, ErrVersionNotFound
		}
		return PromoteResult{}, fmt.Errorf("get version: %w", err)
	}

	// TENANCY GATE (the headline security fix). GetVersion(by id) is unscoped, and the
	// swap below mutates the model's PRODUCTION pointer (and atomically demotes the
	// prior prod version). Without this check, a JWT from ANY team could promote or
	// demote another team's production version — a cross-tenant mutation that directly
	// changes which weights serve another tenant's traffic. We apply loadOwnedModel —
	// the SAME "exists AND is mine" gate UpdateModel/CreateVersion/ArchiveModel use —
	// immediately after the unscoped fetch and BEFORE any state-machine work, the swap,
	// or the idempotency-record write. A team mismatch returns ErrVersionNotFound,
	// indistinguishable from "no such version", so the endpoint is not an enumeration
	// oracle for other teams' version ids.
	if _, err := s.loadOwnedModel(ctx, actor, v.ModelID); err != nil {
		if errors.Is(err, ErrModelNotFound) {
			return PromoteResult{}, ErrVersionNotFound
		}
		return PromoteResult{}, err
	}

	// IDEMPOTENCY: already in the target stage → no-op returning current state, no
	// event (a client retry must not re-fire serving reload / billing re-meter).
	if v.Stage == in.TargetStage {
		return PromoteResult{Promoted: v}, nil
	}

	// STATE-MACHINE GUARD: reject any edge the lifecycle forbids (e.g. DEV→PROD
	// skipping STAGING, or any move out of terminal ARCHIVED). One authoritative
	// validator (ModelStage.CanTransitionTo) — no scattered rules.
	if !v.Stage.CanTransitionTo(in.TargetStage) {
		return PromoteResult{}, fmt.Errorf("%w: %s → %s", ErrIllegalTransition, v.Stage, in.TargetStage)
	}

	// READY precondition for SERVING stages: you cannot route to weights that
	// aren't physically present + verified. Archiving a non-ready version is fine
	// (abandon a candidate), so the check is only for STAGING/PRODUCTION.
	if (in.TargetStage == StageStaging || in.TargetStage == StageProduction) && v.Status != StatusReady {
		return PromoteResult{}, fmt.Errorf("%w: version status is %s", ErrVersionNotReady, v.Status)
	}

	now := s.clock.Now()
	fromStage := v.Stage
	v.Stage = in.TargetStage

	// THE SINGLE-PRODUCTION INVARIANT. Only when targeting PRODUCTION do we look for
	// a prior prod version to atomically demote. Everything below is committed as
	// ONE unit by the WriteStore's PromoteVersionTx so there is never a moment with
	// two PRODUCTION versions of the model.
	var demoted ModelVersion
	if in.TargetStage == StageProduction {
		prior, err := s.write.FindProductionVersion(ctx, v.ModelID)
		switch {
		case err == nil && prior.ID != v.ID:
			// Demote the incumbent to ARCHIVED. (If prior.ID == v.ID we'd be
			// promoting the current prod to prod — already handled by the
			// idempotency short-circuit above, so this case is the genuine swap.)
			prior.Stage = StageArchived
			demoted = prior
		case errors.Is(err, ErrRecordNotFound):
			// No incumbent — first promotion to production. Nothing to demote.
		case err != nil:
			return PromoteResult{}, fmt.Errorf("find production version: %w", err)
		}
	}

	newPromoted, newDemoted, err := s.write.PromoteVersionTx(ctx, v, demoted)
	if err != nil {
		return PromoteResult{}, fmt.Errorf("promote tx: %w", err)
	}

	// Record the key against the promoted version (in production: same tx as the
	// swap). A later same-key retry returns newPromoted's current state and re-fires
	// nothing.
	if err := s.recordCommandIdempotency(ctx, actor.Team, CommandPromoteVersion, in.IdempotencyKey, newPromoted.ID); err != nil {
		return PromoteResult{}, err
	}

	// EMIT (→ fp.models.promoted) carrying BOTH ends + the demoted version, so a
	// consumer sees the whole atomic swap (serving tears down the old, loads the
	// new; billing re-meters; monitor re-baselines). Load the owning model for the
	// event's model_name.
	m, mErr := s.write.GetModel(ctx, newPromoted.ModelID)
	if mErr != nil {
		return PromoteResult{}, fmt.Errorf("load owning model for promote event: %w", mErr)
	}
	s.emit(ctx, ProjectionEvent{
		Kind:           EventModelPromoted,
		Model:          m,
		Version:        newPromoted,
		DemotedVersion: newDemoted,
		FromStage:      fromStage,
		ToStage:        in.TargetStage,
		Actor:          actor,
		OccurredAt:     now,
	})
	return PromoteResult{Promoted: newPromoted, Demoted: newDemoted}, nil
}

// ArchiveModel — soft-delete the model + archive all its versions (one tx).
func (s *registryService) ArchiveModel(ctx context.Context, actor Actor, modelID, idempotencyKey string) (Model, error) {
	if strings.TrimSpace(modelID) == "" {
		return Model{}, fmt.Errorf("%w: model id is required", ErrValidation)
	}

	// IDEMPOTENCY by KEY (checked FIRST). A retried delete with a key we've honored
	// returns the archived model's current state and re-emits NOTHING. This is the
	// proto's explicit guard against a retry re-firing ModelArchived (double-notify).
	// We load the recorded model via loadOwnedModel so the team gate is re-applied
	// even on the idempotent path (a key is scoped to the team that minted it).
	if done, m, err := lookupCommandIdempotency(ctx, s.write, actor.Team, CommandArchiveModel, idempotencyKey,
		func(c context.Context, id string) (Model, error) { return s.loadOwnedModel(c, actor, id) }); err != nil {
		return Model{}, err
	} else if done {
		return m, nil
	}

	m, err := s.loadOwnedModel(ctx, actor, modelID)
	if err != nil {
		return Model{}, err
	}

	// IDEMPOTENCY by STATE (the no-key fallback): archiving an already-archived model
	// is still a no-op success returning it, with NO second event. This covers the
	// caller who supplies no key but retries — natural terminal-state idempotency.
	if m.IsArchived() {
		return m, nil
	}

	now := s.clock.Now()
	m.ArchivedAt = now
	m.UpdatedAt = now

	archived, err := s.write.ArchiveModelTx(ctx, m)
	if err != nil {
		return Model{}, fmt.Errorf("archive model tx: %w", err)
	}

	// Record the key against the archived model (same tx in production).
	if err := s.recordCommandIdempotency(ctx, actor.Team, CommandArchiveModel, idempotencyKey, archived.ID); err != nil {
		return Model{}, err
	}

	s.emit(ctx, ProjectionEvent{
		Kind:       EventModelArchived,
		Model:      archived,
		Actor:      actor,
		OccurredAt: now,
	})
	return archived, nil
}

// ============================================================================
// QUERIES (read projection; eventually consistent)
// ============================================================================

// GetModel — fetch a single model from the projection by id (preferred) or name.
func (s *registryService) GetModel(ctx context.Context, actor Actor, id, name string) (Model, error) {
	id, name = strings.TrimSpace(id), strings.TrimSpace(name)
	if id == "" && name == "" {
		return Model{}, fmt.Errorf("%w: provide a model id or name", ErrValidation)
	}
	var (
		m   Model
		err error
	)
	// Team scoping comes from the ACTOR, never a request field — a caller can only
	// read within their own team.
	if id != "" {
		m, err = s.read.GetModelByID(ctx, actor.Team, id)
	} else {
		m, err = s.read.GetModelByName(ctx, actor.Team, name)
	}
	if err != nil {
		if errors.Is(err, ErrRecordNotFound) {
			// A projection MISS may be transient lag for a just-registered model;
			// the business surfaces it as not-found and the caller may retry.
			return Model{}, ErrModelNotFound
		}
		return Model{}, fmt.Errorf("read model: %w", err)
	}
	return m, nil
}

// ListModels — team-scoped, newest-first page from the projection, WITH an accurate
// total count (the value the BFF dashboard renders).
//
// WHY we fetch the count alongside the page: the dashboard's "N models" tile reads
// total_count from a page_size=1 ListModels call. If we returned only the page (≤
// PageSize rows) and left the total at 0, the dashboard showed 0 while the list showed
// the real rows — the exact symptom this fixes. CountModels gives the whole filtered
// set's size, scoped + filtered identically to the list, so the tile and the list
// agree. We surface a count error as a hard failure of the query (rather than
// silently returning Total=0) so a broken count is visible, not a confusing "0
// models" that looks like an empty tenant.
func (s *registryService) ListModels(ctx context.Context, actor Actor, in ListModelsInput) (Page[Model], error) {
	opts := ListOptions{PageSize: clampPageSize(in.PageSize), PageToken: in.PageToken}
	models, next, err := s.read.ListModels(ctx, actor.Team, in.Filter, opts)
	if err != nil {
		return Page[Model]{}, fmt.Errorf("list models: %w", err)
	}
	total, err := s.read.CountModels(ctx, actor.Team, in.Filter)
	if err != nil {
		return Page[Model]{}, fmt.Errorf("count models: %w", err)
	}
	return Page[Model]{Items: models, NextToken: next, Total: total}, nil
}

// GetVersion — fetch a single version from the projection, TEAM-SCOPED.
//
// SECURITY (cross-tenant read / IDOR): the version read paths (GetVersionByID /
// GetVersionByLabel) are NOT team-scoped — a version id/label is a global handle and
// the version entity does not carry the owning team. Returning the version straight
// from the projection would let any team read another team's version metadata
// (reachable via the BFF GET /models/{id}/versions). We close the hole by re-deriving
// ownership from the version's PARENT model through the TEAM-SCOPED read path
// (GetModelByID applies the actor.Team gate in the adapter): if the caller's team
// cannot see the parent model, the version is reported as not-found — the SAME
// not-found-on-mismatch posture as the write side's loadOwnedModel, so the endpoint
// leaks nothing about other teams' versions.
func (s *registryService) GetVersion(ctx context.Context, actor Actor, id, modelID, version string) (ModelVersion, error) {
	id, modelID, version = strings.TrimSpace(id), strings.TrimSpace(modelID), strings.TrimSpace(version)
	if id == "" && (modelID == "" || version == "") {
		return ModelVersion{}, fmt.Errorf("%w: provide a version id or (model id + version label)", ErrValidation)
	}
	var (
		v   ModelVersion
		err error
	)
	if id != "" {
		v, err = s.read.GetVersionByID(ctx, id)
	} else {
		v, err = s.read.GetVersionByLabel(ctx, modelID, version)
	}
	if err != nil {
		if errors.Is(err, ErrRecordNotFound) {
			return ModelVersion{}, ErrVersionNotFound
		}
		return ModelVersion{}, fmt.Errorf("read version: %w", err)
	}
	// TENANCY GATE: confirm the caller's team owns the version's parent model. A miss
	// here (model absent OR not in the team) collapses to ErrVersionNotFound — no
	// enumeration oracle for cross-team version ids.
	if err := s.assertVersionInTeam(ctx, actor, v.ModelID); err != nil {
		return ModelVersion{}, err
	}
	return v, nil
}

// ListVersions — a model's versions newest-first, optionally stage-filtered. TEAM-SCOPED.
//
// SECURITY (cross-tenant list / IDOR): ListVersions(by model_id) on the projection is
// not team-scoped, so without a gate any team could list another team's versions just
// by knowing/guessing a model id. We FIRST confirm the caller's team can see the
// parent model via the team-scoped read path, returning the version-shaped not-found
// on mismatch BEFORE touching the version list — so we never even read, let alone
// return, another team's versions.
func (s *registryService) ListVersions(ctx context.Context, actor Actor, modelID string, stageFilter ModelStage, pageSize int, pageToken string) (Page[ModelVersion], error) {
	if strings.TrimSpace(modelID) == "" {
		return Page[ModelVersion]{}, fmt.Errorf("%w: model id is required", ErrValidation)
	}
	// TENANCY GATE before the list read (see the method doc). A cross-team or unknown
	// model id is reported as not-found, identical to a genuinely absent model.
	if err := s.assertVersionInTeam(ctx, actor, modelID); err != nil {
		return Page[ModelVersion]{}, err
	}
	opts := ListOptions{PageSize: clampPageSize(pageSize), PageToken: pageToken}
	versions, next, err := s.read.ListVersions(ctx, modelID, stageFilter, opts)
	if err != nil {
		return Page[ModelVersion]{}, fmt.Errorf("list versions: %w", err)
	}
	return Page[ModelVersion]{Items: versions, NextToken: next}, nil
}

// assertVersionInTeam verifies the actor's team owns the model that a version belongs
// to, reading through the TEAM-SCOPED projection path (GetModelByID applies the team
// gate). It is the QUERY-side analog of loadOwnedModel (which gates the WRITE side off
// the Postgres truth): both map a missing-or-wrong-team result to a not-found the
// caller cannot distinguish from genuine absence. We surface ErrVersionNotFound (not
// ErrModelNotFound) because the caller is operating on a version, and that is the
// not-found the version endpoints already return for an unknown version id — so a
// cross-tenant probe and a genuine miss are byte-for-byte identical to the client.
func (s *registryService) assertVersionInTeam(ctx context.Context, actor Actor, modelID string) error {
	if _, err := s.read.GetModelByID(ctx, actor.Team, modelID); err != nil {
		if errors.Is(err, ErrRecordNotFound) {
			return ErrVersionNotFound
		}
		return fmt.Errorf("read version owner model: %w", err)
	}
	return nil
}

// ============================================================================
// INTERNAL HELPERS
// ============================================================================

// lookupCommandIdempotency is the shared FIRST step of every key-deduped MUTATION
// command (MarkVersionReady / PromoteVersion / ArchiveModel). It returns:
//
//	done == false           → no key, or key unseen → the caller proceeds normally.
//	done == true, entity     → key already honored → caller RETURNS this entity's
//	                           CURRENT state verbatim (no re-write, no re-emit).
//
// WHY generic over T (Model | ModelVersion): the three commands operate on different
// aggregates, but the ledger flow is identical — look up the entity id this key
// produced, then reload that entity's truth via the command-specific `load` func.
// Generics let us express the flow ONCE instead of three near-identical copies, with
// the loader injected so the team-gate / not-found mapping each command needs is
// preserved. An empty key short-circuits to done==false (the store would too, but
// skipping the call keeps the no-key path allocation-free and obviously a no-op).
//
// MAKING A NON-CREATING COMMAND IDEMPOTENT BY KEY:
// you cannot hang the key on a new row (there isn't one), so you keep a separate
// (team, command, key)→entity ledger, consult it before acting, and write it with
// the mutation in one tx. State-based idempotency ("already READY") is the fallback
// for keyless retries; the ledger is what honors the wire contract's KEY guarantee.
func lookupCommandIdempotency[T any](
	ctx context.Context,
	write WriteStore,
	team string,
	command Command,
	key string,
	load func(context.Context, string) (T, error),
) (done bool, entity T, err error) {
	var zero T
	if key == "" {
		return false, zero, nil
	}
	entityID, lookupErr := write.LookupCommandIdempotency(ctx, team, command, key)
	if lookupErr != nil {
		if errors.Is(lookupErr, ErrRecordNotFound) {
			return false, zero, nil // first time we've seen this key → proceed.
		}
		return false, zero, fmt.Errorf("idempotency lookup: %w", lookupErr)
	}
	// Key hit: reload the recorded entity's CURRENT truth and hand it back. We reload
	// (rather than cache the entity in the ledger) so a concurrent later mutation of
	// the same entity is reflected — the ledger remembers WHICH entity, the store
	// remembers its state.
	got, loadErr := load(ctx, entityID)
	if loadErr != nil {
		return false, zero, fmt.Errorf("reload idempotent entity: %w", loadErr)
	}
	return true, got, nil
}

// recordCommandIdempotency records (team, command, key)→entityID after a mutation
// committed, so the next same-key retry is deduped by lookupCommandIdempotency. An
// empty key is a no-op (nothing to remember). A ledger ErrWriteConflict means a
// concurrent same-key writer already recorded a DIFFERENT entity — a genuine
// key-reuse race; we surface it as a write conflict rather than silently overwriting
// the first winner's record (which would let the second side effect masquerade as
// idempotent). In production the record + the mutation share one tx, so this races
// only across processes, and ON CONFLICT DO NOTHING makes the first writer win.
func (s *registryService) recordCommandIdempotency(ctx context.Context, team string, command Command, key, entityID string) error {
	if key == "" {
		return nil
	}
	if err := s.write.RecordCommandIdempotency(ctx, team, command, key, entityID); err != nil {
		return fmt.Errorf("record idempotency: %w", err)
	}
	return nil
}

// loadOwnedModel loads a model from the WRITE store (the truth) and enforces that
// it belongs to the actor's team. WHY this exists as a helper: every command that
// targets an existing model needs the identical "exists AND is mine" check, and
// centralizing it guarantees no command forgets the team gate (a cross-tenant
// mutation hole). It maps the storage miss to ErrModelNotFound and — crucially —
// returns the SAME ErrModelNotFound when the model exists but is another team's, so
// a caller cannot probe for the existence of other teams' models.
func (s *registryService) loadOwnedModel(ctx context.Context, actor Actor, modelID string) (Model, error) {
	m, err := s.write.GetModel(ctx, modelID)
	if err != nil {
		if errors.Is(err, ErrRecordNotFound) {
			return Model{}, ErrModelNotFound
		}
		return Model{}, fmt.Errorf("get model: %w", err)
	}
	if m.Team != actor.Team {
		// Indistinguishable from "doesn't exist" — anti-enumeration across tenants.
		return Model{}, ErrModelNotFound
	}
	return m, nil
}

// emit records a projection event, logging-but-not-failing on error per the emit
// posture documented in ports.go: the WriteStore commit already succeeded and is
// the truth; an emit failure must not undo it. We DELIBERATELY swallow the error
// here (the projection catches up on the next event or a rebuild). In the events
// phase the NATS adapter behind this port will add ret/ outbox semantics; the
// service contract — "emit after commit, never roll back the write" — stays put.
//
// (The error is intentionally ignored. A production build would log it via the
// injected logger; the domain has no logger dependency by design, so the adapter
// logs. We keep the signature returning error on the port so an outbox-backed
// adapter can surface it where it matters.)
func (s *registryService) emit(ctx context.Context, ev ProjectionEvent) {
	_ = s.emitter.Emit(ctx, ev)
}

// clampPageSize enforces the platform-wide DoS guard: a zero/negative request
// becomes the default; anything over the cap is clamped to the cap. Centralized
// here so every query gets the identical bound — a store never sees an unbounded
// page size. This is the explicit enforcement the proto's pagination clause
// requires (proto3 can't express the numeric bound).
func clampPageSize(requested int) int {
	if requested <= 0 {
		return DefaultPageSize
	}
	if requested > MaxPageSize {
		return MaxPageSize
	}
	return requested
}

// cloneTags returns a defensive copy of a tag map so the stored model never
// shares the caller's map (a later mutation of the input map must not retroactively
// change a stored aggregate — a subtle aliasing bug). Returns nil for an empty
// input to keep zero-value models clean.
func cloneTags(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// cloneMetrics is the float64 analog of cloneTags for version metrics.
func cloneMetrics(in map[string]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
