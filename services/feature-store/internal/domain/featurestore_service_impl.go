// featurestore_service_impl.go — the concrete, event-sourced FeatureStoreService.
//
// ============================================================================
// THE EVENT-SOURCING WRITE/READ ORCHESTRATION (the centerpiece)
// ============================================================================
//
// This file is where the pattern becomes a service. It NEVER mutates state in
// place; every command produces immutable events that it APPENDS to the EventLog
// (the source of truth), and every query PROJECTS (folds) events into a view. The
// pure fold lives in models.go (ProjectLatest / ProjectAsOf / foldView); this
// file wires those folds to the persistence ports and enforces the business
// rules + security contract around them.
//
//	WRITE PATH (command → event → append → update online projection):
//	  DefineFeatureView  ─► validate ─► FeatureEventViewDefined  ─► Append ─► (publish*)
//	  WriteFeatures      ─► validate ─► FeatureEventValuesWritten ─► Append ─► Online.Put
//	  DeleteFeatureView  ─► FeatureEventViewDeleted ─► Append ─► Online.Purge
//
//	READ PATH (query → project, never replay on the hot path):
//	  GetOnlineFeatures      ─► Online.GetLatest          (Redis "latest")
//	  GetHistoricalFeatures  ─► Load log ─► ProjectAsOf   (point-in-time)
//	  GetFeatureView/List    ─► LoadView ─► foldView      (catalog projection)
//
//	RECOVERY:
//	  RebuildViews ─► Load log ─► ProjectLatest/ProjectAsOf ─► Online.Put/Offline.Rebuild
//
// (*) Publishing FeatureViewDefined / FeaturesWritten to NATS is the events-
// adapter layer's job (a later phase): it wraps WriteFeaturesResult /
// FeatureView into the canonical forgepoint.events.v1 payloads. The domain
// returns AffectedEntityIDs precisely so that layer has what it needs WITHOUT
// the domain importing NATS. Honestly stated: this task delivers the domain +
// orchestration + fold; the Postgres EventLog, Redis/Postgres view stores, and
// the NATS publisher are subsequent phases that implement the ports declared in
// ports.go and consume the values this service already produces.
//
// ============================================================================
// CLEAN ARCHITECTURE PLACEMENT
// ============================================================================
//
// featureStoreService lives in the domain package and depends ONLY on the PORT
// interfaces (EventLog, OnlineViewStore, OfflineViewStore, Clock, IDGenerator)
// and the standard library + google/uuid. NO gRPC, NO NATS, NO sql driver, NO
// proto. The handler injects real adapters at wire time; tests inject in-memory
// fakes. Dependency inversion: the business logic dictates the port contracts;
// infrastructure conforms to them.
package domain

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// featureStoreService is the production implementation. Unexported: callers get
// it only through the FeatureStoreService interface returned by the constructor,
// enforcing program-to-the-interface and keeping fields private.
type featureStoreService struct {
	log     EventLog
	online  OnlineViewStore
	offline OfflineViewStore
	clock   Clock
	idgen   IDGenerator
}

// NewFeatureStoreService wires the ports and returns the interface (the
// constructor half of dependency inversion). The compile-time assertion below
// guarantees the impl satisfies the interface — signature drift fails the build,
// not a call site.
func NewFeatureStoreService(log EventLog, online OnlineViewStore, offline OfflineViewStore, clock Clock, idgen IDGenerator) FeatureStoreService {
	return &featureStoreService{log: log, online: online, offline: offline, clock: clock, idgen: idgen}
}

var _ FeatureStoreService = (*featureStoreService)(nil)

// adminScope is the elevated scope RebuildViews requires (a full replay is
// expensive + operationally sensitive — not reachable with a routine write token).
const adminScope = "features:admin"

// ============================================================================
// DefineFeatureView — append a definition event; SERVER assigns all authority
// ============================================================================

func (s *featureStoreService) DefineFeatureView(ctx context.Context, p Principal, in DefineFeatureViewInput) (FeatureView, error) {
	// 1. VALIDATE the schema-shaping input up front (cheap, before any I/O). We
	//    reject obviously-bad commands so we never append a malformed definition.
	if err := validateDefineInput(in); err != nil {
		return FeatureView{}, err
	}

	// 2. RESOLVE whether this name already exists FOR THIS TEAM. The name index is
	//    team-scoped, so a caller can only ever discover/evolve their own team's
	//    views (a different team's "user_credit" is invisible). This is both the
	//    create-vs-evolve decision AND the authorization boundary.
	existingID, err := s.log.ResolveViewIDByName(ctx, p.Team, in.Name)
	switch {
	case err == nil:
		// EVOLVE path: re-define an existing view. Load its current definition to
		// (a) get the stable id + creation facts and (b) ENFORCE that the Entity is
		// unchanged — the entity is the view's identity; silently re-keying it would
		// corrupt every historical read (ErrViewNameConflict).
		current, ferr := s.foldViewByID(ctx, existingID)
		if ferr != nil {
			return FeatureView{}, ferr
		}
		if current.Entity != in.Entity {
			return FeatureView{}, fmt.Errorf("%w: view %q is keyed by entity %q, cannot redefine with %q",
				ErrViewNameConflict, in.Name, current.Entity.Name, in.Entity.Name)
		}
		return s.appendViewDefinition(ctx, p, in, current.ID, current.SchemaVersion+1)

	case errors.Is(err, ErrEventLogNotFound):
		// CREATE path: brand-new view. Server-assign a fresh id and version 1.
		return s.appendViewDefinition(ctx, p, in, s.idgen.NewID(), 1)

	default:
		return FeatureView{}, fmt.Errorf("resolve view by name: %w", err)
	}
}

// appendViewDefinition builds the FeatureEventViewDefined event, appends it
// (idempotently), and returns the projected FeatureView. SERVER-AUTHORITATIVE
// fields (id, owner, team, version, timestamps) are stamped HERE from the
// Principal/clock — the request never supplied them (mass-assignment guard).
func (s *featureStoreService) appendViewDefinition(ctx context.Context, p Principal, in DefineFeatureViewInput, viewID string, version int64) (FeatureView, error) {
	now := s.clock.Now()
	def := &ViewDefinition{
		ID:            viewID,
		Name:          in.Name,
		Description:   in.Description,
		Entity:        in.Entity,
		Features:      slices.Clone(in.Features), // copy: never alias caller-owned input into the log
		SchemaVersion: version,
		OwnerUserID:   p.UserID, // from TokenClaims, NOT the request
		OwnerTeam:     p.Team,   // from TokenClaims, NOT the request
	}
	event := FeatureEvent{
		ID:            s.idgen.NewID(),
		Type:          FeatureEventViewDefined,
		FeatureViewID: viewID,
		EntityID:      "",
		EventTime:     now, // definition time = server clock (immutable thereafter)
		ViewDef:       def,
		AppendedAt:    now,
	}

	// Append is idempotent on the key: a retried define returns the original event
	// (replayed=true) instead of bumping the version again. We then project the
	// view's full definition history so the returned FeatureView reflects the true
	// stored state (the same one a fresh GetFeatureView would compute).
	if _, _, _, err := s.log.Append(ctx, in.IdempotencyKey, []FeatureEvent{event}); err != nil {
		return FeatureView{}, fmt.Errorf("append view definition: %w", err)
	}
	return s.foldViewByID(ctx, viewID)
}

// ============================================================================
// GetFeatureView (by id / by name) — fold the definition events (catalog read)
// ============================================================================

func (s *featureStoreService) GetFeatureViewByID(ctx context.Context, p Principal, id string) (FeatureView, error) {
	return s.authorizedView(ctx, p, id)
}

func (s *featureStoreService) GetFeatureViewByName(ctx context.Context, p Principal, name string) (FeatureView, error) {
	id, err := s.log.ResolveViewIDByName(ctx, p.Team, name)
	if err != nil {
		if errors.Is(err, ErrEventLogNotFound) {
			return FeatureView{}, ErrViewNotFound
		}
		return FeatureView{}, fmt.Errorf("resolve view by name: %w", err)
	}
	// Name resolution was already team-scoped, but go through authorizedView so the
	// team check is enforced in ONE place (defense in depth — a single source of the
	// scoping rule).
	return s.authorizedView(ctx, p, id)
}

func (s *featureStoreService) ListFeatureViews(ctx context.Context, p Principal, nameFilter string, opts ListOptions) ([]FeatureView, string, error) {
	opts = clampPage(opts)
	ids, next, err := s.log.ListViewIDs(ctx, p.Team, nameFilter, opts)
	if err != nil {
		return nil, "", fmt.Errorf("list view ids: %w", err)
	}
	views := make([]FeatureView, 0, len(ids))
	for _, id := range ids {
		v, ferr := s.foldViewByID(ctx, id)
		if ferr != nil {
			// A listed id that can't be folded is a data-integrity bug, not a
			// per-caller error; surface it loudly rather than silently dropping.
			return nil, "", fmt.Errorf("fold listed view %s: %w", id, ferr)
		}
		views = append(views, v)
	}
	return views, next, nil
}

// ============================================================================
// WriteFeatures — validate against schema, append value events, update online view
// ============================================================================

func (s *featureStoreService) WriteFeatures(ctx context.Context, p Principal, in WriteFeaturesInput) (WriteFeaturesResult, error) {
	// CAP CHECK FIRST: reject an over-cap batch loudly (never truncate — dropping
	// rows would corrupt counts + history). Cheap O(1) guard before any I/O.
	if len(in.Rows) > MaxBatchSize {
		return WriteFeaturesResult{}, fmt.Errorf("%w: %d rows > %d", ErrBatchTooLarge, len(in.Rows), MaxBatchSize)
	}
	if len(in.Rows) == 0 {
		return WriteFeaturesResult{}, fmt.Errorf("%w: no feature rows to write", ErrValidation)
	}

	// AUTHORIZE + LOAD the target view (team-scoped). A cross-team or missing id is
	// ErrViewNotFound; a retired view refuses new writes (ErrViewDeleted) — the log
	// stays queryable but must not accept new facts.
	view, err := s.authorizedView(ctx, p, in.FeatureViewID)
	if err != nil {
		return WriteFeaturesResult{}, err
	}
	if view.IsDeleted() {
		return WriteFeaturesResult{}, fmt.Errorf("%w: %s", ErrViewDeleted, view.ID)
	}

	// VALIDATE every row against the schema BEFORE building any event. A single
	// violation fails the WHOLE batch (all-or-nothing) so a half-corrupt write can
	// never reach the log — the contract that prevents train/serve skew.
	now := s.clock.Now()
	events := make([]FeatureEvent, 0, len(in.Rows))
	affected := make([]string, 0, len(in.Rows))
	for i, row := range in.Rows {
		if err := validateRow(view, row); err != nil {
			return WriteFeaturesResult{}, fmt.Errorf("row %d (entity %q): %w", i, row.EntityID, err)
		}
		eventTime := row.EventTime
		if eventTime.IsZero() {
			// event_time omitted ⇒ default to ingest time. Point-in-time correctness
			// uses event_time, so we must always have one; defaulting to "now" is the
			// documented behavior for non-backfill writes.
			eventTime = now
		}
		events = append(events, FeatureEvent{
			ID:            s.idgen.NewID(),
			Type:          FeatureEventValuesWritten,
			FeatureViewID: view.ID,
			EntityID:      row.EntityID,
			Values:        cloneValues(row.Values), // copy: the log owns an immutable snapshot
			EventTime:     eventTime,
			AppendedAt:    now,
		})
		affected = append(affected, row.EntityID)
	}

	// APPEND atomically (idempotent on the key). A retry returns the original
	// version range without re-appending — exactly-once EFFECT under at-least-once.
	appended, writtenThrough, replayed, err := s.log.Append(ctx, in.IdempotencyKey, events)
	if err != nil {
		return WriteFeaturesResult{}, fmt.Errorf("append feature events: %w", err)
	}

	// UPDATE THE ONLINE PROJECTION from the just-appended events. We fold the NEW
	// events (not the whole log) into latest-per-entity and upsert them — the cheap
	// incremental projection update on the write path. On a replayed (deduped)
	// retry we still re-apply (idempotent Put) so a crash between the original
	// append and the original projection update is self-healing.
	latest := ProjectLatest(appended, view.SchemaVersion)
	if err := s.online.Put(ctx, view.ID, latest); err != nil {
		// The append already committed (the log is the truth); a failed projection
		// update is recoverable (RebuildViews, or the next write). We surface it so
		// the caller knows the online view may lag, but the write is durable.
		return WriteFeaturesResult{}, fmt.Errorf("update online projection (write durable in log): %w", err)
	}

	return WriteFeaturesResult{
		WrittenCount:          len(appended),
		WrittenThroughVersion: writtenThrough,
		AffectedEntityIDs:     dedupeStrings(affected),
	}, replayedNoError(replayed)
}

// replayedNoError exists only to make the WriteFeatures return read intentionally:
// a replayed (idempotent) write is a SUCCESS, not an error. Kept as a named helper
// so the intent is documented at the call site rather than a bare nil.
func replayedNoError(_ bool) error { return nil }

// ============================================================================
// GetOnlineFeatures — read the "latest" projection (inference hot path)
// ============================================================================

func (s *featureStoreService) GetOnlineFeatures(ctx context.Context, p Principal, in GetOnlineFeaturesInput) (FeatureVectorPage, error) {
	if len(in.EntityIDs) > MaxBatchSize {
		return FeatureVectorPage{}, fmt.Errorf("%w: %d entities > %d", ErrBatchTooLarge, len(in.EntityIDs), MaxBatchSize)
	}
	// Authorize + ensure the view exists for this team. A deleted view's online
	// projection was purged on delete, so reads naturally return nothing — but we
	// still resolve it so a cross-team id is ErrViewNotFound, not an empty result.
	view, err := s.authorizedView(ctx, p, in.FeatureViewID)
	if err != nil {
		return FeatureVectorPage{}, err
	}

	found, err := s.online.GetLatest(ctx, view.ID, in.EntityIDs)
	if err != nil {
		return FeatureVectorPage{}, fmt.Errorf("online get: %w", err)
	}
	return assemblePage(in.EntityIDs, found, in.FeatureNames, ""), nil
}

// ============================================================================
// GetHistoricalFeatures — POINT-IN-TIME read (reproducibility / no label leakage)
// ============================================================================

func (s *featureStoreService) GetHistoricalFeatures(ctx context.Context, p Principal, in GetHistoricalFeaturesInput) (FeatureVectorPage, error) {
	// as_of is REQUIRED: a missing cutoff means "now", which is non-reproducible.
	if in.AsOf.IsZero() {
		return FeatureVectorPage{}, ErrAsOfRequired
	}
	if len(in.EntityIDs) > MaxBatchSize {
		return FeatureVectorPage{}, fmt.Errorf("%w: %d entities > %d", ErrBatchTooLarge, len(in.EntityIDs), MaxBatchSize)
	}
	view, err := s.authorizedView(ctx, p, in.FeatureViewID)
	if err != nil {
		return FeatureVectorPage{}, err
	}

	// REFERENCE SEMANTICS: load the log and fold it with ProjectAsOf. A production
	// offline adapter computes this in SQL for scale (and we'd route through
	// s.offline.GetAsOf), but the domain fold is the source-of-truth the SQL must
	// match — and it makes point-in-time correctness directly unit-testable. We
	// fold here so the behavior is identical regardless of the offline store's
	// maturity. Note a deleted view's HISTORY remains readable (reproducibility);
	// only writes/online serving are blocked.
	events, err := s.log.Load(ctx, view.ID)
	if err != nil {
		if errors.Is(err, ErrEventLogNotFound) {
			// View exists (authorizedView passed) but has no value events yet — every
			// requested entity is simply missing.
			return assemblePage(in.EntityIDs, nil, in.FeatureNames, ""), nil
		}
		return FeatureVectorPage{}, fmt.Errorf("load log: %w", err)
	}
	asOfView := ProjectAsOf(events, in.AsOf, view.SchemaVersion)

	// Restrict to the requested entities (the projection covers all entities).
	requested := make(map[string]FeatureVector, len(in.EntityIDs))
	for _, id := range in.EntityIDs {
		if v, ok := asOfView[id]; ok {
			requested[id] = v
		}
	}
	page := assemblePage(in.EntityIDs, requested, in.FeatureNames, "")
	_ = clampPage(in.Pagination) // page-size clamp is applied by the adapter for large pulls; kept here for parity/intent
	return page, nil
}

// ============================================================================
// DeleteFeatureView — soft retire (append terminal event), purge online view
// ============================================================================

func (s *featureStoreService) DeleteFeatureView(ctx context.Context, p Principal, featureViewID, idempotencyKey string) (FeatureView, error) {
	view, err := s.authorizedView(ctx, p, featureViewID)
	if err != nil {
		return FeatureView{}, err
	}
	// Already deleted ⇒ idempotent no-op: return the current terminal state without
	// appending a second deletion event. (The explicit idempotency key ALSO dedups
	// a lost-response retry at the log; this short-circuit covers the case where a
	// DIFFERENT key retries the same logical delete.)
	if view.IsDeleted() {
		return view, nil
	}

	now := s.clock.Now()
	event := FeatureEvent{
		ID:            s.idgen.NewID(),
		Type:          FeatureEventViewDeleted,
		FeatureViewID: view.ID,
		EventTime:     now, // retirement time
		AppendedAt:    now,
	}
	if _, _, _, err := s.log.Append(ctx, idempotencyKey, []FeatureEvent{event}); err != nil {
		return FeatureView{}, fmt.Errorf("append delete event: %w", err)
	}
	// Tear down the online projection (stop serving a retired view). The OFFLINE
	// history is intentionally kept so past training datasets remain reproducible.
	if err := s.online.Purge(ctx, view.ID); err != nil {
		return FeatureView{}, fmt.Errorf("purge online projection: %w", err)
	}
	return s.foldViewByID(ctx, view.ID)
}

// ============================================================================
// RebuildViews — REPLAY the log to regenerate the materialized projections
// ============================================================================

func (s *featureStoreService) RebuildViews(ctx context.Context, p Principal, in RebuildViewsInput, emit func(RebuildProgress) error) error {
	// ADMIN GATE: a full replay is expensive + sensitive; require the elevated
	// scope. We enforce it in the domain (defense in depth) even though the
	// interceptor may also gate it — a security check belongs at the boundary it
	// protects, not only at the network edge.
	if !slices.Contains(p.Scopes, adminScope) {
		return fmt.Errorf("%w: RebuildViews requires the %q scope", ErrValidation, adminScope)
	}

	// Resolve which views to rebuild. Empty id = the whole team's fleet (full
	// recovery); a specific id = a targeted bugfix replay.
	var viewIDs []string
	if in.FeatureViewID != "" {
		// Authorize the single target (team-scoped).
		v, err := s.authorizedView(ctx, p, in.FeatureViewID)
		if err != nil {
			return err
		}
		viewIDs = []string{v.ID}
	} else {
		ids, _, err := s.log.ListViewIDs(ctx, p.Team, "", ListOptions{PageSize: MaxPageSize})
		if err != nil {
			return fmt.Errorf("list views for rebuild: %w", err)
		}
		viewIDs = ids
	}

	var replayed int64
	for _, id := range viewIDs {
		// Load the FULL log for the view and recompute the requested projection(s)
		// FROM it — the log is never touched; only the caches are regenerated,
		// proving the log is the source of truth.
		events, err := s.log.Load(ctx, id)
		if err != nil {
			if errors.Is(err, ErrEventLogNotFound) {
				continue // a defined view with no value events: nothing to project
			}
			return fmt.Errorf("load log for rebuild %s: %w", id, err)
		}
		view, _ := foldView(events) // schema version for provenance

		if in.Target == RebuildTargetAll || in.Target == RebuildTargetOnline {
			// Start clean (Purge) then re-Put the freshly-folded latest projection.
			if err := s.online.Purge(ctx, id); err != nil {
				return fmt.Errorf("purge online for rebuild %s: %w", id, err)
			}
			// A deleted view stays purged (no online serving for a retired view).
			if !view.IsDeleted() {
				if err := s.online.Put(ctx, id, ProjectLatest(events, view.SchemaVersion)); err != nil {
					return fmt.Errorf("rebuild online %s: %w", id, err)
				}
			}
		}
		if in.Target == RebuildTargetAll || in.Target == RebuildTargetOffline {
			// Offline as-of "now" snapshot is rebuilt by the adapter; we hand it the
			// full latest projection as the materialized baseline. (A true historical
			// store keeps every version; ProjectLatest here is the current snapshot the
			// Rebuild port persists. The point-in-time READ always folds the log.)
			if err := s.offline.Rebuild(ctx, id, ProjectLatest(events, view.SchemaVersion)); err != nil {
				return fmt.Errorf("rebuild offline %s: %w", id, err)
			}
		}

		replayed += int64(len(events))
		// Emit a progress frame after each view. emit returning an error (client
		// disconnected) ABORTS the rebuild — no point replaying into a closed stream.
		if err := emit(RebuildProgress{
			EventsReplayed:       replayed,
			TotalEvents:          -1, // counting the whole log up front can be costly; -1 = unknown
			CurrentFeatureViewID: id,
			Done:                 false,
		}); err != nil {
			return fmt.Errorf("emit progress: %w", err)
		}
	}

	// Final frame: done=true signals completion (stream close in the handler).
	return emit(RebuildProgress{EventsReplayed: replayed, TotalEvents: replayed, Done: true})
}

// ============================================================================
// SHARED HELPERS (team scoping, folding, validation, projection assembly)
// ============================================================================

// authorizedView loads a view by id and enforces TEAM SCOPING in one place. A
// view owned by a different team is reported as ErrViewNotFound — NOT
// ErrPermissionDenied — so a caller cannot use the error to probe which ids exist
// in other teams (existence is itself information; we hide it). This is the single
// security chokepoint every id-addressed operation routes through.
func (s *featureStoreService) authorizedView(ctx context.Context, p Principal, id string) (FeatureView, error) {
	if id == "" {
		return FeatureView{}, fmt.Errorf("%w: feature_view_id is required", ErrValidation)
	}
	view, err := s.foldViewByID(ctx, id)
	if err != nil {
		return FeatureView{}, err
	}
	if view.OwnerTeam != p.Team {
		// Deliberately the SAME error as a truly-missing view (anti-enumeration).
		return FeatureView{}, ErrViewNotFound
	}
	return view, nil
}

// foldViewByID loads a view's definition events and folds them into a FeatureView.
// Maps the storage sentinel (ErrEventLogNotFound) to the business one
// (ErrViewNotFound) — the single translation point between vocabularies.
func (s *featureStoreService) foldViewByID(ctx context.Context, id string) (FeatureView, error) {
	events, err := s.log.LoadView(ctx, id)
	if err != nil {
		if errors.Is(err, ErrEventLogNotFound) {
			return FeatureView{}, ErrViewNotFound
		}
		return FeatureView{}, fmt.Errorf("load view events: %w", err)
	}
	view, ok := foldView(events)
	if !ok {
		// Events existed but none defined the view — a corrupt log; treat as not found.
		return FeatureView{}, ErrViewNotFound
	}
	return view, nil
}

// assemblePage turns a found-vectors map into the ordered result page: it
// preserves the request's entity order for found vectors, lists the rest as
// missing (explicit, never silently dropped), and applies the optional
// feature-name projection (return only requested columns).
func assemblePage(requested []string, found map[string]FeatureVector, featureNames []string, nextToken string) FeatureVectorPage {
	page := FeatureVectorPage{NextPageToken: nextToken}
	for _, id := range requested {
		v, ok := found[id]
		if !ok {
			page.MissingEntityIDs = append(page.MissingEntityIDs, id)
			continue
		}
		if len(featureNames) > 0 {
			v.Values = projectColumns(v.Values, featureNames)
		}
		page.Vectors = append(page.Vectors, v)
	}
	return page
}

// projectColumns returns a copy of values restricted to the requested feature
// names (the read-side column projection). A copy so we never mutate the cached
// vector the online store handed us.
func projectColumns(values map[string]FeatureValue, names []string) map[string]FeatureValue {
	out := make(map[string]FeatureValue, len(names))
	for _, n := range names {
		if v, ok := values[n]; ok {
			out[n] = v
		}
	}
	return out
}

// clampPage normalizes pagination: 0 ⇒ DefaultPageSize, and anything over
// MaxPageSize is CLAMPED down (not rejected — a too-large page is harmless to
// bound silently, unlike a too-large write batch).
func clampPage(opts ListOptions) ListOptions {
	if opts.PageSize <= 0 {
		opts.PageSize = DefaultPageSize
	}
	if opts.PageSize > MaxPageSize {
		opts.PageSize = MaxPageSize
	}
	return opts
}

// cloneValues deep-enough-copies a row's value map so the immutable log never
// aliases caller-owned memory (a caller mutating its map after the call must not
// retroactively change a stored event). Slices/maps inside FeatureValue are
// copied too, since those are the mutable parts.
func cloneValues(in map[string]FeatureValue) map[string]FeatureValue {
	out := make(map[string]FeatureValue, len(in))
	for k, v := range in {
		if v.List != nil {
			v.List = slices.Clone(v.List)
		}
		if v.Struct != nil {
			m := make(map[string]any, len(v.Struct))
			for sk, sv := range v.Struct {
				m[sk] = sv
			}
			v.Struct = m
		}
		out[k] = v
	}
	return out
}

// dedupeStrings preserves order while removing duplicate entity ids (a batch may
// write several rows for one entity; the FeaturesWritten event wants the distinct set).
func dedupeStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// ============================================================================
// VALIDATION (the schema contract — the train/serve-skew guard)
// ============================================================================

// validateDefineInput rejects a malformed DefineFeatureView command before any
// I/O: a name, an entity, and a non-empty schema of well-formed specs are required.
func validateDefineInput(in DefineFeatureViewInput) error {
	if in.Name == "" {
		return fmt.Errorf("%w: view name is required", ErrValidation)
	}
	if in.Entity.Name == "" || in.Entity.JoinKey == "" {
		return fmt.Errorf("%w: entity name and join_key are required", ErrValidation)
	}
	if len(in.Features) == 0 {
		return fmt.Errorf("%w: at least one feature spec is required", ErrValidation)
	}
	seen := make(map[string]struct{}, len(in.Features))
	for _, f := range in.Features {
		if f.Name == "" {
			return fmt.Errorf("%w: feature name is required", ErrValidation)
		}
		if _, dup := seen[f.Name]; dup {
			return fmt.Errorf("%w: duplicate feature name %q", ErrValidation, f.Name)
		}
		seen[f.Name] = struct{}{}
		if f.ValueType == FeatureTypeUnspecified {
			return fmt.Errorf("%w: feature %q has UNSPECIFIED value_type", ErrValidation, f.Name)
		}
		// Dimension is only meaningful for DOUBLE_LIST and must be non-negative.
		if f.Dimension < 0 {
			return fmt.Errorf("%w: feature %q has negative dimension %d", ErrValidation, f.Name, f.Dimension)
		}
		if f.Dimension > 0 && f.ValueType != FeatureTypeDoubleList {
			return fmt.Errorf("%w: feature %q declares a dimension but is not DOUBLE_LIST", ErrValidation, f.Name)
		}
	}
	return nil
}

// validateRow checks one WriteFeatures row against the view schema: every value's
// key must be a declared feature, its Kind must match the declared type, and a
// DOUBLE_LIST must match the declared dimension. A failure is ErrSchemaViolation
// (the caller fails the whole batch). An EMPTY entity id is rejected — the entity
// id is the projection key; an empty key would collide every anonymous row.
func validateRow(view FeatureView, row FeatureRow) error {
	if row.EntityID == "" {
		return fmt.Errorf("%w: entity_id is required", ErrSchemaViolation)
	}
	if len(row.Values) == 0 {
		return fmt.Errorf("%w: row has no feature values", ErrSchemaViolation)
	}
	for name, val := range row.Values {
		spec, ok := view.specByName(name)
		if !ok {
			return fmt.Errorf("%w: feature %q is not in the view schema", ErrSchemaViolation, name)
		}
		if val.Kind != spec.ValueType {
			return fmt.Errorf("%w: feature %q is %s but value is %s", ErrSchemaViolation, name, spec.ValueType, val.Kind)
		}
		if spec.ValueType == FeatureTypeDoubleList && spec.Dimension > 0 && len(val.List) != spec.Dimension {
			return fmt.Errorf("%w: feature %q expects dimension %d, got %d", ErrSchemaViolation, name, spec.Dimension, len(val.List))
		}
	}
	return nil
}
