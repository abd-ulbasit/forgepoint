// Package redis is the READ-side adapter of the registry's CQRS split: it implements
// domain.ReadStore against Redis using redis/go-redis/v9, plus the projection-WRITER
// methods a future NATS consumer calls to (re)build the projection from emitted events.
//
// ============================================================================
// THIS IS THE "DENORMALIZED PROJECTION" ADAPTER (the READ side)
// ============================================================================
//
// CQRS recap (see services/registry/internal/domain/ports.go): Postgres is the WRITE
// source of truth; Redis is the eventually-consistent READ model, built by a projection
// consumer from the events commands emit. EVERY query RPC reads through THIS adapter; no
// query ever touches Postgres. The shape here is optimized for the queries, not for
// normalization — "prod version of model X" is a STORED field, never a read-time join.
//
// THE TWO HALVES OF THIS FILE:
//
//	domain.ReadStore (the READ port the service consumes) — GetModelByID/ByName,
//	ListModels, GetVersionByID/ByLabel, ListVersions. These are the hot query paths.
//
//	The PROJECTION WRITER (UpsertModel/UpsertVersion/...) — NOT a domain port; it is the
//	write surface the projection consumer (events phase) drives from each event. We
//	implement it HERE so the read model is self-contained and so the integration tests
//	can seed the projection (the test plays the role of the consumer) and then assert
//	read-after-write. In production the same methods are called by the NATS consumer.
//
// ============================================================================
// THE REDIS KEY/DATA-STRUCTURE DESIGN (the centerpiece of the read side)
// ============================================================================
//
//	fp:reg:model:{id}                  HASH   — the projected model (all fields).
//	fp:reg:team:{team}:name:{name}     STRING — name→id index (GetModelByName is O(1)).
//	fp:reg:team:{team}:models          ZSET   — member=modelID, score=created_at(ns).
//	                                            Newest-first list + cursor pagination.
//	fp:reg:version:{id}                HASH   — the projected version (all fields).
//	fp:reg:model:{modelID}:label:{lbl} STRING — (modelID,label)→versionID index.
//	fp:reg:model:{modelID}:versions    ZSET   — member=versionID, score=created_at(ns).
//	                                            Per-model newest-first list + pagination.
//
// WHY THESE STRUCTURES:
//
//	HASH for an entity — a model/version is a flat record; a hash stores all its fields
//	under one key, fetched in one HGETALL (one round-trip), and updated field-wise.
//
//	ZSET (sorted set) for a LIST — Redis sorted sets give an ordered index with O(log n)
//	range scans. Scoring by created_at (nanoseconds) and reading with ZREVRANGEBYSCORE
//	yields newest-first directly. CURSOR PAGINATION rides the score: the next-page token
//	encodes (created_at, id) of the last item, and the next page asks for "score < that"
//	— stable under concurrent inserts (a new model added mid-pagination doesn't shift a
//	cursor anchored on a score), which is exactly why the domain mandates cursor (not
//	OFFSET) pagination in ListOptions.
//
//	STRING secondary indexes (name→id, label→versionID) — so a lookup the caller
//	expresses by HUMAN identity (name/label) is a single GET of the id, then a HGETALL.
//	Denormalizing these avoids any scan.
//
// TEAM SCOPING: every model key path is NOT team-prefixed on the entity hash (the id is
// globally unique), but the NAME index and the LIST zset ARE team-scoped, and the
// service passes the actor's team into GetModelByID too so a cross-team id read returns
// not-found. This mirrors the write side's team gate.
//
// EVENTUAL CONSISTENCY: a just-registered model may be briefly ABSENT here (the event
// hasn't been projected yet). A miss returns domain.ErrRecordNotFound, which the service
// surfaces as not-found — the documented projection-lag tradeoff.
package redis

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
)

// keyPrefix namespaces every key this service owns in Redis. A shared Redis (or one
// later partitioned by ACLs) stays collision-free across services because each service
// owns a distinct prefix. "reg" = registry.
const keyPrefix = "fp:reg"

// ReadStore is the Redis-backed implementation of domain.ReadStore (+ the projection
// writer). It holds a go-redis client, which is itself a connection pool safe for
// concurrent use — every method issues commands through it.
type ReadStore struct {
	rdb *goredis.Client
}

// Compile-time proof the adapter satisfies the READ port. Drift fails the build here.
var _ domain.ReadStore = (*ReadStore)(nil)

// NewReadStore builds a ReadStore from a Redis address ("host:port"), pinging to fail
// fast on an unreachable server (same fail-fast rationale as the Postgres constructor).
func NewReadStore(ctx context.Context, addr string) (*ReadStore, error) {
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("registry/redis: ping: %w", err)
	}
	return &ReadStore{rdb: rdb}, nil
}

// NewReadStoreFromClient wraps an existing go-redis client (tests share one client).
func NewReadStoreFromClient(rdb *goredis.Client) *ReadStore {
	return &ReadStore{rdb: rdb}
}

// Close releases the underlying client.
func (s *ReadStore) Close() error { return s.rdb.Close() }

// ============================================================================
// KEY BUILDERS — one place that knows the key schema (above), so a typo can't
// silently fragment the projection across two key spellings.
// ============================================================================

func modelKey(id string) string            { return keyPrefix + ":model:" + id }
func versionKey(id string) string          { return keyPrefix + ":version:" + id }
func teamNameKey(team, name string) string { return keyPrefix + ":team:" + team + ":name:" + name }
func teamModelsKey(team string) string     { return keyPrefix + ":team:" + team + ":models" }
func modelLabelKey(modelID, lbl string) string {
	return keyPrefix + ":model:" + modelID + ":label:" + lbl
}
func modelVersionsKey(modelID string) string { return keyPrefix + ":model:" + modelID + ":versions" }

// ============================================================================
// domain.ReadStore — QUERIES (the hot read paths)
// ============================================================================

// GetModelByID fetches a projected model, then enforces team scoping in the ADAPTER:
// a model whose stored team != the caller's team returns ErrRecordNotFound, so a caller
// cannot read another team's model even with its id (anti cross-tenant enumeration,
// mirroring the write side's loadOwnedModel gate).
func (s *ReadStore) GetModelByID(ctx context.Context, team, id string) (domain.Model, error) {
	m, err := s.loadModel(ctx, id)
	if err != nil {
		return domain.Model{}, err
	}
	if m.Team != team {
		return domain.Model{}, domain.ErrRecordNotFound
	}
	return m, nil
}

// GetModelByIDUnscoped loads a projected model by id WITHOUT enforcing team
// scoping. It exists for the CQRS PROJECTION CONSUMER (internal/events/projection.go),
// which rebuilds the read model from fp.models.* events and must load-merge an
// existing model to apply a partial update (e.g. a version event that only changes
// the latest/production pointer) without clobbering the fields the event doesn't
// carry. The projection is a TRUSTED, server-side writer — it is not a tenant query
// path — so the cross-tenant gate that GetModelByID applies (and which needs a team
// the version events don't carry) is deliberately absent here. Tenant-facing READS
// must always go through GetModelByID/GetModelByName, never this.
func (s *ReadStore) GetModelByIDUnscoped(ctx context.Context, id string) (domain.Model, error) {
	return s.loadModel(ctx, id)
}

// GetModelByName resolves the team-scoped name index to an id, then loads the hash.
// Two round-trips (GET id, HGETALL hash); both O(1). A missing name index OR a missing
// hash is a clean not-found.
func (s *ReadStore) GetModelByName(ctx context.Context, team, name string) (domain.Model, error) {
	id, err := s.rdb.Get(ctx, teamNameKey(team, name)).Result()
	if errors.Is(err, goredis.Nil) {
		return domain.Model{}, domain.ErrRecordNotFound
	}
	if err != nil {
		return domain.Model{}, fmt.Errorf("registry/redis: get model name index: %w", err)
	}
	return s.GetModelByID(ctx, team, id)
}

// ListModels returns a team-scoped, NEWEST-FIRST page from the team's models ZSET,
// applying the in-memory filter (task_type/framework/archived) AFTER fetching each
// candidate hash. Cursor pagination rides the ZSET score (created_at).
//
// WHY filter in the adapter rather than in a Redis query: Redis sorted sets index by ONE
// score (created_at) — they cannot also filter by task_type. Maintaining a separate ZSET
// per (team,task_type,framework) facet would be a combinatorial key explosion. So we
// page the ordered id list and filter the small page in memory. For the registry's
// cardinality (models per team is modest) this is the right tradeoff; a high-cardinality
// facet would instead get its own index ZSET. We over-fetch slightly to still return a
// FULL page after filtering (see the fetch-extra loop).
func (s *ReadStore) ListModels(ctx context.Context, team string, filter domain.ListModelsFilter, opts domain.ListOptions) ([]domain.Model, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = domain.DefaultPageSize
	}
	maxScore, err := decodeCursor(opts.PageToken)
	if err != nil {
		return nil, "", err
	}

	out := make([]domain.Model, 0, pageSize)
	var lastModel domain.Model
	// Walk the ordered id list in batches, applying the filter, until we have a full
	// page or run out. We request pageSize+1-style batches with a moving score ceiling
	// so the filter never short-changes the page.
	batch := pageSize + 1
	for {
		ids, scores, err := s.zrevRangeByScore(ctx, teamModelsKey(team), maxScore, batch)
		if err != nil {
			return nil, "", err
		}
		if len(ids) == 0 {
			break // exhausted the set
		}
		for i, id := range ids {
			m, err := s.loadModel(ctx, id)
			if err != nil {
				if errors.Is(err, domain.ErrRecordNotFound) {
					// A ZSET member whose hash vanished (a torn projection write); skip it
					// rather than fail the whole list. The projection rebuild heals it.
					continue
				}
				return nil, "", err
			}
			// Move the cursor ceiling so the NEXT batch starts strictly below this score
			// (exclusive), preventing re-reading the same member across batches.
			maxScore = scoreCursor{score: scores[i], id: id}
			if !modelMatchesFilter(m, filter) {
				continue
			}
			out = append(out, m)
			lastModel = m
			if len(out) == pageSize {
				// Page full. The next token anchors on this last item's (score,id).
				next := encodeCursor(scoreCursor{score: scores[i], id: id})
				return out, next, nil
			}
		}
		if len(ids) < batch {
			break // last batch was short ⇒ no more members
		}
	}
	// Exhausted without filling a page ⇒ no next token.
	_ = lastModel
	return out, "", nil
}

// CountModels returns the TOTAL number of a team's models matching the filter — the
// accurate aggregate the dashboard renders (and the value ListModels' Page.Total /
// the proto total_count carry).
//
// WHY iterate-and-filter rather than a single ZCARD: ZCARD would give the raw team
// ZSET cardinality, which INCLUDES archived models — but ListModels HIDES archived by
// default (IncludeArchived=false), and also narrows by task_type/framework. A count
// computed differently from the list is exactly how the dashboard ("0") and the list
// ("3") came to disagree. So we count through the SAME modelMatchesFilter predicate
// the list uses, loading each member's hash, so the two can never drift. We read the
// whole team ZSET (no score ceiling) because a COUNT must see every member, not a
// page. For the registry's cardinality (models-per-team is modest) this is the right
// tradeoff; a high-cardinality tenant would instead maintain a per-(team,facet)
// counter key bumped by the projection — documented as the scale-up path, same
// observable contract.
//
// A torn projection (a ZSET member whose hash vanished) is SKIPPED, mirroring
// ListModels, so the count reflects only models a list would actually return.
func (s *ReadStore) CountModels(ctx context.Context, team string, filter domain.ListModelsFilter) (int, error) {
	res, err := s.rdb.ZRevRangeByScoreWithScores(ctx, teamModelsKey(team), &goredis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("registry/redis: zrevrangebyscore (count) %s: %w", teamModelsKey(team), err)
	}
	count := 0
	for _, z := range res {
		id, _ := z.Member.(string)
		m, err := s.loadModel(ctx, id)
		if err != nil {
			if errors.Is(err, domain.ErrRecordNotFound) {
				continue // torn projection member; skip, exactly as ListModels does
			}
			return 0, err
		}
		if modelMatchesFilter(m, filter) {
			count++
		}
	}
	return count, nil
}

// GetVersionByID loads a projected version hash by its own id.
func (s *ReadStore) GetVersionByID(ctx context.Context, id string) (domain.ModelVersion, error) {
	return s.loadVersion(ctx, id)
}

// GetVersionByLabel resolves the (modelID,label)→id index then loads the hash.
func (s *ReadStore) GetVersionByLabel(ctx context.Context, modelID, version string) (domain.ModelVersion, error) {
	id, err := s.rdb.Get(ctx, modelLabelKey(modelID, version)).Result()
	if errors.Is(err, goredis.Nil) {
		return domain.ModelVersion{}, domain.ErrRecordNotFound
	}
	if err != nil {
		return domain.ModelVersion{}, fmt.Errorf("registry/redis: get version label index: %w", err)
	}
	return s.loadVersion(ctx, id)
}

// ListVersions returns a model's versions newest-first, optionally filtered to one
// stage (StageUnspecified = any). Same ZSET-paginate-then-filter approach as ListModels.
func (s *ReadStore) ListVersions(ctx context.Context, modelID string, stageFilter domain.ModelStage, opts domain.ListOptions) ([]domain.ModelVersion, string, error) {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = domain.DefaultPageSize
	}
	maxScore, err := decodeCursor(opts.PageToken)
	if err != nil {
		return nil, "", err
	}

	out := make([]domain.ModelVersion, 0, pageSize)
	batch := pageSize + 1
	for {
		ids, scores, err := s.zrevRangeByScore(ctx, modelVersionsKey(modelID), maxScore, batch)
		if err != nil {
			return nil, "", err
		}
		if len(ids) == 0 {
			break
		}
		for i, id := range ids {
			v, err := s.loadVersion(ctx, id)
			if err != nil {
				if errors.Is(err, domain.ErrRecordNotFound) {
					continue
				}
				return nil, "", err
			}
			maxScore = scoreCursor{score: scores[i], id: id}
			if stageFilter != domain.StageUnspecified && v.Stage != stageFilter {
				continue
			}
			out = append(out, v)
			if len(out) == pageSize {
				next := encodeCursor(scoreCursor{score: scores[i], id: id})
				return out, next, nil
			}
		}
		if len(ids) < batch {
			break
		}
	}
	return out, "", nil
}

// modelMatchesFilter applies the in-memory filter facets. IncludeArchived defaults
// false, hiding soft-deleted models from default lists (the domain's documented default).
func modelMatchesFilter(m domain.Model, f domain.ListModelsFilter) bool {
	if !f.IncludeArchived && m.IsArchived() {
		return false
	}
	if f.TaskType != "" && m.TaskType != f.TaskType {
		return false
	}
	if f.Framework != "" && m.Framework != f.Framework {
		return false
	}
	return true
}

// ============================================================================
// CURSOR — the (score, id) pagination token shared by both lists.
// ============================================================================

// scoreCursor anchors a page boundary on the last item's ZSET score (created_at ns) and
// id. The id breaks ties when two items share a created_at (the SAME total-order rule the
// Postgres composite index uses), so the cursor is TOTAL and pagination can't skip or
// duplicate an item with a duplicate timestamp.
type scoreCursor struct {
	score float64
	id    string
}

// noCursor is the "start from newest" sentinel (no token supplied). We represent
// "unbounded ceiling" with +Inf so the first page reads from the highest score down.
var noCursor = scoreCursor{score: posInf, id: ""}

// posInf is the +Inf ceiling for the first page (no cursor): every real created_at
// score is strictly below it, so the first ZREVRANGEBYSCORE reads from the top.
var posInf = math.Inf(1)

// encodeCursor renders a scoreCursor as an opaque base64 token ("score|id"). Opaque so
// callers treat it as a black box (we can change the encoding later) and base64 so it is
// URL/JSON-safe in the proto's page_token string.
func encodeCursor(c scoreCursor) string {
	raw := strconv.FormatFloat(c.score, 'f', -1, 64) + "|" + c.id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor parses a token back to a scoreCursor. An EMPTY token means "first page"
// → the +Inf ceiling. A malformed token is a client error surfaced as ErrValidation so
// the handler maps it to InvalidArgument (a garbage cursor is bad input, not a 500).
func decodeCursor(token string) (scoreCursor, error) {
	if token == "" {
		return noCursor, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return scoreCursor{}, fmt.Errorf("%w: malformed page token", domain.ErrValidation)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return scoreCursor{}, fmt.Errorf("%w: malformed page token", domain.ErrValidation)
	}
	score, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return scoreCursor{}, fmt.Errorf("%w: malformed page token", domain.ErrValidation)
	}
	return scoreCursor{score: score, id: parts[1]}, nil
}

// zrevRangeByScore reads up to `limit` members of a ZSET with score STRICTLY LESS THAN
// the cursor's ceiling, newest (highest score) first, returning ids + their scores.
//
// THE EXCLUSIVE-CEILING TRICK (why pagination doesn't duplicate the boundary item):
//
//	A first page (noCursor) uses "+inf" as an INCLUSIVE max — read everything from the
//	top. A subsequent page uses "(score" (the leading '(' is Redis's EXCLUSIVE-bound
//	syntax) so the item the previous page ended ON is NOT re-read. But ties: two items
//	can share a score, and an exclusive score bound would DROP the tied item we haven't
//	shown yet. So when paginating we use an INCLUSIVE score ceiling and then SKIP ids ≥
//	the cursor id at the same score in the caller — handled by the cursor.id compare in
//	skipConsumed below. This keeps duplicate-timestamp items correctly ordered by id.
func (s *ReadStore) zrevRangeByScore(ctx context.Context, key string, cursor scoreCursor, limit int) ([]string, []float64, error) {
	var maxArg string
	if cursor.id == "" && cursor.score >= posInf {
		maxArg = "+inf" // first page: inclusive top
	} else {
		// Inclusive on score so we don't drop tied items; we filter out already-seen ids
		// at the boundary score below.
		maxArg = strconv.FormatFloat(cursor.score, 'f', -1, 64)
	}
	res, err := s.rdb.ZRevRangeByScoreWithScores(ctx, key, &goredis.ZRangeBy{
		Min:    "-inf",
		Max:    maxArg,
		Offset: 0,
		Count:  int64(limit) + 16, // small over-fetch to absorb same-score boundary skips
	}).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("registry/redis: zrevrangebyscore %s: %w", key, err)
	}
	ids := make([]string, 0, limit)
	scores := make([]float64, 0, limit)
	for _, z := range res {
		id, _ := z.Member.(string)
		// Skip items already consumed by a previous page: at the BOUNDARY score, anything
		// with an id >= the cursor id was already shown (ZSET ties break by member
		// lexical order; we mirror that with the id compare). cursor.id=="" (first page)
		// skips nothing.
		if cursor.id != "" && z.Score == cursor.score && id >= cursor.id {
			continue
		}
		ids = append(ids, id)
		scores = append(scores, z.Score)
		if len(ids) == limit {
			break
		}
	}
	return ids, scores, nil
}

// ============================================================================
// PROJECTION WRITER — the surface the projection consumer (events phase) drives.
// NOT a domain port; co-located here because it owns the same key schema. Tests play
// the consumer's role by calling these to seed read-after-write scenarios.
// ============================================================================

// UpsertModel writes/overwrites a model's projection: the entity hash, the team name
// index, and the team list ZSET membership (scored by created_at). Idempotent — replaying
// the same event twice yields the same state (the projection-consumer idempotency the
// platform requires). Done as a MULTI/EXEC transaction so the hash, index, and zset land
// together (a reader never sees a hash without its index).
func (s *ReadStore) UpsertModel(ctx context.Context, m domain.Model) error {
	fields, err := modelToHash(m)
	if err != nil {
		return err
	}
	score := float64(m.CreatedAt.UnixNano())
	_, err = s.rdb.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		p.HSet(ctx, modelKey(m.ID), fields)
		p.Set(ctx, teamNameKey(m.Team, m.Name), m.ID, 0)
		p.ZAdd(ctx, teamModelsKey(m.Team), goredis.Z{Score: score, Member: m.ID})
		return nil
	})
	if err != nil {
		return fmt.Errorf("registry/redis: upsert model: %w", err)
	}
	return nil
}

// UpsertVersion writes/overwrites a version's projection: the entity hash, the
// (modelID,label)→id index, and the per-model versions ZSET membership.
func (s *ReadStore) UpsertVersion(ctx context.Context, v domain.ModelVersion) error {
	fields, err := versionToHash(v)
	if err != nil {
		return err
	}
	score := float64(v.CreatedAt.UnixNano())
	_, err = s.rdb.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		p.HSet(ctx, versionKey(v.ID), fields)
		p.Set(ctx, modelLabelKey(v.ModelID, v.Version), v.ID, 0)
		p.ZAdd(ctx, modelVersionsKey(v.ModelID), goredis.Z{Score: score, Member: v.ID})
		return nil
	})
	if err != nil {
		return fmt.Errorf("registry/redis: upsert version: %w", err)
	}
	return nil
}

// ============================================================================
// HASH (DE)SERIALIZATION — a flat string map mirrors the entity fields 1:1.
// ============================================================================

// modelToHash flattens a Model to a Redis hash. Times are stored as Unix-nanos strings
// (compact, monotonic, lossless to round-trip); tags/metrics as a JSON-ish encoded
// string; bools/ints as their decimal text. We store every field the read model serves
// so a HGETALL fully reconstructs the projected entity with no second lookup.
func modelToHash(m domain.Model) (map[string]string, error) {
	tags, err := encodeStringMap(m.Tags)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"id":                 m.ID,
		"name":               m.Name,
		"description":        m.Description,
		"owner_id":           m.OwnerID,
		"team":               m.Team,
		"framework":          m.Framework,
		"task_type":          m.TaskType,
		"tags":               tags,
		"production_version": m.ProductionVersion,
		"latest_version":     m.LatestVersion,
		"created_at":         formatNanos(m.CreatedAt),
		"updated_at":         formatNanos(m.UpdatedAt),
		"archived_at":        formatNanos(m.ArchivedAt), // "" for zero time
	}, nil
}

// loadModel HGETALLs a model hash and reconstructs the domain.Model. An empty result
// (no fields) means the key is absent ⇒ ErrRecordNotFound (Redis returns an empty map,
// not an error, for a missing hash — so absence is detected by len==0).
func (s *ReadStore) loadModel(ctx context.Context, id string) (domain.Model, error) {
	h, err := s.rdb.HGetAll(ctx, modelKey(id)).Result()
	if err != nil {
		return domain.Model{}, fmt.Errorf("registry/redis: hgetall model: %w", err)
	}
	if len(h) == 0 {
		return domain.Model{}, domain.ErrRecordNotFound
	}
	tags, err := decodeStringMap(h["tags"])
	if err != nil {
		return domain.Model{}, err
	}
	return domain.Model{
		ID:                h["id"],
		Name:              h["name"],
		Description:       h["description"],
		OwnerID:           h["owner_id"],
		Team:              h["team"],
		Framework:         h["framework"],
		TaskType:          h["task_type"],
		Tags:              tags,
		ProductionVersion: h["production_version"],
		LatestVersion:     h["latest_version"],
		CreatedAt:         parseNanos(h["created_at"]),
		UpdatedAt:         parseNanos(h["updated_at"]),
		ArchivedAt:        parseNanos(h["archived_at"]),
	}, nil
}

// versionToHash / loadVersion are the version analogs.
func versionToHash(v domain.ModelVersion) (map[string]string, error) {
	metrics, err := encodeFloatMap(v.Metrics)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"id":              v.ID,
		"model_id":        v.ModelID,
		"version":         v.Version,
		"description":     v.Description,
		"metrics":         metrics,
		"artifact_path":   v.ArtifactPath,
		"artifact_digest": v.ArtifactDigest,
		"size_bytes":      strconv.FormatInt(v.SizeBytes, 10),
		"stage":           strconv.Itoa(int(v.Stage)),
		"status":          strconv.Itoa(int(v.Status)),
		"created_by":      v.CreatedBy,
		"created_at":      formatNanos(v.CreatedAt),
	}, nil
}

func (s *ReadStore) loadVersion(ctx context.Context, id string) (domain.ModelVersion, error) {
	h, err := s.rdb.HGetAll(ctx, versionKey(id)).Result()
	if err != nil {
		return domain.ModelVersion{}, fmt.Errorf("registry/redis: hgetall version: %w", err)
	}
	if len(h) == 0 {
		return domain.ModelVersion{}, domain.ErrRecordNotFound
	}
	metrics, err := decodeFloatMap(h["metrics"])
	if err != nil {
		return domain.ModelVersion{}, err
	}
	size, _ := strconv.ParseInt(h["size_bytes"], 10, 64)
	stage, _ := strconv.Atoi(h["stage"])
	status, _ := strconv.Atoi(h["status"])
	return domain.ModelVersion{
		ID:             h["id"],
		ModelID:        h["model_id"],
		Version:        h["version"],
		Description:    h["description"],
		Metrics:        metrics,
		ArtifactPath:   h["artifact_path"],
		ArtifactDigest: h["artifact_digest"],
		SizeBytes:      size,
		Stage:          domain.ModelStage(stage),
		Status:         domain.VersionStatus(status),
		CreatedBy:      h["created_by"],
		CreatedAt:      parseNanos(h["created_at"]),
	}, nil
}

// ============================================================================
// SMALL ENCODING HELPERS (times, maps).
// ============================================================================

// formatNanos renders a time as its Unix-nanosecond decimal, or "" for the zero time
// (so an active model's archived_at round-trips back to the zero time, not epoch).
func formatNanos(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

// parseNanos inverts formatNanos. "" → zero time; a valid number → that instant in UTC.
func parseNanos(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// encodeStringMap / decodeStringMap serialize a string→string map for a hash FIELD.
// We use a length-prefixed key=value join via JSON to be robust to any character in a
// tag (a tag value could contain '=' or ','); JSON handles quoting/escaping correctly.
func encodeStringMap(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	return jsonString(m)
}

func decodeStringMap(s string) (map[string]string, error) {
	if s == "" {
		return nil, nil
	}
	var m map[string]string
	if err := jsonParse(s, &m); err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

func encodeFloatMap(m map[string]float64) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	return jsonString(m)
}

func decodeFloatMap(s string) (map[string]float64, error) {
	if s == "" {
		return nil, nil
	}
	var m map[string]float64
	if err := jsonParse(s, &m); err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}
