// online.go — the Redis ADAPTER implementing domain.OnlineViewStore: the "latest
// value" READ MODEL that serves the low-latency inference hot path.
//
// ============================================================================
// PATTERN: CQRS online read model (eventually-consistent cache over the log)
// ============================================================================
//
// The online store is a CACHE rebuildable from the event log. The service updates
// it just AFTER an Append (Put the new latest vectors), reads it on the hot path
// (GetLatest), and tears it down on delete / before a rebuild (Purge). It is
// eventually consistent: a GetLatest immediately after a write may miss it until the
// projection catches up — the domain surfaces that via FeatureVector.AsOfVersion so
// a caller can detect staleness.
//
// ============================================================================
// REDIS DATA MODEL (why these keys/structures)
// ============================================================================
//
// Per (view, entity) we store ONE Redis STRING holding the JSON-encoded
// FeatureVector:
//
//	key:  fp:fs:online:{viewID}:vec:{entityID}   value: JSON(FeatureVector)
//
// WHY a JSON string per entity rather than a Redis HASH of field->value: a served
// vector is read and written as a WHOLE unit (GetLatest returns the entire vector;
// Put replaces it), and it carries provenance (AsOfVersion/EventTime/SchemaVersion)
// that must travel atomically with the values. A single JSON string makes each
// GetLatest/Put one O(1) key op, and a batch of them one pipelined MGET/MSET — the
// cheapest possible hot path. A HASH would split one logical vector across fields,
// turning an atomic vector swap into a multi-field write with partial-update hazards
// for no benefit (we never read a single feature in isolation here).
//
// To support Purge (delete the WHOLE view's online projection) without SCANning the
// keyspace, we maintain a companion SET of the view's entity ids:
//
//	key:  fp:fs:online:{viewID}:entities   members: {entityID, ...}
//
// Purge reads the set, deletes every vector key + the set in one pipeline. WHY a SET
// index instead of KEYS/SCAN by prefix: KEYS is O(N) over the entire keyspace and
// blocks the server; SCAN is cursor-based but still scans everything. An explicit
// per-view index makes Purge proportional to the view's entity count, not the whole
// Redis instance — the production-safe choice.
//
// ============================================================================
// CONSISTENCY of multi-key writes
// ============================================================================
//
// Put writes each entity's vector key AND adds its id to the entity set. We do this
// in a single PIPELINE (one round-trip) rather than a MULTI/EXEC transaction: the
// operations are idempotent upserts (re-applying the same event yields the same
// state), so we do not need all-or-nothing atomicity across keys — a partial Put is
// self-healing on the next write or a RebuildViews. Pipelining gives the latency win
// without the transaction's blocking semantics. (If a stricter invariant ever
// required atomicity we'd wrap it in TxPipeline/WATCH; it is not needed for an
// idempotent cache.)
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// keyPrefix namespaces all of this service's keys so the online store can share a
// Redis instance with other tenants/services without collision. fp = forgepoint,
// fs = feature-store.
const keyPrefix = "fp:fs:online"

// OnlineViewStore is the Redis-backed implementation of domain.OnlineViewStore.
type OnlineViewStore struct {
	client goredis.UniversalClient
}

// NewOnlineViewStore constructs the adapter over a go-redis client. We accept the
// UniversalClient interface (not the concrete *redis.Client) so the same adapter
// works against a single node, a cluster, or a failover/sentinel setup — the caller
// (main.go) picks the topology. Matches the documented constructor:
// redis.NewOnlineViewStore(client).
func NewOnlineViewStore(client goredis.UniversalClient) *OnlineViewStore {
	return &OnlineViewStore{client: client}
}

var _ domain.OnlineViewStore = (*OnlineViewStore)(nil)

// vectorKey is the per-entity vector key. We keep view and entity as separate path
// segments so a Purge can address the view subtree via its entity index.
func vectorKey(viewID, entityID string) string {
	return fmt.Sprintf("%s:%s:vec:%s", keyPrefix, viewID, entityID)
}

// entitiesKey is the per-view SET of entity ids (the Purge index).
func entitiesKey(viewID string) string {
	return fmt.Sprintf("%s:%s:entities", keyPrefix, viewID)
}

// jsonVector is the stored shape of a FeatureVector. We reuse the same explicit
// tagged-union value encoding as the Postgres codec so a vector round-trips byte-for-
// byte regardless of which store it came from — the two read models agree on the
// wire format. (Defined locally to keep the redis package self-contained; it mirrors
// the postgres codec's jsonValue.)
type jsonVector struct {
	EntityID      string               `json:"entity_id"`
	Values        map[string]jsonValue `json:"values"`
	AsOfVersion   int64                `json:"as_of_version"`
	EventTime     time.Time            `json:"event_time"`
	SchemaVersion int64                `json:"schema_version"`
}

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

func encodeVector(v domain.FeatureVector) ([]byte, error) {
	jv := jsonVector{
		EntityID:      v.EntityID,
		AsOfVersion:   v.AsOfVersion,
		EventTime:     v.EventTime.UTC(),
		SchemaVersion: v.SchemaVersion,
		Values:        make(map[string]jsonValue, len(v.Values)),
	}
	for name, val := range v.Values {
		out := jsonValue{Type: valueTypeTag(val.Kind)}
		switch val.Kind {
		case domain.FeatureTypeInt64:
			out.Int = val.Int
		case domain.FeatureTypeDouble:
			out.Double = val.Double
		case domain.FeatureTypeString:
			out.Str = val.Str
		case domain.FeatureTypeBool:
			out.Bool = val.Bool
		case domain.FeatureTypeTimestamp:
			t := val.Time.UTC()
			out.Time = &t
		case domain.FeatureTypeDoubleList:
			out.List = val.List
		case domain.FeatureTypeStruct:
			out.Struct = val.Struct
		}
		jv.Values[name] = out
	}
	return json.Marshal(jv)
}

func decodeVector(b []byte) (domain.FeatureVector, error) {
	var jv jsonVector
	if err := json.Unmarshal(b, &jv); err != nil {
		return domain.FeatureVector{}, fmt.Errorf("decode online vector: %w", err)
	}
	v := domain.FeatureVector{
		EntityID:      jv.EntityID,
		AsOfVersion:   jv.AsOfVersion,
		EventTime:     jv.EventTime,
		SchemaVersion: jv.SchemaVersion,
		Values:        make(map[string]domain.FeatureValue, len(jv.Values)),
	}
	for name, ov := range jv.Values {
		val := domain.FeatureValue{Kind: valueTypeFromTag(ov.Type)}
		switch val.Kind {
		case domain.FeatureTypeInt64:
			val.Int = ov.Int
		case domain.FeatureTypeDouble:
			val.Double = ov.Double
		case domain.FeatureTypeString:
			val.Str = ov.Str
		case domain.FeatureTypeBool:
			val.Bool = ov.Bool
		case domain.FeatureTypeTimestamp:
			if ov.Time != nil {
				val.Time = *ov.Time
			}
		case domain.FeatureTypeDoubleList:
			val.List = ov.List
		case domain.FeatureTypeStruct:
			val.Struct = ov.Struct
		}
		v.Values[name] = val
	}
	return v, nil
}

// ============================================================================
// GetLatest — batched hot-path read
// ============================================================================
//
// Fetches the latest vectors for a set of entities in ONE round-trip via a pipeline
// of GETs. Entities with no online value are simply absent from the result map (the
// service reports them missing). WHY a pipeline of GETs rather than MGET: a pipeline
// gives the same single-round-trip batching while keeping per-key error handling
// (a corrupt value for one entity does not poison the whole batch); MGET would be
// marginally fewer bytes but loses that granularity. Both are one network round-trip.
func (s *OnlineViewStore) GetLatest(ctx context.Context, featureViewID string, entityIDs []string) (map[string]domain.FeatureVector, error) {
	out := make(map[string]domain.FeatureVector)
	if len(entityIDs) == 0 {
		return out, nil
	}

	pipe := s.client.Pipeline()
	cmds := make([]*goredis.StringCmd, len(entityIDs))
	for i, id := range entityIDs {
		cmds[i] = pipe.Get(ctx, vectorKey(featureViewID, id))
	}
	// Exec returns redis.Nil as the top-level error when ANY command in the pipeline
	// missed (a key not found). That is EXPECTED — missing entities are normal — so we
	// tolerate redis.Nil here and inspect each command individually below.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return nil, fmt.Errorf("online get pipeline: %w", err)
	}

	for i, cmd := range cmds {
		b, err := cmd.Bytes()
		if errors.Is(err, goredis.Nil) {
			continue // this entity has no online value — leave it absent (missing)
		}
		if err != nil {
			return nil, fmt.Errorf("online get %q: %w", entityIDs[i], err)
		}
		vec, derr := decodeVector(b)
		if derr != nil {
			return nil, derr
		}
		out[entityIDs[i]] = vec
	}
	return out, nil
}

// ============================================================================
// Put — upsert the latest vectors (after Append, and during Rebuild)
// ============================================================================
//
// Upserts each entity's vector and records its id in the view's entity-index SET, in
// one pipeline. UPSERT (not insert) because re-applying an event during a rebuild,
// or a retried write, must be idempotent — SET overwrites, SADD is a no-op for an
// existing member. So a duplicate Put converges to the same state.
func (s *OnlineViewStore) Put(ctx context.Context, featureViewID string, vectors map[string]domain.FeatureVector) error {
	if len(vectors) == 0 {
		return nil
	}
	pipe := s.client.Pipeline()
	eKey := entitiesKey(featureViewID)
	for entityID, vec := range vectors {
		b, err := encodeVector(vec)
		if err != nil {
			return err
		}
		// No TTL: the online value is the current truth and must not silently expire
		// out from under the inference path. Lifecycle is explicit (overwritten on the
		// next write, removed by Purge), not time-based. (If we wanted a safety net
		// against an orphaned key we could set a long TTL refreshed on each write; we
		// keep it explicit so "the value is gone" always means a deliberate Purge.)
		pipe.Set(ctx, vectorKey(featureViewID, entityID), b, 0)
		pipe.SAdd(ctx, eKey, entityID)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("online put pipeline: %w", err)
	}
	return nil
}

// ============================================================================
// Purge — tear down a view's entire online projection
// ============================================================================
//
// Reads the view's entity-index SET, then deletes every vector key plus the set
// itself in one pipeline. Used by DeleteFeatureView (stop serving a retired view)
// and at the start of RebuildViews(online) (start clean). Idempotent: purging an
// already-empty view is a no-op.
func (s *OnlineViewStore) Purge(ctx context.Context, featureViewID string) error {
	eKey := entitiesKey(featureViewID)

	// SMEMBERS gives us every entity id without SCANning the keyspace — the reason we
	// maintain the index set in Put. For a very large view this could be paginated
	// with SSCAN; SMEMBERS is fine at feature-view entity scale and keeps Purge a
	// single read + single delete pipeline.
	members, err := s.client.SMembers(ctx, eKey).Result()
	if err != nil {
		return fmt.Errorf("online purge read entities: %w", err)
	}

	pipe := s.client.Pipeline()
	for _, entityID := range members {
		pipe.Del(ctx, vectorKey(featureViewID, entityID))
	}
	// Delete the index set last (after queuing the vector deletes). Even if the set
	// was empty (members == nil), deleting a non-existent key is a harmless no-op, so
	// Purge stays idempotent.
	pipe.Del(ctx, eKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("online purge pipeline: %w", err)
	}
	return nil
}
