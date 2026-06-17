// Package events is the NATS adapter ring for the Model Serving service: the
// thin translation layer between the async event bus (pkg/natsutil over NATS
// JetStream) and the serving DOMAIN (internal/domain.ServingService).
//
// ============================================================================
// THIS SERVICE'S EVENT ROLE — A PURE CONSUMER (it PRODUCES NOTHING)
// ============================================================================
//
// A model-serving pod is a SIDECAR that loads ONE model version and answers
// Predict. It is auth-agnostic and bus-quiet by design:
//
//   - It PUBLISHES no events. The canonical "an inference happened, bill it"
//     event (InferenceCompleted) is owned by the Inference Gateway — the only
//     component that knows end-to-end latency, the billed principal, and the
//     post-traffic-split served version (event-contract.md, conflict #1). The
//     old serving.PredictionCompletedEvent / ModelLoadedEvent / ModelUnloadedEvent
//     were DELETED. So there is NO publisher adapter and NO outbox here.
//
//   - It CONSUMES five lifecycle events and reconciles its resident model:
//
//       SUBJECT                        PAYLOAD             → DOMAIN REACTION
//       ───────────────────────────────────────────────────────────────────
//       fp.models.version.ready        ModelVersionReady   → EnsureLoaded
//       fp.pipelines.model.deployed    ModelDeployed       → EnsureLoaded
//       fp.models.promoted             ModelPromoted       → EnsureLoaded
//       fp.pipelines.model.undeployed  ModelUndeployed     → Unload
//       fp.models.archived             ModelArchived       → Unload
//
// (Grounded in docs/design/event-contract.md → the per-service migration note
// for "serving": consume ModelVersionReady, ModelDeployed, ModelUndeployed,
// ModelPromoted, ModelArchived.)
//
// ============================================================================
// THE PATTERN — A KUBERNETES-STYLE RECONCILE CONTROLLER OVER A MESSAGE BUS
// ============================================================================
//
// The five events are not commands; they are DESIRED-STATE FACTS. The adapter
// is a controller that reconciles the pod's actual loaded model toward the
// desired state the events describe:
//
//   load-class events (version.ready / deployed / promoted) → "this version
//       SHOULD be resident and Ready" → EnsureLoaded (idempotent).
//   unload-class events (undeployed / archived)             → "this version
//       should NOT be resident" → Unload (idempotent).
//
// Because EnsureLoaded/Unload are idempotent (domain-enforced: a re-load of an
// already-Ready identical artifact is a no-op; an unload of an absent model is a
// no-op), a redelivered event produces the SAME effect as a single delivery —
// which is exactly what at-least-once delivery requires. We ALSO layer a
// transport-level dedupe (ProcessedStore on EventEnvelope.id) so a duplicate is
// recognized before the handler even runs. Belt and braces:
//   at-least-once delivery + idempotent reaction + envelope-id dedupe
//     = exactly-once IN EFFECT.
//
// ============================================================================
// THE ARTIFACT-URI PROBLEM — WHY THIS ADAPTER KEEPS A SMALL DESIRED-ARTIFACT MAP
// ============================================================================
//
// LoadModel/EnsureLoaded REQUIRE an artifact URI (and validate it against the
// SSRF allow-list before any fetch). Of the three load-class events, ONLY
// ModelVersionReady carries the artifact location (artifact_path + digest);
// ModelDeployed and ModelPromoted carry NO URI — they announce "serve this
// version" but not "from here".
//
// In the deployment saga the ordering is: registry emits ModelVersionReady
// (artifact uploaded+verified) BEFORE the orchestrator's deploy/promote steps.
// So by the time deployed/promoted arrive, the pod has already learned the
// version's artifact URI from version.ready. We remember it in a tiny
// per-ref desiredArtifacts map; deployed/promoted then reconcile using that
// remembered URI. If a deploy/promote arrives for a ref we have never seen a
// version.ready for, we cannot fabricate a URI — we ACK and skip (logged), the
// honest behavior: a serving pod cannot load weights from nowhere. (Fabricating
// a URI would defeat the SSRF allow-list and the supply-chain digest check.)
//
// This is a deliberate, documented tradeoff. The ALTERNATIVE — having the pod
// call back into the Registry's GetVersion RPC to resolve the URI on a deploy
// event — re-couples serving to the registry on the reconcile path and adds a
// synchronous dependency; we avoid it (fat-but-flat event-carried state is the
// platform's choice). The remembered-URI map is bounded by the pod's single
// model in practice (a sidecar serves one model), so it does not grow unbounded.
//
// ============================================================================
// CLEAN ARCHITECTURE PLACEMENT
// ============================================================================
//
//	cmd/server/main.go              (next stage: wires NATS conn + this adapter)
//	   ↓
//	internal/events  (THIS PACKAGE) ─ depends on → domain.ServingService (a port)
//	   ↓ uses                                       eventsv1 (wire payloads)
//	pkg/natsutil (Subscriber: envelopes, idempotency, DLQ, trace propagation)
//
// The adapter depends on the domain's DRIVING port (ServingService) and on the
// canonical event payloads (forgepoint.events.v1). It contains NO business
// logic: the BEHAVIOR behind each event lives in the domain (EnsureLoaded /
// Unload), unit-tested there; this layer only translates wire→domain and owns
// the delivery mechanics (dedupe, DLQ, ACK/NAK).
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// SUBJECTS + STREAMS (the canonical contract this adapter binds to)
// ============================================================================
//
// The five consumed subjects live in TWO subject trees, hence two JetStream
// streams. We bind each consumer to the SPECIFIC subject (not a wildcard) so the
// pod only receives the exact event types it reacts to — a wildcard like
// fp.models.> would also deliver registered/version.created/drift.detected that
// this pod has no reaction for, wasting redelivery budget on ACK-only no-ops.
const (
	// Model-lifecycle subjects (resource domain = models → stream MODELS).
	subjectModelVersionReady = "fp.models.version.ready"
	subjectModelPromoted     = "fp.models.promoted"
	subjectModelArchived     = "fp.models.archived"

	// Deployment-workflow subjects (resource domain = pipelines → stream PIPELINES).
	// The act of deploying is a workflow OUTCOME owned by the orchestrator saga,
	// so it lives under fp.pipelines.model.* (event-contract.md, conflict #2/#7).
	subjectModelDeployed   = "fp.pipelines.model.deployed"
	subjectModelUndeployed = "fp.pipelines.model.undeployed"

	// StreamModels / StreamPipelines are the JetStream streams these subjects are
	// stored in. A stream is a persistence boundary; we keep one per top-level
	// resource tree (fp.models.>, fp.pipelines.>) so retention/limits can be tuned
	// per domain. The adapter ensures both exist before subscribing (EnsureStreams)
	// so a fresh cluster (or a test) has somewhere for the consumer to attach.
	StreamModels    = "MODELS"
	StreamPipelines = "PIPELINES"

	// subjectModelsAll / subjectPipelinesAll are the stream's capture filters —
	// the stream stores EVERYTHING under the tree even though our CONSUMERS filter
	// to specific subjects. This means a producer that publishes fp.models.promoted
	// finds the message persisted whether or not a serving consumer exists yet.
	subjectModelsAll    = "fp.models.>"
	subjectPipelinesAll = "fp.pipelines.>"
)

// consumerGroupPrefix is the durable-consumer name PREFIX for this service. The
// full durable name is "<prefix>-<subject>" (built in Start), one per consumed
// subject. WHY per-subject durables: JetStream keys a consumer by (stream,
// durable name) and a durable consumer binds to exactly ONE filter subject — a
// single shared durable cannot span the three subjects we consume from the MODELS
// stream. Per-subject durables also let the retry/DLQ budget be reasoned about
// independently per event type.
//
// All REPLICAS of the serving Deployment use the same prefix, so for a given
// subject they share one durable and JetStream load-balances that subject's
// events across them (Kafka-consumer-group semantics) — each event handled by
// exactly one replica; idempotency makes a crash-redelivery to a DIFFERENT
// replica safe too.
const consumerGroupPrefix = "model-serving"

// ============================================================================
// Subscriber — the consumer adapter
// ============================================================================

// Subscriber wires the five consumed subjects to the serving domain. It builds
// one natsutil.Subscriber PER subject (each with its own durable name + the
// shared idempotency/DLQ options) so the durables don't collide, and keeps a
// small desired-artifact map (the URI/digest learned from ModelVersionReady,
// reused by deployed/promoted).
//
// It depends on the domain.ServingService INTERFACE, not the concrete impl, so
// it is unit-testable against a fake service and the same adapter serves the
// real service in production (dependency inversion).
type Subscriber struct {
	svc     domain.ServingService
	js      jetstream.JetStream
	subOpts []natsutil.SubOption // shared options (idempotency store, DLQ, timeouts)
	log     *slog.Logger

	// subs holds the per-subject natsutil.Subscribers created by Start, so Close
	// can stop every consume loop on shutdown.
	subsMu sync.Mutex
	subs   []*natsutil.Subscriber

	// desiredArtifacts remembers the artifact location/digest learned from
	// ModelVersionReady, keyed by the model ref. deployed/promoted reconcile
	// against it because their payloads carry no URI (see the package doc's
	// "artifact-uri problem"). Guarded by mu — multiple subject consumers (and,
	// per consumer, the managed pull loop) can touch it concurrently.
	mu               sync.RWMutex
	desiredArtifacts map[domain.ModelRef]artifactRef
}

// artifactRef is the minimal load input remembered per version: where to fetch
// and what digest to expect. (Not the full domain.LoadModelInput because the
// idempotency key is event-specific and built per reaction.)
type artifactRef struct {
	uri    string
	digest string
}

// NewSubscriber builds the consumer adapter.
//
// It takes the JetStream context plus the SHARED subscriber options the service
// standardizes on (an idempotency ProcessedStore via WithIdempotencyStore, a DLQ
// subject + MaxRetries via WithDLQSubject/WithMaxRetries, a per-message timeout).
// The adapter applies those options to EACH per-subject natsutil.Subscriber and
// appends a per-subject WithConsumerGroup durable name. Passing options (not a
// pre-built Subscriber) is what lets the adapter own the durable-per-subject
// requirement while the caller (next-stage main.go) owns the dedupe/DLQ policy.
func NewSubscriber(svc domain.ServingService, js jetstream.JetStream, log *slog.Logger, subOpts ...natsutil.SubOption) *Subscriber {
	if log == nil {
		log = slog.Default()
	}
	return &Subscriber{
		svc:              svc,
		js:               js,
		subOpts:          subOpts,
		log:              log,
		desiredArtifacts: make(map[domain.ModelRef]artifactRef),
	}
}

// Start binds all five subject consumers, each on its own durable. Each Subscribe
// spins a managed pull loop that runs until ctx is cancelled or Close() is called.
//
// WHY one consumer per subject (not a single fp.>): JetStream durable consumers
// bind to ONE filter subject, and a wildcard would also deliver event types this
// pod has no reaction for (registered/version.created/drift.detected/...),
// wasting work on ACK-only no-ops.
func (s *Subscriber) Start(ctx context.Context) error {
	type binding struct {
		stream  string
		subject string
		handler natsutil.EventHandler
	}
	bindings := []binding{
		{StreamModels, subjectModelVersionReady, s.handleModelVersionReady},
		{StreamModels, subjectModelPromoted, s.handleModelPromoted},
		{StreamModels, subjectModelArchived, s.handleModelArchived},
		{StreamPipelines, subjectModelDeployed, s.handleModelDeployed},
		{StreamPipelines, subjectModelUndeployed, s.handleModelUndeployed},
	}

	for _, b := range bindings {
		if err := s.subscribeOne(ctx, b.stream, b.subject, b.handler); err != nil {
			return fmt.Errorf("events: subscribe %s: %w", b.subject, err)
		}
	}
	return nil
}

// Close stops every per-subject consume loop. Safe to call multiple times.
func (s *Subscriber) Close() {
	s.subsMu.Lock()
	defer s.subsMu.Unlock()
	for _, sub := range s.subs {
		sub.Close()
	}
	s.subs = nil
}

// subscribeOne builds a per-subject natsutil.Subscriber (shared options + a
// per-subject durable name) and starts its consume loop on the given subject.
//
// DURABLE NAME: "<prefix>-<subject-with-dots-as-dashes>". JetStream durable names
// may not contain '.', '*', or '>', so we sanitize the subject into the name.
func (s *Subscriber) subscribeOne(ctx context.Context, stream, subject string, handler natsutil.EventHandler) error {
	opts := make([]natsutil.SubOption, 0, len(s.subOpts)+1)
	opts = append(opts, s.subOpts...)
	opts = append(opts, natsutil.WithConsumerGroup(durableName(subject)))

	sub := natsutil.NewSubscriber(s.js, opts...)

	s.subsMu.Lock()
	s.subs = append(s.subs, sub)
	s.subsMu.Unlock()

	return sub.Subscribe(ctx, stream, subject, handler)
}

// durableName turns a subject into a JetStream-legal durable consumer name:
// "<prefix>-<subject>" with '.' replaced by '-' (durable names forbid '.').
func durableName(subject string) string {
	out := make([]byte, 0, len(consumerGroupPrefix)+1+len(subject))
	out = append(out, consumerGroupPrefix...)
	out = append(out, '-')
	for i := 0; i < len(subject); i++ {
		c := subject[i]
		if c == '.' || c == '*' || c == '>' {
			c = '-'
		}
		out = append(out, c)
	}
	return string(out)
}

// ============================================================================
// HANDLERS — wire → domain translation (one per consumed event)
// ============================================================================
//
// Each handler:
//   1. Decodes envelope.Data into its eventsv1 payload (encoding/json — the
//      symmetric inverse of natsutil.Publisher's json.Marshal; producers on this
//      platform publish via that same Publisher, so the wire form of Data is JSON
//      over the proto struct's json tags).
//   2. Validates the minimum fields it needs (name+version).
//   3. Calls the idempotent domain reaction.
//
// Returning nil ACKs; returning an error NAKs (→ retry → DLQ after MaxRetries).
// A decode failure of a STRUCTURALLY-INVALID payload is a poison message: it
// will never succeed on redelivery, so we return nil to ACK-and-drop it rather
// than spin it through the retry budget — except we DO route truly-poison cases
// to DLQ by letting the natsutil term path handle unparsable ENVELOPES (it Terms
// on json.Unmarshal of the envelope). Here the envelope already parsed; a bad
// PAYLOAD inside a valid envelope we treat as non-retryable and ACK after logging
// (returning an error would pointlessly retry an unparseable payload N times).

// handleModelVersionReady reacts to fp.models.version.ready → EnsureLoaded.
// This is the ONLY load-class event that carries the artifact location, so it
// also POPULATES the desired-artifact map that deployed/promoted reuse.
func (s *Subscriber) handleModelVersionReady(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelVersionReady
	if err := json.Unmarshal(env.Data, &p); err != nil {
		s.log.WarnContext(ctx, "drop unparseable ModelVersionReady payload",
			slog.String("event.id", env.ID), slog.String("error.type", "payload_decode"))
		return nil // non-retryable: a bad payload won't parse on redelivery
	}
	ref := domain.ModelRef{Name: p.GetModelName(), Version: p.GetVersion()}
	if ref.Name == "" || ref.Version == "" || p.GetArtifactPath() == "" {
		s.log.WarnContext(ctx, "drop ModelVersionReady missing name/version/artifact",
			slog.String("event.id", env.ID))
		return nil
	}

	// Remember the artifact so a later deployed/promoted (no URI) can reconcile.
	s.rememberArtifact(ref, artifactRef{uri: p.GetArtifactPath(), digest: p.GetArtifactDigest()})

	input := domain.LoadModelInput{
		Ref:            ref,
		ArtifactURI:    p.GetArtifactPath(),
		ExpectedDigest: p.GetArtifactDigest(),
		// Use the envelope id as the load idempotency key: a redelivery of the
		// SAME event carries the same id, so the domain's idempotent load short-
		// circuits. (The ProcessedStore also dedupes upstream of the handler;
		// this is defense in depth at the domain layer.)
		IdempotencyKey: env.ID,
	}
	return s.ensureLoaded(ctx, env, input, "version.ready")
}

// handleModelDeployed reacts to fp.pipelines.model.deployed → EnsureLoaded,
// reconciling against the URI remembered from version.ready (the payload has no
// URI of its own — see the package doc's artifact-uri problem).
func (s *Subscriber) handleModelDeployed(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelDeployed
	if err := json.Unmarshal(env.Data, &p); err != nil {
		s.log.WarnContext(ctx, "drop unparseable ModelDeployed payload",
			slog.String("event.id", env.ID), slog.String("error.type", "payload_decode"))
		return nil
	}
	ref := domain.ModelRef{Name: p.GetModelName(), Version: p.GetVersion()}
	return s.reconcileLoadFromMemory(ctx, env, ref, "deployed")
}

// handleModelPromoted reacts to fp.models.promoted → EnsureLoaded for the newly
// promoted version (same URI-from-memory reconcile as deployed). A promotion is
// "serve this version now"; the matching teardown of the demoted version is the
// orchestrator's ModelUndeployed, so we deliberately do NOT unload the demoted
// version here on promotion alone — a serving pod serves one version and the
// undeploy event is the authoritative teardown signal.
func (s *Subscriber) handleModelPromoted(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelPromoted
	if err := json.Unmarshal(env.Data, &p); err != nil {
		s.log.WarnContext(ctx, "drop unparseable ModelPromoted payload",
			slog.String("event.id", env.ID), slog.String("error.type", "payload_decode"))
		return nil
	}
	ref := domain.ModelRef{Name: p.GetModelName(), Version: p.GetVersion()}
	return s.reconcileLoadFromMemory(ctx, env, ref, "promoted")
}

// handleModelUndeployed reacts to fp.pipelines.model.undeployed → Unload.
func (s *Subscriber) handleModelUndeployed(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelUndeployed
	if err := json.Unmarshal(env.Data, &p); err != nil {
		s.log.WarnContext(ctx, "drop unparseable ModelUndeployed payload",
			slog.String("event.id", env.ID), slog.String("error.type", "payload_decode"))
		return nil
	}
	ref := domain.ModelRef{Name: p.GetModelName(), Version: p.GetVersion()}
	if ref.Name == "" || ref.Version == "" {
		s.log.WarnContext(ctx, "drop ModelUndeployed missing name/version",
			slog.String("event.id", env.ID))
		return nil
	}
	reason := p.GetReason()
	if reason == "" {
		reason = "undeployed"
	}
	return s.unload(ctx, env, ref, reason)
}

// handleModelArchived reacts to fp.models.archived → Unload of every resident
// version of the archived model. The archive event carries only the MODEL
// (no version), so we tear down whatever version of that model this pod holds.
func (s *Subscriber) handleModelArchived(ctx context.Context, env natsutil.EventEnvelope) error {
	var p eventsv1.ModelArchived
	if err := json.Unmarshal(env.Data, &p); err != nil {
		s.log.WarnContext(ctx, "drop unparseable ModelArchived payload",
			slog.String("event.id", env.ID), slog.String("error.type", "payload_decode"))
		return nil
	}
	name := p.GetModelName()
	if name == "" {
		s.log.WarnContext(ctx, "drop ModelArchived missing model name",
			slog.String("event.id", env.ID))
		return nil
	}

	// Find every resident version of this model and unload it. A sidecar holds at
	// most one, but ListLoadedModels keeps the adapter correct for any topology.
	statuses, _, err := s.svc.ListLoadedModels(ctx, domain.ListOptions{})
	if err != nil {
		// A transient list failure is retryable — NAK so JetStream redelivers.
		s.log.WarnContext(ctx, "ModelArchived: list loaded models failed",
			slog.String("event.id", env.ID), slog.String("model", name))
		return fmt.Errorf("%w: list loaded models", natsutil.ErrProcessingFailed)
	}
	for _, st := range statuses {
		if st.Ref.Name != name {
			continue
		}
		if _, err := s.svc.Unload(ctx, st.Ref, "archived"); err != nil {
			s.log.WarnContext(ctx, "ModelArchived: unload failed",
				slog.String("event.id", env.ID),
				slog.String("model", st.Ref.Name), slog.String("version", st.Ref.Version))
			return fmt.Errorf("%w: unload %s/%s", natsutil.ErrProcessingFailed, st.Ref.Name, st.Ref.Version)
		}
		s.forgetArtifact(st.Ref)
	}
	return nil
}

// ============================================================================
// SHARED REACTION HELPERS
// ============================================================================

// ensureLoaded calls the idempotent domain EnsureLoaded and maps its error to an
// ACK/NAK decision.
//
// ERROR CLASSIFICATION (the crux of a correct consumer):
//   - domain.ErrValidation / ErrArtifactURINotAllowed / ErrModelAlreadyExists are
//     PERMANENT: the event will fail identically on every redelivery, so retrying
//     wastes the budget and eventually DLQs a message that was never going to
//     succeed. We log and ACK (drop) these.
//   - any OTHER error (a transient fetch/engine failure surfaced as an error) is
//     TRANSIENT: NAK so JetStream redelivers and the load is retried.
//   - a load that ends in StateFailed is NOT an error return from EnsureLoaded
//     (the domain models load failure as a STATE, not an error — see LoadModel's
//     contract). We ACK it: the failure is recorded on the model; re-running the
//     same event won't change a bad artifact. Operators observe StateFailed.
func (s *Subscriber) ensureLoaded(ctx context.Context, env natsutil.EventEnvelope, input domain.LoadModelInput, reaction string) error {
	status, err := s.svc.EnsureLoaded(ctx, input)
	if err != nil {
		if isPermanentLoadError(err) {
			s.log.WarnContext(ctx, "ensureLoaded permanent error — ACK/drop",
				slog.String("event.id", env.ID), slog.String("reaction", reaction),
				slog.String("model", input.Ref.Name), slog.String("version", input.Ref.Version),
				slog.String("error.type", classifyLoadError(err)))
			return nil
		}
		s.log.WarnContext(ctx, "ensureLoaded transient error — NAK/retry",
			slog.String("event.id", env.ID), slog.String("reaction", reaction),
			slog.String("model", input.Ref.Name), slog.String("version", input.Ref.Version))
		return fmt.Errorf("%w: ensureLoaded %s/%s", natsutil.ErrProcessingFailed, input.Ref.Name, input.Ref.Version)
	}
	s.log.InfoContext(ctx, "model reconciled (loaded)",
		slog.String("event.id", env.ID), slog.String("reaction", reaction),
		slog.String("model", input.Ref.Name), slog.String("version", input.Ref.Version),
		slog.String("state", status.State.String()))
	return nil
}

// reconcileLoadFromMemory is the deployed/promoted path: it looks up the artifact
// URI remembered from version.ready and ensures the version is loaded from it. If
// no URI is known yet (deploy/promote arrived before version.ready, or for a
// model this pod doesn't serve), it ACKs and skips — we cannot and must not
// fabricate an artifact location (SSRF allow-list + supply-chain digest).
func (s *Subscriber) reconcileLoadFromMemory(ctx context.Context, env natsutil.EventEnvelope, ref domain.ModelRef, reaction string) error {
	if ref.Name == "" || ref.Version == "" {
		s.log.WarnContext(ctx, "drop "+reaction+" missing name/version",
			slog.String("event.id", env.ID))
		return nil
	}
	art, ok := s.lookupArtifact(ref)
	if !ok {
		// No remembered artifact. This is normal for a pod that does not serve
		// this model (a wildcard-stored event it has no business loading) and for
		// the race where deploy precedes version.ready. ACK and skip.
		s.log.InfoContext(ctx, reaction+": no known artifact for ref — skip",
			slog.String("event.id", env.ID),
			slog.String("model", ref.Name), slog.String("version", ref.Version))
		return nil
	}
	input := domain.LoadModelInput{
		Ref:            ref,
		ArtifactURI:    art.uri,
		ExpectedDigest: art.digest,
		IdempotencyKey: env.ID,
	}
	return s.ensureLoaded(ctx, env, input, reaction)
}

// unload calls the idempotent domain Unload. Unload never errors for an
// absent/already-unloaded model (domain contract), so any error returned here is
// unexpected and treated as transient (NAK/retry).
func (s *Subscriber) unload(ctx context.Context, env natsutil.EventEnvelope, ref domain.ModelRef, reason string) error {
	status, err := s.svc.Unload(ctx, ref, reason)
	if err != nil {
		s.log.WarnContext(ctx, "unload error — NAK/retry",
			slog.String("event.id", env.ID),
			slog.String("model", ref.Name), slog.String("version", ref.Version))
		return fmt.Errorf("%w: unload %s/%s", natsutil.ErrProcessingFailed, ref.Name, ref.Version)
	}
	s.forgetArtifact(ref)
	s.log.InfoContext(ctx, "model reconciled (unloaded)",
		slog.String("event.id", env.ID), slog.String("reason", reason),
		slog.String("model", ref.Name), slog.String("version", ref.Version),
		slog.String("state", status.State.String()))
	return nil
}

// ============================================================================
// desired-artifact map (small, guarded)
// ============================================================================

func (s *Subscriber) rememberArtifact(ref domain.ModelRef, art artifactRef) {
	s.mu.Lock()
	s.desiredArtifacts[ref] = art
	s.mu.Unlock()
}

func (s *Subscriber) lookupArtifact(ref domain.ModelRef) (artifactRef, bool) {
	s.mu.RLock()
	art, ok := s.desiredArtifacts[ref]
	s.mu.RUnlock()
	return art, ok
}

func (s *Subscriber) forgetArtifact(ref domain.ModelRef) {
	s.mu.Lock()
	delete(s.desiredArtifacts, ref)
	s.mu.Unlock()
}
