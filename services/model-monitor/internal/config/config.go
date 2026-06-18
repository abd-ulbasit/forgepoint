// Package config holds the Model Monitor service's configuration struct.
//
// WHY a service-specific struct that EMBEDS pkg/config.BaseConfig: every
// Forgepoint service needs the same baseline knobs (ports, log level, OTel
// endpoint, NATS url, database url). Embedding BaseConfig inherits those field
// definitions + their env tags, so we only declare the Model-Monitor-SPECIFIC
// settings here. config.Load[MonitorConfig]("FP") reads FP_* env vars into the
// struct via reflection (see pkg/config), failing fast on a missing required field
// — the correct 12-factor behavior (config from the environment, fail-fast at boot).
package config

import (
	"time"

	"github.com/abd-ulbasit/forgepoint/pkg/config"
)

// MonitorConfig is the Model Monitor service configuration.
//
// The embedded BaseConfig supplies: Port (health HTTP), GRPCPort, LogLevel,
// OTelEndpoint, NATSUrl, DatabaseURL.
type MonitorConfig struct {
	config.BaseConfig

	// JWTSecret is the HMAC-SHA256 signing key the shared auth interceptor uses to
	// VERIFY incoming JWTs (it does NOT mint them — only the auth service mints).
	//
	// WHY required:"true": the gRPC server wires grpcutil.WithAuthValidator with a
	// JWT validator built from this secret; without it the validator can't be
	// constructed and authn would be impossible — so the service refuses to start
	// (fail-fast on security config is correct). The secret MUST be the SAME value
	// the auth service signs with and at least 32 bytes (256-bit floor matching
	// SHA-256's width); pkg/auth.NewJWTValidator rejects a shorter key at startup
	// (ErrWeakSecret-equivalent), turning a misconfig into a loud boot failure
	// rather than a fleet that accepts trivially-forgeable tokens. This is design
	// D2 (LOCAL in-process JWT verify — zero network on the hot path; see
	// pkg/auth/validator.go). K8s: mounted from a Secret (FP_JWT_SECRET), NEVER a
	// ConfigMap — secrets are access-controlled, ConfigMaps are plaintext in etcd.
	JWTSecret string `env:"JWT_SECRET" required:"true"`

	// RedisURL is the connection string for the live sliding-window store. WHY
	// Redis (not Postgres) for the live window: the window is a hot, frequently-
	// mutated summary updated on EVERY inference event — Redis's in-memory
	// structures + TTLs fit that write rate and the natural "expire old windows"
	// semantics far better than a relational table. Postgres holds the durable
	// drift_reports; Redis holds the ephemeral in-flight windows. Optional at boot
	// (the scaffold doesn't connect yet) — required once the Redis adapter lands.
	RedisURL string `env:"REDIS_URL" default:"redis://localhost:6379"`

	// RetrainCooldown is the minimum gap between auto-retrains for one model — the
	// ANTI-STORM interval of the closed loop. A sustained drift breaches CRITICAL on
	// every window close; without this gap a busy model would fire a retrain every
	// few seconds, flooding the Pipeline Orchestrator and thrashing the served
	// model. Default 1h: long enough for one retrain to finish → canary → promote
	// (which resets the baseline) before another is even considered. Operators tune
	// it per model criticality. See domain.DecideRetrain's cooldown gate.
	RetrainCooldown time.Duration `env:"RETRAIN_COOLDOWN" default:"1h"`

	// OrchestratorEndpoint is the FIXED gRPC address of the Pipeline Orchestrator
	// the closed loop calls to trigger a retrain. WHY a fixed, configured endpoint
	// (and not a target derived from event data): a poisoned inference/drift payload
	// must NOT be able to redirect the retrain call to an attacker-chosen host
	// (an SSRF-class footgun). The loop's target is operator config, full stop.
	// Optional at boot; required once the orchestrator client adapter lands.
	//
	// DEFAULT = the Helm chart's Service FQDN. The chart names the Service
	// "fp-pipeline-orchestrator" (release-prefixed) in namespace fp-system, so the
	// bare "pipeline-orchestrator:9090" would NOT resolve in-cluster — DNS has no
	// such Service. We default to the fully-qualified
	// fp-pipeline-orchestrator.fp-system.svc.cluster.local:9090 so the closed loop
	// works out of the box on the chart's wiring; still env-overridable for other
	// topologies (a different namespace/release name, or a local/test target).
	OrchestratorEndpoint string `env:"ORCHESTRATOR_ENDPOINT" default:"fp-pipeline-orchestrator.fp-system.svc.cluster.local:9090"`

	// ========================================================================
	// M7/L4 — LLM QUALITY EVALUATION + QUALITY DRIFT
	// ========================================================================
	//
	// L4 adds a SECOND data-plane pipeline: an AI-completion consumer
	// (fp.ai.completion.served) that SAMPLES completions, sends them to a local
	// LLM-as-judge (Ollama), records the scores, and raises a QUALITY-drift report
	// into the EXISTING alert + retrain loop. The whole feature SELF-GATES on
	// EvalEnabled: when off (or OllamaURL empty), the consumer is never started and
	// model-monitor runs its tabular data-drift function exactly as before.

	// EvalEnabled is the kill-switch for the L4 quality-eval pipeline. DEFAULT false:
	// quality eval depends on a local judge model being served by Ollama, so it is
	// opt-in (a deployment without the judge must not start a consumer that judges
	// every sampled completion against an absent backend). When false the AI consumer
	// is a NO-OP (not started) and the rest of model-monitor is unaffected.
	EvalEnabled bool `env:"EVAL_ENABLED" default:"false"`

	// OllamaURL is the in-cluster Ollama serving endpoint the LLM-as-judge calls — the
	// SAME endpoint the ai-gateway serves from. Defaulted to the fp-ml Service FQDN
	// (the task's specified endpoint). Only used when EvalEnabled.
	OllamaURL string `env:"OLLAMA_URL" default:"http://ollama.fp-ml.svc.cluster.local:11434"`

	// EvalJudgeModel is the local model that grades completions. A SMALL model keeps
	// judge latency/cost low; the robust parser absorbs its messy output. Overridable
	// per deployment (a larger judge → cleaner JSON, higher cost).
	EvalJudgeModel string `env:"EVAL_JUDGE_MODEL" default:"smollm2:135m"`

	// EvalSampleRate is the 1-in-N sampling rate that bounds the judge's Ollama load.
	// DEFAULT 1 = judge EVERY monitored completion (the homelab default — volume is
	// low). Set to N>1 to judge ~1-in-N when traffic grows. The CountingSampler makes
	// this deterministic (every Nth monitored event), not random.
	EvalSampleRate int `env:"EVAL_SAMPLE_RATE" default:"1"`

	// EvalWindow is how many of the most-recent SCORED evals to average for the rolling
	// quality drift check, per (team, model). Too small = noisy; too large = slow to
	// react. A few dozen is a reasonable homelab default. 0 ⇒ quality-DRIFT detection
	// OFF (the consumer still judges + records evals as an eval log, but never alerts).
	EvalWindow int `env:"EVAL_WINDOW" default:"30"`

	// EvalMinSamples is the floor below which we don't judge drift (averaging a couple
	// of scores is noise, not signal — mirrors the tabular MinSamples discipline).
	EvalMinSamples int `env:"EVAL_MIN_SAMPLES" default:"10"`

	// EvalFloorScore is the absolute quality floor on the 1–5 scale: if the rolling
	// average drops BELOW this, that is drift regardless of any baseline. Default 3.0
	// (a model averaging below "3/5" is producing poor answers). 0 disables the floor
	// rule (baseline-drop only).
	EvalFloorScore float64 `env:"EVAL_FLOOR_SCORE" default:"3.0"`

	// EvalBaselineScore is the expected/healthy average (the LLM's "known good"
	// quality). With EvalBaselineDrop it triggers a RELATIVE-regression drift even when
	// still above the floor. 0 disables the baseline rule (floor-only). Default 0
	// (floor-only out of the box; an operator sets a baseline once they know the model's
	// healthy average).
	EvalBaselineScore float64 `env:"EVAL_BASELINE_SCORE" default:"0"`

	// EvalBaselineDrop is how far BELOW EvalBaselineScore the rolling average must fall
	// to count as drift (points on the 1–5 scale). Only used when EvalBaselineScore>0.
	EvalBaselineDrop float64 `env:"EVAL_BASELINE_DROP" default:"0.5"`

	// EvalWarnDrop / EvalCriticalDrop map the quality DROP (how far the average fell
	// below the floor/baseline reference) onto the severity ladder — exactly like a
	// data-drift ThresholdConfig's warn/critical. WarnDrop <= CriticalDrop. CRITICAL is
	// the rung that ARMS auto-retrain (if the monitor has auto_retrain + a pipeline).
	// Defaults: warn at a 0.5-point drop, critical at a 1.0-point drop below reference.
	EvalWarnDrop     float64 `env:"EVAL_WARN_DROP" default:"0.5"`
	EvalCriticalDrop float64 `env:"EVAL_CRITICAL_DROP" default:"1.0"`
}
