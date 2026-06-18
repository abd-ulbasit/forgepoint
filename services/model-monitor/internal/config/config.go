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
}
