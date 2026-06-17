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
	OrchestratorEndpoint string `env:"ORCHESTRATOR_ENDPOINT" default:"pipeline-orchestrator:9090"`
}
