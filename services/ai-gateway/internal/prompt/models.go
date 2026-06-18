// Package prompt is the PROMPT REGISTRY domain (M7/L3) of the AI Gateway: a
// versioned, team-scoped store of prompt templates with server-side rendering.
//
// ============================================================================
// CLEAN ARCHITECTURE — THIS PACKAGE IS PURE DOMAIN (zero framework imports)
// ============================================================================
//
// This package imports ONLY the standard library. It has NO gRPC, NO pgx/SQL, NO
// proto, NO NATS. It defines:
//
//	models.go   — the Prompt entity + the Stage enum (the domain vocabulary).
//	errors.go   — the BUSINESS sentinels (ErrNotFound/ErrAlreadyExists/...).
//	parser.go   — the {{variable}} extractor + the renderer (pure functions).
//	ports.go    — the PromptRepository PORT (consumer-owned) + the IDGenerator/Clock.
//	service.go  — the PromptService: CreatePrompt/GetPrompt/ListPrompts/RenderPrompt.
//
// The Postgres adapter (internal/repository/postgres) IMPORTS this package to
// implement PromptRepository; the handler IMPORTS this package to call the service
// and map domain↔proto. The dependency arrow points INWARD — exactly the hexagonal
// discipline the Model Registry uses (which is the structural template for this
// whole package: a prompt is to the gateway what a model is to the registry).
//
// WHY A SEPARATE PACKAGE FROM internal/domain: internal/domain already holds the
// gateway's hot-path service (ChatCompletion, budgets, breakers, providers). The
// prompt registry is an orthogonal, Postgres-backed concern with its OWN ports and
// errors. Keeping it in its own package avoids bloating the gateway service's
// surface and keeps the two dependency graphs (Redis/HTTP vs Postgres) cleanly
// separable — the gateway can run with the prompt registry entirely absent.
package prompt

import "time"

// Stage is the prompt lifecycle stage — the domain's OWN enum, deliberately
// decoupled from the proto's PromptStage (the handler maps between them with an
// explicit switch, never an int cast, so the wire enum can evolve independently).
//
// The numeric values MATCH the proto (DEV=1, PRODUCTION=2, ARCHIVED=3) so the
// Postgres SMALLINT column round-trips with a trivial cast, but the mapping is
// still made explicit at the handler boundary for evolvability.
type Stage int

const (
	// StageUnspecified (0) is the zero value — never persisted (a CHECK rejects it).
	// It exists so a missing/unknown wire value has a name rather than aliasing DEV.
	StageUnspecified Stage = 0
	// StageDev (1) — the default stage a freshly-created prompt version is born into.
	// Work-in-progress; not yet the team's blessed production prompt.
	StageDev Stage = 1
	// StageProduction (2) — the blessed version GetPrompt(version=0) prefers. A team
	// promotes a version here when it's ready to be the default rendered prompt.
	StageProduction Stage = 2
	// StageArchived (3) — retired. Still readable by explicit version, but never the
	// "latest production" target.
	StageArchived Stage = 3
)

// IsValid reports whether s is a real, persistable stage (DEV/PRODUCTION/ARCHIVED).
// The service uses it to reject StageUnspecified before a write.
func (s Stage) IsValid() bool {
	return s == StageDev || s == StageProduction || s == StageArchived
}

// Prompt is the domain entity: one immutable version of a named prompt template,
// owned by a team. Every CreatePrompt cuts a NEW Prompt (a new version); a Prompt
// is never mutated in place (stage promotion would be a separate command — out of
// scope for L3's create/get/list/render surface).
//
// FIELD OWNERSHIP (the trust boundary, mirroring the Registry's Model):
//   - SERVER-authoritative (never from the client request body): ID, Version, Team,
//     Stage, Variables, CreatedAt. The service stamps these from the IDGenerator,
//     the Clock, the parsed template, and the auth claims' team.
//   - CLIENT-supplied: Name, Template, Description (validated, then stored verbatim).
//
// Variables are the DECLARED placeholders parsed out of Template at create time —
// the contract a caller must satisfy when rendering. Storing them (rather than
// re-parsing on every render) makes "what variables does this prompt need?" a cheap
// read and a stable part of the version's identity.
type Prompt struct {
	ID          string
	Name        string
	Version     int
	Stage       Stage
	Template    string
	Variables   []string
	Description string
	Team        string
	CreatedAt   time.Time
}
