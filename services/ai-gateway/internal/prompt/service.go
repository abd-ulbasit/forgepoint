// service.go — the PromptService: the prompt-registry business logic.
//
// ============================================================================
// THE FOUR COMMANDS/QUERIES (the L3 surface) AND THEIR RULES
// ============================================================================
//
//	CreatePrompt  — parse {{variables}} from the template, cut version 1 for a new
//	                (team,name) or N+1 for an existing name (newest wins), stage = DEV.
//	                IDEMPOTENT on idempotency_key (a retried create returns the SAME
//	                version, no duplicate — the Stripe contract).
//	GetPrompt     — by (team,name) + optional version. version>0 → that exact version.
//	                version==0 → the LATEST PRODUCTION version, or, if no version is in
//	                production, the LATEST version overall (documented fallback below).
//	GetPrompt is TEAM-SCOPED: another team's prompt is ErrNotFound, never readable.
//	ListPrompts   — team-scoped, newest-first, keyset-paginated.
//	RenderPrompt  — load the prompt (team-scoped), substitute {{var}} from the request
//	                map under the STRICT missing-var policy (missing var → error),
//	                return rendered text + the resolved version.
//
// TEAM is ALWAYS a parameter the handler derives from the auth claims — NEVER from a
// request body. This service trusts the team it is given; the handler is responsible
// for it being the verified caller team. Every repository call is scoped to that team,
// so there is no code path that reads or writes across tenants.
//
// COMMAND SHAPE (CreatePrompt follows this skeleton, mirroring the Registry):
//  1. VALIDATE input (cheap, before any I/O).
//  2. IDEMPOTENCY: if a key is set, look it up; on hit RETURN the original WITHOUT
//     re-writing (the dedup contract).
//  3. STAMP server-authoritative fields (id/team/stage/variables/created_at).
//  4. WRITE via the repository (atomic next-version assignment), translating the
//     storage ErrWriteConflict → a RETRY (version race) or the business error.
package prompt

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PromptService is the prompt-registry use-case interface the handler depends on.
// Returning the interface (not the concrete type) from NewPromptService keeps the
// handler programming-to-an-abstraction; tests can substitute a stub service if
// they ever need to, though the service's OWN tests use a real service + fake repo.
type PromptService interface {
	// CreatePrompt cuts a new version of (team, name). See the file header for rules.
	CreatePrompt(ctx context.Context, team string, in CreatePromptInput) (Prompt, error)
	// GetPrompt resolves (team, name, version) per the version-resolution rule.
	GetPrompt(ctx context.Context, team, name string, version int) (Prompt, error)
	// ListPrompts returns a team-scoped, newest-first page.
	ListPrompts(ctx context.Context, team string, in ListPromptsInput) (Page, error)
	// RenderPrompt loads the prompt and substitutes the supplied variables.
	RenderPrompt(ctx context.Context, team string, in RenderPromptInput) (RenderResult, error)
}

// CreatePromptInput is the client-supplied half of a create. Team is intentionally
// ABSENT — it is a separate parameter the handler sets from auth claims, so a request
// can never carry a spoofed team (the same mass-assignment guard the Registry uses).
type CreatePromptInput struct {
	Name           string
	Template       string
	Description    string
	IdempotencyKey string
}

// ListPromptsInput carries pagination. Team is again a separate parameter, not here.
type ListPromptsInput struct {
	PageSize  int
	PageToken string
}

// RenderPromptInput names the prompt to render and supplies the variable values.
type RenderPromptInput struct {
	Name      string
	Version   int // 0 = resolve per the GetPrompt rule (latest production, else latest)
	Variables map[string]string
}

// RenderResult is RenderPrompt's output: the substituted text and the version that
// was actually rendered (so the caller learns WHICH version produced this text —
// important when version=0 resolved to "latest production").
type RenderResult struct {
	Rendered string
	Version  int
}

// Page is a paginated ListPrompts result: the items plus the next-page cursor.
type Page struct {
	Items     []Prompt
	NextToken string
}

// Pagination bounds (mirroring the Registry's clamp). DefaultPageSize applies when a
// caller asks for 0; MaxPageSize caps an over-large request (DoS guard).
const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// maxVersionRaceRetries bounds the create's optimistic version-assignment retries.
// Under concurrency two creates can compute the same "next" version; the loser hits
// ErrWriteConflict and retries with a freshly recomputed next version. A small bound
// is plenty (real contention on ONE (team,name) is low) and prevents an unbounded
// spin if something is pathologically wrong.
const maxVersionRaceRetries = 5

// promptService is the production PromptService. Unexported: callers receive it only
// through the PromptService interface returned by NewPromptService.
type promptService struct {
	repo  PromptRepository
	ids   IDGenerator
	clock Clock
}

// Compile-time proof *promptService satisfies the interface — interface skew fails
// the BUILD here, the cheapest place to catch it.
var _ PromptService = (*promptService)(nil)

// NewPromptService wires the ports and returns the PromptService interface. main.go
// passes the Postgres repo + the production UUID generator + the real clock; tests
// pass a fake repo + a deterministic id generator + a fixed clock.
func NewPromptService(repo PromptRepository, ids IDGenerator, clock Clock) PromptService {
	return &promptService{repo: repo, ids: ids, clock: clock}
}

// ============================================================================
// CreatePrompt
// ============================================================================
//
// THE VERSIONING RULE: a brand-new (team,name) gets version 1; an existing name gets
// MAX(version)+1. The repository computes that atomically inside a tx (no read-modify-
// write race in the service). The service just hands over a Prompt with Version unset.
//
// THE IDEMPOTENCY RULE: with an idempotency_key, a retried CreatePrompt must return
// the ORIGINAL version, not cut a new one. We check the key FIRST (LookupByIdempotency
// Key); a hit returns that prompt with no write. The DB's unique (team,idempotency_key)
// index is the backstop if two retries race past the check.
func (s *promptService) CreatePrompt(ctx context.Context, team string, in CreatePromptInput) (Prompt, error) {
	// 1. VALIDATE (cheap, pre-I/O). Team must be present (defense-in-depth: the
	//    handler guarantees it, but we refuse to write a team-less row).
	if team == "" {
		return Prompt{}, fmt.Errorf("%w: team is required", ErrValidation)
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Prompt{}, fmt.Errorf("%w: prompt name is required", ErrValidation)
	}
	if strings.TrimSpace(in.Template) == "" {
		return Prompt{}, fmt.Errorf("%w: prompt template is required", ErrValidation)
	}

	// 2. IDEMPOTENCY: a retried create with the same key returns the original version.
	if in.IdempotencyKey != "" {
		existing, err := s.repo.LookupByIdempotencyKey(ctx, team, in.IdempotencyKey)
		if err == nil {
			return existing, nil // same key → same prompt, no new version cut
		}
		if !errors.Is(err, ErrRecordNotFound) {
			// A real storage error (not "unseen key") — surface it.
			return Prompt{}, fmt.Errorf("prompt: idempotency lookup: %w", err)
		}
		// ErrRecordNotFound ⇒ first time we've seen this key; fall through to create.
	}

	// 3. STAMP server-authoritative fields. Variables are PARSED from the template (the
	//    declared {{placeholders}}); Stage is DEV (a new version is always born dev);
	//    Version is left 0 for the repository to assign atomically.
	base := Prompt{
		Name:        name,
		Stage:       StageDev,
		Template:    in.Template,
		Variables:   ExtractVariables(in.Template),
		Description: in.Description,
		Team:        team,
		CreatedAt:   s.clock.Now().UTC(),
	}

	// 4. WRITE with an optimistic version-assignment RETRY LOOP. CreateNextVersion
	//    computes next=MAX+1 and inserts in one tx; if a concurrent create won the
	//    same version (ErrWriteConflict on the (team,name,version) index), we mint a
	//    fresh id and retry — the loser recomputes the next version. A same-KEY race
	//    that slipped past step 2 surfaces here too; on that ErrWriteConflict we
	//    re-check the idempotency key and return the now-present original.
	var lastErr error
	for attempt := 0; attempt < maxVersionRaceRetries; attempt++ {
		p := base
		p.ID = s.ids.NewID() // fresh server-minted id per attempt
		created, err := s.repo.CreateNextVersion(ctx, p, in.IdempotencyKey)
		if err == nil {
			return created, nil
		}
		if !errors.Is(err, ErrWriteConflict) {
			return Prompt{}, fmt.Errorf("prompt: create: %w", err)
		}
		lastErr = err
		// The conflict might be the idempotency-key index (a racing retry committed the
		// SAME key first). If so, the original is now present — return it, honoring dedup.
		if in.IdempotencyKey != "" {
			if existing, lookErr := s.repo.LookupByIdempotencyKey(ctx, team, in.IdempotencyKey); lookErr == nil {
				return existing, nil
			}
		}
		// Otherwise it was a version race; loop to recompute MAX+1 and retry.
	}
	// Exhausted retries under persistent contention — surface as a business conflict.
	return Prompt{}, fmt.Errorf("%w: version assignment lost too many races: %v", ErrAlreadyExists, lastErr)
}

// ============================================================================
// GetPrompt — the VERSION-RESOLUTION RULE
// ============================================================================
//
// version > 0  → return that EXACT (team, name, version). Missing → ErrNotFound.
// version == 0 → "give me the current prompt", resolved as:
//
//	(a) the LATEST PRODUCTION version, if any version of this name is in PRODUCTION;
//	(b) ELSE the LATEST version overall (highest version number, any stage).
//
// WHY this fallback (a→b): a team that has PROMOTED a version clearly wants that
// blessed version to be the default — production is the "stable channel". But a team
// that has only ever created DEV versions (never promoted) still expects GetPrompt to
// return SOMETHING (their newest draft), not ErrNotFound. So we prefer production and
// gracefully fall back to latest. This mirrors the Model Registry's "production
// pointer, else latest" resolution and matches the proto comment on GetPromptRequest
// ("0 = latest prod"). The rule is TOTAL: any existing (team,name) resolves to a row.
//
// TEAM ISOLATION: every branch calls a team-scoped repo method. A (team,name) the
// caller doesn't own simply isn't found → ErrNotFound, with no signal that it exists
// for another team.
func (s *promptService) GetPrompt(ctx context.Context, team, name string, version int) (Prompt, error) {
	if team == "" {
		return Prompt{}, fmt.Errorf("%w: team is required", ErrValidation)
	}
	if strings.TrimSpace(name) == "" {
		return Prompt{}, fmt.Errorf("%w: prompt name is required", ErrValidation)
	}
	if version < 0 {
		return Prompt{}, fmt.Errorf("%w: version must not be negative", ErrValidation)
	}

	// Pinned version → exact lookup.
	if version > 0 {
		p, err := s.repo.GetVersion(ctx, team, name, version)
		return s.mapGetResult(p, err)
	}

	// version == 0 → prefer latest PRODUCTION.
	p, err := s.repo.GetLatestProduction(ctx, team, name)
	if err == nil {
		return p, nil
	}
	if !errors.Is(err, ErrRecordNotFound) {
		return Prompt{}, fmt.Errorf("prompt: get latest production: %w", err)
	}
	// No production version → fall back to the latest version overall.
	p, err = s.repo.GetLatest(ctx, team, name)
	return s.mapGetResult(p, err)
}

// mapGetResult translates a repository result for a Get: a storage ErrRecordNotFound
// becomes the business ErrNotFound (team-scoped — see the sentinel doc); any other
// error is wrapped; success passes through. Factored out because three Get branches
// share it.
func (s *promptService) mapGetResult(p Prompt, err error) (Prompt, error) {
	if err == nil {
		return p, nil
	}
	if errors.Is(err, ErrRecordNotFound) {
		return Prompt{}, ErrNotFound
	}
	return Prompt{}, fmt.Errorf("prompt: get: %w", err)
}

// ============================================================================
// ListPrompts — team-scoped, newest-first, keyset-paginated
// ============================================================================
func (s *promptService) ListPrompts(ctx context.Context, team string, in ListPromptsInput) (Page, error) {
	if team == "" {
		return Page{}, fmt.Errorf("%w: team is required", ErrValidation)
	}
	opts := ListOptions{PageSize: clampPageSize(in.PageSize), PageToken: in.PageToken}
	items, next, err := s.repo.List(ctx, team, opts)
	if err != nil {
		return Page{}, fmt.Errorf("prompt: list: %w", err)
	}
	return Page{Items: items, NextToken: next}, nil
}

// ============================================================================
// RenderPrompt — load (team-scoped) + substitute under the STRICT missing-var policy
// ============================================================================
//
// We resolve the prompt with the SAME version rule as GetPrompt (so version=0 renders
// the current/production prompt), then call the pure Render with MissingVarError: a
// declared {{variable}} with no supplied value is an InvalidArgument, NOT a silently
// blank slot. See parser.go's MissingVarError doc for why strict is right for a
// billed LLM prompt. The resolved version is returned so the caller knows which
// version produced the text.
func (s *promptService) RenderPrompt(ctx context.Context, team string, in RenderPromptInput) (RenderResult, error) {
	// GetPrompt already validates team/name/version and applies the resolution rule
	// and team scoping — reuse it so render and get can never diverge on "which
	// version" or "is this my team's prompt".
	p, err := s.GetPrompt(ctx, team, in.Name, in.Version)
	if err != nil {
		return RenderResult{}, err
	}
	rendered, err := Render(p.Template, in.Variables, MissingVarError)
	if err != nil {
		// Render returns ErrValidation (missing var) — pass it through; the handler
		// maps it to InvalidArgument.
		return RenderResult{}, err
	}
	return RenderResult{Rendered: rendered, Version: p.Version}, nil
}

// clampPageSize coerces a requested page size into [1, MaxPageSize], defaulting 0 to
// DefaultPageSize. The DoS guard + sensible-default in one place, identical to the
// Registry's clamp.
func clampPageSize(n int) int {
	switch {
	case n <= 0:
		return DefaultPageSize
	case n > MaxPageSize:
		return MaxPageSize
	default:
		return n
	}
}

// missingVarError builds the ErrValidation a strict Render returns for an unsatisfied
// variable. Lives here (not in parser.go's hot path) so the parser stays a tiny pure
// file; it wraps ErrValidation so the handler maps it to InvalidArgument via errors.Is.
func missingVarError(name string) error {
	return fmt.Errorf("%w: missing value for variable %q", ErrValidation, name)
}

// ============================================================================
// PRODUCTION PORT IMPLEMENTATIONS — UUID id generator + wall clock
// ============================================================================

// UUIDGenerator is the production IDGenerator: a hand-rolled RFC-4122 v4 UUID from
// crypto/rand. We hand-roll (rather than import github.com/google/uuid) for the SAME
// reason the Registry domain does: the uuid package transitively imports
// database/sql/driver (it implements driver.Valuer), which would leak a storage-
// flavored import into this PURE domain package. Six lines of stdlib keep the domain's
// transitive graph free of any driver vocabulary.
type UUIDGenerator struct{}

// NewID returns a fresh v4 UUID string (8-4-4-4-12 hex). It reads 16 CSPRNG bytes and
// sets the version/variant bits. A crypto/rand failure means the OS entropy source is
// broken — there is no safe way to continue minting ids, so we panic (a process that
// can't generate unguessable ids must not serve).
func (UUIDGenerator) NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("prompt: crypto/rand failed generating an id: " + err.Error())
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

// RealClock is the production Clock: it returns the wall-clock time. Tests inject a
// fixed clock instead so CreatedAt is deterministic. main.go wires RealClock{}.
type RealClock struct{}

// Now returns the current wall time (the service normalizes the result to UTC).
func (RealClock) Now() time.Time { return time.Now() }
