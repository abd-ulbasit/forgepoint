// service_test.go — UNIT tests for the PromptService (NO database, NO testcontainers).
//
// ============================================================================
// THE FAKE REPOSITORY (in-memory PromptRepository)
// ============================================================================
//
// These tests inject a fakeRepo that faithfully models the Postgres adapter's
// OBSERVABLE semantics WITHOUT a database:
//   - per-(team,name) monotonic version (next = max+1),
//   - the (team,name,version) and (team,idempotency_key) uniqueness,
//   - TEAM SCOPING (a method for team A never returns team B's rows),
//   - latest / latest-production resolution.
//
// This lets us assert the SERVICE's logic — versioning, idempotency, the version-
// resolution rule, render, and team isolation — in fast, deterministic unit tests.
// The real Postgres adapter is exercised separately by integration tests; the port
// boundary is exactly what makes this substitution possible.
package prompt

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// Fakes: repository, id generator, clock
// ----------------------------------------------------------------------------

// fakeRepo is an in-memory PromptRepository. It stores rows in a slice and enforces
// the same uniqueness + team-scoping the SQL does. A mutex makes it safe under -race
// even though the unit tests are single-goroutine (cheap insurance).
type fakeRepo struct {
	mu   sync.Mutex
	rows []Prompt
	// keyByID maps a stored prompt's ID → its idempotency key. We keep the key in a
	// SIDE map (not on the Prompt struct) because Prompt is the pure domain entity and
	// deliberately has NO idempotency field — the key flows as a separate argument, so
	// the fake models the storage column without polluting the domain type.
	keyByID map[string]string
	// forceConflict, when > 0, makes the next N CreateNextVersion calls return
	// ErrWriteConflict regardless — used to exercise the service's retry loop.
	forceConflict int
}

func newFakeRepo() *fakeRepo { return &fakeRepo{keyByID: map[string]string{}} }

// maxVersion returns the highest version for (team,name), or 0 if none. The atomic
// "next = max+1" the adapter computes in a tx.
func (r *fakeRepo) maxVersion(team, name string) int {
	max := 0
	for _, p := range r.rows {
		if p.Team == team && p.Name == name && p.Version > max {
			max = p.Version
		}
	}
	return max
}

func (r *fakeRepo) CreateNextVersion(_ context.Context, p Prompt, key string) (Prompt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.forceConflict > 0 {
		r.forceConflict--
		return Prompt{}, ErrWriteConflict
	}

	// (team, idempotency_key) uniqueness: a duplicate key for a DIFFERENT row is a
	// conflict (the service checks the key first, so this guards the race path).
	if key != "" {
		for _, existing := range r.rows {
			if existing.Team == p.Team && r.keyByID[existing.ID] == key {
				return Prompt{}, ErrWriteConflict
			}
		}
	}

	p.Version = r.maxVersion(p.Team, p.Name) + 1
	// (team,name,version) uniqueness — by construction max+1 is unique, but model the
	// index anyway for completeness.
	for _, existing := range r.rows {
		if existing.Team == p.Team && existing.Name == p.Name && existing.Version == p.Version {
			return Prompt{}, ErrWriteConflict
		}
	}
	r.rows = append(r.rows, p)
	if key != "" {
		r.keyByID[p.ID] = key
	}
	return p, nil
}

func (r *fakeRepo) LookupByIdempotencyKey(_ context.Context, team, key string) (Prompt, error) {
	if key == "" {
		return Prompt{}, ErrRecordNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.rows {
		if p.Team == team && r.keyByID[p.ID] == key {
			return p, nil
		}
	}
	return Prompt{}, ErrRecordNotFound
}

func (r *fakeRepo) GetVersion(_ context.Context, team, name string, version int) (Prompt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.rows {
		if p.Team == team && p.Name == name && p.Version == version {
			return p, nil
		}
	}
	return Prompt{}, ErrRecordNotFound
}

func (r *fakeRepo) GetLatest(_ context.Context, team, name string) (Prompt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var best Prompt
	found := false
	for _, p := range r.rows {
		if p.Team == team && p.Name == name && (!found || p.Version > best.Version) {
			best, found = p, true
		}
	}
	if !found {
		return Prompt{}, ErrRecordNotFound
	}
	return best, nil
}

func (r *fakeRepo) GetLatestProduction(_ context.Context, team, name string) (Prompt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var best Prompt
	found := false
	for _, p := range r.rows {
		if p.Team == team && p.Name == name && p.Stage == StageProduction && (!found || p.Version > best.Version) {
			best, found = p, true
		}
	}
	if !found {
		return Prompt{}, ErrRecordNotFound
	}
	return best, nil
}

func (r *fakeRepo) List(_ context.Context, team string, opts ListOptions) ([]Prompt, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = opts // pagination is exercised by the adapter's tests; the fake returns all team rows
	var out []Prompt
	for _, p := range r.rows {
		if p.Team == team { // TEAM SCOPING — never another team's rows
			out = append(out, p)
		}
	}
	return out, "", nil
}

// seqIDGen is a deterministic IDGenerator: prompt-1, prompt-2, ... so tests can
// assert exact ids and the version-race loop mints a fresh id per attempt.
type seqIDGen struct {
	mu sync.Mutex
	n  int
}

func (g *seqIDGen) NewID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return "prompt-" + itoa(g.n)
}

// fixedClock returns a pinned time so CreatedAt is deterministic.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func newService(repo PromptRepository) PromptService {
	return NewPromptService(repo, &seqIDGen{}, fixedClock{t: time.Unix(1_700_000_000, 0).UTC()})
}

// ----------------------------------------------------------------------------
// VERSIONING: version increments per (team, name); independent per name and team
// ----------------------------------------------------------------------------

func TestCreatePrompt_VersionIncrementsPerName(t *testing.T) {
	svc := newService(newFakeRepo())
	ctx := context.Background()

	p1, err := svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "summarize", Template: "v {{x}}"})
	if err != nil {
		t.Fatalf("create 1: %v", err)
	}
	if p1.Version != 1 {
		t.Fatalf("first version = %d, want 1", p1.Version)
	}
	if p1.Stage != StageDev {
		t.Fatalf("new prompt stage = %v, want DEV", p1.Stage)
	}

	p2, err := svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "summarize", Template: "v2 {{x}}"})
	if err != nil {
		t.Fatalf("create 2: %v", err)
	}
	if p2.Version != 2 {
		t.Fatalf("second version = %d, want 2", p2.Version)
	}

	// A DIFFERENT name for the same team starts back at version 1.
	other, err := svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "classify", Template: "c"})
	if err != nil {
		t.Fatalf("create other: %v", err)
	}
	if other.Version != 1 {
		t.Fatalf("new name version = %d, want 1", other.Version)
	}
}

func TestCreatePrompt_VersionIsPerTeam(t *testing.T) {
	// The SAME name in two teams versions INDEPENDENTLY — team is part of the key.
	svc := newService(newFakeRepo())
	ctx := context.Background()

	a1, _ := svc.CreatePrompt(ctx, "team-a", CreatePromptInput{Name: "shared", Template: "a"})
	a2, _ := svc.CreatePrompt(ctx, "team-a", CreatePromptInput{Name: "shared", Template: "a2"})
	b1, _ := svc.CreatePrompt(ctx, "team-b", CreatePromptInput{Name: "shared", Template: "b"})

	if a1.Version != 1 || a2.Version != 2 {
		t.Fatalf("team-a versions = %d,%d; want 1,2", a1.Version, a2.Version)
	}
	if b1.Version != 1 {
		t.Fatalf("team-b first version = %d; want 1 (independent of team-a)", b1.Version)
	}
}

func TestCreatePrompt_ParsesVariablesFromTemplate(t *testing.T) {
	svc := newService(newFakeRepo())
	p, err := svc.CreatePrompt(context.Background(), "acme", CreatePromptInput{
		Name:     "greet",
		Template: "Hi {{name}}, your role is {{role}} ({{name}})",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	want := []string{"name", "role"}
	if len(p.Variables) != len(want) || p.Variables[0] != "name" || p.Variables[1] != "role" {
		t.Fatalf("parsed variables = %v, want %v", p.Variables, want)
	}
}

func TestCreatePrompt_ValidatesInput(t *testing.T) {
	svc := newService(newFakeRepo())
	ctx := context.Background()
	if _, err := svc.CreatePrompt(ctx, "", CreatePromptInput{Name: "x", Template: "t"}); !errors.Is(err, ErrValidation) {
		t.Fatalf("empty team err = %v, want ErrValidation", err)
	}
	if _, err := svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "", Template: "t"}); !errors.Is(err, ErrValidation) {
		t.Fatalf("empty name err = %v, want ErrValidation", err)
	}
	if _, err := svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "x", Template: "   "}); !errors.Is(err, ErrValidation) {
		t.Fatalf("blank template err = %v, want ErrValidation", err)
	}
}

// ----------------------------------------------------------------------------
// IDEMPOTENCY: a retried create with the same key returns the SAME version
// ----------------------------------------------------------------------------

func TestCreatePrompt_IdempotentOnKey(t *testing.T) {
	svc := newService(newFakeRepo())
	ctx := context.Background()

	first, err := svc.CreatePrompt(ctx, "acme", CreatePromptInput{
		Name: "p", Template: "t {{x}}", IdempotencyKey: "key-123",
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// SAME key → SAME prompt (same id, same version), NO new version cut.
	retry, err := svc.CreatePrompt(ctx, "acme", CreatePromptInput{
		Name: "p", Template: "t {{x}}", IdempotencyKey: "key-123",
	})
	if err != nil {
		t.Fatalf("retry create: %v", err)
	}
	if retry.ID != first.ID || retry.Version != first.Version {
		t.Fatalf("idempotent retry returned id=%s v=%d; want id=%s v=%d",
			retry.ID, retry.Version, first.ID, first.Version)
	}
	if retry.Version != 1 {
		t.Fatalf("idempotent retry version = %d, want 1 (no duplicate version)", retry.Version)
	}

	// A DIFFERENT key DOES cut a new version.
	third, err := svc.CreatePrompt(ctx, "acme", CreatePromptInput{
		Name: "p", Template: "t {{x}}", IdempotencyKey: "key-456",
	})
	if err != nil {
		t.Fatalf("third create: %v", err)
	}
	if third.Version != 2 {
		t.Fatalf("new-key version = %d, want 2", third.Version)
	}
}

func TestCreatePrompt_RetriesVersionRace(t *testing.T) {
	// The service must transparently retry a transient version-assignment conflict and
	// still succeed (the loser recomputes max+1). We force the first 2 inserts to
	// conflict; the 3rd attempt succeeds.
	repo := newFakeRepo()
	repo.forceConflict = 2
	svc := newService(repo)

	p, err := svc.CreatePrompt(context.Background(), "acme", CreatePromptInput{Name: "p", Template: "t"})
	if err != nil {
		t.Fatalf("create with forced conflicts: %v", err)
	}
	if p.Version != 1 {
		t.Fatalf("version after retries = %d, want 1", p.Version)
	}
}

// ----------------------------------------------------------------------------
// GET version-resolution rule: pinned, latest-production, latest-fallback
// ----------------------------------------------------------------------------

func TestGetPrompt_PinnedVersion(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo)
	ctx := context.Background()
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "p", Template: "v1"})
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "p", Template: "v2"})

	got, err := svc.GetPrompt(ctx, "acme", "p", 1)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if got.Version != 1 || got.Template != "v1" {
		t.Fatalf("get v1 = v%d %q, want v1 'v1'", got.Version, got.Template)
	}
}

func TestGetPrompt_Version0_PrefersProductionElseLatest(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo)
	ctx := context.Background()
	// Three DEV versions; no production yet → version 0 resolves to LATEST (v3).
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "p", Template: "v1"})
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "p", Template: "v2"})
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "p", Template: "v3"})

	got, err := svc.GetPrompt(ctx, "acme", "p", 0)
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if got.Version != 3 {
		t.Fatalf("version-0 (no prod) = v%d, want latest v3", got.Version)
	}

	// Promote v2 to production directly in the repo (a promote command is out of L3
	// scope; we model the stored state). Now version 0 must prefer the PRODUCTION v2
	// over the newer DEV v3.
	repo.mu.Lock()
	for i := range repo.rows {
		if repo.rows[i].Name == "p" && repo.rows[i].Version == 2 {
			repo.rows[i].Stage = StageProduction
		}
	}
	repo.mu.Unlock()

	got, err = svc.GetPrompt(ctx, "acme", "p", 0)
	if err != nil {
		t.Fatalf("get production: %v", err)
	}
	if got.Version != 2 || got.Stage != StageProduction {
		t.Fatalf("version-0 (with prod) = v%d stage %v, want production v2", got.Version, got.Stage)
	}
}

func TestGetPrompt_NotFound(t *testing.T) {
	svc := newService(newFakeRepo())
	if _, err := svc.GetPrompt(context.Background(), "acme", "nope", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing err = %v, want ErrNotFound", err)
	}
}

// ----------------------------------------------------------------------------
// RENDER: substitution + missing-var policy
// ----------------------------------------------------------------------------

func TestRenderPrompt_SubstitutesVariables(t *testing.T) {
	svc := newService(newFakeRepo())
	ctx := context.Background()
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{
		Name: "greet", Template: "Hello {{name}}, welcome to {{place}}",
	})

	res, err := svc.RenderPrompt(ctx, "acme", RenderPromptInput{
		Name:      "greet",
		Variables: map[string]string{"name": "Sam", "place": "Forgepoint"},
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if res.Rendered != "Hello Sam, welcome to Forgepoint" {
		t.Fatalf("rendered = %q", res.Rendered)
	}
	if res.Version != 1 {
		t.Fatalf("rendered version = %d, want 1", res.Version)
	}
}

func TestRenderPrompt_MissingVariableIsValidationError(t *testing.T) {
	svc := newService(newFakeRepo())
	ctx := context.Background()
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{
		Name: "greet", Template: "Hello {{name}} from {{place}}",
	})

	_, err := svc.RenderPrompt(ctx, "acme", RenderPromptInput{
		Name:      "greet",
		Variables: map[string]string{"name": "Sam"}, // missing "place"
	})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("render with missing var err = %v, want ErrValidation", err)
	}
}

func TestRenderPrompt_ResolvesProductionVersion(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo)
	ctx := context.Background()
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "p", Template: "v1 {{x}}"})
	_, _ = svc.CreatePrompt(ctx, "acme", CreatePromptInput{Name: "p", Template: "v2 {{x}}"})
	// Promote v1 to production; v2 stays DEV. Render(version=0) must use the PRODUCTION
	// v1's template, and report version 1.
	repo.mu.Lock()
	for i := range repo.rows {
		if repo.rows[i].Version == 1 {
			repo.rows[i].Stage = StageProduction
		}
	}
	repo.mu.Unlock()

	res, err := svc.RenderPrompt(ctx, "acme", RenderPromptInput{Name: "p", Variables: map[string]string{"x": "Z"}})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if res.Version != 1 || res.Rendered != "v1 Z" {
		t.Fatalf("render resolved = v%d %q, want v1 'v1 Z'", res.Version, res.Rendered)
	}
}

// ----------------------------------------------------------------------------
// TEAM ISOLATION — the tenancy boundary. Team A can NEVER reach Team B's prompts.
// ----------------------------------------------------------------------------

func TestTeamIsolation_GetListRender(t *testing.T) {
	repo := newFakeRepo()
	svc := newService(repo)
	ctx := context.Background()

	const teamA, teamB = "team-a", "team-b"

	// Team B owns a "secret" prompt. Team A owns NOTHING.
	if _, err := svc.CreatePrompt(ctx, teamB, CreatePromptInput{
		Name: "secret", Template: "B-only {{token}}",
	}); err != nil {
		t.Fatalf("seed team-b: %v", err)
	}

	// (1) GET: Team A cannot read Team B's prompt by name — it's NotFound, not a leak.
	if _, err := svc.GetPrompt(ctx, teamA, "secret", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("team-a GetPrompt(team-b's 'secret') err = %v, want ErrNotFound", err)
	}
	if _, err := svc.GetPrompt(ctx, teamA, "secret", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("team-a GetPrompt(team-b's 'secret' v1) err = %v, want ErrNotFound", err)
	}

	// (2) LIST: Team A's list is EMPTY (it owns nothing); Team B's list has its prompt.
	pageA, err := svc.ListPrompts(ctx, teamA, ListPromptsInput{PageSize: 100})
	if err != nil {
		t.Fatalf("team-a list: %v", err)
	}
	if len(pageA.Items) != 0 {
		t.Fatalf("team-a list returned %d items; want 0 (must not see team-b's prompt)", len(pageA.Items))
	}
	pageB, err := svc.ListPrompts(ctx, teamB, ListPromptsInput{PageSize: 100})
	if err != nil {
		t.Fatalf("team-b list: %v", err)
	}
	if len(pageB.Items) != 1 || pageB.Items[0].Name != "secret" {
		t.Fatalf("team-b list = %+v; want exactly its own 'secret' prompt", pageB.Items)
	}

	// (3) RENDER: Team A cannot render Team B's prompt — NotFound, no rendered text.
	if _, err := svc.RenderPrompt(ctx, teamA, RenderPromptInput{
		Name: "secret", Variables: map[string]string{"token": "x"},
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("team-a RenderPrompt(team-b's 'secret') err = %v, want ErrNotFound", err)
	}

	// (4) Cross-check: Team B CAN render its own prompt (proves the prompt exists and
	// the isolation above is real scoping, not a render bug).
	res, err := svc.RenderPrompt(ctx, teamB, RenderPromptInput{
		Name: "secret", Variables: map[string]string{"token": "x"},
	})
	if err != nil {
		t.Fatalf("team-b render own prompt: %v", err)
	}
	if res.Rendered != "B-only x" {
		t.Fatalf("team-b rendered = %q, want %q", res.Rendered, "B-only x")
	}
}

func TestTeamIsolation_SameNameDistinctPrompts(t *testing.T) {
	// Both teams create a prompt with the SAME name; each gets its OWN version-1 row,
	// and each reads back ITS OWN template — no cross-contamination.
	svc := newService(newFakeRepo())
	ctx := context.Background()

	if _, err := svc.CreatePrompt(ctx, "team-a", CreatePromptInput{Name: "p", Template: "A's body"}); err != nil {
		t.Fatalf("team-a create: %v", err)
	}
	if _, err := svc.CreatePrompt(ctx, "team-b", CreatePromptInput{Name: "p", Template: "B's body"}); err != nil {
		t.Fatalf("team-b create: %v", err)
	}

	a, err := svc.GetPrompt(ctx, "team-a", "p", 0)
	if err != nil {
		t.Fatalf("team-a get: %v", err)
	}
	b, err := svc.GetPrompt(ctx, "team-b", "p", 0)
	if err != nil {
		t.Fatalf("team-b get: %v", err)
	}
	if a.Template != "A's body" || b.Template != "B's body" {
		t.Fatalf("cross-tenant bleed: team-a=%q team-b=%q", a.Template, b.Template)
	}
}

// itoa is a tiny int→string for the deterministic id generator (avoids importing
// strconv solely for the test fake).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
