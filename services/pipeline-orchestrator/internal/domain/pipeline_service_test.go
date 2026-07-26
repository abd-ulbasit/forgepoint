// pipeline_service_test.go — TDD specification for the saga/DAG engine.
//
// ============================================================================
// WHY THIS TEST IS THE CENTERPIECE
// ============================================================================
//
// The saga ORCHESTRATOR is the crown jewel of this service, so these
// tests assert REAL BEHAVIOR, not mock-call counts:
//
//   - Execution order is captured in a shared, append-only trace slice; we
//     assert the engine ran steps in the right order and ran COMPENSATIONS in
//     the EXACT REVERSE of completion order.
//   - State transitions are asserted on the returned Execution (status actually
//     became COMPENSATED/FAILED, the right steps are COMPENSATED vs FAILED vs
//     SKIPPED), and on the persisted StepExecutions in the mock repo.
//   - Idempotent retry is proven by checking the executor ran the right number
//     of times AND the attempt counter advanced.
//   - Validation invariants (cycles, dangling edges, dup ids, caps) are proven
//     by errors.Is against the sentinels.
//
// These tests are written BEFORE pipeline_service_impl.go exists. Hand-written
// mocks (function-field structs) keep every line explicit.
//
// PACKAGE CHOICE: this is an IN-PACKAGE test (package domain) — unlike Auth,
// nothing here forms a domain↔repository cycle (the ports live in domain), so we
// can test unexported helpers (validateGraph, topoSort) directly alongside the
// exported surface.
// ============================================================================
package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// TEST DOUBLES (hand-written; every line explainable)
// ----------------------------------------------------------------------------

// fakeClock returns a controllable, monotonically advancing time so we can
// assert ordering by CompletedAt deterministically (no wall-clock flakiness).
type fakeClock struct {
	mu  sync.Mutex
	cur time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{cur: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// Now advances 1 second per call so successive timestamps strictly increase —
// this is what lets completedStepsInOrder produce a stable forward order and the
// compensator a stable reverse order.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur = c.cur.Add(time.Second)
	return c.cur
}

// seqIDGen produces deterministic ids (id-1, id-2, ...) so test assertions on
// ids are stable.
type seqIDGen struct {
	mu sync.Mutex
	n  int
}

func (g *seqIDGen) NewID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.n++
	return fmt.Sprintf("id-%d", g.n)
}

// memPipelineRepo is an in-memory PipelineRepository. It stores real
// PipelineDefinitions and honors the idempotency key, so a retried Create
// returns the SAME pipeline (proving the dedup contract for real, not via a
// call-count assertion).
type memPipelineRepo struct {
	mu    sync.Mutex
	byID  map[string]PipelineDefinition
	byKey map[string]string // idempotencyKey -> pipeline id
}

func newMemPipelineRepo() *memPipelineRepo {
	return &memPipelineRepo{byID: map[string]PipelineDefinition{}, byKey: map[string]string{}}
}

func (r *memPipelineRepo) Create(_ context.Context, p PipelineDefinition, key string) (PipelineDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if key != "" {
		if id, ok := r.byKey[key]; ok {
			return r.byID[id], nil // dedup: return the original
		}
	}
	r.byID[p.ID] = p
	if key != "" {
		r.byKey[key] = p.ID
	}
	return p, nil
}

func (r *memPipelineRepo) GetByID(_ context.Context, id string) (PipelineDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.byID[id]
	if !ok {
		return PipelineDefinition{}, ErrRepoNotFound
	}
	return p, nil
}

func (r *memPipelineRepo) Update(_ context.Context, p PipelineDefinition) (PipelineDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[p.ID]; !ok {
		return PipelineDefinition{}, ErrRepoNotFound
	}
	r.byID[p.ID] = p
	return p, nil
}

func (r *memPipelineRepo) Archive(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.byID[id]
	if !ok {
		return ErrRepoNotFound
	}
	p.Archived = true
	r.byID[id] = p
	return nil
}

func (r *memPipelineRepo) List(_ context.Context, f ListPipelinesFilter) ([]PipelineDefinition, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []PipelineDefinition
	for _, p := range r.byID {
		if p.Archived || p.Team != f.Team {
			continue
		}
		if f.Type != PipelineTypeUnspecified && p.Type != f.Type {
			continue
		}
		out = append(out, p)
	}
	return out, "", nil
}

// memExecutionRepo is an in-memory ExecutionRepository. Crucially, SaveStep and
// Save mutate the stored Execution so the engine's write-ahead checkpoints are
// observable after a run — the test asserts the FINAL persisted state matches
// the returned Execution (durability is real, not faked).
type memExecutionRepo struct {
	mu      sync.Mutex
	byID    map[string]Execution
	byKey   map[string]string
	saveErr error // optional injected failure on Save (durability-failure test)
}

func newMemExecutionRepo() *memExecutionRepo {
	return &memExecutionRepo{byID: map[string]Execution{}, byKey: map[string]string{}}
}

func (r *memExecutionRepo) Create(_ context.Context, e Execution, key string) (Execution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if key != "" {
		if id, ok := r.byKey[key]; ok {
			return r.byID[id], nil
		}
	}
	r.byID[e.ID] = cloneExec(e)
	if key != "" {
		r.byKey[key] = e.ID
	}
	return e, nil
}

func (r *memExecutionRepo) Save(_ context.Context, e Execution) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	r.byID[e.ID] = cloneExec(e)
	return nil
}

func (r *memExecutionRepo) SaveStep(_ context.Context, step StepExecution) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.byID[step.ExecutionID]
	if !ok {
		return ErrRepoNotFound
	}
	found := false
	for i := range e.Steps {
		if e.Steps[i].StepID == step.StepID {
			e.Steps[i] = step
			found = true
			break
		}
	}
	if !found {
		e.Steps = append(e.Steps, step)
	}
	r.byID[step.ExecutionID] = e
	return nil
}

func (r *memExecutionRepo) GetByID(_ context.Context, id string) (Execution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.byID[id]
	if !ok {
		return Execution{}, ErrRepoNotFound
	}
	return cloneExec(e), nil
}

func (r *memExecutionRepo) List(_ context.Context, f ListExecutionsFilter) ([]Execution, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Execution
	for _, e := range r.byID {
		if f.PipelineID != "" && e.PipelineID != f.PipelineID {
			continue
		}
		if f.Status != ExecutionStatusUnspecified && e.Status != f.Status {
			continue
		}
		out = append(out, cloneExec(e))
	}
	return out, "", nil
}

func cloneExec(e Execution) Execution {
	c := e
	c.Steps = append([]StepExecution(nil), e.Steps...)
	return c
}

// recordingExecutor is the workhorse mock StepExecutor. It appends to a shared
// trace on every Execute/Compensate call so the test can assert the exact ORDER
// of forward execution and reverse compensation. failExecute / failCompensate
// let a test make a specific step fail. failUntilAttempt makes a step fail on
// early attempts and succeed later (retry/idempotency test).
type recordingExecutor struct {
	stepID           string
	trace            *traceLog
	failExecute      bool
	failCompensate   bool
	failUntilAttempt int            // 0 = never; otherwise Execute fails while attempt < this
	output           map[string]any // optional output to return
	endpoint         string         // optional resolved endpoint (DEPLOY)
}

func (e *recordingExecutor) Execute(_ context.Context, in StepInput) (StepResult, error) {
	if e.trace != nil {
		e.trace.add("exec:" + in.StepID)
	}
	if e.failUntilAttempt > 0 && in.Attempt < e.failUntilAttempt {
		return StepResult{}, fmt.Errorf("transient failure on attempt %d", in.Attempt)
	}
	if e.failExecute {
		return StepResult{}, fmt.Errorf("step %s failed", in.StepID)
	}
	return StepResult{Output: e.output, Endpoint: e.endpoint}, nil
}

func (e *recordingExecutor) Compensate(_ context.Context, in StepInput) error {
	if e.trace != nil {
		e.trace.add("comp:" + in.StepID)
	}
	if e.failCompensate {
		return fmt.Errorf("compensation for %s failed", in.StepID)
	}
	return nil
}

// traceLog is a goroutine-safe append-only log of "exec:X"/"comp:X" entries.
// The DAG scheduler may run steps concurrently, so the trace must be safe under
// -race; we lock every append.
type traceLog struct {
	mu  sync.Mutex
	seq []string
}

func (t *traceLog) add(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.seq = append(t.seq, s)
}

func (t *traceLog) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.seq...)
}

// ---- harness ---------------------------------------------------------------

// perStepRegistry is the registry the engine consults. The engine looks
// executors up by StepType (the production contract), so each test assigns its
// steps DISTINCT StepTypes from the known set and maps each type to its own
// recordingExecutor — giving every step independent success/failure behavior
// while keeping the registry keyed by StepType exactly as production wires it.
type perStepRegistry struct {
	byType map[StepType]*recordingExecutor
}

func (r *perStepRegistry) Executor(t StepType) (StepExecutor, bool) {
	e, ok := r.byType[t]
	return e, ok
}

// newTestService builds a pipelineService with in-memory repos, a fake clock,
// deterministic ids, and the given registry. Event publishing uses a capturing
// publisher so we can assert lifecycle events were emitted.
func newTestService(reg ExecutorRegistry) (*pipelineService, *memPipelineRepo, *memExecutionRepo, *capturingPublisher) {
	pr := newMemPipelineRepo()
	er := newMemExecutionRepo()
	pub := &capturingPublisher{}
	svc := NewPipelineService(pr, er, reg, pub, newFakeClock(), &seqIDGen{})
	return svc, pr, er, pub
}

// capturingPublisher records every emitted StepEvent so tests can assert the
// lifecycle feed (PipelineStarted, StepCompleted, CompensationTriggered, ...).
type capturingPublisher struct {
	mu     sync.Mutex
	events []StepEvent
}

func (p *capturingPublisher) Publish(_ context.Context, ev StepEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	return nil
}

func (p *capturingPublisher) types() []EventType {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]EventType, len(p.events))
	for i, e := range p.events {
		out[i] = e.Type
	}
	return out
}

func hasEvent(types []EventType, want EventType) bool {
	for _, t := range types {
		if t == want {
			return true
		}
	}
	return false
}

const testTeam = "team-a"

var testActor = Actor{Subject: "user-1", Team: testTeam}

// ============================================================================
// VALIDATION TESTS (graph invariants, enforced at CreatePipeline)
// ============================================================================

func TestCreatePipeline_RejectsEmptyPipeline(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	_, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "empty",
		Type:  PipelineTypeDeploymentSaga,
		Steps: nil,
	})
	if !errors.Is(err, ErrEmptyPipeline) {
		t.Fatalf("want ErrEmptyPipeline, got %v", err)
	}
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("ErrEmptyPipeline must wrap ErrValidation, got %v", err)
	}
}

func TestCreatePipeline_RejectsUnknownStepType(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	_, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "bad",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "s1", Type: StepTypeUnspecified}, // zero value = unknown
		},
	})
	if !errors.Is(err, ErrUnknownStepType) {
		t.Fatalf("want ErrUnknownStepType, got %v", err)
	}
}

func TestCreatePipeline_RejectsDuplicateStepID(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	_, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "dup",
		Type: PipelineTypeTrainingDAG,
		Steps: []StepDefinition{
			{ID: "s1", Type: StepTypeTrain},
			{ID: "s1", Type: StepTypeEvaluate},
		},
	})
	if !errors.Is(err, ErrDuplicateStepID) {
		t.Fatalf("want ErrDuplicateStepID, got %v", err)
	}
}

func TestCreatePipeline_RejectsDanglingDependsOn(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	_, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "dangle",
		Type: PipelineTypeTrainingDAG,
		Steps: []StepDefinition{
			{ID: "s1", Type: StepTypeTrain, DependsOn: []string{"ghost"}},
		},
	})
	if !errors.Is(err, ErrDanglingDependency) {
		t.Fatalf("want ErrDanglingDependency, got %v", err)
	}
}

func TestCreatePipeline_RejectsDanglingCompensationPointer(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	_, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "badcomp",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "s1", Type: StepTypeDeploy, CompensationStepID: "ghost"},
		},
	})
	if !errors.Is(err, ErrDanglingDependency) {
		t.Fatalf("want ErrDanglingDependency for bad compensation pointer, got %v", err)
	}
}

func TestCreatePipeline_RejectsCycle(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	// s1 -> s2 -> s3 -> s1 (a cycle)
	_, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "cycle",
		Type: PipelineTypeTrainingDAG,
		Steps: []StepDefinition{
			{ID: "s1", Type: StepTypeTrain, DependsOn: []string{"s3"}},
			{ID: "s2", Type: StepTypeEvaluate, DependsOn: []string{"s1"}},
			{ID: "s3", Type: StepTypeRegister, DependsOn: []string{"s2"}},
		},
	})
	if !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("want ErrCycleDetected, got %v", err)
	}
}

func TestCreatePipeline_RejectsTooManySteps(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	steps := make([]StepDefinition, MaxStepsPerPipeline+1)
	for i := range steps {
		steps[i] = StepDefinition{ID: fmt.Sprintf("s%d", i), Type: StepTypeCustom}
	}
	_, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "huge",
		Type:  PipelineTypeTrainingDAG,
		Steps: steps,
	})
	if !errors.Is(err, ErrTooManySteps) {
		t.Fatalf("want ErrTooManySteps, got %v", err)
	}
}

func TestCreatePipeline_StampsServerAuthoritativeFields(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "ok",
		Type:  PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{{ID: "s1", Type: StepTypeValidate}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Server-authoritative: id assigned, created_by/team from the ACTOR (not input).
	if p.ID == "" {
		t.Fatal("expected server-assigned id")
	}
	if p.CreatedBy != testActor.Subject {
		t.Fatalf("created_by must come from actor, got %q", p.CreatedBy)
	}
	if p.Team != testActor.Team {
		t.Fatalf("team must come from actor, got %q", p.Team)
	}
	if p.CreatedAt.IsZero() {
		t.Fatal("expected server-set created_at")
	}
}

func TestCreatePipeline_ClampsMaxRetries(t *testing.T) {
	svc, pr, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "retry",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "s1", Type: StepTypeValidate, MaxRetries: 9999}, // absurd → clamp
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	stored, _ := pr.GetByID(context.Background(), p.ID)
	if stored.Steps[0].MaxRetries != MaxRetriesCap {
		t.Fatalf("MaxRetries should be clamped to %d, got %d", MaxRetriesCap, stored.Steps[0].MaxRetries)
	}
}

func TestCreatePipeline_IsIdempotent(t *testing.T) {
	svc, _, _, _ := newTestService(&perStepRegistry{byType: map[StepType]*recordingExecutor{}})
	in := CreatePipelineInput{
		Name:           "idem",
		Type:           PipelineTypeDeploymentSaga,
		Steps:          []StepDefinition{{ID: "s1", Type: StepTypeValidate}},
		IdempotencyKey: "key-123",
	}
	p1, err := svc.CreatePipeline(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	p2, err := svc.CreatePipeline(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if p1.ID != p2.ID {
		t.Fatalf("idempotent create must return same id, got %q vs %q", p1.ID, p2.ID)
	}
}

// ============================================================================
// TOPOLOGICAL SORT (DAG ordering) — direct unit test of the helper
// ============================================================================

func TestTopoSort_OrdersByDependency(t *testing.T) {
	// fetch -> preprocess -> {train1, train2} -> aggregate
	steps := []StepDefinition{
		{ID: "aggregate", DependsOn: []string{"train1", "train2"}},
		{ID: "train1", DependsOn: []string{"preprocess"}},
		{ID: "train2", DependsOn: []string{"preprocess"}},
		{ID: "preprocess", DependsOn: []string{"fetch"}},
		{ID: "fetch"},
	}
	levels, err := topoLevels(steps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Flatten and check every dependency precedes its dependent.
	pos := map[string]int{}
	idx := 0
	for _, lvl := range levels {
		for _, id := range lvl {
			pos[id] = idx
			idx++
		}
	}
	assertBefore := func(a, b string) {
		if pos[a] >= pos[b] {
			t.Fatalf("%s must come before %s (pos %d vs %d)", a, b, pos[a], pos[b])
		}
	}
	assertBefore("fetch", "preprocess")
	assertBefore("preprocess", "train1")
	assertBefore("preprocess", "train2")
	assertBefore("train1", "aggregate")
	assertBefore("train2", "aggregate")

	// train1 and train2 are independent → must share a level (parallelizable).
	if len(levels) != 4 {
		t.Fatalf("expected 4 levels (fetch | preprocess | train1,train2 | aggregate), got %d: %v", len(levels), levels)
	}
	if len(levels[2]) != 2 {
		t.Fatalf("expected train1+train2 to be parallel in one level, got %v", levels[2])
	}
}

func TestTopoSort_DetectsCycle(t *testing.T) {
	steps := []StepDefinition{
		{ID: "a", DependsOn: []string{"b"}},
		{ID: "b", DependsOn: []string{"a"}},
	}
	_, err := topoLevels(steps)
	if !errors.Is(err, ErrCycleDetected) {
		t.Fatalf("want ErrCycleDetected, got %v", err)
	}
}

// ============================================================================
// SAGA HAPPY PATH — sequential, in dependency order, no compensation
// ============================================================================

func TestTriggerExecution_SagaHappyPath_RunsInOrderAndCompletes(t *testing.T) {
	trace := &traceLog{}
	// A 4-step deployment saga: validate -> deploy -> canary -> promote.
	// Each step is a distinct StepType so the type-keyed registry gives each its
	// own executor. depends_on encodes the linear order.
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "validate", trace: trace},
		StepTypeDeploy:   {stepID: "deploy", trace: trace, endpoint: "http://fp-models.svc/m1"},
		StepTypeCanary:   {stepID: "canary", trace: trace},
		StepTypePromote:  {stepID: "promote", trace: trace},
	}}
	svc, _, er, pub := newTestService(reg)

	pid := mustCreateSaga(t, svc)
	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: pid})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}

	if exec.Status != ExecutionStatusCompleted {
		t.Fatalf("want COMPLETED, got %s (err=%q)", exec.Status, exec.Error)
	}
	got := trace.snapshot()
	want := []string{"exec:validate", "exec:deploy", "exec:canary", "exec:promote"}
	if !equalSeq(got, want) {
		t.Fatalf("execution order wrong:\n got=%v\nwant=%v", got, want)
	}
	// No compensation on the happy path.
	for _, e := range got {
		if strings.HasPrefix(e, "comp:") {
			t.Fatalf("no compensation expected on happy path, saw %s", e)
		}
	}
	// Every step persisted COMPLETED (durability checkpoint).
	stored, _ := er.GetByID(context.Background(), exec.ID)
	for _, se := range stored.Steps {
		if se.Status != StepStatusCompleted {
			t.Fatalf("step %s persisted as %s, want COMPLETED", se.StepID, se.Status)
		}
	}
	// Lifecycle events: started + completed + a ModelDeployed (deploy resolved an endpoint).
	types := pub.types()
	if !hasEvent(types, EventPipelineStarted) || !hasEvent(types, EventPipelineCompleted) {
		t.Fatalf("missing started/completed events: %v", types)
	}
	if !hasEvent(types, EventModelDeployed) {
		t.Fatalf("deploy step resolved an endpoint; expected ModelDeployed event: %v", types)
	}
}

// ============================================================================
// SAGA FAILURE → COMPENSATION IN REVERSE ORDER (the centerpiece)
// ============================================================================

func TestTriggerExecution_SagaFailure_CompensatesInReverseOrder(t *testing.T) {
	trace := &traceLog{}
	// validate -> deploy -> canary(FAILS) -> promote.
	// validate & deploy complete; canary fails; compensation must undo the
	// COMPLETED steps (deploy, then validate) in REVERSE completion order.
	// promote never runs (it's after the failure).
	validate := &recordingExecutor{stepID: "validate", trace: trace}
	deploy := &recordingExecutor{stepID: "deploy", trace: trace, endpoint: "http://fp-models/m1"}
	canary := &recordingExecutor{stepID: "canary", trace: trace, failExecute: true}
	promote := &recordingExecutor{stepID: "promote", trace: trace}
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: validate,
		StepTypeDeploy:   deploy,
		StepTypeCanary:   canary,
		StepTypePromote:  promote,
	}}
	svc, _, er, pub := newTestService(reg)

	pid := mustCreateSaga(t, svc)
	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: pid})
	if err != nil {
		t.Fatalf("trigger returned error (engine should settle to FAILED, not error): %v", err)
	}

	if exec.Status != ExecutionStatusFailed {
		t.Fatalf("want FAILED, got %s", exec.Status)
	}

	got := trace.snapshot()
	// Forward: validate, deploy, canary(fail). Then compensation in REVERSE of
	// completion order: comp:deploy, comp:validate. promote must NEVER appear.
	want := []string{
		"exec:validate", "exec:deploy", "exec:canary",
		"comp:deploy", "comp:validate",
	}
	if !equalSeq(got, want) {
		t.Fatalf("compensation order wrong:\n got=%v\nwant=%v", got, want)
	}
	for _, e := range got {
		if e == "exec:promote" {
			t.Fatal("promote must not run — it is after the failing step")
		}
	}

	// Persisted per-step states: validate+deploy COMPENSATED, canary FAILED,
	// promote SKIPPED (never ran).
	stored, _ := er.GetByID(context.Background(), exec.ID)
	wantStepStatus := map[string]StepStatus{
		"validate": StepStatusCompensated,
		"deploy":   StepStatusCompensated,
		"canary":   StepStatusFailed,
		"promote":  StepStatusSkipped,
	}
	for _, se := range stored.Steps {
		if want := wantStepStatus[se.StepID]; se.Status != want {
			t.Fatalf("step %s = %s, want %s", se.StepID, se.Status, want)
		}
	}

	// Lifecycle events: StepFailed + CompensationTriggered + PipelineFailed +
	// ModelUndeployed (the deploy compensation tore down the serving instance).
	types := pub.types()
	for _, want := range []EventType{EventStepFailed, EventCompensationTriggered, EventPipelineFailed} {
		if !hasEvent(types, want) {
			t.Fatalf("missing event %v in %v", want, types)
		}
	}
}

// ============================================================================
// COMPENSATION SKIPS STEPS WITH NO COMPENSATION DEFINED
// ============================================================================

func TestTriggerExecution_CompensationSkipsStepsWithoutCompensation(t *testing.T) {
	trace := &traceLog{}
	// validate (NO compensation) -> deploy (HAS compensation) -> canary(FAILS).
	// On failure, only deploy is compensated; validate has no compensation_step_id
	// so the engine must NOT call Compensate on it.
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "validate", trace: trace},
		StepTypeDeploy:   {stepID: "deploy", trace: trace},
		StepTypeCanary:   {stepID: "canary", trace: trace, failExecute: true},
	}}
	svc, _, er, _ := newTestService(reg)

	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "partial-comp",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "validate", Type: StepTypeValidate}, // no CompensationStepID
			{ID: "deploy", Type: StepTypeDeploy, DependsOn: []string{"validate"}, CompensationStepID: "deploy"},
			{ID: "canary", Type: StepTypeCanary, DependsOn: []string{"deploy"}},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if exec.Status != ExecutionStatusFailed {
		t.Fatalf("want FAILED, got %s", exec.Status)
	}
	got := trace.snapshot()
	// validate has no compensation → comp:validate must NOT appear.
	for _, e := range got {
		if e == "comp:validate" {
			t.Fatalf("validate has no compensation; comp:validate must not run. trace=%v", got)
		}
	}
	if !containsSeq(got, "comp:deploy") {
		t.Fatalf("deploy should be compensated. trace=%v", got)
	}
	stored, _ := er.GetByID(context.Background(), exec.ID)
	// validate stays COMPLETED (it ran and was never undone).
	if se := findByStepID(stored.Steps, "validate"); se.Status != StepStatusCompleted {
		t.Fatalf("validate (no compensation) should stay COMPLETED, got %s", se.Status)
	}
}

// ============================================================================
// COMPENSATION ITSELF FAILS → COMPENSATION_FAILED (stuck saga)
// ============================================================================

func TestTriggerExecution_CompensationFailure_MarksStuckSaga(t *testing.T) {
	trace := &traceLog{}
	// deploy completes but its COMPENSATION fails; canary then fails. Rolling back
	// deploy errors → the step is COMPENSATION_FAILED and the run still settles to
	// FAILED, but GetExecution shows a step that could not be undone.
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeDeploy: {stepID: "deploy", trace: trace, failCompensate: true},
		StepTypeCanary: {stepID: "canary", trace: trace, failExecute: true},
	}}
	svc, _, er, _ := newTestService(reg)
	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "stuck",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "deploy", Type: StepTypeDeploy, CompensationStepID: "deploy"},
			{ID: "canary", Type: StepTypeCanary, DependsOn: []string{"deploy"}},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	// The run still reaches a terminal state.
	if exec.Status != ExecutionStatusFailed {
		t.Fatalf("want FAILED, got %s", exec.Status)
	}
	stored, _ := er.GetByID(context.Background(), exec.ID)
	if se := findByStepID(stored.Steps, "deploy"); se.Status != StepStatusCompensationFailed {
		t.Fatalf("deploy compensation failed; want COMPENSATION_FAILED, got %s", se.Status)
	}
}

// ============================================================================
// IDEMPOTENT RETRY — a transient failure is retried, then succeeds
// ============================================================================

func TestTriggerExecution_RetriesTransientFailure_ThenSucceeds(t *testing.T) {
	trace := &traceLog{}
	// deploy fails on attempts 1 and 2, succeeds on attempt 3. With MaxRetries=3
	// the saga should NOT compensate — it should retry and complete.
	deploy := &recordingExecutor{stepID: "deploy", trace: trace, failUntilAttempt: 3}
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeDeploy: deploy,
	}}
	svc, _, er, _ := newTestService(reg)
	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "retry-ok",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "deploy", Type: StepTypeDeploy, MaxRetries: 3},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if exec.Status != ExecutionStatusCompleted {
		t.Fatalf("retry-then-succeed should COMPLETE, got %s", exec.Status)
	}
	// Executor ran exactly 3 times (2 failures + 1 success).
	got := trace.snapshot()
	execCount := 0
	for _, e := range got {
		if e == "exec:deploy" {
			execCount++
		}
	}
	if execCount != 3 {
		t.Fatalf("want 3 deploy attempts, got %d (trace=%v)", execCount, got)
	}
	// The persisted attempt counter reflects the successful attempt number.
	stored, _ := er.GetByID(context.Background(), exec.ID)
	if se := findByStepID(stored.Steps, "deploy"); se.Attempt != 3 {
		t.Fatalf("want attempt=3 recorded, got %d", se.Attempt)
	}
}

func TestTriggerExecution_ExhaustsRetries_ThenFails(t *testing.T) {
	trace := &traceLog{}
	// deploy needs attempt 5 to pass but MaxRetries=2 → exhausts and FAILS.
	deploy := &recordingExecutor{stepID: "deploy", trace: trace, failUntilAttempt: 5}
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{StepTypeDeploy: deploy}}
	svc, _, _, _ := newTestService(reg)
	p, _ := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "retry-exhaust",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "deploy", Type: StepTypeDeploy, MaxRetries: 2},
		},
	})
	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if exec.Status != ExecutionStatusFailed {
		t.Fatalf("exhausted retries should FAIL, got %s", exec.Status)
	}
	// MaxRetries=2 means 1 initial + 2 retries = 3 total attempts.
	got := trace.snapshot()
	execCount := 0
	for _, e := range got {
		if e == "exec:deploy" {
			execCount++
		}
	}
	if execCount != 3 {
		t.Fatalf("MaxRetries=2 → 3 attempts; got %d", execCount)
	}
}

// ============================================================================
// TRIGGER IDEMPOTENCY — same key returns the same Execution, runs once
// ============================================================================

func TestTriggerExecution_IsIdempotent(t *testing.T) {
	trace := &traceLog{}
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "validate", trace: trace},
	}}
	svc, _, _, _ := newTestService(reg)
	p, _ := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "idem-trigger",
		Type:  PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{{ID: "validate", Type: StepTypeValidate}},
	})
	in := TriggerInput{PipelineID: p.ID, IdempotencyKey: "trig-1"}
	e1, err := svc.TriggerExecution(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("first trigger: %v", err)
	}
	e2, err := svc.TriggerExecution(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("second trigger: %v", err)
	}
	if e1.ID != e2.ID {
		t.Fatalf("idempotent trigger must return same execution, got %q vs %q", e1.ID, e2.ID)
	}
	// The step ran only ONCE despite two triggers (the second short-circuited).
	execCount := 0
	for _, e := range trace.snapshot() {
		if e == "exec:validate" {
			execCount++
		}
	}
	if execCount != 1 {
		t.Fatalf("idempotent trigger must run the saga once, ran %d times", execCount)
	}
}

func TestTriggerExecution_ArchivedPipeline_Rejected(t *testing.T) {
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{StepTypeValidate: {stepID: "v"}}}
	svc, _, _, _ := newTestService(reg)
	p, _ := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "to-archive",
		Type:  PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{{ID: "v", Type: StepTypeValidate}},
	})
	if err := svc.DeletePipeline(context.Background(), testActor, p.ID); err != nil {
		t.Fatalf("archive: %v", err)
	}
	_, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if !errors.Is(err, ErrPipelineArchived) {
		t.Fatalf("want ErrPipelineArchived, got %v", err)
	}
}

func TestTriggerExecution_UnknownPipeline_Rejected(t *testing.T) {
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{}}
	svc, _, _, _ := newTestService(reg)
	_, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: "nope"})
	if !errors.Is(err, ErrPipelineNotFound) {
		t.Fatalf("want ErrPipelineNotFound, got %v", err)
	}
}

// ============================================================================
// DAG EXECUTION — failure propagates to dependents (SKIPPED), no compensation
// ============================================================================

func TestTriggerExecution_DAGFailure_SkipsDependents(t *testing.T) {
	trace := &traceLog{}
	// fetch -> preprocess(FAILS) -> train -> register.
	// preprocess fails; its dependents (train, register) are SKIPPED. A DAG does
	// NOT compensate (no compensation pointers) — it propagates failure.
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "fetch", trace: trace}, // reuse VALIDATE type as "fetch"
		StepTypeBuild:    {stepID: "preprocess", trace: trace, failExecute: true},
		StepTypeTrain:    {stepID: "train", trace: trace},
		StepTypeRegister: {stepID: "register", trace: trace},
	}}
	svc, _, er, _ := newTestService(reg)
	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "dag",
		Type: PipelineTypeTrainingDAG,
		Steps: []StepDefinition{
			{ID: "fetch", Type: StepTypeValidate},
			{ID: "preprocess", Type: StepTypeBuild, DependsOn: []string{"fetch"}},
			{ID: "train", Type: StepTypeTrain, DependsOn: []string{"preprocess"}},
			{ID: "register", Type: StepTypeRegister, DependsOn: []string{"train"}},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if exec.Status != ExecutionStatusFailed {
		t.Fatalf("DAG with a failed step should FAIL, got %s", exec.Status)
	}
	got := trace.snapshot()
	// No compensation in a DAG.
	for _, e := range got {
		if strings.HasPrefix(e, "comp:") {
			t.Fatalf("DAG must not compensate, saw %s", e)
		}
	}
	// train and register must be SKIPPED (their parent failed), never executed.
	for _, e := range got {
		if e == "exec:train" || e == "exec:register" {
			t.Fatalf("dependents of a failed step must not run, saw %s", e)
		}
	}
	stored, _ := er.GetByID(context.Background(), exec.ID)
	for _, id := range []string{"train", "register"} {
		if se := findByStepID(stored.Steps, id); se.Status != StepStatusSkipped {
			t.Fatalf("%s should be SKIPPED, got %s", id, se.Status)
		}
	}
}

func TestTriggerExecution_DAGHappyPath_RunsParallelLevelsInOrder(t *testing.T) {
	trace := &traceLog{}
	// fetch -> {train1, train2} -> aggregate. Independent trains share a level.
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "fetch", trace: trace},
		StepTypeTrain:    {stepID: "train1", trace: trace},
		StepTypeEvaluate: {stepID: "train2", trace: trace},
		StepTypeRegister: {stepID: "aggregate", trace: trace},
	}}
	svc, _, _, _ := newTestService(reg)
	p, _ := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "dag-ok",
		Type: PipelineTypeTrainingDAG,
		Steps: []StepDefinition{
			{ID: "fetch", Type: StepTypeValidate},
			{ID: "train1", Type: StepTypeTrain, DependsOn: []string{"fetch"}},
			{ID: "train2", Type: StepTypeEvaluate, DependsOn: []string{"fetch"}},
			{ID: "aggregate", Type: StepTypeRegister, DependsOn: []string{"train1", "train2"}},
		},
	})
	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	if exec.Status != ExecutionStatusCompleted {
		t.Fatalf("want COMPLETED, got %s", exec.Status)
	}
	got := trace.snapshot()
	// fetch must be first; aggregate must be last; trains in between (any order).
	if got[0] != "exec:fetch" {
		t.Fatalf("fetch must run first, got %v", got)
	}
	if got[len(got)-1] != "exec:aggregate" {
		t.Fatalf("aggregate must run last, got %v", got)
	}
}

// ============================================================================
// CANCELLATION
// ============================================================================

func TestCancelExecution_TerminalRun_Rejected(t *testing.T) {
	trace := &traceLog{}
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "v", trace: trace},
	}}
	svc, _, _, _ := newTestService(reg)
	p, _ := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "cancel-terminal",
		Type:  PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{{ID: "v", Type: StepTypeValidate}},
	})
	exec, _ := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	// The run already completed; cancelling it must be rejected.
	_, err := svc.CancelExecution(context.Background(), testActor, exec.ID, "too late")
	if !errors.Is(err, ErrExecutionNotCancellable) {
		t.Fatalf("want ErrExecutionNotCancellable for a terminal run, got %v", err)
	}
}

// TestCancelExecution_InFlight_CompensatesAndCancels drives a cancel WHILE the
// saga is mid-run. We use a gate executor that blocks until the test signals,
// triggers the run in a goroutine, cancels it, and asserts the run settles to
// CANCELLED with completed steps compensated.
func TestCancelExecution_InFlight_CompensatesAndCancels(t *testing.T) {
	trace := &traceLog{}
	started := make(chan struct{}, 1)

	validate := &recordingExecutor{stepID: "validate", trace: trace}
	// gate blocks inside Execute until its context is cancelled, simulating a
	// long-running step (a canary watching metrics) interrupted by a cancel.
	gate := &gateExecutor{stepID: "deploy", trace: trace, started: started}

	reg := &gatedRegistry{byType: map[StepType]*gateOrRec{
		StepTypeValidate: {rec: validate},
		StepTypeDeploy:   {gate: gate},
	}}
	// gateOrRec adapts to the registry; see its Executor method.
	svc, _, er, _ := newTestServiceGated(reg)

	p, _ := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "cancel-inflight",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "validate", Type: StepTypeValidate, CompensationStepID: "validate"},
			{ID: "deploy", Type: StepTypeDeploy, DependsOn: []string{"validate"}, CompensationStepID: "deploy"},
		},
	})

	type res struct {
		e   Execution
		err error
	}
	done := make(chan res, 1)
	var execID string
	// Pre-create the execution id is server-generated; we discover it via the
	// repo once the run starts. Simpler: trigger in a goroutine and grab the id
	// from the started signal path. We expose the id by having the engine persist
	// the PENDING execution before running — so we poll the repo.
	go func() {
		e, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID, IdempotencyKey: "cancel-key"})
		done <- res{e, err}
	}()

	// Wait until deploy is executing (validate already completed).
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("deploy step never started")
	}
	// Find the in-flight execution by its idempotency key path: list executions.
	execs, _, _ := er.List(context.Background(), ListExecutionsFilter{})
	if len(execs) != 1 {
		t.Fatalf("expected 1 in-flight execution, got %d", len(execs))
	}
	execID = execs[0].ID

	// Cancel it. CancelExecution trips the run's cancel func; the gate's Execute
	// is blocked on ctx.Done() and returns a context error the moment we cancel,
	// which drives the saga into compensation and then CANCELLED.
	cancelDone := make(chan error, 1)
	go func() {
		_, err := svc.CancelExecution(context.Background(), testActor, execID, "user cancelled")
		cancelDone <- err
	}()

	r := <-done
	if r.err != nil {
		t.Fatalf("trigger returned error: %v", r.err)
	}
	if r.e.Status != ExecutionStatusCancelled {
		t.Fatalf("cancelled run should settle to CANCELLED, got %s", r.e.Status)
	}
	if err := <-cancelDone; err != nil {
		t.Fatalf("cancel returned error: %v", err)
	}
	// validate completed before cancel → it must be COMPENSATED.
	stored, _ := er.GetByID(context.Background(), execID)
	if se := findByStepID(stored.Steps, "validate"); se.Status != StepStatusCompensated {
		t.Fatalf("validate should be COMPENSATED after cancel, got %s", se.Status)
	}
	// Compensation ran for validate (the completed step).
	if !containsSeq(trace.snapshot(), "comp:validate") {
		t.Fatalf("expected comp:validate after cancel, trace=%v", trace.snapshot())
	}
}

// ============================================================================
// GET / LIST scoping
// ============================================================================

func TestGetExecution_ScopedToTeam(t *testing.T) {
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{StepTypeValidate: {stepID: "v"}}}
	svc, _, _, _ := newTestService(reg)
	p, _ := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "scoped",
		Type:  PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{{ID: "v", Type: StepTypeValidate}},
	})
	exec, _ := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})

	// A different team must NOT be able to read this execution.
	other := Actor{Subject: "user-2", Team: "team-b"}
	_, err := svc.GetExecution(context.Background(), other, exec.ID)
	if !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("cross-team read must be ErrExecutionNotFound (no info leak), got %v", err)
	}
	// Same team can read it.
	if _, err := svc.GetExecution(context.Background(), testActor, exec.ID); err != nil {
		t.Fatalf("same-team read failed: %v", err)
	}
}

// ----------------------------------------------------------------------------
// gate executor + harness for the cancellation test
// ----------------------------------------------------------------------------

type gateExecutor struct {
	stepID  string
	trace   *traceLog
	started chan struct{}
}

func (g *gateExecutor) Execute(ctx context.Context, in StepInput) (StepResult, error) {
	g.trace.add("exec:" + in.StepID)
	// Signal we've started, then BLOCK until the engine cancels our context. This
	// simulates a long-running step (e.g. a canary watching metrics) that is
	// interrupted by CancelExecution. A well-behaved executor honors ctx.Done() —
	// that is exactly the cancellation responsiveness we are testing.
	select {
	case g.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return StepResult{}, ctx.Err()
}

func (g *gateExecutor) Compensate(_ context.Context, in StepInput) error {
	g.trace.add("comp:" + in.StepID)
	return nil
}

// gateOrRec lets a registry hold either a recording or a gate executor.
type gateOrRec struct {
	rec  *recordingExecutor
	gate *gateExecutor
}

type gatedRegistry struct {
	byType map[StepType]*gateOrRec
}

func (r *gatedRegistry) Executor(t StepType) (StepExecutor, bool) {
	g, ok := r.byType[t]
	if !ok {
		return nil, false
	}
	if g.gate != nil {
		return g.gate, true
	}
	return g.rec, true
}

func newTestServiceGated(reg *gatedRegistry) (*pipelineService, *memPipelineRepo, *memExecutionRepo, *capturingPublisher) {
	pr := newMemPipelineRepo()
	er := newMemExecutionRepo()
	pub := &capturingPublisher{}
	svc := NewPipelineService(pr, er, reg, pub, newFakeClock(), &seqIDGen{})
	return svc, pr, er, pub
}

// ============================================================================
// DURABILITY UNDER CANCELLATION + WRITE FAILURE (Findings 1 & 2)
// ============================================================================
//
// The in-memory repos above IGNORE ctx, so they cannot catch the cancelled-ctx
// durability bug. ctxRespectingExecutionRepo is a SPY that behaves like a real
// pgx/Postgres adapter in the one way that matters: it REJECTS any write issued on
// a cancelled context with context.Canceled, and it COUNTS how many writes arrived
// on a cancelled context. It also supports injecting a failure on the Nth Save /
// SaveStep so we can prove the engine surfaces (does not swallow) durability errors.

type ctxRespectingExecutionRepo struct {
	mu    sync.Mutex
	byID  map[string]Execution
	byKey map[string]string

	// Spy counters: how many writes arrived on an ALREADY-CANCELLED context. The
	// whole point of Finding 1 is that this stays 0 on the cancel path.
	cancelledSaves     int
	cancelledSaveSteps int

	// Failure injection (Finding 2). failTerminalSave fails any Save whose status
	// is terminal (COMPLETED/FAILED/CANCELLED). failAllSaveSteps fails every
	// SaveStep. These simulate a DB that accepts the RUNNING write but then rejects
	// the terminal/step writes — exactly the silent-drift scenario.
	failTerminalSave error
	failAllSaveSteps error
}

func newCtxRespectingExecutionRepo() *ctxRespectingExecutionRepo {
	return &ctxRespectingExecutionRepo{byID: map[string]Execution{}, byKey: map[string]string{}}
}

func (r *ctxRespectingExecutionRepo) Create(ctx context.Context, e Execution, key string) (Execution, error) {
	// Create happens on the caller's (un-cancelled) ctx before the engine runs.
	if err := ctx.Err(); err != nil {
		return Execution{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if key != "" {
		if id, ok := r.byKey[key]; ok {
			return r.byID[id], nil
		}
	}
	r.byID[e.ID] = cloneExec(e)
	if key != "" {
		r.byKey[key] = e.ID
	}
	return e, nil
}

func (r *ctxRespectingExecutionRepo) Save(ctx context.Context, e Execution) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// REAL-ADAPTER BEHAVIOR: a cancelled ctx means the write is rejected. We also
	// record it so the test can assert the engine NEVER persists on a cancelled ctx.
	if ctx.Err() != nil {
		r.cancelledSaves++
		return ctx.Err()
	}
	if r.failTerminalSave != nil && e.Status.IsTerminal() {
		return r.failTerminalSave
	}
	r.byID[e.ID] = cloneExec(e)
	return nil
}

func (r *ctxRespectingExecutionRepo) SaveStep(ctx context.Context, step StepExecution) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ctx.Err() != nil {
		r.cancelledSaveSteps++
		return ctx.Err()
	}
	if r.failAllSaveSteps != nil {
		return r.failAllSaveSteps
	}
	e, ok := r.byID[step.ExecutionID]
	if !ok {
		return ErrRepoNotFound
	}
	found := false
	for i := range e.Steps {
		if e.Steps[i].StepID == step.StepID {
			e.Steps[i] = step
			found = true
			break
		}
	}
	if !found {
		e.Steps = append(e.Steps, step)
	}
	r.byID[step.ExecutionID] = e
	return nil
}

func (r *ctxRespectingExecutionRepo) GetByID(_ context.Context, id string) (Execution, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.byID[id]
	if !ok {
		return Execution{}, ErrRepoNotFound
	}
	return cloneExec(e), nil
}

func (r *ctxRespectingExecutionRepo) List(_ context.Context, f ListExecutionsFilter) ([]Execution, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Execution
	for _, e := range r.byID {
		if f.PipelineID != "" && e.PipelineID != f.PipelineID {
			continue
		}
		if f.Status != ExecutionStatusUnspecified && e.Status != f.Status {
			continue
		}
		out = append(out, cloneExec(e))
	}
	return out, "", nil
}

func (r *ctxRespectingExecutionRepo) cancelledWrites() (saves, steps int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cancelledSaves, r.cancelledSaveSteps
}

// TestCancelExecution_PersistsDurablyOnDetachedContext is the Finding-1 regression
// test. It drives a cancel WHILE the saga is mid-run against a ctx-respecting repo
// and asserts:
//
//	(1) NO write (exec Save or step SaveStep) ever arrived on the cancelled ctx —
//	    every cancel-path write rode a DETACHED, bounded context (the fix);
//	(2) the DURABLE row is CANCELLED (not stuck RUNNING) — i.e. the terminal Save
//	    and COMPENSATING Save actually landed, which on the OLD code they could not
//	    because they ran on the cancelled ctx and the repo would reject them;
//	(3) the completed step is durably COMPENSATED (its checkpoint persisted).
//
// On the PRE-FIX engine this test fails: cancelledSaves >= 1 (the COMPENSATING and
// CANCELLED Saves hit the cancelled ctx) and the durable row stays RUNNING.
func TestCancelExecution_PersistsDurablyOnDetachedContext(t *testing.T) {
	trace := &traceLog{}
	started := make(chan struct{}, 1)

	validate := &recordingExecutor{stepID: "validate", trace: trace}
	gate := &gateExecutor{stepID: "deploy", trace: trace, started: started}
	reg := &gatedRegistry{byType: map[StepType]*gateOrRec{
		StepTypeValidate: {rec: validate},
		StepTypeDeploy:   {gate: gate},
	}}

	pr := newMemPipelineRepo()
	er := newCtxRespectingExecutionRepo()
	pub := &capturingPublisher{}
	svc := NewPipelineService(pr, er, reg, pub, newFakeClock(), &seqIDGen{})

	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "cancel-durable",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "validate", Type: StepTypeValidate, CompensationStepID: "validate"},
			{ID: "deploy", Type: StepTypeDeploy, DependsOn: []string{"validate"}, CompensationStepID: "deploy"},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	type res struct {
		e   Execution
		err error
	}
	done := make(chan res, 1)
	go func() {
		e, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID, IdempotencyKey: "cd-key"})
		done <- res{e, err}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("deploy step never started")
	}
	execs, _, _ := er.List(context.Background(), ListExecutionsFilter{})
	if len(execs) != 1 {
		t.Fatalf("expected 1 in-flight execution, got %d", len(execs))
	}
	execID := execs[0].ID

	if _, err := svc.CancelExecution(context.Background(), testActor, execID, "user cancelled"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	r := <-done
	if r.err != nil {
		t.Fatalf("trigger returned error: %v", r.err)
	}
	if r.e.Status != ExecutionStatusCancelled {
		t.Fatalf("cancelled run should settle to CANCELLED, got %s", r.e.Status)
	}

	// (1) THE CORE ASSERTION: no write hit a cancelled context.
	saves, steps := er.cancelledWrites()
	if saves != 0 || steps != 0 {
		t.Fatalf("Finding 1 regression: %d Save(s) and %d SaveStep(s) hit a CANCELLED context; "+
			"cancel-path writes must use a detached context", saves, steps)
	}

	// (2) The DURABLE row is CANCELLED, not stuck RUNNING.
	stored, err := er.GetByID(context.Background(), execID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if stored.Status != ExecutionStatusCancelled {
		t.Fatalf("durable row must be CANCELLED (not stuck RUNNING), got %s", stored.Status)
	}

	// (3) The completed step is durably COMPENSATED.
	if se := findByStepID(stored.Steps, "validate"); se.Status != StepStatusCompensated {
		t.Fatalf("validate must be durably COMPENSATED after cancel, got %s", se.Status)
	}
}

// TestTriggerExecution_TerminalSaveFailure_IsSurfaced is the Finding-2 regression
// test for the TERMINAL write. The repo accepts the RUNNING Save (failTerminalSave
// only trips on terminal statuses) but rejects the COMPLETED Save. The engine must
// NOT report a clean COMPLETED while the durable row is stale — it must surface
// ErrTerminalPersist so the caller/leader knows the run's durable state is unknown.
//
// On the PRE-FIX engine this fails: settle() did `_ = s.executions.Save(...)` and
// returned (exec, nil), so the caller was told COMPLETED with no error.
func TestTriggerExecution_TerminalSaveFailure_IsSurfaced(t *testing.T) {
	trace := &traceLog{}
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "validate", trace: trace},
	}}
	pr := newMemPipelineRepo()
	er := newCtxRespectingExecutionRepo()
	er.failTerminalSave = errors.New("db down: terminal write rejected")
	pub := &capturingPublisher{}
	svc := NewPipelineService(pr, er, reg, pub, newFakeClock(), &seqIDGen{})

	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "terminal-save-fail",
		Type:  PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{{ID: "validate", Type: StepTypeValidate}},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_, err = svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if !errors.Is(err, ErrTerminalPersist) {
		t.Fatalf("terminal Save failure must surface ErrTerminalPersist, got %v", err)
	}

	// The terminal lifecycle event must NOT have been emitted — a consumer must
	// never see PipelineCompleted for a run whose COMPLETED row did not land.
	if hasEvent(pub.types(), EventPipelineCompleted) {
		t.Fatal("PipelineCompleted must not be emitted when the terminal Save failed")
	}
}

// TestTriggerExecution_TerminalSaveRetries proves the terminal Save is RETRIED
// (it is the run's most important write) rather than abandoned after one failure.
// The repo fails the first two terminal Saves, then succeeds — the engine's
// saveTerminal backoff loop (maxAttempts=3) must land the write and report success.
func TestTriggerExecution_TerminalSaveRetries(t *testing.T) {
	trace := &traceLog{}
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "validate", trace: trace},
	}}
	pr := newMemPipelineRepo()
	er := &flakyTerminalRepo{ctxRespectingExecutionRepo: newCtxRespectingExecutionRepo(), failFirstN: 2}
	pub := &capturingPublisher{}
	svc := NewPipelineService(pr, er, reg, pub, newFakeClock(), &seqIDGen{})

	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name:  "terminal-retry",
		Type:  PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{{ID: "validate", Type: StepTypeValidate}},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	if err != nil {
		t.Fatalf("retried terminal Save should ultimately succeed, got err %v", err)
	}
	if exec.Status != ExecutionStatusCompleted {
		t.Fatalf("want COMPLETED after terminal retry, got %s", exec.Status)
	}
	if got := er.terminalAttempts(); got != 3 {
		t.Fatalf("expected 3 terminal Save attempts (2 fail + 1 ok), got %d", got)
	}
}

// flakyTerminalRepo fails the first N TERMINAL Saves, then succeeds — for the
// retry test. It counts terminal-Save attempts so the test can assert the backoff
// loop actually retried.
type flakyTerminalRepo struct {
	*ctxRespectingExecutionRepo
	mu           sync.Mutex
	failFirstN   int
	terminalSeen int
}

func (r *flakyTerminalRepo) Save(ctx context.Context, e Execution) error {
	if ctx.Err() != nil {
		// Defer to embedded behavior (counts cancelled writes).
		return r.ctxRespectingExecutionRepo.Save(ctx, e)
	}
	if e.Status.IsTerminal() {
		r.mu.Lock()
		r.terminalSeen++
		fail := r.terminalSeen <= r.failFirstN
		r.mu.Unlock()
		if fail {
			return errors.New("transient terminal write failure")
		}
	}
	return r.ctxRespectingExecutionRepo.Save(ctx, e)
}

func (r *flakyTerminalRepo) terminalAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.terminalSeen
}

// TestTriggerExecution_StepCheckpointFailure_IsSurfaced is the Finding-2
// regression test for STEP checkpoints. Every SaveStep fails; the run still
// reaches a terminal STATUS (the in-memory state machine advances and the terminal
// Save succeeds), but the engine must surface ErrStepPersist so the caller knows
// the durable per-step history is incomplete — instead of swallowing it with `_ =`.
func TestTriggerExecution_StepCheckpointFailure_IsSurfaced(t *testing.T) {
	trace := &traceLog{}
	reg := &perStepRegistry{byType: map[StepType]*recordingExecutor{
		StepTypeValidate: {stepID: "validate", trace: trace},
		StepTypeDeploy:   {stepID: "deploy", trace: trace},
	}}
	pr := newMemPipelineRepo()
	er := newCtxRespectingExecutionRepo()
	er.failAllSaveSteps = errors.New("db down: step checkpoint rejected")
	pub := &capturingPublisher{}
	svc := NewPipelineService(pr, er, reg, pub, newFakeClock(), &seqIDGen{})

	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "step-save-fail",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "validate", Type: StepTypeValidate},
			{ID: "deploy", Type: StepTypeDeploy, DependsOn: []string{"validate"}},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	exec, err := svc.TriggerExecution(context.Background(), testActor, TriggerInput{PipelineID: p.ID})
	// The terminal STATUS is still trustworthy (the in-memory machine + terminal
	// Save succeeded), but the step checkpoints did not land → ErrStepPersist.
	if !errors.Is(err, ErrStepPersist) {
		t.Fatalf("SaveStep failures must surface ErrStepPersist, got %v", err)
	}
	if exec.Status != ExecutionStatusCompleted {
		t.Fatalf("status should still be COMPLETED (terminal Save ok), got %s", exec.Status)
	}
}

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

func mustCreateSaga(t *testing.T, svc *pipelineService) string {
	t.Helper()
	p, err := svc.CreatePipeline(context.Background(), testActor, CreatePipelineInput{
		Name: "deploy-saga",
		Type: PipelineTypeDeploymentSaga,
		Steps: []StepDefinition{
			{ID: "validate", Type: StepTypeValidate, CompensationStepID: "validate"},
			{ID: "deploy", Type: StepTypeDeploy, DependsOn: []string{"validate"}, CompensationStepID: "deploy"},
			{ID: "canary", Type: StepTypeCanary, DependsOn: []string{"deploy"}, CompensationStepID: "canary"},
			{ID: "promote", Type: StepTypePromote, DependsOn: []string{"canary"}, CompensationStepID: "promote"},
		},
	})
	if err != nil {
		t.Fatalf("create saga: %v", err)
	}
	return p.ID
}

func equalSeq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func containsSeq(a []string, want string) bool {
	for _, x := range a {
		if x == want {
			return true
		}
	}
	return false
}

func findByStepID(steps []StepExecution, id string) StepExecution {
	for _, s := range steps {
		if s.StepID == id {
			return s
		}
	}
	return StepExecution{}
}
