// ============================================================================
// Forgepoint Experiment Tracker Service Proto Definitions
// ============================================================================
//
// WHY: ML is empirical. You don't "write" a good model — you run dozens of
// experiments (different hyperparameters, features, architectures) and keep the
// one whose metrics win. The Experiment Tracker is the system of record for
// that loop: it stores every run's PARAMETERS (the knobs you set) and METRICS
// (the time-series numbers the run produced — loss per step, accuracy per
// epoch), and lets you COMPARE runs to pick a winner. This is Forgepoint's
// MLflow / Weights & Biases.
//
// WHAT'S HERE:
//   - Domain messages: Experiment, Run, Param, MetricPoint, MetricSeries
//   - RPCs: CreateExperiment/ListExperiments/GetExperiment/ArchiveExperiment,
//           StartRun/UpdateRunStatus/GetRun/ListRuns/DeleteRun,
//           LogMetrics (BATCH), LogParams, GetMetricHistory, CompareRuns,
//           SetRunArtifacts (free-form attachments)
//
// WHAT'S NOT HERE (deliberately): the event PAYLOAD messages this service
// publishes/consumes. They are NOT redefined locally — they live in the SINGLE
// canonical contract forgepoint/events/v1/events.proto and are imported below.
// This service PUBLISHES events.RunCreated (fp.experiments.run.created) and
// events.RunFinished (fp.experiments.run.finished); it CONSUMES a wide slice of
// the platform firehose (see the async-consumer note at the service definition).
// Defining event payloads here once led to producer/consumer schema drift across
// services (e.g. FeaturesWritten vs FeaturesIngested) — the events package fixes
// that by being the one place both ends of every pipe agree on.
//
// ============================================================================
// PATTERN — Event-Driven (Async Batch Ingestion)
// ============================================================================
//
// This service's defining trait is HOW metrics get in, not just the gRPC API.
// Two ingestion paths exist and the asymmetry is the whole lesson:
//
//   1) SYNC (this proto, the LogMetrics RPC): a training job's SDK calls
//      LogMetrics directly to push the loss/accuracy curve it's producing.
//      Used while a human or a training pod is actively driving a run.
//
//   2) ASYNC (NOT in this proto — it's a NATS consumer): the tracker also
//      SUBSCRIBES to high-volume platform events (the canonical subjects, see
//      forgepoint/events/v1/events.proto) — fp.inference.completed,
//      fp.models.version.created, fp.pipelines.step.completed,
//      fp.pipelines.completed, fp.features.written, fp.billing.usage.recorded,
//      fp.models.drift.detected, fp.pipelines.model.deployed,
//      fp.notifications.delivered/failed — buffers them in memory, and FLUSHES
//      to Postgres in batches (every N ms or every M events, whichever first).
//      If the DB falls behind, the buffer fills and the consumer NAKs / stops
//      acking, which makes NATS JetStream slow delivery: that is BACK-PRESSURE.
//
//      CRITICAL SUBJECT FIXES (event-contract alignment): the async consumer
//      subscribes to fp.features.WRITTEN (NOT the old fp.features.ingested — that
//      subject has no producer and would never deliver) and to the PLURAL
//      fp.pipelines.step.completed (NOT fp.pipeline.step.completed). These were
//      reconciled against events.proto so every subject we subscribe to actually
//      has a matching producer.
//
// WHY ASYNC FOR THE HOT PATH:
//   Inference happens thousands of times per second. If the inference gateway
//   had to make a synchronous "record this metric" RPC to the tracker on every
//   prediction, the tracker's latency and availability would be IN the serving
//   hot loop — a slow tracker would slow every prediction. By moving that to
//   fire-and-forget NATS events, serving never blocks on tracking. The tracker
//   absorbs spikes in its buffer and writes efficiently in batches (one
//   multi-row INSERT beats thousands of single-row INSERTs).
//
// WHY KEEP A SYNC LogMetrics RPC AT ALL THEN:
//   A training job legitimately wants its curve recorded promptly and wants an
//   immediate ack ("did my metric land?"). For that first-party, lower-volume
//   producer, a direct RPC is simpler than forcing it onto the event bus. The
//   RPC is still BATCH (repeated MetricPoint) so a job can flush 500 points in
//   one call — the same batching idea, just client-initiated.
//
// REAL-WORLD COMPARISON:
//   - MLflow: tracking server with log_metric/log_param; batches via log_batch.
//   - Weights & Biases: agent buffers locally and ships batches to the backend.
//   - Kafka Connect / ClickHouse async inserts: same batch-on-ingest idea for
//     high-cardinality time-series.
//
// DESIGN NOTES:
//   - "How do you handle metric volume?" → batch consumer + back-pressure via
//     NAK, time-partitioned Postgres table for the metrics.
//   - "Why is LogMetrics a unary batch RPC and not a client-streaming RPC?"
//     → see the LogMetrics comment; the short answer is idempotent retry +
//     a single transactional flush beat a long-lived stream for this shape.
//   - "How do runs connect to the rest of the platform?" → a Run carries a
//     model_version_id linking the experiment that produced a model to the
//     model artifact in the Registry, WITHOUT importing the registry proto.
//
// VERSIONING: Package path includes v1 (Buf/Google convention). Breaking
// changes require a new forgepoint.experiment.v2 package.
// ============================================================================

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.6.2
// - protoc             (unknown)
// source: forgepoint/experiment/v1/experiment.proto

package experimentv1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	ExperimentTrackerService_CreateExperiment_FullMethodName  = "/forgepoint.experiment.v1.ExperimentTrackerService/CreateExperiment"
	ExperimentTrackerService_GetExperiment_FullMethodName     = "/forgepoint.experiment.v1.ExperimentTrackerService/GetExperiment"
	ExperimentTrackerService_ListExperiments_FullMethodName   = "/forgepoint.experiment.v1.ExperimentTrackerService/ListExperiments"
	ExperimentTrackerService_UpdateExperiment_FullMethodName  = "/forgepoint.experiment.v1.ExperimentTrackerService/UpdateExperiment"
	ExperimentTrackerService_ArchiveExperiment_FullMethodName = "/forgepoint.experiment.v1.ExperimentTrackerService/ArchiveExperiment"
	ExperimentTrackerService_StartRun_FullMethodName          = "/forgepoint.experiment.v1.ExperimentTrackerService/StartRun"
	ExperimentTrackerService_UpdateRunStatus_FullMethodName   = "/forgepoint.experiment.v1.ExperimentTrackerService/UpdateRunStatus"
	ExperimentTrackerService_GetRun_FullMethodName            = "/forgepoint.experiment.v1.ExperimentTrackerService/GetRun"
	ExperimentTrackerService_ListRuns_FullMethodName          = "/forgepoint.experiment.v1.ExperimentTrackerService/ListRuns"
	ExperimentTrackerService_DeleteRun_FullMethodName         = "/forgepoint.experiment.v1.ExperimentTrackerService/DeleteRun"
	ExperimentTrackerService_LogMetrics_FullMethodName        = "/forgepoint.experiment.v1.ExperimentTrackerService/LogMetrics"
	ExperimentTrackerService_LogParams_FullMethodName         = "/forgepoint.experiment.v1.ExperimentTrackerService/LogParams"
	ExperimentTrackerService_SetRunArtifacts_FullMethodName   = "/forgepoint.experiment.v1.ExperimentTrackerService/SetRunArtifacts"
	ExperimentTrackerService_GetMetricHistory_FullMethodName  = "/forgepoint.experiment.v1.ExperimentTrackerService/GetMetricHistory"
	ExperimentTrackerService_CompareRuns_FullMethodName       = "/forgepoint.experiment.v1.ExperimentTrackerService/CompareRuns"
)

// ExperimentTrackerServiceClient is the client API for ExperimentTrackerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// ============================================================================
// EXPERIMENT TRACKER SERVICE
// ============================================================================
//
// WHY a single service boundary: experiments, runs, params, and metrics form
// one cohesive aggregate (a run is meaningless outside its experiment; metrics
// are meaningless outside their run). Splitting them would force inter-service
// calls for every common flow. One service owns the fp_experiment database and
// its time-partitioned metrics table.
//
// RPC CATEGORIES:
//
//	EXPERIMENTS: CreateExperiment, GetExperiment, ListExperiments,
//	             UpdateExperiment, ArchiveExperiment (soft delete)
//	RUN LIFECYCLE: StartRun, UpdateRunStatus, GetRun, ListRuns, DeleteRun
//	INGESTION (sync path): LogMetrics (batch), LogParams, SetRunArtifacts
//	ANALYSIS / READ: GetMetricHistory (paginated curve), CompareRuns
//
// NOT IN THIS PROTO (by design): the NATS batch consumer that ingests the
// canonical subjects — fp.inference.completed / fp.models.version.created /
// fp.pipelines.step.completed / fp.pipelines.completed / fp.features.written /
// fp.billing.usage.recorded / fp.models.drift.detected /
// fp.pipelines.model.deployed / fp.notifications.delivered|failed. That's the
// async, high-volume path with back-pressure — it has no RPC surface; it's wired
// in the service's events package (decoding events.* payloads). The gRPC API
// above is the sync, first-party + read/analysis surface. Keeping the hot loop
// OFF gRPC is the entire point of the event-driven pattern.
//
// SUBJECT-NAME FIXES baked into the list above (vs the old draft): plural
// fp.pipelines.step.completed (was fp.pipeline.*) and fp.features.WRITTEN (was
// the producerless fp.features.ingested) — reconciled against events.proto.
//
// ASCII — Dual ingestion (the pattern in one picture):
//
//	Training job ──gRPC StartRun/LogMetrics(batch)──► [Experiment Tracker] ──► Postgres
//	                                                       ▲   (time-partitioned
//	                                                       │    metrics table)
//	Inference GW ─┐                                        │
//	Pipeline Orch ┼─ NATS events ─► [batch consumer: buffer → flush every N/M]
//	Registry ─────┘                  (NAK on overload = back-pressure)
//
// ============================================================================
type ExperimentTrackerServiceClient interface {
	// CreateExperiment creates a named grouping of runs. owner_id/team are set
	// from the caller's auth claims (not the request). Requires experiments:write.
	CreateExperiment(ctx context.Context, in *CreateExperimentRequest, opts ...grpc.CallOption) (*CreateExperimentResponse, error)
	// GetExperiment fetches one experiment by ID (scoped to the caller's team).
	GetExperiment(ctx context.Context, in *GetExperimentRequest, opts ...grpc.CallOption) (*GetExperimentResponse, error)
	// ListExperiments returns a paginated list (cursor-based, page size capped
	// server-side at 100). team_filter only NARROWS within the caller's permitted
	// teams; include_archived toggles soft-deleted rows.
	ListExperiments(ctx context.Context, in *ListExperimentsRequest, opts ...grpc.CallOption) (*ListExperimentsResponse, error)
	// UpdateExperiment edits mutable fields (name/description/tags) via a field
	// mask. owner_id/team/timestamps are server-authoritative and cannot be set.
	UpdateExperiment(ctx context.Context, in *UpdateExperimentRequest, opts ...grpc.CallOption) (*UpdateExperimentResponse, error)
	// ArchiveExperiment SOFT-deletes an experiment (sets archived_at, hides from
	// default lists) while preserving its runs/metrics for lineage/audit.
	ArchiveExperiment(ctx context.Context, in *ArchiveExperimentRequest, opts ...grpc.CallOption) (*ArchiveExperimentResponse, error)
	// StartRun opens a new run (status RUNNING) in an experiment. Supports an
	// idempotency_key so retries don't create duplicate runs. This is the
	// design doc's StartRun/CreateRun. On success the server PUBLISHES
	// events.RunCreated (fp.experiments.run.created).
	StartRun(ctx context.Context, in *StartRunRequest, opts ...grpc.CallOption) (*StartRunResponse, error)
	// UpdateRunStatus transitions a run to a terminal state (FINISHED/FAILED/
	// KILLED). Server validates the state-machine transition, stamps ended_at, and
	// PUBLISHES events.RunFinished (fp.experiments.run.finished) carrying the
	// run's final_metrics (fat event — consumers rank/alert with no callback).
	UpdateRunStatus(ctx context.Context, in *UpdateRunStatusRequest, opts ...grpc.CallOption) (*UpdateRunStatusResponse, error)
	// GetRun fetches a run's metadata, params, headline metrics, and artifacts
	// (NOT the full metric time-series — use GetMetricHistory for the curve).
	GetRun(ctx context.Context, in *GetRunRequest, opts ...grpc.CallOption) (*GetRunResponse, error)
	// ListRuns returns a paginated list of runs, normally within one experiment,
	// optionally filtered by status. Cursor-based; page size capped server-side.
	ListRuns(ctx context.Context, in *ListRunsRequest, opts ...grpc.CallOption) (*ListRunsResponse, error)
	// DeleteRun hard-deletes one run + its metrics/params (pruning experimental
	// noise). Rejected if the run's model_version_id is still referenced by a live
	// Registry version (lineage guard); idempotent via idempotency_key.
	DeleteRun(ctx context.Context, in *DeleteRunRequest, opts ...grpc.CallOption) (*DeleteRunResponse, error)
	// LogMetrics is the high-throughput BATCH ingestion RPC: a training job
	// flushes many MetricPoints in one idempotent, transactional call. Timestamps
	// are server-stamped; batch size capped server-side. See the message comment
	// for why this is unary-batch rather than client-streaming.
	LogMetrics(ctx context.Context, in *LogMetricsRequest, opts ...grpc.CallOption) (*LogMetricsResponse, error)
	// LogParams appends write-once hyperparameters to a RUNNING run.
	LogParams(ctx context.Context, in *LogParamsRequest, opts ...grpc.CallOption) (*LogParamsResponse, error)
	// SetRunArtifacts attaches free-form JSON-shaped artifacts (confusion matrix,
	// manifest, eval report) to a run. Struct payload, server-size-capped.
	SetRunArtifacts(ctx context.Context, in *SetRunArtifactsRequest, opts ...grpc.CallOption) (*SetRunArtifactsResponse, error)
	// GetMetricHistory returns the FULL metric time-series for ONE run, PAGINATED
	// (page size capped server-side, default 1000 / max 5000) with optional key
	// and step-window filters. This is the "metric-history read" the rest of the
	// proto refers to — the pull-shaped curve read GetRun deliberately omits.
	GetMetricHistory(ctx context.Context, in *GetMetricHistoryRequest, opts ...grpc.CallOption) (*GetMetricHistoryResponse, error)
	// CompareRuns returns the metric series of several runs side-by-side — the
	// data behind "which run/model version wins?". Run count and payload bounded
	// server-side.
	CompareRuns(ctx context.Context, in *CompareRunsRequest, opts ...grpc.CallOption) (*CompareRunsResponse, error)
}

type experimentTrackerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewExperimentTrackerServiceClient(cc grpc.ClientConnInterface) ExperimentTrackerServiceClient {
	return &experimentTrackerServiceClient{cc}
}

func (c *experimentTrackerServiceClient) CreateExperiment(ctx context.Context, in *CreateExperimentRequest, opts ...grpc.CallOption) (*CreateExperimentResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CreateExperimentResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_CreateExperiment_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) GetExperiment(ctx context.Context, in *GetExperimentRequest, opts ...grpc.CallOption) (*GetExperimentResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetExperimentResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_GetExperiment_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) ListExperiments(ctx context.Context, in *ListExperimentsRequest, opts ...grpc.CallOption) (*ListExperimentsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListExperimentsResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_ListExperiments_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) UpdateExperiment(ctx context.Context, in *UpdateExperimentRequest, opts ...grpc.CallOption) (*UpdateExperimentResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UpdateExperimentResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_UpdateExperiment_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) ArchiveExperiment(ctx context.Context, in *ArchiveExperimentRequest, opts ...grpc.CallOption) (*ArchiveExperimentResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ArchiveExperimentResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_ArchiveExperiment_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) StartRun(ctx context.Context, in *StartRunRequest, opts ...grpc.CallOption) (*StartRunResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(StartRunResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_StartRun_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) UpdateRunStatus(ctx context.Context, in *UpdateRunStatusRequest, opts ...grpc.CallOption) (*UpdateRunStatusResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UpdateRunStatusResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_UpdateRunStatus_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) GetRun(ctx context.Context, in *GetRunRequest, opts ...grpc.CallOption) (*GetRunResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetRunResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_GetRun_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) ListRuns(ctx context.Context, in *ListRunsRequest, opts ...grpc.CallOption) (*ListRunsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ListRunsResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_ListRuns_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) DeleteRun(ctx context.Context, in *DeleteRunRequest, opts ...grpc.CallOption) (*DeleteRunResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeleteRunResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_DeleteRun_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) LogMetrics(ctx context.Context, in *LogMetricsRequest, opts ...grpc.CallOption) (*LogMetricsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LogMetricsResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_LogMetrics_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) LogParams(ctx context.Context, in *LogParamsRequest, opts ...grpc.CallOption) (*LogParamsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LogParamsResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_LogParams_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) SetRunArtifacts(ctx context.Context, in *SetRunArtifactsRequest, opts ...grpc.CallOption) (*SetRunArtifactsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(SetRunArtifactsResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_SetRunArtifacts_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) GetMetricHistory(ctx context.Context, in *GetMetricHistoryRequest, opts ...grpc.CallOption) (*GetMetricHistoryResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetMetricHistoryResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_GetMetricHistory_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *experimentTrackerServiceClient) CompareRuns(ctx context.Context, in *CompareRunsRequest, opts ...grpc.CallOption) (*CompareRunsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CompareRunsResponse)
	err := c.cc.Invoke(ctx, ExperimentTrackerService_CompareRuns_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExperimentTrackerServiceServer is the server API for ExperimentTrackerService service.
// All implementations must embed UnimplementedExperimentTrackerServiceServer
// for forward compatibility.
//
// ============================================================================
// EXPERIMENT TRACKER SERVICE
// ============================================================================
//
// WHY a single service boundary: experiments, runs, params, and metrics form
// one cohesive aggregate (a run is meaningless outside its experiment; metrics
// are meaningless outside their run). Splitting them would force inter-service
// calls for every common flow. One service owns the fp_experiment database and
// its time-partitioned metrics table.
//
// RPC CATEGORIES:
//
//	EXPERIMENTS: CreateExperiment, GetExperiment, ListExperiments,
//	             UpdateExperiment, ArchiveExperiment (soft delete)
//	RUN LIFECYCLE: StartRun, UpdateRunStatus, GetRun, ListRuns, DeleteRun
//	INGESTION (sync path): LogMetrics (batch), LogParams, SetRunArtifacts
//	ANALYSIS / READ: GetMetricHistory (paginated curve), CompareRuns
//
// NOT IN THIS PROTO (by design): the NATS batch consumer that ingests the
// canonical subjects — fp.inference.completed / fp.models.version.created /
// fp.pipelines.step.completed / fp.pipelines.completed / fp.features.written /
// fp.billing.usage.recorded / fp.models.drift.detected /
// fp.pipelines.model.deployed / fp.notifications.delivered|failed. That's the
// async, high-volume path with back-pressure — it has no RPC surface; it's wired
// in the service's events package (decoding events.* payloads). The gRPC API
// above is the sync, first-party + read/analysis surface. Keeping the hot loop
// OFF gRPC is the entire point of the event-driven pattern.
//
// SUBJECT-NAME FIXES baked into the list above (vs the old draft): plural
// fp.pipelines.step.completed (was fp.pipeline.*) and fp.features.WRITTEN (was
// the producerless fp.features.ingested) — reconciled against events.proto.
//
// ASCII — Dual ingestion (the pattern in one picture):
//
//	Training job ──gRPC StartRun/LogMetrics(batch)──► [Experiment Tracker] ──► Postgres
//	                                                       ▲   (time-partitioned
//	                                                       │    metrics table)
//	Inference GW ─┐                                        │
//	Pipeline Orch ┼─ NATS events ─► [batch consumer: buffer → flush every N/M]
//	Registry ─────┘                  (NAK on overload = back-pressure)
//
// ============================================================================
type ExperimentTrackerServiceServer interface {
	// CreateExperiment creates a named grouping of runs. owner_id/team are set
	// from the caller's auth claims (not the request). Requires experiments:write.
	CreateExperiment(context.Context, *CreateExperimentRequest) (*CreateExperimentResponse, error)
	// GetExperiment fetches one experiment by ID (scoped to the caller's team).
	GetExperiment(context.Context, *GetExperimentRequest) (*GetExperimentResponse, error)
	// ListExperiments returns a paginated list (cursor-based, page size capped
	// server-side at 100). team_filter only NARROWS within the caller's permitted
	// teams; include_archived toggles soft-deleted rows.
	ListExperiments(context.Context, *ListExperimentsRequest) (*ListExperimentsResponse, error)
	// UpdateExperiment edits mutable fields (name/description/tags) via a field
	// mask. owner_id/team/timestamps are server-authoritative and cannot be set.
	UpdateExperiment(context.Context, *UpdateExperimentRequest) (*UpdateExperimentResponse, error)
	// ArchiveExperiment SOFT-deletes an experiment (sets archived_at, hides from
	// default lists) while preserving its runs/metrics for lineage/audit.
	ArchiveExperiment(context.Context, *ArchiveExperimentRequest) (*ArchiveExperimentResponse, error)
	// StartRun opens a new run (status RUNNING) in an experiment. Supports an
	// idempotency_key so retries don't create duplicate runs. This is the
	// design doc's StartRun/CreateRun. On success the server PUBLISHES
	// events.RunCreated (fp.experiments.run.created).
	StartRun(context.Context, *StartRunRequest) (*StartRunResponse, error)
	// UpdateRunStatus transitions a run to a terminal state (FINISHED/FAILED/
	// KILLED). Server validates the state-machine transition, stamps ended_at, and
	// PUBLISHES events.RunFinished (fp.experiments.run.finished) carrying the
	// run's final_metrics (fat event — consumers rank/alert with no callback).
	UpdateRunStatus(context.Context, *UpdateRunStatusRequest) (*UpdateRunStatusResponse, error)
	// GetRun fetches a run's metadata, params, headline metrics, and artifacts
	// (NOT the full metric time-series — use GetMetricHistory for the curve).
	GetRun(context.Context, *GetRunRequest) (*GetRunResponse, error)
	// ListRuns returns a paginated list of runs, normally within one experiment,
	// optionally filtered by status. Cursor-based; page size capped server-side.
	ListRuns(context.Context, *ListRunsRequest) (*ListRunsResponse, error)
	// DeleteRun hard-deletes one run + its metrics/params (pruning experimental
	// noise). Rejected if the run's model_version_id is still referenced by a live
	// Registry version (lineage guard); idempotent via idempotency_key.
	DeleteRun(context.Context, *DeleteRunRequest) (*DeleteRunResponse, error)
	// LogMetrics is the high-throughput BATCH ingestion RPC: a training job
	// flushes many MetricPoints in one idempotent, transactional call. Timestamps
	// are server-stamped; batch size capped server-side. See the message comment
	// for why this is unary-batch rather than client-streaming.
	LogMetrics(context.Context, *LogMetricsRequest) (*LogMetricsResponse, error)
	// LogParams appends write-once hyperparameters to a RUNNING run.
	LogParams(context.Context, *LogParamsRequest) (*LogParamsResponse, error)
	// SetRunArtifacts attaches free-form JSON-shaped artifacts (confusion matrix,
	// manifest, eval report) to a run. Struct payload, server-size-capped.
	SetRunArtifacts(context.Context, *SetRunArtifactsRequest) (*SetRunArtifactsResponse, error)
	// GetMetricHistory returns the FULL metric time-series for ONE run, PAGINATED
	// (page size capped server-side, default 1000 / max 5000) with optional key
	// and step-window filters. This is the "metric-history read" the rest of the
	// proto refers to — the pull-shaped curve read GetRun deliberately omits.
	GetMetricHistory(context.Context, *GetMetricHistoryRequest) (*GetMetricHistoryResponse, error)
	// CompareRuns returns the metric series of several runs side-by-side — the
	// data behind "which run/model version wins?". Run count and payload bounded
	// server-side.
	CompareRuns(context.Context, *CompareRunsRequest) (*CompareRunsResponse, error)
	mustEmbedUnimplementedExperimentTrackerServiceServer()
}

// UnimplementedExperimentTrackerServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedExperimentTrackerServiceServer struct{}

func (UnimplementedExperimentTrackerServiceServer) CreateExperiment(context.Context, *CreateExperimentRequest) (*CreateExperimentResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CreateExperiment not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) GetExperiment(context.Context, *GetExperimentRequest) (*GetExperimentResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetExperiment not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) ListExperiments(context.Context, *ListExperimentsRequest) (*ListExperimentsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListExperiments not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) UpdateExperiment(context.Context, *UpdateExperimentRequest) (*UpdateExperimentResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method UpdateExperiment not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) ArchiveExperiment(context.Context, *ArchiveExperimentRequest) (*ArchiveExperimentResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ArchiveExperiment not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) StartRun(context.Context, *StartRunRequest) (*StartRunResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method StartRun not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) UpdateRunStatus(context.Context, *UpdateRunStatusRequest) (*UpdateRunStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method UpdateRunStatus not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) GetRun(context.Context, *GetRunRequest) (*GetRunResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetRun not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) ListRuns(context.Context, *ListRunsRequest) (*ListRunsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListRuns not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) DeleteRun(context.Context, *DeleteRunRequest) (*DeleteRunResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method DeleteRun not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) LogMetrics(context.Context, *LogMetricsRequest) (*LogMetricsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method LogMetrics not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) LogParams(context.Context, *LogParamsRequest) (*LogParamsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method LogParams not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) SetRunArtifacts(context.Context, *SetRunArtifactsRequest) (*SetRunArtifactsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SetRunArtifacts not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) GetMetricHistory(context.Context, *GetMetricHistoryRequest) (*GetMetricHistoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetMetricHistory not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) CompareRuns(context.Context, *CompareRunsRequest) (*CompareRunsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method CompareRuns not implemented")
}
func (UnimplementedExperimentTrackerServiceServer) mustEmbedUnimplementedExperimentTrackerServiceServer() {
}
func (UnimplementedExperimentTrackerServiceServer) testEmbeddedByValue() {}

// UnsafeExperimentTrackerServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ExperimentTrackerServiceServer will
// result in compilation errors.
type UnsafeExperimentTrackerServiceServer interface {
	mustEmbedUnimplementedExperimentTrackerServiceServer()
}

func RegisterExperimentTrackerServiceServer(s grpc.ServiceRegistrar, srv ExperimentTrackerServiceServer) {
	// If the following call panics, it indicates UnimplementedExperimentTrackerServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ExperimentTrackerService_ServiceDesc, srv)
}

func _ExperimentTrackerService_CreateExperiment_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateExperimentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).CreateExperiment(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_CreateExperiment_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).CreateExperiment(ctx, req.(*CreateExperimentRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_GetExperiment_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetExperimentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).GetExperiment(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_GetExperiment_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).GetExperiment(ctx, req.(*GetExperimentRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_ListExperiments_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListExperimentsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).ListExperiments(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_ListExperiments_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).ListExperiments(ctx, req.(*ListExperimentsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_UpdateExperiment_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateExperimentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).UpdateExperiment(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_UpdateExperiment_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).UpdateExperiment(ctx, req.(*UpdateExperimentRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_ArchiveExperiment_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ArchiveExperimentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).ArchiveExperiment(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_ArchiveExperiment_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).ArchiveExperiment(ctx, req.(*ArchiveExperimentRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_StartRun_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartRunRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).StartRun(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_StartRun_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).StartRun(ctx, req.(*StartRunRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_UpdateRunStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateRunStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).UpdateRunStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_UpdateRunStatus_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).UpdateRunStatus(ctx, req.(*UpdateRunStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_GetRun_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRunRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).GetRun(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_GetRun_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).GetRun(ctx, req.(*GetRunRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_ListRuns_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListRunsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).ListRuns(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_ListRuns_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).ListRuns(ctx, req.(*ListRunsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_DeleteRun_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRunRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).DeleteRun(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_DeleteRun_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).DeleteRun(ctx, req.(*DeleteRunRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_LogMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LogMetricsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).LogMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_LogMetrics_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).LogMetrics(ctx, req.(*LogMetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_LogParams_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LogParamsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).LogParams(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_LogParams_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).LogParams(ctx, req.(*LogParamsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_SetRunArtifacts_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetRunArtifactsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).SetRunArtifacts(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_SetRunArtifacts_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).SetRunArtifacts(ctx, req.(*SetRunArtifactsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_GetMetricHistory_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetMetricHistoryRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).GetMetricHistory(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_GetMetricHistory_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).GetMetricHistory(ctx, req.(*GetMetricHistoryRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ExperimentTrackerService_CompareRuns_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CompareRunsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExperimentTrackerServiceServer).CompareRuns(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ExperimentTrackerService_CompareRuns_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExperimentTrackerServiceServer).CompareRuns(ctx, req.(*CompareRunsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ExperimentTrackerService_ServiceDesc is the grpc.ServiceDesc for ExperimentTrackerService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ExperimentTrackerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "forgepoint.experiment.v1.ExperimentTrackerService",
	HandlerType: (*ExperimentTrackerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateExperiment",
			Handler:    _ExperimentTrackerService_CreateExperiment_Handler,
		},
		{
			MethodName: "GetExperiment",
			Handler:    _ExperimentTrackerService_GetExperiment_Handler,
		},
		{
			MethodName: "ListExperiments",
			Handler:    _ExperimentTrackerService_ListExperiments_Handler,
		},
		{
			MethodName: "UpdateExperiment",
			Handler:    _ExperimentTrackerService_UpdateExperiment_Handler,
		},
		{
			MethodName: "ArchiveExperiment",
			Handler:    _ExperimentTrackerService_ArchiveExperiment_Handler,
		},
		{
			MethodName: "StartRun",
			Handler:    _ExperimentTrackerService_StartRun_Handler,
		},
		{
			MethodName: "UpdateRunStatus",
			Handler:    _ExperimentTrackerService_UpdateRunStatus_Handler,
		},
		{
			MethodName: "GetRun",
			Handler:    _ExperimentTrackerService_GetRun_Handler,
		},
		{
			MethodName: "ListRuns",
			Handler:    _ExperimentTrackerService_ListRuns_Handler,
		},
		{
			MethodName: "DeleteRun",
			Handler:    _ExperimentTrackerService_DeleteRun_Handler,
		},
		{
			MethodName: "LogMetrics",
			Handler:    _ExperimentTrackerService_LogMetrics_Handler,
		},
		{
			MethodName: "LogParams",
			Handler:    _ExperimentTrackerService_LogParams_Handler,
		},
		{
			MethodName: "SetRunArtifacts",
			Handler:    _ExperimentTrackerService_SetRunArtifacts_Handler,
		},
		{
			MethodName: "GetMetricHistory",
			Handler:    _ExperimentTrackerService_GetMetricHistory_Handler,
		},
		{
			MethodName: "CompareRuns",
			Handler:    _ExperimentTrackerService_CompareRuns_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "forgepoint/experiment/v1/experiment.proto",
}
