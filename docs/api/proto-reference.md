# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [forgepoint/common/v1/common.proto](#forgepoint_common_v1_common-proto)
    - [ErrorDetail](#forgepoint-common-v1-ErrorDetail)
    - [EventEnvelope](#forgepoint-common-v1-EventEnvelope)
    - [PaginationRequest](#forgepoint-common-v1-PaginationRequest)
    - [PaginationResponse](#forgepoint-common-v1-PaginationResponse)
  
- [forgepoint/auth/v1/auth.proto](#forgepoint_auth_v1_auth-proto)
    - [APIKey](#forgepoint-auth-v1-APIKey)
    - [AssignRoleRequest](#forgepoint-auth-v1-AssignRoleRequest)
    - [AssignRoleResponse](#forgepoint-auth-v1-AssignRoleResponse)
    - [CheckPermissionRequest](#forgepoint-auth-v1-CheckPermissionRequest)
    - [CheckPermissionResponse](#forgepoint-auth-v1-CheckPermissionResponse)
    - [CreateAPIKeyRequest](#forgepoint-auth-v1-CreateAPIKeyRequest)
    - [CreateAPIKeyResponse](#forgepoint-auth-v1-CreateAPIKeyResponse)
    - [CreateUserRequest](#forgepoint-auth-v1-CreateUserRequest)
    - [CreateUserResponse](#forgepoint-auth-v1-CreateUserResponse)
    - [ListUsersRequest](#forgepoint-auth-v1-ListUsersRequest)
    - [ListUsersResponse](#forgepoint-auth-v1-ListUsersResponse)
    - [LoginRequest](#forgepoint-auth-v1-LoginRequest)
    - [LoginResponse](#forgepoint-auth-v1-LoginResponse)
    - [Permission](#forgepoint-auth-v1-Permission)
    - [RevokeAPIKeyRequest](#forgepoint-auth-v1-RevokeAPIKeyRequest)
    - [RevokeAPIKeyResponse](#forgepoint-auth-v1-RevokeAPIKeyResponse)
    - [Role](#forgepoint-auth-v1-Role)
    - [TokenClaims](#forgepoint-auth-v1-TokenClaims)
    - [User](#forgepoint-auth-v1-User)
    - [ValidateTokenRequest](#forgepoint-auth-v1-ValidateTokenRequest)
    - [ValidateTokenResponse](#forgepoint-auth-v1-ValidateTokenResponse)
  
    - [AuthService](#forgepoint-auth-v1-AuthService)
  
- [forgepoint/billing/v1/billing.proto](#forgepoint_billing_v1_billing-proto)
    - [CheckQuotaRequest](#forgepoint-billing-v1-CheckQuotaRequest)
    - [CheckQuotaResponse](#forgepoint-billing-v1-CheckQuotaResponse)
    - [CreateRatePlanRequest](#forgepoint-billing-v1-CreateRatePlanRequest)
    - [CreateRatePlanRequest.IncludedQuantitiesEntry](#forgepoint-billing-v1-CreateRatePlanRequest-IncludedQuantitiesEntry)
    - [CreateRatePlanRequest.QuotaLimitsEntry](#forgepoint-billing-v1-CreateRatePlanRequest-QuotaLimitsEntry)
    - [CreateRatePlanRequest.UnitPricesEntry](#forgepoint-billing-v1-CreateRatePlanRequest-UnitPricesEntry)
    - [CreateRatePlanResponse](#forgepoint-billing-v1-CreateRatePlanResponse)
    - [GetInvoiceRequest](#forgepoint-billing-v1-GetInvoiceRequest)
    - [GetInvoiceResponse](#forgepoint-billing-v1-GetInvoiceResponse)
    - [GetRatePlanRequest](#forgepoint-billing-v1-GetRatePlanRequest)
    - [GetRatePlanResponse](#forgepoint-billing-v1-GetRatePlanResponse)
    - [GetUsageRequest](#forgepoint-billing-v1-GetUsageRequest)
    - [GetUsageResponse](#forgepoint-billing-v1-GetUsageResponse)
    - [Invoice](#forgepoint-billing-v1-Invoice)
    - [InvoiceLineItem](#forgepoint-billing-v1-InvoiceLineItem)
    - [ListInvoicesRequest](#forgepoint-billing-v1-ListInvoicesRequest)
    - [ListInvoicesResponse](#forgepoint-billing-v1-ListInvoicesResponse)
    - [MeterUsage](#forgepoint-billing-v1-MeterUsage)
    - [Money](#forgepoint-billing-v1-Money)
    - [RatePlan](#forgepoint-billing-v1-RatePlan)
    - [RatePlan.IncludedQuantitiesEntry](#forgepoint-billing-v1-RatePlan-IncludedQuantitiesEntry)
    - [RatePlan.QuotaLimitsEntry](#forgepoint-billing-v1-RatePlan-QuotaLimitsEntry)
    - [RatePlan.UnitPricesEntry](#forgepoint-billing-v1-RatePlan-UnitPricesEntry)
    - [RecordUsageRequest](#forgepoint-billing-v1-RecordUsageRequest)
    - [RecordUsageResponse](#forgepoint-billing-v1-RecordUsageResponse)
    - [UsageRecord](#forgepoint-billing-v1-UsageRecord)
    - [UsageSummary](#forgepoint-billing-v1-UsageSummary)
    - [UsageSummary.ByMeterEntry](#forgepoint-billing-v1-UsageSummary-ByMeterEntry)
  
    - [InvoiceStatus](#forgepoint-billing-v1-InvoiceStatus)
    - [MeterType](#forgepoint-billing-v1-MeterType)
  
    - [BillingService](#forgepoint-billing-v1-BillingService)
  
- [forgepoint/events/v1/events.proto](#forgepoint_events_v1_events-proto)
    - [ApiKeyRotated](#forgepoint-events-v1-ApiKeyRotated)
    - [CompensationTriggered](#forgepoint-events-v1-CompensationTriggered)
    - [DriftMetric](#forgepoint-events-v1-DriftMetric)
    - [FeatureSummary](#forgepoint-events-v1-FeatureSummary)
    - [FeatureSummary.FeatureValuesEntry](#forgepoint-events-v1-FeatureSummary-FeatureValuesEntry)
    - [FeatureViewDefined](#forgepoint-events-v1-FeatureViewDefined)
    - [FeaturesWritten](#forgepoint-events-v1-FeaturesWritten)
    - [InferenceCompleted](#forgepoint-events-v1-InferenceCompleted)
    - [InferenceFailed](#forgepoint-events-v1-InferenceFailed)
    - [InvoiceGenerated](#forgepoint-events-v1-InvoiceGenerated)
    - [MetricPoint](#forgepoint-events-v1-MetricPoint)
    - [ModelArchived](#forgepoint-events-v1-ModelArchived)
    - [ModelDeployed](#forgepoint-events-v1-ModelDeployed)
    - [ModelDriftDetected](#forgepoint-events-v1-ModelDriftDetected)
    - [ModelPromoted](#forgepoint-events-v1-ModelPromoted)
    - [ModelRegistered](#forgepoint-events-v1-ModelRegistered)
    - [ModelUndeployed](#forgepoint-events-v1-ModelUndeployed)
    - [ModelVersionCreated](#forgepoint-events-v1-ModelVersionCreated)
    - [ModelVersionReady](#forgepoint-events-v1-ModelVersionReady)
    - [NotificationDelivered](#forgepoint-events-v1-NotificationDelivered)
    - [NotificationFailed](#forgepoint-events-v1-NotificationFailed)
    - [PipelineCompleted](#forgepoint-events-v1-PipelineCompleted)
    - [PipelineFailed](#forgepoint-events-v1-PipelineFailed)
    - [PipelineStarted](#forgepoint-events-v1-PipelineStarted)
    - [PredictionSummary](#forgepoint-events-v1-PredictionSummary)
    - [PredictionSummary.OutputStatsEntry](#forgepoint-events-v1-PredictionSummary-OutputStatsEntry)
    - [QuotaExceeded](#forgepoint-events-v1-QuotaExceeded)
    - [RunCreated](#forgepoint-events-v1-RunCreated)
    - [RunFinished](#forgepoint-events-v1-RunFinished)
    - [StepCompleted](#forgepoint-events-v1-StepCompleted)
    - [StepFailed](#forgepoint-events-v1-StepFailed)
    - [UsageRecorded](#forgepoint-events-v1-UsageRecorded)
    - [UserCreated](#forgepoint-events-v1-UserCreated)
  
    - [DriftMethod](#forgepoint-events-v1-DriftMethod)
    - [DriftSeverity](#forgepoint-events-v1-DriftSeverity)
    - [DriftType](#forgepoint-events-v1-DriftType)
    - [InferenceFailureReason](#forgepoint-events-v1-InferenceFailureReason)
    - [MeterType](#forgepoint-events-v1-MeterType)
    - [ModelStage](#forgepoint-events-v1-ModelStage)
    - [NotificationChannel](#forgepoint-events-v1-NotificationChannel)
    - [PipelineType](#forgepoint-events-v1-PipelineType)
    - [RunStatus](#forgepoint-events-v1-RunStatus)
    - [StepType](#forgepoint-events-v1-StepType)
  
- [forgepoint/experiment/v1/experiment.proto](#forgepoint_experiment_v1_experiment-proto)
    - [ArchiveExperimentRequest](#forgepoint-experiment-v1-ArchiveExperimentRequest)
    - [ArchiveExperimentResponse](#forgepoint-experiment-v1-ArchiveExperimentResponse)
    - [CompareRunsRequest](#forgepoint-experiment-v1-CompareRunsRequest)
    - [CompareRunsResponse](#forgepoint-experiment-v1-CompareRunsResponse)
    - [CreateExperimentRequest](#forgepoint-experiment-v1-CreateExperimentRequest)
    - [CreateExperimentRequest.TagsEntry](#forgepoint-experiment-v1-CreateExperimentRequest-TagsEntry)
    - [CreateExperimentResponse](#forgepoint-experiment-v1-CreateExperimentResponse)
    - [DeleteRunRequest](#forgepoint-experiment-v1-DeleteRunRequest)
    - [DeleteRunResponse](#forgepoint-experiment-v1-DeleteRunResponse)
    - [Experiment](#forgepoint-experiment-v1-Experiment)
    - [Experiment.TagsEntry](#forgepoint-experiment-v1-Experiment-TagsEntry)
    - [GetExperimentRequest](#forgepoint-experiment-v1-GetExperimentRequest)
    - [GetExperimentResponse](#forgepoint-experiment-v1-GetExperimentResponse)
    - [GetMetricHistoryRequest](#forgepoint-experiment-v1-GetMetricHistoryRequest)
    - [GetMetricHistoryResponse](#forgepoint-experiment-v1-GetMetricHistoryResponse)
    - [GetRunRequest](#forgepoint-experiment-v1-GetRunRequest)
    - [GetRunResponse](#forgepoint-experiment-v1-GetRunResponse)
    - [ListExperimentsRequest](#forgepoint-experiment-v1-ListExperimentsRequest)
    - [ListExperimentsResponse](#forgepoint-experiment-v1-ListExperimentsResponse)
    - [ListRunsRequest](#forgepoint-experiment-v1-ListRunsRequest)
    - [ListRunsResponse](#forgepoint-experiment-v1-ListRunsResponse)
    - [LogMetricsRequest](#forgepoint-experiment-v1-LogMetricsRequest)
    - [LogMetricsResponse](#forgepoint-experiment-v1-LogMetricsResponse)
    - [LogParamsRequest](#forgepoint-experiment-v1-LogParamsRequest)
    - [LogParamsResponse](#forgepoint-experiment-v1-LogParamsResponse)
    - [MetricPoint](#forgepoint-experiment-v1-MetricPoint)
    - [MetricSeries](#forgepoint-experiment-v1-MetricSeries)
    - [Param](#forgepoint-experiment-v1-Param)
    - [Run](#forgepoint-experiment-v1-Run)
    - [RunComparison](#forgepoint-experiment-v1-RunComparison)
    - [SetRunArtifactsRequest](#forgepoint-experiment-v1-SetRunArtifactsRequest)
    - [SetRunArtifactsResponse](#forgepoint-experiment-v1-SetRunArtifactsResponse)
    - [StartRunRequest](#forgepoint-experiment-v1-StartRunRequest)
    - [StartRunResponse](#forgepoint-experiment-v1-StartRunResponse)
    - [UpdateExperimentRequest](#forgepoint-experiment-v1-UpdateExperimentRequest)
    - [UpdateExperimentRequest.TagsEntry](#forgepoint-experiment-v1-UpdateExperimentRequest-TagsEntry)
    - [UpdateExperimentResponse](#forgepoint-experiment-v1-UpdateExperimentResponse)
    - [UpdateRunStatusRequest](#forgepoint-experiment-v1-UpdateRunStatusRequest)
    - [UpdateRunStatusResponse](#forgepoint-experiment-v1-UpdateRunStatusResponse)
  
    - [RunSource](#forgepoint-experiment-v1-RunSource)
    - [RunStatus](#forgepoint-experiment-v1-RunStatus)
  
    - [ExperimentTrackerService](#forgepoint-experiment-v1-ExperimentTrackerService)
  
- [forgepoint/featurestore/v1/featurestore.proto](#forgepoint_featurestore_v1_featurestore-proto)
    - [DefineFeatureViewRequest](#forgepoint-featurestore-v1-DefineFeatureViewRequest)
    - [DefineFeatureViewResponse](#forgepoint-featurestore-v1-DefineFeatureViewResponse)
    - [DeleteFeatureViewRequest](#forgepoint-featurestore-v1-DeleteFeatureViewRequest)
    - [DeleteFeatureViewResponse](#forgepoint-featurestore-v1-DeleteFeatureViewResponse)
    - [Entity](#forgepoint-featurestore-v1-Entity)
    - [FeatureSpec](#forgepoint-featurestore-v1-FeatureSpec)
    - [FeatureValues](#forgepoint-featurestore-v1-FeatureValues)
    - [FeatureValues.ValuesEntry](#forgepoint-featurestore-v1-FeatureValues-ValuesEntry)
    - [FeatureVector](#forgepoint-featurestore-v1-FeatureVector)
    - [FeatureVector.ValuesEntry](#forgepoint-featurestore-v1-FeatureVector-ValuesEntry)
    - [FeatureView](#forgepoint-featurestore-v1-FeatureView)
    - [GetFeatureViewRequest](#forgepoint-featurestore-v1-GetFeatureViewRequest)
    - [GetFeatureViewResponse](#forgepoint-featurestore-v1-GetFeatureViewResponse)
    - [GetHistoricalFeaturesRequest](#forgepoint-featurestore-v1-GetHistoricalFeaturesRequest)
    - [GetHistoricalFeaturesResponse](#forgepoint-featurestore-v1-GetHistoricalFeaturesResponse)
    - [GetOnlineFeaturesRequest](#forgepoint-featurestore-v1-GetOnlineFeaturesRequest)
    - [GetOnlineFeaturesResponse](#forgepoint-featurestore-v1-GetOnlineFeaturesResponse)
    - [ListFeatureViewsRequest](#forgepoint-featurestore-v1-ListFeatureViewsRequest)
    - [ListFeatureViewsResponse](#forgepoint-featurestore-v1-ListFeatureViewsResponse)
    - [RebuildViewsRequest](#forgepoint-featurestore-v1-RebuildViewsRequest)
    - [RebuildViewsResponse](#forgepoint-featurestore-v1-RebuildViewsResponse)
    - [WriteFeaturesRequest](#forgepoint-featurestore-v1-WriteFeaturesRequest)
    - [WriteFeaturesResponse](#forgepoint-featurestore-v1-WriteFeaturesResponse)
  
    - [FeatureValueType](#forgepoint-featurestore-v1-FeatureValueType)
    - [RebuildTarget](#forgepoint-featurestore-v1-RebuildTarget)
  
    - [FeatureStoreService](#forgepoint-featurestore-v1-FeatureStoreService)
  
- [forgepoint/inference/v1/inference.proto](#forgepoint_inference_v1_inference-proto)
    - [BatchPredictItem](#forgepoint-inference-v1-BatchPredictItem)
    - [BatchPredictItem.InputsEntry](#forgepoint-inference-v1-BatchPredictItem-InputsEntry)
    - [BatchPredictRequest](#forgepoint-inference-v1-BatchPredictRequest)
    - [BatchPredictResponse](#forgepoint-inference-v1-BatchPredictResponse)
    - [BatchPredictResult](#forgepoint-inference-v1-BatchPredictResult)
    - [BatchPredictResult.OutputsEntry](#forgepoint-inference-v1-BatchPredictResult-OutputsEntry)
    - [CircuitState](#forgepoint-inference-v1-CircuitState)
    - [DeleteRouteRequest](#forgepoint-inference-v1-DeleteRouteRequest)
    - [DeleteRouteResponse](#forgepoint-inference-v1-DeleteRouteResponse)
    - [GetCircuitStateRequest](#forgepoint-inference-v1-GetCircuitStateRequest)
    - [GetCircuitStateResponse](#forgepoint-inference-v1-GetCircuitStateResponse)
    - [GetModelInfoRequest](#forgepoint-inference-v1-GetModelInfoRequest)
    - [GetModelInfoResponse](#forgepoint-inference-v1-GetModelInfoResponse)
    - [GetRouteRequest](#forgepoint-inference-v1-GetRouteRequest)
    - [GetRouteResponse](#forgepoint-inference-v1-GetRouteResponse)
    - [ListCircuitStatesRequest](#forgepoint-inference-v1-ListCircuitStatesRequest)
    - [ListCircuitStatesResponse](#forgepoint-inference-v1-ListCircuitStatesResponse)
    - [ListRoutesRequest](#forgepoint-inference-v1-ListRoutesRequest)
    - [ListRoutesResponse](#forgepoint-inference-v1-ListRoutesResponse)
    - [ModelInfo](#forgepoint-inference-v1-ModelInfo)
    - [PredictRequest](#forgepoint-inference-v1-PredictRequest)
    - [PredictRequest.InputsEntry](#forgepoint-inference-v1-PredictRequest-InputsEntry)
    - [PredictResponse](#forgepoint-inference-v1-PredictResponse)
    - [PredictResponse.OutputsEntry](#forgepoint-inference-v1-PredictResponse-OutputsEntry)
    - [Route](#forgepoint-inference-v1-Route)
    - [RouteTarget](#forgepoint-inference-v1-RouteTarget)
    - [SetTrafficSplitRequest](#forgepoint-inference-v1-SetTrafficSplitRequest)
    - [SetTrafficSplitResponse](#forgepoint-inference-v1-SetTrafficSplitResponse)
    - [StreamPredictRequest](#forgepoint-inference-v1-StreamPredictRequest)
    - [StreamPredictResponse](#forgepoint-inference-v1-StreamPredictResponse)
    - [TensorData](#forgepoint-inference-v1-TensorData)
    - [TensorSpec](#forgepoint-inference-v1-TensorSpec)
    - [TrafficWeight](#forgepoint-inference-v1-TrafficWeight)
    - [UpsertRouteRequest](#forgepoint-inference-v1-UpsertRouteRequest)
    - [UpsertRouteResponse](#forgepoint-inference-v1-UpsertRouteResponse)
    - [VersionInfo](#forgepoint-inference-v1-VersionInfo)
  
    - [CircuitBreakerState](#forgepoint-inference-v1-CircuitBreakerState)
    - [DataType](#forgepoint-inference-v1-DataType)
    - [TargetStatus](#forgepoint-inference-v1-TargetStatus)
  
    - [InferenceGatewayService](#forgepoint-inference-v1-InferenceGatewayService)
  
- [forgepoint/monitor/v1/monitor.proto](#forgepoint_monitor_v1_monitor-proto)
    - [ConfigureMonitorRequest](#forgepoint-monitor-v1-ConfigureMonitorRequest)
    - [ConfigureMonitorResponse](#forgepoint-monitor-v1-ConfigureMonitorResponse)
    - [DeleteMonitorRequest](#forgepoint-monitor-v1-DeleteMonitorRequest)
    - [DeleteMonitorResponse](#forgepoint-monitor-v1-DeleteMonitorResponse)
    - [DriftMetric](#forgepoint-monitor-v1-DriftMetric)
    - [DriftReport](#forgepoint-monitor-v1-DriftReport)
    - [GetDriftReportRequest](#forgepoint-monitor-v1-GetDriftReportRequest)
    - [GetDriftReportResponse](#forgepoint-monitor-v1-GetDriftReportResponse)
    - [GetModelHealthRequest](#forgepoint-monitor-v1-GetModelHealthRequest)
    - [GetModelHealthResponse](#forgepoint-monitor-v1-GetModelHealthResponse)
    - [GetMonitorStatusRequest](#forgepoint-monitor-v1-GetMonitorStatusRequest)
    - [GetMonitorStatusResponse](#forgepoint-monitor-v1-GetMonitorStatusResponse)
    - [GroundTruthLabel](#forgepoint-monitor-v1-GroundTruthLabel)
    - [ListDriftReportsRequest](#forgepoint-monitor-v1-ListDriftReportsRequest)
    - [ListDriftReportsResponse](#forgepoint-monitor-v1-ListDriftReportsResponse)
    - [ListMonitorsRequest](#forgepoint-monitor-v1-ListMonitorsRequest)
    - [ListMonitorsResponse](#forgepoint-monitor-v1-ListMonitorsResponse)
    - [ListMonitorsResponse.Entry](#forgepoint-monitor-v1-ListMonitorsResponse-Entry)
    - [ModelHealth](#forgepoint-monitor-v1-ModelHealth)
    - [ModelHealth.SeverityByTypeEntry](#forgepoint-monitor-v1-ModelHealth-SeverityByTypeEntry)
    - [Monitor](#forgepoint-monitor-v1-Monitor)
    - [MonitorStatus](#forgepoint-monitor-v1-MonitorStatus)
    - [ResetBaselineRequest](#forgepoint-monitor-v1-ResetBaselineRequest)
    - [ResetBaselineResponse](#forgepoint-monitor-v1-ResetBaselineResponse)
    - [StreamDriftEventsRequest](#forgepoint-monitor-v1-StreamDriftEventsRequest)
    - [StreamDriftEventsResponse](#forgepoint-monitor-v1-StreamDriftEventsResponse)
    - [SubmitGroundTruthRequest](#forgepoint-monitor-v1-SubmitGroundTruthRequest)
    - [SubmitGroundTruthResponse](#forgepoint-monitor-v1-SubmitGroundTruthResponse)
    - [ThresholdConfig](#forgepoint-monitor-v1-ThresholdConfig)
  
    - [DriftMethod](#forgepoint-monitor-v1-DriftMethod)
    - [DriftSeverity](#forgepoint-monitor-v1-DriftSeverity)
    - [DriftType](#forgepoint-monitor-v1-DriftType)
    - [MonitorState](#forgepoint-monitor-v1-MonitorState)
  
    - [MonitorService](#forgepoint-monitor-v1-MonitorService)
  
- [forgepoint/notification/v1/notification.proto](#forgepoint_notification_v1_notification-proto)
    - [ChannelPreference](#forgepoint-notification-v1-ChannelPreference)
    - [DeliveryAttempt](#forgepoint-notification-v1-DeliveryAttempt)
    - [GetNotificationRequest](#forgepoint-notification-v1-GetNotificationRequest)
    - [GetNotificationResponse](#forgepoint-notification-v1-GetNotificationResponse)
    - [GetPreferencesRequest](#forgepoint-notification-v1-GetPreferencesRequest)
    - [GetPreferencesResponse](#forgepoint-notification-v1-GetPreferencesResponse)
    - [ListDeliveryAttemptsRequest](#forgepoint-notification-v1-ListDeliveryAttemptsRequest)
    - [ListDeliveryAttemptsResponse](#forgepoint-notification-v1-ListDeliveryAttemptsResponse)
    - [ListNotificationsRequest](#forgepoint-notification-v1-ListNotificationsRequest)
    - [ListNotificationsResponse](#forgepoint-notification-v1-ListNotificationsResponse)
    - [MarkReadRequest](#forgepoint-notification-v1-MarkReadRequest)
    - [MarkReadResponse](#forgepoint-notification-v1-MarkReadResponse)
    - [Notification](#forgepoint-notification-v1-Notification)
    - [NotificationPreferences](#forgepoint-notification-v1-NotificationPreferences)
    - [TestChannelRequest](#forgepoint-notification-v1-TestChannelRequest)
    - [TestChannelResponse](#forgepoint-notification-v1-TestChannelResponse)
    - [UpdatePreferencesRequest](#forgepoint-notification-v1-UpdatePreferencesRequest)
    - [UpdatePreferencesResponse](#forgepoint-notification-v1-UpdatePreferencesResponse)
  
    - [DeliveryStatus](#forgepoint-notification-v1-DeliveryStatus)
    - [NotificationChannel](#forgepoint-notification-v1-NotificationChannel)
    - [NotificationSeverity](#forgepoint-notification-v1-NotificationSeverity)
  
    - [NotificationService](#forgepoint-notification-v1-NotificationService)
  
- [forgepoint/pipeline/v1/pipeline.proto](#forgepoint_pipeline_v1_pipeline-proto)
    - [CancelExecutionRequest](#forgepoint-pipeline-v1-CancelExecutionRequest)
    - [CancelExecutionResponse](#forgepoint-pipeline-v1-CancelExecutionResponse)
    - [CreatePipelineRequest](#forgepoint-pipeline-v1-CreatePipelineRequest)
    - [CreatePipelineResponse](#forgepoint-pipeline-v1-CreatePipelineResponse)
    - [DeletePipelineRequest](#forgepoint-pipeline-v1-DeletePipelineRequest)
    - [DeletePipelineResponse](#forgepoint-pipeline-v1-DeletePipelineResponse)
    - [Execution](#forgepoint-pipeline-v1-Execution)
    - [GetExecutionRequest](#forgepoint-pipeline-v1-GetExecutionRequest)
    - [GetExecutionResponse](#forgepoint-pipeline-v1-GetExecutionResponse)
    - [GetPipelineRequest](#forgepoint-pipeline-v1-GetPipelineRequest)
    - [GetPipelineResponse](#forgepoint-pipeline-v1-GetPipelineResponse)
    - [ListExecutionsRequest](#forgepoint-pipeline-v1-ListExecutionsRequest)
    - [ListExecutionsResponse](#forgepoint-pipeline-v1-ListExecutionsResponse)
    - [ListPipelinesRequest](#forgepoint-pipeline-v1-ListPipelinesRequest)
    - [ListPipelinesResponse](#forgepoint-pipeline-v1-ListPipelinesResponse)
    - [PipelineDefinition](#forgepoint-pipeline-v1-PipelineDefinition)
    - [StepDefinition](#forgepoint-pipeline-v1-StepDefinition)
    - [StepExecution](#forgepoint-pipeline-v1-StepExecution)
    - [TriggerExecutionRequest](#forgepoint-pipeline-v1-TriggerExecutionRequest)
    - [TriggerExecutionResponse](#forgepoint-pipeline-v1-TriggerExecutionResponse)
    - [UpdatePipelineRequest](#forgepoint-pipeline-v1-UpdatePipelineRequest)
    - [UpdatePipelineResponse](#forgepoint-pipeline-v1-UpdatePipelineResponse)
    - [WatchExecutionRequest](#forgepoint-pipeline-v1-WatchExecutionRequest)
    - [WatchExecutionResponse](#forgepoint-pipeline-v1-WatchExecutionResponse)
  
    - [ExecutionStatus](#forgepoint-pipeline-v1-ExecutionStatus)
    - [PipelineType](#forgepoint-pipeline-v1-PipelineType)
    - [StepStatus](#forgepoint-pipeline-v1-StepStatus)
    - [StepType](#forgepoint-pipeline-v1-StepType)
  
    - [PipelineOrchestratorService](#forgepoint-pipeline-v1-PipelineOrchestratorService)
  
- [forgepoint/registry/v1/registry.proto](#forgepoint_registry_v1_registry-proto)
    - [ConfirmVersionUploadRequest](#forgepoint-registry-v1-ConfirmVersionUploadRequest)
    - [ConfirmVersionUploadResponse](#forgepoint-registry-v1-ConfirmVersionUploadResponse)
    - [CreateVersionRequest](#forgepoint-registry-v1-CreateVersionRequest)
    - [CreateVersionResponse](#forgepoint-registry-v1-CreateVersionResponse)
    - [DeleteModelRequest](#forgepoint-registry-v1-DeleteModelRequest)
    - [DeleteModelResponse](#forgepoint-registry-v1-DeleteModelResponse)
    - [GetDownloadURLRequest](#forgepoint-registry-v1-GetDownloadURLRequest)
    - [GetDownloadURLResponse](#forgepoint-registry-v1-GetDownloadURLResponse)
    - [GetModelRequest](#forgepoint-registry-v1-GetModelRequest)
    - [GetModelResponse](#forgepoint-registry-v1-GetModelResponse)
    - [GetUploadURLRequest](#forgepoint-registry-v1-GetUploadURLRequest)
    - [GetUploadURLResponse](#forgepoint-registry-v1-GetUploadURLResponse)
    - [GetVersionRequest](#forgepoint-registry-v1-GetVersionRequest)
    - [GetVersionResponse](#forgepoint-registry-v1-GetVersionResponse)
    - [ListModelsRequest](#forgepoint-registry-v1-ListModelsRequest)
    - [ListModelsResponse](#forgepoint-registry-v1-ListModelsResponse)
    - [ListVersionsRequest](#forgepoint-registry-v1-ListVersionsRequest)
    - [ListVersionsResponse](#forgepoint-registry-v1-ListVersionsResponse)
    - [Model](#forgepoint-registry-v1-Model)
    - [Model.TagsEntry](#forgepoint-registry-v1-Model-TagsEntry)
    - [ModelVersion](#forgepoint-registry-v1-ModelVersion)
    - [PromoteVersionRequest](#forgepoint-registry-v1-PromoteVersionRequest)
    - [PromoteVersionResponse](#forgepoint-registry-v1-PromoteVersionResponse)
    - [RegisterModelRequest](#forgepoint-registry-v1-RegisterModelRequest)
    - [RegisterModelRequest.TagsEntry](#forgepoint-registry-v1-RegisterModelRequest-TagsEntry)
    - [RegisterModelResponse](#forgepoint-registry-v1-RegisterModelResponse)
    - [SearchByTagRequest](#forgepoint-registry-v1-SearchByTagRequest)
    - [SearchByTagResponse](#forgepoint-registry-v1-SearchByTagResponse)
    - [UpdateModelRequest](#forgepoint-registry-v1-UpdateModelRequest)
    - [UpdateModelRequest.TagsEntry](#forgepoint-registry-v1-UpdateModelRequest-TagsEntry)
    - [UpdateModelResponse](#forgepoint-registry-v1-UpdateModelResponse)
  
    - [ModelStage](#forgepoint-registry-v1-ModelStage)
    - [VersionStatus](#forgepoint-registry-v1-VersionStatus)
  
    - [RegistryService](#forgepoint-registry-v1-RegistryService)
  
- [forgepoint/serving/v1/serving.proto](#forgepoint_serving_v1_serving-proto)
    - [GetModelInfoRequest](#forgepoint-serving-v1-GetModelInfoRequest)
    - [GetModelInfoResponse](#forgepoint-serving-v1-GetModelInfoResponse)
    - [GetModelStatusRequest](#forgepoint-serving-v1-GetModelStatusRequest)
    - [GetModelStatusResponse](#forgepoint-serving-v1-GetModelStatusResponse)
    - [GetServingMetricsRequest](#forgepoint-serving-v1-GetServingMetricsRequest)
    - [GetServingMetricsResponse](#forgepoint-serving-v1-GetServingMetricsResponse)
    - [HealthCheckRequest](#forgepoint-serving-v1-HealthCheckRequest)
    - [HealthCheckResponse](#forgepoint-serving-v1-HealthCheckResponse)
    - [ListLoadedModelsRequest](#forgepoint-serving-v1-ListLoadedModelsRequest)
    - [ListLoadedModelsResponse](#forgepoint-serving-v1-ListLoadedModelsResponse)
    - [LoadModelRequest](#forgepoint-serving-v1-LoadModelRequest)
    - [LoadModelResponse](#forgepoint-serving-v1-LoadModelResponse)
    - [ModelInfo](#forgepoint-serving-v1-ModelInfo)
    - [ModelStatus](#forgepoint-serving-v1-ModelStatus)
    - [PredictRequest](#forgepoint-serving-v1-PredictRequest)
    - [PredictRequest.InputsEntry](#forgepoint-serving-v1-PredictRequest-InputsEntry)
    - [PredictResponse](#forgepoint-serving-v1-PredictResponse)
    - [PredictResponse.OutputsEntry](#forgepoint-serving-v1-PredictResponse-OutputsEntry)
    - [ServingMetrics](#forgepoint-serving-v1-ServingMetrics)
    - [StreamPredictRequest](#forgepoint-serving-v1-StreamPredictRequest)
    - [StreamPredictRequest.InputsEntry](#forgepoint-serving-v1-StreamPredictRequest-InputsEntry)
    - [StreamPredictResponse](#forgepoint-serving-v1-StreamPredictResponse)
    - [StreamPredictResponse.OutputsEntry](#forgepoint-serving-v1-StreamPredictResponse-OutputsEntry)
    - [TensorData](#forgepoint-serving-v1-TensorData)
    - [TensorSpec](#forgepoint-serving-v1-TensorSpec)
    - [UnloadModelRequest](#forgepoint-serving-v1-UnloadModelRequest)
    - [UnloadModelResponse](#forgepoint-serving-v1-UnloadModelResponse)
  
    - [DataType](#forgepoint-serving-v1-DataType)
    - [HealthStatus](#forgepoint-serving-v1-HealthStatus)
    - [ModelState](#forgepoint-serving-v1-ModelState)
  
    - [ModelServingService](#forgepoint-serving-v1-ModelServingService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="forgepoint_common_v1_common-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/common/v1/common.proto



<a name="forgepoint-common-v1-ErrorDetail"></a>

### ErrorDetail
============================================================================
ERROR DETAILS
============================================================================

WHY structured errors over plain strings:
  - Clients need machine-readable error information to show field-specific
    validation errors, retry on specific codes, or display localized messages.
  - gRPC status codes (OK, NOT_FOUND, etc.) are too coarse. ErrorDetail
    provides field-level granularity within a single RPC response.

HOW gRPC ERROR MODEL WORKS:
  - gRPC has 16 status codes (OK through DATA_LOSS)
  - For richer errors, attach ErrorDetail as &#34;status details&#34; using
    google.golang.org/grpc/status package:
      st := status.New(codes.InvalidArgument, &#34;validation failed&#34;)
      st, _ = st.WithDetails(&amp;ErrorDetail{code: &#34;INVALID_EMAIL&#34;, ...})
      return nil, st.Err()
  - Client extracts details: status.FromError(err).Details()

HOW GOOGLE DOES IT:
  - Google APIs use google.rpc.Status with google.rpc.ErrorInfo,
    google.rpc.BadRequest, google.rpc.RetryInfo as detail types.
  - We simplify to a single ErrorDetail type covering the common cases.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| code | [string](#string) |  | Machine-readable error code: &#34;INVALID_EMAIL&#34;, &#34;DUPLICATE_MODEL_NAME&#34;, &#34;QUOTA_EXCEEDED&#34;. Clients switch on this for programmatic handling. |
| message | [string](#string) |  | Human-readable error message for debugging/logging. NOT for end-user display (localization is the client&#39;s job). |
| field | [string](#string) |  | Field path that caused the error (for validation errors). Uses dot notation: &#34;model.name&#34;, &#34;version.metrics.accuracy&#34;. Empty for non-field-specific errors. |






<a name="forgepoint-common-v1-EventEnvelope"></a>

### EventEnvelope
============================================================================
EVENT ENVELOPE
============================================================================

WHY: Every async event (NATS) is wrapped in a standard envelope. This
ensures consistent metadata across all events regardless of which service
publishes them. It&#39;s the async equivalent of HTTP headers.

HOW IT WORKS:
  - Producer wraps domain event in EventEnvelope before publishing to NATS
  - Consumer unwraps envelope, uses metadata for:
    * Deduplication: `id` field (UUID) → idempotent consumer checks
    * Tracing: `correlation_id` → links async events to original request
    * Routing: `type` &#43; `source` → consumers filter on subject hierarchy
    * Debugging: `timestamp` → when event was produced
  - `data` is google.protobuf.Any → type-safe payload with URL-based type ID

ALTERNATIVES:
  - CloudEvents spec: Industry standard for event metadata. Our envelope
    is a simplified CloudEvents. Full CloudEvents adds: specversion,
    datacontenttype, dataschema, subject, time. We keep it minimal since
    all our events are internal (not crossing org boundaries).
  - Raw JSON: No schema, no type safety, easy to break consumers silently.
  - Protobuf oneof for data: Would require listing ALL event types in this
    file — tight coupling. Any &#43; type URL keeps it extensible.

HOW UBER/CONFLUENT DO IT:
  - Uber: Uses Protobuf envelopes with Kafka (very similar to this)
  - Confluent: Schema Registry &#43; Avro envelopes with subject-based routing
  - AWS EventBridge: CloudEvents-like envelope with source &#43; detail-type
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | Unique event ID (UUID v4). Used for idempotent consumption — consumers track processed IDs to safely handle redelivery. |
| type | [string](#string) |  | Event type following dot notation: &#34;model.registered&#34;, &#34;pipeline.started&#34;. Maps to NATS subject hierarchy: fp.{source}.{type} |
| source | [string](#string) |  | Source service that produced this event: &#34;auth&#34;, &#34;registry&#34;, &#34;pipeline&#34;. |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the event was produced (not when it was consumed). |
| correlation_id | [string](#string) |  | Correlation ID linking this event to the original request. Propagated from the inbound gRPC request&#39;s trace context. Enables distributed tracing across sync (gRPC) and async (NATS) paths. |
| data | [google.protobuf.Any](#google-protobuf-Any) |  | The actual event payload. Uses google.protobuf.Any for type safety with runtime type resolution via the type_url field. |






<a name="forgepoint-common-v1-PaginationRequest"></a>

### PaginationRequest
============================================================================
PAGINATION
============================================================================

WHY cursor-based over offset-based:
  - Offset pagination (LIMIT/OFFSET) breaks when data changes between pages.
    If a new item is inserted at page 1, offset-based page 2 shows a
    duplicate. Cursor-based uses a stable pointer (usually the last item&#39;s
    ID or timestamp).
  - Performance: OFFSET N scans and discards N rows. Cursor-based uses
    WHERE id &gt; cursor with an index — O(1) vs O(N) seek time.
  - Used by: Google APIs, Stripe, GitHub, Slack, Twitter

HOW IT WORKS:
  - Client sends page_size &#43; page_token (opaque string, usually base64)
  - Server decodes token → gets cursor value → queries with WHERE clause
  - Server returns next_page_token &#43; total_count
  - Empty next_page_token = last page

ALTERNATIVES:
  - Offset/Limit: Simple but breaks with concurrent mutations, O(N) seek
  - Keyset (seek): Similar to cursor but exposes the actual key value
  - Relay-style (GraphQL): First/After/Last/Before — more flexible but complex
  - We use Google&#39;s convention (page_token/next_page_token) — most Go APIs do
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| page_size | [int32](#int32) |  | Maximum items per page. Server may return fewer. Default: 20, Max: 100 (enforced server-side to prevent abuse). |
| page_token | [string](#string) |  | Opaque cursor from a previous response&#39;s next_page_token. Empty string = first page. |






<a name="forgepoint-common-v1-PaginationResponse"></a>

### PaginationResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| next_page_token | [string](#string) |  | Opaque cursor for the next page. Empty = no more pages. |
| total_count | [int32](#int32) |  | Total number of items across all pages (if efficiently computable). May be -1 if total count is expensive (e.g., requires full table scan). |





 

 

 

 



<a name="forgepoint_auth_v1_auth-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/auth/v1/auth.proto



<a name="forgepoint-auth-v1-APIKey"></a>

### APIKey
============================================================================
APIKey
============================================================================

WHY: API keys allow programmatic access without browser-based login flows.
They&#39;re used by CI/CD pipelines, training scripts, and SDK clients.

SECURITY DESIGN — Key Prefix &#43; Hash:
  We NEVER store the full raw API key. Instead:
    1. Generate a random key: &#34;fp_live_&lt;32-random-bytes-base58&gt;&#34;
    2. Store key_prefix (first 8 chars, e.g., &#34;fp_live_&#34;) for human display
    3. Store SHA-256 hash of the full key for validation
  The full raw key is returned ONCE to the caller (in CreateAPIKeyResponse)
  and then it&#39;s gone. If lost, user must revoke and create a new key.

  This follows Stripe&#39;s API key design. Alternatives:
    - Store encrypted key: Requires key management, adds complexity
    - Store plaintext: Security anti-pattern, plaintext secrets in DB
    - JWT as API key: Can&#39;t be revoked without a blacklist; larger payload

SCOPES — Layered Authorization:
  A key&#39;s scopes are a SUBSET of the owning user&#39;s role permissions.
  E.g., a user with &#34;engineer&#34; role (can read/write models) can create
  a key scoped to [&#34;models:read&#34;] only — for a read-only CI job.
  Scope strings follow the format &#34;{resource}:{action}&#34;.

EXPIRY: expires_at is optional (zero value = no expiry). For service
accounts, short-lived keys (rotated by CI) are preferred over eternal keys.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Used for revocation — never the raw key. |
| key_prefix | [string](#string) |  | Human-readable prefix (first 8 chars of the raw key). Shown in UI for identification without exposing the secret. E.g., &#34;fp_live_&#34;. |
| user_id | [string](#string) |  | The user this key belongs to. When a key is used, the auth service looks up this user_id to resolve the role and enforce scope restrictions. |
| scopes | [string](#string) | repeated | OAuth 2.0-style scopes restricting what this key can do. Must be a subset of the user&#39;s role permissions at key creation time. Format: &#34;{resource}:{action}&#34; e.g., [&#34;models:read&#34;, &#34;pipelines:write&#34;]. |
| expires_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this key stops being valid. Zero value (not set) means no expiry. Recommended: set short TTLs (days/weeks) for keys used in automation. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this key was created. Useful for audit and rotation reminders. |






<a name="forgepoint-auth-v1-AssignRoleRequest"></a>

### AssignRoleRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user_id | [string](#string) |  | The user whose role is being changed. |
| role_name | [string](#string) |  | The role name to assign (e.g., &#34;admin&#34;, &#34;engineer&#34;, &#34;viewer&#34;). Must exist in the roles table. Auth service validates this. |






<a name="forgepoint-auth-v1-AssignRoleResponse"></a>

### AssignRoleResponse
AssignRoleResponse is intentionally empty — success is implicit.
WHY named empty vs google.protobuf.Empty:
  Same reason as RevokeAPIKeyResponse: Buf STANDARD naming &#43; forward compat.
  If we later add &#34;effective_from&#34; or &#34;previous_role&#34; for audit, we can
  add it here without changing the RPC signature.






<a name="forgepoint-auth-v1-CheckPermissionRequest"></a>

### CheckPermissionRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user_id | [string](#string) |  | The user_id whose permissions to check. Typically taken from TokenClaims returned by a prior ValidateToken call. |
| resource | [string](#string) |  | The resource being accessed (e.g., &#34;models&#34;, &#34;pipelines&#34;). |
| action | [string](#string) |  | The action being attempted (e.g., &#34;read&#34;, &#34;write&#34;, &#34;delete&#34;). |






<a name="forgepoint-auth-v1-CheckPermissionResponse"></a>

### CheckPermissionResponse
CheckPermissionResponse is a simple allow/deny gate.
WHY include reason:
  A plain bool `allowed` is sufficient for enforcement, but the optional
  `reason` field enables better error messages and audit logs:
    - &#34;User has &#39;viewer&#39; role, which lacks &#39;write&#39; on &#39;models&#39;&#34;
  This is the design used by AWS IAM policy evaluation responses.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| allowed | [bool](#bool) |  | true = access granted, false = access denied. |
| reason | [string](#string) |  | Human-readable reason for the decision. Always populated. Examples: &#34;allowed: user role &#39;admin&#39; has wildcard permission&#34; &#34;denied: role &#39;viewer&#39; does not have action &#39;write&#39; on resource &#39;models&#39;&#34; |






<a name="forgepoint-auth-v1-CreateAPIKeyRequest"></a>

### CreateAPIKeyRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user_id | [string](#string) |  | The user for whom this key is being created. Usually the authenticated caller&#39;s own user_id; admins can create keys for other users. |
| scopes | [string](#string) | repeated | Scope restrictions for this key. Must be a subset of the user&#39;s role permissions. Auth service validates this at creation time. |
| expires_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Optional: when this key should expire. Zero value = no expiry. |






<a name="forgepoint-auth-v1-CreateAPIKeyResponse"></a>

### CreateAPIKeyResponse
CreateAPIKeyResponse returns the raw key ONCE. It is never retrievable again.
WHY return only once:
  We store a SHA-256 hash of the key, not the plaintext. After this response
  is sent, there is no way to recover the raw key — only revoke it.
  This is the same design used by Stripe, GitHub, and AWS IAM access keys.
  The key_prefix in the APIKey metadata lets users identify which key they&#39;re
  looking at in the UI without revealing the secret.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| raw_key | [string](#string) |  | The full raw API key. COPY THIS NOW — it will not be shown again. Format: &#34;fp_&lt;32-chars-base58&gt;&#34; (url-safe, easy to copy-paste). |
| api_key | [APIKey](#forgepoint-auth-v1-APIKey) |  | Metadata about the created key (id, key_prefix, scopes, etc.). Does NOT include the raw key — that&#39;s in raw_key above. |






<a name="forgepoint-auth-v1-CreateUserRequest"></a>

### CreateUserRequest
CreateUserRequest carries the fields needed to provision a new user account.
Password is only accepted here (creation time) — password changes would be
a separate ChangePassword RPC (not in M1 scope, omitted deliberately).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| email | [string](#string) |  | New user&#39;s email. Must be unique across the platform. |
| name | [string](#string) |  | Display name. Not unique. |
| password | [string](#string) |  | Plaintext password. Auth service bcrypt-hashes before storage. |
| team | [string](#string) |  | Team this user belongs to (e.g., &#34;ml-platform&#34;). |






<a name="forgepoint-auth-v1-CreateUserResponse"></a>

### CreateUserResponse
CreateUserResponse wraps the created User.
WHY not return User directly:
  Buf STANDARD lint rule RPC_RESPONSE_STANDARD_NAME requires every RPC
  response to be named &#34;&lt;Rpc&gt;Response&#34; or &#34;&lt;Service&gt;&lt;Rpc&gt;Response&#34;. This is
  a forward-compatibility guard — if we later add fields (e.g., a welcome
  email sent flag), we can add them to CreateUserResponse without changing
  the User message (which is a shared domain type).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [User](#forgepoint-auth-v1-User) |  | The newly created user. Password field is never populated in responses. |






<a name="forgepoint-auth-v1-ListUsersRequest"></a>

### ListUsersRequest
ListUsersRequest reuses PaginationRequest from common.proto.
WHY: Consistency across all list RPCs in the platform. Every service
uses the same cursor-based pagination shape. See common.proto for
the WHY on cursor-based vs offset-based pagination.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| team_filter | [string](#string) |  | Optional filter: return only users in this team. Empty = return all users (admin only). |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20, max 100. |






<a name="forgepoint-auth-v1-ListUsersResponse"></a>

### ListUsersResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| users | [User](#forgepoint-auth-v1-User) | repeated | The page of users matching the request. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata: next_page_token &#43; total_count. |






<a name="forgepoint-auth-v1-LoginRequest"></a>

### LoginRequest
LoginRequest carries email &#43; password credentials for the password grant flow.
WHY NOT OAuth 2.0 authorization_code flow:
  For an internal ML platform (no external users, no 3rd-party apps), the
  resource owner password credentials grant (email&#43;password → JWT) is simpler
  and avoids running a full OAuth 2.0 authorization server.
  In production: migrate to OIDC &#43; device flow for CLI, and authorization_code
  flow for web UI. This is a known tradeoff, intentional for M1 scope.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| email | [string](#string) |  | User&#39;s email address. Case-insensitive, normalized server-side. |
| password | [string](#string) |  | Plaintext password (TLS in transit). Auth service hashes and compares against bcrypt hash in database. NEVER log this field. |






<a name="forgepoint-auth-v1-LoginResponse"></a>

### LoginResponse
LoginResponse returns a short-lived JWT access token.
WHY no refresh_token in M1:
  Refresh tokens require a separate token store and rotation logic. For an
  internal platform where sessions are re-established frequently (CLI logins,
  notebook kernels), a slightly longer-lived access_token (e.g., 24h) is
  pragmatic. Refresh tokens are an M3 hardening item.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| access_token | [string](#string) |  | Signed JWT. Include in gRPC metadata: &#34;authorization: Bearer &lt;token&gt;&#34; |
| expires_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this token expires. Client should re-login before this time. |
| user | [User](#forgepoint-auth-v1-User) |  | The authenticated user&#39;s profile (avoids a separate GetUser call after login). |






<a name="forgepoint-auth-v1-Permission"></a>

### Permission
============================================================================
Permission
============================================================================

WHY resource &#43; action vs. a flat permission string:
  Structured fields (resource &#43; action) enable:
    - Wildcard matching at code level: resource=&#34;*&#34; grants all resources
    - Consistent naming enforced by validation: actions are an enum-like set
    - Clear audit log: &#34;user X was denied action=write on resource=models&#34;

  A flat string (&#34;models:write&#34;) is simpler but loses structure for
  programmatic checks. We choose structured fields.

RESOURCE NAMES (not enforced in proto, documented here):
  &#34;models&#34;, &#34;experiments&#34;, &#34;pipelines&#34;, &#34;features&#34;, &#34;billing&#34;, &#34;users&#34;,
  &#34;api-keys&#34;, &#34;deployments&#34;, &#34;monitoring&#34;

ACTION NAMES:
  &#34;read&#34;, &#34;write&#34;, &#34;delete&#34;, &#34;admin&#34;
  &#34;admin&#34; action means full control including granting the resource to others.

WILDCARD ADMIN PERMISSIONS — a role that grants everything: CheckPermission
on the Auth service iterates the user&#39;s role permissions. If any Permission has
resource=&#34;*&#34; and action=&#34;*&#34;, it short-circuits to allowed.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resource | [string](#string) |  | The resource being protected. E.g., &#34;models&#34;, &#34;pipelines&#34;, &#34;billing&#34;. Use &#34;*&#34; to mean all resources (admin wildcard). |
| action | [string](#string) |  | The action being requested. E.g., &#34;read&#34;, &#34;write&#34;, &#34;delete&#34;, &#34;admin&#34;. Use &#34;*&#34; to mean all actions (admin wildcard). |






<a name="forgepoint-auth-v1-RevokeAPIKeyRequest"></a>

### RevokeAPIKeyRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key_id | [string](#string) |  | The UUID of the API key to revoke (from APIKey.id, NOT the raw key). Using ID (not key prefix) prevents any accidental partial-match revocation. |






<a name="forgepoint-auth-v1-RevokeAPIKeyResponse"></a>

### RevokeAPIKeyResponse
RevokeAPIKeyResponse is intentionally empty.
WHY a named empty response instead of google.protobuf.Empty:
  Buf STANDARD lint requires &#34;&lt;Rpc&gt;Response&#34; naming for all RPC responses.
  A dedicated named empty message is also forward-compatible: if we later
  need to return a confirmation (e.g., revoked_at timestamp), we can add
  fields without changing the RPC signature.






<a name="forgepoint-auth-v1-Role"></a>

### Role
============================================================================
Role
============================================================================

WHY: Roles are named collections of Permissions. Rather than assigning
permissions individually to each user, we assign a role. This is the
defining characteristic of RBAC over simple ACLs.

BUILT-IN ROLES (not defined in proto, just examples for documentation):
  - &#34;admin&#34;:    all permissions on all resources
  - &#34;engineer&#34;: read/write on models, pipelines, experiments; read billing
  - &#34;viewer&#34;:   read-only on models, experiments; no billing access
  - &#34;svc-acct&#34;: narrow set of scopes used by machine API keys

WHY NOT HIERARCHICAL ROLES:
  Some systems support role inheritance (admin extends engineer). We keep
  roles flat for simplicity — explicit permission lists are easier to audit
  (&#34;what exactly can an engineer do?&#34;) and reduce surprise.
  If we need hierarchy later, we add parent_role_id to this message.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. |
| name | [string](#string) |  | Unique, human-readable role name: &#34;admin&#34;, &#34;engineer&#34;, &#34;viewer&#34;. This name is embedded in JWT claims (TokenClaims.role), so it must be stable — renaming a role is a breaking change requiring a token re-issue. |
| permissions | [Permission](#forgepoint-auth-v1-Permission) | repeated | The set of permissions this role grants. Evaluated as a logical OR — if ANY permission matches the requested resource&#43;action, access is granted. |






<a name="forgepoint-auth-v1-TokenClaims"></a>

### TokenClaims
============================================================================
TokenClaims
============================================================================

WHY: The decoded payload of a JWT. Other services call ValidateToken and
receive these claims — they don&#39;t need to decode the JWT themselves, which
avoids distributing the JWT secret and simplifies revocation checks.

JWT STRUCTURE RECAP:
  Header.Payload.Signature (base64url encoded, dot-separated)
  Payload contains claims: sub (subject/user_id), exp, iat, custom fields.
  We embed role &#43; scopes in the payload so the auth interceptor has enough
  context to call CheckPermission without a separate DB lookup per request.

WHY Timestamp for exp/iat instead of int64:
  The JWT standard uses Unix epoch integers for exp/iat. We use
  google.protobuf.Timestamp for consistency with other timestamp fields in
  this proto (Timestamp is richer: has nanos, implements proto well-known
  type). The Auth service converts when issuing/validating JWTs.

SCOPES IN CLAIMS:
  When a user logs in, scopes defaults to the role&#39;s full permission set
  (serialized as &#34;resource:action&#34; strings).
  When a user authenticates via API key, scopes is the key&#39;s restricted
  scope list. This lets downstream services check scopes without knowing
  whether it&#39;s a JWT or API key auth.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user_id | [string](#string) |  | The authenticated user&#39;s UUID. |
| email | [string](#string) |  | The authenticated user&#39;s email. Included for audit logging in downstream services without requiring a lookup to the auth service. |
| team | [string](#string) |  | The team this user belongs to. Useful for namespacing resources by team without an extra auth check. |
| role | [string](#string) |  | The user&#39;s role name. Downstream services can use this for coarse-grained checks (&#34;is this an admin?&#34;) without a CheckPermission RPC. |
| scopes | [string](#string) | repeated | Fine-grained scope strings for this token. Format: &#34;resource:action&#34;. For JWT logins: derived from role permissions. For API key auth: the key&#39;s restricted scopes. |
| exp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Token expiry time. Auth interceptors must reject requests where exp is in the past (the auth service validates this in ValidateToken). |
| iat | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Token issued-at time. Used for detecting clock skew and for audit logs. |






<a name="forgepoint-auth-v1-User"></a>

### User
============================================================================
User
============================================================================

WHY: The core identity principal. A User belongs to a Team and has a Role
that determines what they can do on the platform. All audit trails and
API keys are linked to a User.

TEAM vs ORGANIZATION:
  We use &#34;team&#34; (a string label) rather than a first-class Team entity for
  now. This avoids a separate teams table and FK joins on every auth check.
  Tradeoff: teams can&#39;t have their own settings or quotas without a schema
  change. For M1, this simplicity is the right call; the billing service can
  track quotas per team_id string without a foreign key.

ROLE MODEL — RBAC (Role-Based Access Control):
  User → Role → [Permission, Permission, ...]
  This is the classic RBAC model (NIST Level 1). Alternatives:
    - ABAC (Attribute-Based): Rules on arbitrary attributes. Very flexible,
      very complex to audit. Overkill for an internal ML platform.
    - ReBAC (Relation-Based, e.g., Zanzibar/OpenFGA): Object-level
      relationships. Necessary for &#34;user A can see model B because A is
      in team B&#34;. We can migrate to this in M4 if needed.
    - API-key scopes (OAuth 2.0 scopes): Fine-grained, per-key. We layer
      these ON TOP of RBAC — an API key can only do a subset of what the
      user&#39;s role permits.

WHY role is stored as a string name here (denormalized) rather than a
role_id FK: this is the JWT claims shape — when we issue a token, we embed role name (not ID) so ValidateToken
can return claims without a database join. The canonical role definition
lives in the roles table; the name is stable and human-readable.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Primary key, immutable once created. |
| email | [string](#string) |  | Email is the unique login identifier. Normalized to lowercase on write. |
| name | [string](#string) |  | Display name, not unique. Used in audit logs and UI. |
| team | [string](#string) |  | Team label (e.g., &#34;ml-platform&#34;, &#34;search&#34;). Groups users for billing and access namespacing. See design note above on why this is a string, not FK. |
| role | [string](#string) |  | The role assigned to this user (e.g., &#34;admin&#34;, &#34;engineer&#34;, &#34;viewer&#34;). Role defines the set of Permissions the user has. See Role message below. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the user account was created. Immutable. |






<a name="forgepoint-auth-v1-ValidateTokenRequest"></a>

### ValidateTokenRequest
ValidateTokenRequest is called by auth interceptors in every other service.
The interceptor extracts the Bearer token from gRPC metadata and passes it
here. The auth service validates signature, expiry, and revocation status.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| token | [string](#string) |  | The raw JWT string (without &#34;Bearer &#34; prefix) OR a raw API key. Auth service detects which format by the token prefix (&#34;fp_&#34; = API key). |






<a name="forgepoint-auth-v1-ValidateTokenResponse"></a>

### ValidateTokenResponse
ValidateTokenResponse wraps the decoded TokenClaims.
WHY not return TokenClaims directly:
  Buf STANDARD lint requires &#34;&lt;Rpc&gt;Response&#34; naming. A wrapper also lets us
  add fields in the future (e.g., token_type: &#34;jwt&#34; | &#34;api_key&#34;, or a
  warning when the token is close to expiry) without touching TokenClaims.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| claims | [TokenClaims](#forgepoint-auth-v1-TokenClaims) |  | The decoded claims from the validated token. |





 

 

 


<a name="forgepoint-auth-v1-AuthService"></a>

### AuthService
============================================================================
AUTH SERVICE
============================================================================

WHY a single AuthService vs separate UserService, TokenService, etc.:
  Identity, credentials, and access control are tightly coupled. Splitting
  them into separate services would require inter-service calls for common
  flows (e.g., Login needs User lookup, token issuance, and permission
  loading). A single AuthService keeps the auth boundary clean.

  Real-world parallel: Keycloak, Auth0, and Ory Kratos all handle user
  management &#43; token issuance in a single service boundary. The split only
  makes sense at org scale (separate teams owning identity vs. access control).

RPC CATEGORIES:
  IDENTITY MANAGEMENT: CreateUser, ListUsers
  CREDENTIALS: Login, CreateAPIKey, RevokeAPIKey
  TOKEN VALIDATION: ValidateToken (called by every other service&#39;s interceptor)
  ACCESS CONTROL: CheckPermission, AssignRole

INTERCEPTOR CALL PATTERN:
  Every gRPC service in Forgepoint has an auth interceptor that:
    1. Extracts &#34;authorization: Bearer &lt;token&gt;&#34; from incoming metadata
    2. Calls AuthService.ValidateToken() → gets TokenClaims
    3. Calls AuthService.CheckPermission() with the resource&#43;action for that RPC
    4. Injects TokenClaims into gRPC context for handler use
  The interceptor is registered via grpc.ChainUnaryInterceptor in pkg/grpcutil.

ASCII DIAGRAM — Token Validation Flow:

  gRPC Client
      │ metadata: authorization=Bearer &lt;jwt&gt;
      ▼
  ModelRegistry gRPC Server
      │
      ├─ Recovery Interceptor (panic → gRPC Internal)
      ├─ Logging Interceptor (structured log per RPC)
      ├─ Tracing Interceptor (OpenTelemetry span)
      └─ Auth Interceptor ──► AuthService.ValidateToken(token)
                          ──► AuthService.CheckPermission(user_id, &#34;models&#34;, &#34;write&#34;)
                          ──► injects claims into ctx
                              │
                              ▼
                          Handler (sees claims via ctx)
============================================================================

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Login | [LoginRequest](#forgepoint-auth-v1-LoginRequest) | [LoginResponse](#forgepoint-auth-v1-LoginResponse) | Login authenticates a user with email &#43; password and returns a signed JWT. This is the entry point for human users (CLI login, web UI login). For machine-to-machine auth, use API keys instead. |
| CreateUser | [CreateUserRequest](#forgepoint-auth-v1-CreateUserRequest) | [CreateUserResponse](#forgepoint-auth-v1-CreateUserResponse) | CreateUser provisions a new user account. Requires admin role. Password is bcrypt-hashed before storage. Returns the user profile (no password field — passwords are write-only). |
| ListUsers | [ListUsersRequest](#forgepoint-auth-v1-ListUsersRequest) | [ListUsersResponse](#forgepoint-auth-v1-ListUsersResponse) | ListUsers returns a paginated list of users. Requires admin role. Supports filtering by team via ListUsersRequest.team_filter. |
| CreateAPIKey | [CreateAPIKeyRequest](#forgepoint-auth-v1-CreateAPIKeyRequest) | [CreateAPIKeyResponse](#forgepoint-auth-v1-CreateAPIKeyResponse) | CreateAPIKey creates a new API key for a user. The raw key is returned ONCE in CreateAPIKeyResponse.raw_key. After that, only the key_prefix is visible (for identification in UI). Use API keys for CI/CD pipelines, training scripts, and SDK clients. |
| RevokeAPIKey | [RevokeAPIKeyRequest](#forgepoint-auth-v1-RevokeAPIKeyRequest) | [RevokeAPIKeyResponse](#forgepoint-auth-v1-RevokeAPIKeyResponse) | RevokeAPIKey immediately invalidates an API key by ID. Revocation takes effect immediately — the next request using this key will receive UNAUTHENTICATED. No expiry window; this is why centralized auth beats distributed JWT validation for revocability. |
| ValidateToken | [ValidateTokenRequest](#forgepoint-auth-v1-ValidateTokenRequest) | [ValidateTokenResponse](#forgepoint-auth-v1-ValidateTokenResponse) | ValidateToken is the workhorse called by auth interceptors in ALL other services. It validates the JWT signature, checks expiry, verifies the token is not in the revocation blacklist, and returns decoded TokenClaims.

Performance note: the auth service caches validation results in Redis with a 30-second TTL. This keeps auth overhead below 1ms p99 while still enforcing near-real-time revocation.

NOTE — what happens if the auth service goes down: the other services&#39; interceptors can be configured with a fail-open (allow) or fail-closed (deny) policy. Forgepoint uses fail-closed (safe default for a security service). A circuit breaker on the auth gRPC client prevents cascade failures during auth service outages. |
| CheckPermission | [CheckPermissionRequest](#forgepoint-auth-v1-CheckPermissionRequest) | [CheckPermissionResponse](#forgepoint-auth-v1-CheckPermissionResponse) | CheckPermission evaluates whether a user&#39;s role permits a specific resource&#43;action. Called by auth interceptors after ValidateToken to enforce RBAC. Returns allowed bool &#43; human-readable reason. |
| AssignRole | [AssignRoleRequest](#forgepoint-auth-v1-AssignRoleRequest) | [AssignRoleResponse](#forgepoint-auth-v1-AssignRoleResponse) | AssignRole changes a user&#39;s role. Requires admin permission. NOTE: The newly assigned role takes effect on the NEXT token issuance — existing JWTs retain the old role until they expire (or are revoked). For immediate role change enforcement, also revoke the user&#39;s existing tokens. |

 



<a name="forgepoint_billing_v1_billing-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/billing/v1/billing.proto



<a name="forgepoint-billing-v1-CheckQuotaRequest"></a>

### CheckQuotaRequest
============================================================================
CheckQuota  (gateway pre-flight)
============================================================================

CheckQuotaRequest asks &#34;how much of its quota does this team have left on this
meter?&#34;. WHY this RPC exists (design Task 8.4): the Inference Gateway calls it
BEFORE forwarding a request, to reject/throttle a team that is over quota. The
gateway CACHES the answer in Redis (eventual consistency with the DB is
acceptable for a pre-flight) and the events.QuotaExceeded outbox event flips
that cache to &#34;blocked&#34; between calls — CheckQuota is the cache-fill / cold
path, the event is the invalidation.

AUTHORIZATION/TENANCY: `team` is the team to check. For a non-admin caller the
server OVERRIDES it with the caller&#39;s own team derived from auth claims — the
field exists so the gateway (a trusted internal caller) and admins can scope
the check; it is NOT a way to probe another tenant&#39;s quota. Same rule as
GetUsage&#39;s team filter.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| team | [string](#string) |  | Team to check. Server overrides with the caller&#39;s own team for non-admins. |
| meter_type | [MeterType](#forgepoint-billing-v1-MeterType) |  | Which meter&#39;s quota to check. Must be a known, non-UNSPECIFIED meter. |






<a name="forgepoint-billing-v1-CheckQuotaResponse"></a>

### CheckQuotaResponse
CheckQuotaResponse reports the team&#39;s standing on the requested meter. All
fields are SERVER-computed from the team&#39;s current plan &#43; period usage.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| team | [string](#string) |  | The team checked &#43; the plan whose quota applies (echoed for the cache key). |
| rate_plan_id | [string](#string) |  |  |
| meter_type | [MeterType](#forgepoint-billing-v1-MeterType) |  |  |
| quota_limit | [int64](#int64) |  | The plan&#39;s hard cap for this meter (0 = unlimited / no cap configured). |
| current_usage | [int64](#int64) |  | The team&#39;s usage of this meter SO FAR in the current billing period. |
| remaining | [int64](#int64) |  | remaining = max(0, quota_limit - current_usage); 0 when over or at the cap. Server-computed so the gateway needn&#39;t re-derive it. |
| exceeded | [bool](#bool) |  | True if current_usage &gt;= quota_limit (i.e. the team is OVER quota and the gateway should reject/throttle). False when quota_limit == 0 (no cap). |






<a name="forgepoint-billing-v1-CreateRatePlanRequest"></a>

### CreateRatePlanRequest
============================================================================
CreateRatePlan  (ADMIN — sets real prices)
============================================================================

CreateRatePlanRequest defines a NEW priced plan. This is an ADMIN-ONLY write:
it sets the unit prices, free allowances, and quota caps the metering math
will apply to real money, so the server enforces an admin role before
accepting it. It is also the second-most security-sensitive write here, so the
same mass-assignment discipline as RecordUsage applies.

SERVER-AUTHORITATIVE (absent from this request by design):
  id          → server-assigned UUID (a client may not choose/overwrite a
                plan id — that&#39;s how you&#39;d hijack an existing plan).
  created_at  → server-stamped.
The client supplies only the pricing DEFINITION (name &#43; the three maps).

PRICE VALIDATION (server-side invariant): every unit_prices entry must use the
SAME currency_code (no mixed-currency plan) and a NON-NEGATIVE amount; every
map key must be a valid MeterType name. Invalid → InvalidArgument.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Human-readable plan name (&#34;free-tier&#34;,&#34;standard&#34;,&#34;enterprise&#34;). Unique per deployment is a server-enforced constraint, not a wire concern. |
| unit_prices | [CreateRatePlanRequest.UnitPricesEntry](#forgepoint-billing-v1-CreateRatePlanRequest-UnitPricesEntry) | repeated | Per-meter unit prices. Key = MeterType name; value = price per ONE unit. All values MUST share one currency_code (server-validated). |
| included_quantities | [CreateRatePlanRequest.IncludedQuantitiesEntry](#forgepoint-billing-v1-CreateRatePlanRequest-IncludedQuantitiesEntry) | repeated | Included free allowance per billing period, keyed by MeterType name. Empty = no free tier. |
| quota_limits | [CreateRatePlanRequest.QuotaLimitsEntry](#forgepoint-billing-v1-CreateRatePlanRequest-QuotaLimitsEntry) | repeated | Hard monthly quota cap per MeterType name. Crossing it fires the events.QuotaExceeded outbox event. 0 / absent = no hard cap for that meter. |
| idempotency_key | [string](#string) |  | Idempotency key (UUID/stable string). A retry with the same key returns the ORIGINAL plan instead of creating a duplicate — admin tooling retries too. |






<a name="forgepoint-billing-v1-CreateRatePlanRequest-IncludedQuantitiesEntry"></a>

### CreateRatePlanRequest.IncludedQuantitiesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [int64](#int64) |  |  |






<a name="forgepoint-billing-v1-CreateRatePlanRequest-QuotaLimitsEntry"></a>

### CreateRatePlanRequest.QuotaLimitsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [int64](#int64) |  |  |






<a name="forgepoint-billing-v1-CreateRatePlanRequest-UnitPricesEntry"></a>

### CreateRatePlanRequest.UnitPricesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [Money](#forgepoint-billing-v1-Money) |  |  |






<a name="forgepoint-billing-v1-CreateRatePlanResponse"></a>

### CreateRatePlanResponse
CreateRatePlanResponse returns the server-built plan (id &#43; created_at set).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rate_plan | [RatePlan](#forgepoint-billing-v1-RatePlan) |  | The committed plan, with its server-assigned id and created_at. |
| deduplicated | [bool](#bool) |  | True if the idempotency key already existed and `rate_plan` is the pre-existing one rather than a newly created plan. |






<a name="forgepoint-billing-v1-GetInvoiceRequest"></a>

### GetInvoiceRequest
GetInvoiceRequest fetches a single invoice by id.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invoice_id | [string](#string) |  | The invoice UUID (Invoice.id). Server enforces that the caller&#39;s team owns this invoice (or the caller is admin) — no cross-team invoice reads. |






<a name="forgepoint-billing-v1-GetInvoiceResponse"></a>

### GetInvoiceResponse
GetInvoiceResponse wraps the invoice.
WHY wrap rather than return Invoice directly: Buf RPC_RESPONSE_STANDARD_NAME,
and forward-compat (we may later add e.g. a payment_url alongside the invoice
without touching the Invoice domain message).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invoice | [Invoice](#forgepoint-billing-v1-Invoice) |  | The requested invoice with its line items and server-computed totals. |






<a name="forgepoint-billing-v1-GetRatePlanRequest"></a>

### GetRatePlanRequest
GetRatePlanRequest fetches a single rate plan by id. WHY expose this: a BFF /
the `fp` CLI needs to SHOW a team the pricing it will be billed under, and a
usage report references the plan that was applied. Read-only; a non-admin may
read the plan currently assigned to its own team (server-enforced), not an
arbitrary plan id.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rate_plan_id | [string](#string) |  | The rate plan UUID (RatePlan.id). |






<a name="forgepoint-billing-v1-GetRatePlanResponse"></a>

### GetRatePlanResponse
GetRatePlanResponse wraps the plan (forward-compat per RPC_RESPONSE_STANDARD).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rate_plan | [RatePlan](#forgepoint-billing-v1-RatePlan) |  | The requested rate plan with its unit prices, allowances, and quota caps. |






<a name="forgepoint-billing-v1-GetUsageRequest"></a>

### GetUsageRequest
============================================================================
GetUsage
============================================================================

GetUsageRequest asks for an AGGREGATED, paginated usage view for a team over
a time window. Returns UsageSummary rollups, not raw records (see UsageSummary
WHY note). Pagination applies when the window is broken into sub-period
buckets (e.g. daily) so a long range stays bounded.

AUTHORIZATION NOTE: a non-admin caller may only query their OWN team. The
`team` filter is enforced server-side against auth claims — supplying another
team is rejected unless the caller is an admin. The field exists so admins and
the BFF can scope queries; it is NOT a way to bypass tenant isolation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| team | [string](#string) |  | Team to report on. For non-admins the server overrides this with the caller&#39;s own team (cannot read another team&#39;s spend). Required for admins. |
| period_start | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Inclusive start / exclusive end of the reporting window. Empty start = current billing period start; empty end = now. The server CAPS the maximum span to MAX_USAGE_WINDOW (server constant, e.g. 366 days) to bound query cost; a wider request is rejected with InvalidArgument rather than silently truncated, so a caller can&#39;t accidentally scan the whole ledger. |
| period_end | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| meter_type_filter | [MeterType](#forgepoint-billing-v1-MeterType) |  | Optional: restrict to a single meter type. UNSPECIFIED = all meters. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination over sub-period buckets. page_size defaults to 20, capped at MAX_PAGE_SIZE = 100 server-side (see common.proto). A request above the cap is clamped to 100, never honored as-is. See PaginationRequest. |






<a name="forgepoint-billing-v1-GetUsageResponse"></a>

### GetUsageResponse
GetUsageResponse returns the page of usage summaries plus a grand total.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| summaries | [UsageSummary](#forgepoint-billing-v1-UsageSummary) | repeated | Per-bucket rollups for this page (e.g. one summary per day in the window). |
| grand_total | [Money](#forgepoint-billing-v1-Money) |  | The aggregate cost across the ENTIRE requested window (not just this page), so a dashboard can show the period total without paging through everything. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata: next_page_token &#43; total_count of buckets. |






<a name="forgepoint-billing-v1-Invoice"></a>

### Invoice
============================================================================
Invoice — a finalized (or draft) bill for a team over a billing period.
============================================================================

WHY invoices are derived, never client-authored:
  An invoice is a server-built aggregation of UsageRecords plus the plan&#39;s
  pricing over a closed period. There is no &#34;create invoice from client data&#34;
  path — that would let a customer write their own bill. Invoices are produced
  by the period-close job, which emits InvoiceGenerated through the outbox.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Stable invoice identifier. |
| invoice_number | [string](#string) |  | Human-facing invoice number (e.g. &#34;INV-2026-000042&#34;). Server-assigned, monotonic, distinct from the UUID so it&#39;s safe to print/share. |
| team | [string](#string) |  | Team being billed. |
| rate_plan_id | [string](#string) |  | The rate plan in effect for this period. |
| status | [InvoiceStatus](#forgepoint-billing-v1-InvoiceStatus) |  | Lifecycle status. SERVER-authoritative; drives collectibility. |
| period_start | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Billing period covered (inclusive start, exclusive end). |
| period_end | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| line_items | [InvoiceLineItem](#forgepoint-billing-v1-InvoiceLineItem) | repeated | The priced breakdown that sums to the total. One line per meter (and/or per model), so the customer can see WHERE cost came from. |
| total | [Money](#forgepoint-billing-v1-Money) |  | The invoice total. Server-computed sum of line_items, rounded to the currency&#39;s minor unit (cents) at finalization. Authoritative. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the invoice was created (period open) and finalized (period close). finalized_at is unset while status == DRAFT. |
| finalized_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| due_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When payment is due (set on finalization). Past-due drives OVERDUE. |






<a name="forgepoint-billing-v1-InvoiceLineItem"></a>

### InvoiceLineItem
InvoiceLineItem — one priced row on an invoice.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meter_type | [MeterType](#forgepoint-billing-v1-MeterType) |  | What this line meters. |
| description | [string](#string) |  | Free-text description for the customer: &#34;Inference tokens (gpt-mini v3)&#34;. |
| quantity | [int64](#int64) |  | Total billable units on this line (after free allowance). |
| unit_price | [Money](#forgepoint-billing-v1-Money) |  | Per-unit price applied (from the pinned rate plan), for transparency. |
| amount | [Money](#forgepoint-billing-v1-Money) |  | Line total = quantity * unit_price. Server-computed. |






<a name="forgepoint-billing-v1-ListInvoicesRequest"></a>

### ListInvoicesRequest
ListInvoicesRequest lists a team&#39;s invoices, newest first, paginated.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| team | [string](#string) |  | Team whose invoices to list. Non-admins are scoped to their own team server-side regardless of this value (tenant isolation). |
| status_filter | [InvoiceStatus](#forgepoint-billing-v1-InvoiceStatus) |  | Optional status filter (e.g. only OVERDUE for a dunning view). UNSPECIFIED = all statuses. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20, capped at MAX_PAGE_SIZE = 100 server-side (clamped, not honored as-is). See common.proto PaginationRequest for the WHY on cursors. |






<a name="forgepoint-billing-v1-ListInvoicesResponse"></a>

### ListInvoicesResponse
ListInvoicesResponse returns the page of invoices.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invoices | [Invoice](#forgepoint-billing-v1-Invoice) | repeated | The page of invoices (server orders by created_at DESC — newest first). |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata: next_page_token &#43; total_count. |






<a name="forgepoint-billing-v1-MeterUsage"></a>

### MeterUsage
MeterUsage — one meter&#39;s bucket inside a UsageSummary.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meter_type | [MeterType](#forgepoint-billing-v1-MeterType) |  | Which meter this bucket is for (redundant with the map key, included so the value is self-describing if extracted from the map). |
| total_quantity | [int64](#int64) |  | Total units of this meter over the window. |
| total_cost | [Money](#forgepoint-billing-v1-Money) |  | Total cost attributed to this meter over the window. |






<a name="forgepoint-billing-v1-Money"></a>

### Money
============================================================================
Money — a precise monetary amount.
============================================================================

WHY integer minor units, NOT double:
  Floating point cannot represent most decimal money values exactly
  (0.10 &#43; 0.20 != 0.30 in IEEE-754). Accumulating float errors over millions
  of metered calls produces real money discrepancies and fails reconciliation.
  We store the amount as an INTEGER in the currency&#39;s minor unit so all
  arithmetic is exact integer math. This mirrors Stripe&#39;s Money model and the
  google.type.Money guidance.

WHY micro-units (1e-6) rather than cents:
  Per-token / per-request prices are tiny — a single token might cost
  0.0000004 USD. Cents (1e-2) would round that to zero. We use MICRO units
  (1/1,000,000 of the major unit, i.e. micro-dollars) so we can price and sum
  sub-cent meters exactly and only round to cents at invoice finalization.
  So amount_micros = 1_000_000 means 1.00 USD; 4 means 0.000004 USD.

WHY currency_code is here on every amount:
  Self-describing money prevents the classic &#34;is this dollars or cents? USD
  or EUR?&#34; ambiguity at every boundary. ISO 4217 code (&#34;USD&#34;, &#34;EUR&#34;).

SECURITY: Money only ever appears in RESPONSES and in server-built event
payloads. It is NEVER a field on a client write request — clients submit
quantities, the server attaches the price.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount_micros | [int64](#int64) |  | Amount in micro-units of the currency&#39;s major unit (1e-6). Exact integer. 1_000_000 == 1.00 of the currency. May be negative for credits/refunds. |
| currency_code | [string](#string) |  | ISO 4217 currency code, uppercase: &#34;USD&#34;, &#34;EUR&#34;. Empty is invalid. |






<a name="forgepoint-billing-v1-RatePlan"></a>

### RatePlan
============================================================================
RatePlan — the pricing the server applies to metered usage.
============================================================================

WHY this lives server-side and is referenced, not supplied:
  The rate plan is the SOURCE of every price. A usage record references a rate
  plan by id; the server reads the plan&#39;s unit prices to compute cost. The
  client never sees or sends prices on the write path — it can only learn them
  after the fact via GetUsage/GetInvoice. This is the structural guarantee
  that &#34;the client cannot set its own price&#34;.

RATE-PLAN MANAGEMENT RPCs: CreateRatePlan (admin) and GetRatePlan are part of
  this surface — the platform design (Phase 8.1) lists CreateRatePlan, and the
  metering math is undefined without a plan to resolve. CreateRatePlan is
  ADMIN-ONLY (it sets prices for real money) and id/created_at are server-set
  (mass-assignment guard). GetRatePlan lets a client/BFF read the pricing it
  will be billed under. Mutating an existing plan&#39;s prices in place is
  deliberately NOT offered: prices are immutable once a usage record has pinned
  them (reproducible billing), so &#34;changing&#34; a plan means creating a new one —
  adding a versioned UpdateRatePlan later is additive (buf-breaking-safe).
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Stable identifier referenced by usage records and invoices. |
| name | [string](#string) |  | Human-readable plan name: &#34;free-tier&#34;, &#34;standard&#34;, &#34;enterprise&#34;. |
| unit_prices | [RatePlan.UnitPricesEntry](#forgepoint-billing-v1-RatePlan-UnitPricesEntry) | repeated | Per-meter unit prices. Key is the MeterType&#39;s string name (e.g. &#34;METER_TYPE_INFERENCE_TOKENS&#34;); value is the price for ONE unit of that meter (per request, per token, per compute-second, per byte) as Money. WHY a map keyed by meter rather than fixed fields: meter types grow over time; a map lets us add a meter&#39;s price without a schema change. |
| included_quantities | [RatePlan.IncludedQuantitiesEntry](#forgepoint-billing-v1-RatePlan-IncludedQuantitiesEntry) | repeated | Included free allowance per billing period, keyed by meter name. Usage beyond the allowance is what gets charged. Empty = no free tier. |
| quota_limits | [RatePlan.QuotaLimitsEntry](#forgepoint-billing-v1-RatePlan-QuotaLimitsEntry) | repeated | Monthly quota cap per meter name (hard limit). Exceeding it is what fires the QuotaExceeded event. 0 / absent for a given meter = no hard cap. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this plan was created. Server-set, immutable. |






<a name="forgepoint-billing-v1-RatePlan-IncludedQuantitiesEntry"></a>

### RatePlan.IncludedQuantitiesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [int64](#int64) |  |  |






<a name="forgepoint-billing-v1-RatePlan-QuotaLimitsEntry"></a>

### RatePlan.QuotaLimitsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [int64](#int64) |  |  |






<a name="forgepoint-billing-v1-RatePlan-UnitPricesEntry"></a>

### RatePlan.UnitPricesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [Money](#forgepoint-billing-v1-Money) |  |  |






<a name="forgepoint-billing-v1-RecordUsageRequest"></a>

### RecordUsageRequest
============================================================================
RecordUsage
============================================================================

RecordUsageRequest is the STRICT metering-input contract. This is the single
most security-sensitive write in the platform, so the input surface is
deliberately MINIMAL: the caller supplies only the verifiable raw facts, and
the server attaches everything that touches money or attribution.

WHAT THE CLIENT MAY SEND (and only this):
  meter_type, quantity, model_id, model_version, source_request_id,
  occurred_at, idempotency_key.

WHAT THE CLIENT MAY NOT SEND (server-authoritative — absent by design):
  team / billed account  → derived from auth claims or the inference API key
  rate_plan_id           → resolved from the team&#39;s current plan
  cost / price / Money   → computed by the server from the plan
  any invoice/status     → never settable

WHY occurred_at is accepted but still constrained:
  The metering source (InferenceCompleted) carries the true event time, which
  can legitimately be slightly in the past (queue lag). We accept it so usage
  lands in the correct billing period, but the server CLAMPS it to a sane
  window (e.g. not in the future, not older than the open period) so a caller
  can&#39;t backdate usage into a closed/paid invoice. Empty = server uses now().

IDEMPOTENCY (critical for an at-least-once meter):
  The upstream InferenceCompleted is delivered at-least-once, and clients
  retry. A duplicate must NOT bill twice. idempotency_key (client-generated,
  stable per logical action — typically the inference request id) is stored
  with the created UsageRecord; a repeat with the same key returns the
  ORIGINAL record instead of inserting a second one. This is the NATS→DB half
  of the exactly-once-in-effect guarantee (the outbox is the DB→NATS half).
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| meter_type | [MeterType](#forgepoint-billing-v1-MeterType) |  | What is being metered. Must be a known, non-UNSPECIFIED meter. |
| quantity | [int64](#int64) |  | How many units (requests, tokens, compute-seconds, bytes). Server validates NON-NEGATIVE (a client may never submit a negative quantity; only server- issued credits are negative) and within a sane per-record cap: MAX_QUANTITY_PER_RECORD (server constant, e.g. 1_000_000_000) — chosen so quantity * the largest unit price still cannot overflow int64 micro-units, so the cost multiply is provably safe. Out-of-range → InvalidArgument (rejected, never truncated). See the INTEGER OVERFLOW note in the header. |
| model_id | [string](#string) |  | What produced the usage (for attribution / per-model reports). Optional but strongly recommended; used only for reporting, never for pricing. |
| model_version | [string](#string) |  |  |
| source_request_id | [string](#string) |  | The originating inference request id (or upstream event id). Used for lineage and as the natural dedupe anchor. SHOULD be set for inference usage. |
| occurred_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Event time of the metered action. Server clamps to the valid window; empty means &#34;use server now()&#34;. See WHY note above — cannot backdate into a closed period. |
| idempotency_key | [string](#string) |  | Idempotency key (UUID/stable string). A repeat returns the original record. See IDEMPOTENCY note above — this is what makes redelivery safe. |






<a name="forgepoint-billing-v1-RecordUsageResponse"></a>

### RecordUsageResponse
RecordUsageResponse returns the server-built, priced UsageRecord.
WHY return the full record: the caller (and the integration test) can see the
authoritative team, rate_plan_id, and computed cost the server attached —
proving the price was server-derived, not client-supplied.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| record | [UsageRecord](#forgepoint-billing-v1-UsageRecord) |  | The committed, priced ledger record (id, team, cost all server-set). |
| deduplicated | [bool](#bool) |  | True if this call hit the idempotency path (the key was already recorded) and `record` is the pre-existing one rather than a newly inserted row. Lets retrying clients distinguish &#34;I just created it&#34; from &#34;already there&#34;. |






<a name="forgepoint-billing-v1-UsageRecord"></a>

### UsageRecord
============================================================================
UsageRecord — one metered, priced fact (the atom of the ledger).
============================================================================

WHY this is the unit of record:
  Every billable action becomes exactly one UsageRecord. Invoices are just
  aggregations of UsageRecords over a period. Keeping the atom immutable and
  append-only (we never UPDATE a recorded usage row, we only INSERT) makes the
  ledger auditable and reconciliation tractable — the same append-only
  discipline as the Feature Store&#39;s event log.

EVERY money/attribution field below is SERVER-COMPUTED. The client side of a
RecordUsage supplies only meter_type &#43; quantity (&#43; what was served). See
RecordUsageRequest for the strict input contract.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4 assigned by the server. Primary key of the ledger row. |
| team | [string](#string) |  | The team this usage is billed to. SERVER-derived from the authenticated caller&#39;s claims (team) or, for the InferenceCompleted consumer, from the API key that made the inference. NEVER taken from a client write field — letting a caller name the billed team is account-takeover-for-money. |
| rate_plan_id | [string](#string) |  | The rate plan that was applied to price this record. SERVER-resolved from the team&#39;s current plan at metering time, and pinned here so the price is reproducible even if the team&#39;s plan later changes. |
| meter_type | [MeterType](#forgepoint-billing-v1-MeterType) |  | What was metered. |
| quantity | [int64](#int64) |  | How many units of that meter (request count, token count, compute-seconds, bytes). This is the one quantity the client/event supplies and the server validates (non-negative, sane bounds). |
| cost | [Money](#forgepoint-billing-v1-Money) |  | The price the server computed for this record: quantity * plan unit price (after free-allowance logic). Authoritative; clients cannot supply it. |
| model_id | [string](#string) |  | What produced the usage — for attribution and per-model usage reports. SERVER-populated from the inference event / request context. |
| model_version | [string](#string) |  |  |
| source_request_id | [string](#string) |  | The inference request id (or upstream event id) this record was derived from. Doubles as the natural idempotency anchor: two records must never share a source_request_id. SERVER-stored from the metering input. |
| occurred_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the metered action occurred (event time, not insert time). SERVER-stamped from the source event; clients cannot backdate usage. |






<a name="forgepoint-billing-v1-UsageSummary"></a>

### UsageSummary
============================================================================
UsageSummary — an aggregated usage rollup (the shape GetUsage returns).
============================================================================

WHY a summary type distinct from UsageRecord:
  GetUsage answers &#34;how much has team X used / spent this period?&#34; — callers
  (dashboards, the BFF) want a ROLLUP, not millions of raw records. Returning
  raw UsageRecords for a heavy team would be unbounded and useless for a
  dashboard. The summary buckets quantity and cost per meter type.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| team | [string](#string) |  | The team this summary covers. |
| period_start | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | The window this rollup aggregates over (inclusive start, exclusive end). |
| period_end | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| by_meter | [UsageSummary.ByMeterEntry](#forgepoint-billing-v1-UsageSummary-ByMeterEntry) | repeated | Per-meter aggregation. Key is the MeterType string name; value is the bucketed totals for that meter over the window. |
| total_cost | [Money](#forgepoint-billing-v1-Money) |  | Total cost across all meters in the window. Server-computed sum. |






<a name="forgepoint-billing-v1-UsageSummary-ByMeterEntry"></a>

### UsageSummary.ByMeterEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [MeterUsage](#forgepoint-billing-v1-MeterUsage) |  |  |





 


<a name="forgepoint-billing-v1-InvoiceStatus"></a>

### InvoiceStatus
============================================================================
InvoiceStatus — lifecycle of an invoice.
============================================================================

WHY an enum: invoice status drives money-moving side effects (you can only
charge a card on a FINALIZED invoice, you can&#39;t re-finalize a PAID one). A
closed validated set with explicit transitions is mandatory for an auditable
ledger. Status is SERVER-authoritative — a client can never set it.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| INVOICE_STATUS_UNSPECIFIED | 0 | Required zero value. Unknown/unset. |
| INVOICE_STATUS_DRAFT | 1 | Accumulating: the billing period is still open and line items are still being metered onto this invoice. Not yet collectible. |
| INVOICE_STATUS_FINALIZED | 2 | Period closed and totals locked. The InvoiceGenerated event fires on this transition. Collectible but not yet paid. |
| INVOICE_STATUS_PAID | 3 | Payment received and reconciled. Terminal (happy path). |
| INVOICE_STATUS_OVERDUE | 4 | Finalized but unpaid past its due date. Drives dunning / Notification. |
| INVOICE_STATUS_VOID | 5 | Reversed/written off (e.g. credit issued). Terminal. Kept for audit, never deleted — ledgers are append-only. |



<a name="forgepoint-billing-v1-MeterType"></a>

### MeterType
============================================================================
MeterType — what kind of resource a usage record measures.
============================================================================

WHY an enum, not a free string:
  Closed, validated set at the wire boundary. The rate plan prices each meter
  type differently, so a typo (&#34;infernce&#34;) must not silently fall through to
  a zero price. Buf STANDARD requires the _UNSPECIFIED zero value and the
  ENUM_NAME prefix on every value.

WHY separate inference-request vs inference-tokens:
  Real ML billing meters on MORE than one axis simultaneously — a single
  inference call costs a per-request fee AND a per-token (or per-compute-unit)
  fee. Modeling them as distinct meter types lets one InferenceCompleted
  produce multiple priced line items, which is how OpenAI/Anthropic-style
  token billing actually works.

MIRRORED ON THE BUS: events.v1.MeterType has byte-identical values. This is
  the SERVICE/API enum; the handler maps billing.MeterType &lt;-&gt; events.MeterType
  at the publish/consume boundary so the event schema can evolve independently
  of this API enum (the events.proto decoupling discipline). Keep the two in
  lockstep when adding a meter.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| METER_TYPE_UNSPECIFIED | 0 | Required zero value. Unknown/unset meter — the server rejects RecordUsage with this value rather than guessing a price. |
| METER_TYPE_INFERENCE_REQUEST | 1 | One billable inference REQUEST (the per-call fee), regardless of size. |
| METER_TYPE_INFERENCE_TOKENS | 2 | Tokens processed by an inference call (prompt &#43; completion). Priced per-1k-tokens by the rate plan. The dominant cost axis for LLM serving. |
| METER_TYPE_COMPUTE_SECONDS | 3 | Compute-seconds consumed by serving (for non-token models priced on wall-clock / GPU time rather than tokens). |
| METER_TYPE_STORAGE_BYTES | 4 | Artifact bytes stored in the model registry / object store, metered periodically. Lets storage cost ride the same pipeline as inference cost. |


 

 


<a name="forgepoint-billing-v1-BillingService"></a>

### BillingService
============================================================================
BILLING SERVICE
============================================================================

WHY a single BillingService (not separate Usage / Invoice services):
  Usage and invoicing are the same bounded context — invoices ARE aggregated
  usage. Splitting them would force a cross-service call on every period close
  and fracture the ledger&#39;s single source of truth. One service owns the
  `fp_billing` database (usage_events, quotas, rate_plans, invoices, outbox)
  per the database-per-service standard.

RPC CATEGORIES:
  METERING (write): RecordUsage — the outbox-protected write path. In steady
    state this is driven by the events.InferenceCompleted NATS consumer (and
    events.ModelVersionReady for storage); exposed on gRPC for backfills and
    direct metering.
  PRICING (admin/read): CreateRatePlan (admin write — sets real prices),
    GetRatePlan (read the pricing a team is billed under).
  QUOTA (read): CheckQuota — the gateway&#39;s pre-flight; cheap &#34;how much is left&#34;
    answer the gateway caches in Redis (the QuotaExceeded event invalidates it).
  REPORTING (read): GetUsage (aggregated, paged), GetInvoice, ListInvoices
    (paged) — read-only views over the ledger.

WHY ALL UNARY (no streaming):
  - RecordUsage / CreateRatePlan / CheckQuota are single fact-in / record-out —
    request/reply.
  - GetUsage/ListInvoices return BOUNDED, PAGINATED pages; pagination (not a
    server stream) is the right tool because it gives clients resumability,
    cacheable page tokens, and a natural total_count — the same choice the
    registry&#39;s list RPCs made. A server stream would suit an UNBOUNDED live
    feed (e.g. tail usage in real time); that real-time need is served by the
    async events.UsageRecorded NATS event instead, keeping the gRPC surface
    simple.

QUOTA ENFORCEMENT — two complementary mechanisms:
  CheckQuota is the PULL/pre-flight (gateway asks before forwarding; cache-fill
  cold path). The events.QuotaExceeded outbox event is the PUSH/invalidation
  (the moment a team crosses the cap, the event flips the gateway&#39;s Redis cache
  to &#34;blocked&#34;). Eventual consistency between DB and the gateway cache is
  acceptable for a pre-flight — the worst case is a handful of over-quota calls
  slip through before the cache flips, which the next RecordUsage still meters.

ASCII DIAGRAM — the outbox metering pipeline:

  Inference Gateway ──fp.inference.completed──► Billing NATS consumer
                                                     │ (dedupe on request_id)
                                                     ▼
                                        ┌─ BEGIN tx ───────────────────────────┐
                                        │  INSERT usage_record (priced)        │
                                        │  UPDATE quota counter                │
                                        │  INSERT outbox(events.UsageRecorded) │
                                        │  if over quota:                      │
                                        │     INSERT outbox(events.QuotaExceeded)│
                                        └─ COMMIT ─────────────────────────────┘
                                                     │
                           outbox poller (separate goroutine, at-least-once)
                                                     ▼
                           NATS  fp.billing.usage.recorded / .quota.exceeded
                                                     ▼
                           Notification / Inference Gateway / Experiment Tracker
============================================================================

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| RecordUsage | [RecordUsageRequest](#forgepoint-billing-v1-RecordUsageRequest) | [RecordUsageResponse](#forgepoint-billing-v1-RecordUsageResponse) | RecordUsage meters one billable action and writes it to the ledger via the outbox. The cost is COMPUTED server-side from the team&#39;s rate plan — the request carries only meter facts, never a price (see RecordUsageRequest). Idempotent via idempotency_key, so a redelivered events.InferenceCompleted or a client retry never double-bills. This is the outbox pattern&#39;s write half. |
| CreateRatePlan | [CreateRatePlanRequest](#forgepoint-billing-v1-CreateRatePlanRequest) | [CreateRatePlanResponse](#forgepoint-billing-v1-CreateRatePlanResponse) | CreateRatePlan defines a new priced plan (unit prices, free allowances, quota caps). ADMIN-ONLY — it sets real money. id/created_at are server-assigned (mass-assignment guard); idempotent via idempotency_key. |
| GetRatePlan | [GetRatePlanRequest](#forgepoint-billing-v1-GetRatePlanRequest) | [GetRatePlanResponse](#forgepoint-billing-v1-GetRatePlanResponse) | GetRatePlan returns a single rate plan&#39;s pricing so a BFF/CLI can show a team what it will be billed. Non-admins may read only their own team&#39;s plan. Read-only. |
| CheckQuota | [CheckQuotaRequest](#forgepoint-billing-v1-CheckQuotaRequest) | [CheckQuotaResponse](#forgepoint-billing-v1-CheckQuotaResponse) | CheckQuota is the gateway&#39;s pre-flight: returns remaining quota &#43; an `exceeded` flag for a team/meter so the gateway can reject/throttle before forwarding. Server-computed; non-admins scoped to their own team. Read-only. |
| GetUsage | [GetUsageRequest](#forgepoint-billing-v1-GetUsageRequest) | [GetUsageResponse](#forgepoint-billing-v1-GetUsageResponse) | GetUsage returns aggregated, paginated usage summaries (per-meter rollups &#43; grand total) for a team over a time window. Non-admins are scoped to their own team server-side. Read-only. |
| GetInvoice | [GetInvoiceRequest](#forgepoint-billing-v1-GetInvoiceRequest) | [GetInvoiceResponse](#forgepoint-billing-v1-GetInvoiceResponse) | GetInvoice fetches a single invoice by id, with its server-computed line items and total. Cross-team reads are denied (tenant isolation). Read-only. |
| ListInvoices | [ListInvoicesRequest](#forgepoint-billing-v1-ListInvoicesRequest) | [ListInvoicesResponse](#forgepoint-billing-v1-ListInvoicesResponse) | ListInvoices returns a team&#39;s invoices newest-first, paginated, optionally filtered by status (e.g. OVERDUE for dunning). Read-only. |

 



<a name="forgepoint_events_v1_events-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/events/v1/events.proto



<a name="forgepoint-events-v1-ApiKeyRotated"></a>

### ApiKeyRotated
ApiKeyRotated → fp.auth.apikey.rotated
  PRODUCER:  auth (after an API key is minted — and, on a rotation, the prior
             key is revoked in the same logical operation; see the blue/green
             rotation note in the auth domain&#39;s APIKey model).
  CONSUMERS: notification (tell the owner a new key was issued — a security-
             relevant event a human should see), audit/security log (record the
             credential lifecycle), inference-gateway / caches (proactively
             invalidate any cached validation for the REPLACED key so the
             revoked key stops working before its 30s validation-cache TTL
             would naturally expire).
  SECURITY/PII (this is the whole point): we publish ONLY the key id &#43; the
  8-char display prefix (e.g. &#34;fp_a1b2&#34;) and the owner/scopes — NEVER the raw
  key. The raw key exists for exactly one moment in CreateAPIKey&#39;s return value
  and is shown to the caller once (Stripe/GitHub-PAT model); it must never
  touch the bus, a log, or the DB. A consumer that needs to know &#34;which key&#34;
  uses key_id; a human-facing surface shows key_prefix.
  WHY &#34;rotated&#34; (not &#34;created&#34;): the canonical event-contract names this
  fp.auth.apikey.rotated. A plain create and a rotation (create-new &#43;
  revoke-old) are the same observable fact to consumers — &#34;the set of valid
  keys for this user changed&#34;. replaced_key_id distinguishes the two: empty on
  a first/independent create, set to the revoked key&#39;s id on a true rotation,
  so a cache-invalidating consumer knows exactly which key to evict.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key_id | [string](#string) |  | The NEW API key&#39;s id (UUID) — the stable handle, NOT the secret. |
| user_id | [string](#string) |  | The owning user (auth user id). Lets a consumer route the &#34;new key issued&#34; notice to the right human and scope audit entries. |
| key_prefix | [string](#string) |  | The new key&#39;s 8-char display prefix (e.g. &#34;fp_a1b2&#34;) — safe to show/log; it is NOT enough to authenticate. For human-facing surfaces only. |
| scopes | [string](#string) | repeated | The new key&#39;s scopes (&#34;resource:action&#34; strings). Carried so a security/audit consumer can see the granted capability set without a callback. The effective grant is still role ∩ scope, enforced at validation time. |
| replaced_key_id | [string](#string) |  | The id of the key this one REPLACES (and that was revoked as part of the rotation). Empty on a first/independent create; set on a true rotation. A cache-invalidating consumer evicts exactly this key id. |
| rotated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the new key was issued / the rotation committed (producer clock). |






<a name="forgepoint-events-v1-CompensationTriggered"></a>

### CompensationTriggered
CompensationTriggered → fp.pipelines.compensation.triggered
  CONSUMERS: notification (a first-class &#34;the saga is now rolling back&#34; signal).
  WHY a distinct event from StepFailed: a failure is the cause; this marks the
  UNDO phase beginning. compensating_step_ids exposes the rollback PLAN (the
  reverse-completion order steps will be undone in) so operators can watch it.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  |  |
| pipeline_id | [string](#string) |  |  |
| failed_step_id | [string](#string) |  | The step whose failure triggered compensation. |
| compensating_step_ids | [string](#string) | repeated | The ordered step ids that will be compensated, in REVERSE completion order (the order they&#39;ll actually be undone). Makes the rollback plan observable. |
| triggered_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When compensation began (producer clock). |






<a name="forgepoint-events-v1-DriftMetric"></a>

### DriftMetric
DriftMetric — one feature&#39;s/output&#39;s/metric&#39;s drift measurement. Carried
(repeated) inside ModelDriftDetected so a consumer sees WHICH feature moved
(&#34;income PSI=0.41, everything else &lt;0.05&#34;) — the actionable detail.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | What was measured: a feature name (data drift), output/class name (prediction drift), or metric name like &#34;accuracy&#34;/&#34;f1&#34; (performance). |
| method | [DriftMethod](#forgepoint-events-v1-DriftMethod) |  | Statistical method behind `score` (PSI/KL/KS) — travels with the score because thresholds are method-specific. |
| score | [double](#double) |  | The computed drift score under `method`. Higher = more drift. Producer-computed by the Monitor (server-authoritative; a client cannot fabricate a verdict). |
| baseline_value | [double](#double) |  | Baseline (training-time) summary value for context (e.g. baseline mean). |
| current_value | [double](#double) |  | Current-window summary value, paired with baseline_value. |
| severity | [DriftSeverity](#forgepoint-events-v1-DriftSeverity) |  | Severity for THIS metric (vs the monitor&#39;s thresholds). The event&#39;s overall severity is the max across metrics. |






<a name="forgepoint-events-v1-FeatureSummary"></a>

### FeatureSummary
FeatureSummary — a compact summary of a prediction&#39;s INPUT features, carried
on InferenceCompleted for DATA-drift detection. Same privacy rationale as
PredictionSummary: per-feature scalar summaries the Monitor folds into its
PSI/KS windows, never the raw feature vector and never a join-key to a person.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_values | [FeatureSummary.FeatureValuesEntry](#forgepoint-events-v1-FeatureSummary-FeatureValuesEntry) | repeated | Per-feature numeric value/summary keyed by feature name (e.g. {&#34;sepal_length&#34;:5.1}). High-dimensional inputs may be sampled/aggregated. |
| feature_count | [int32](#int32) |  | Total number of input features (dimensionality). Lets Monitor detect schema/shape drift independent of the sampled values above. |






<a name="forgepoint-events-v1-FeatureSummary-FeatureValuesEntry"></a>

### FeatureSummary.FeatureValuesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [double](#double) |  |  |






<a name="forgepoint-events-v1-FeatureViewDefined"></a>

### FeatureViewDefined
FeatureViewDefined → fp.features.view.defined
  PRODUCER:  feature-store (after DefineFeatureView appends a FeatureViewDefined
             event to the log / bumps schema_version).
  CONSUMERS: experiment-tracker (correlate which feature schema versions fed a
             training run); audit subscribers (log schema changes).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view_id | [string](#string) |  | The view&#39;s stable id &#43; name (name for human-friendly routing/filtering). |
| feature_view_name | [string](#string) |  |  |
| schema_version | [int64](#int64) |  | Schema version this event represents (1 on create, &#43;1 on each evolve). |
| owner_team | [string](#string) |  | Owning team (team-scoped consumers/audit). Producer-assigned from auth claims. |
| defined_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the definition/evolution happened (producer clock). |






<a name="forgepoint-events-v1-FeaturesWritten"></a>

### FeaturesWritten
FeaturesWritten → fp.features.written
  PRODUCER:  feature-store (after a WriteFeatures append commits to the log).
  CONSUMERS: experiment-tracker (record which feature versions fed a run),
             model-monitor (scope drift checks / cache invalidation to the
             changed entities).
  NAME RESOLUTION (conflict #4): the platform settles on ONE name —
  FeaturesWritten on subject fp.features.written. The design doc&#39;s
  &#34;FeaturesIngested&#34; / &#34;fp.features.ingested&#34; and the experiment-tracker&#39;s
  previously-consumed &#34;fp.features.ingested&#34; are RECONCILED to this. Fix agents
  MUST repoint the experiment-tracker subscription to fp.features.written.
  THIN BY DESIGN (the thin-event side of the tradeoff): carries the version
  range &#43; affected entity ids &#43; a count, NOT the feature values themselves.
  Events are for notification/correlation; a consumer that needs the actual
  values calls GetOnlineFeatures/GetHistoricalFeatures (the read models). This
  keeps NATS small and stops the bus becoming a second source of truth.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view_id | [string](#string) |  | The view that received the append (id &#43; name; name is a subscriber convenience). |
| feature_view_name | [string](#string) |  |  |
| entity_ids | [string](#string) | repeated | Entity ids whose features changed in this batch. Consumers (Monitor) use them to scope drift checks / invalidate caches. MAY be truncated for very large batches — written_count remains the authoritative total. |
| written_count | [int32](#int32) |  | Total rows appended (authoritative even if entity_ids is truncated). |
| written_through_version | [int64](#int64) |  | Highest event-log version assigned by this append. A consumer can request features as_of_version &gt;= this to be sure it sees the new data. |
| written_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the append committed (producer clock). |






<a name="forgepoint-events-v1-InferenceCompleted"></a>

### InferenceCompleted
InferenceCompleted → fp.inference.completed
  PRODUCER:  inference-gateway — THE SINGLE CANONICAL inference event. The
             gateway is the entry point and the ONLY component that knows the
             full picture: end-to-end latency, which version actually served
             (post traffic-split / canary), the billed principal (api_key_id),
             and the request_id it minted. serving MUST NOT publish a competing
             metering event (serving.PredictionCompletedEvent is deleted — see
             conflict #1).
  CONSUMERS: billing (meter usage — keys on api_key_id &#43; request_id, dedupes on
             request_id), experiment-tracker (record A/B outcomes per served
             version), model-monitor (fold feature_summary into DATA-drift
             windows, prediction_summary into PREDICTION-drift windows, latency
             into performance signals; request_id is the JOIN KEY for delayed
             ground truth).
  SECURITY/PII (this leaves the gateway over the bus): IDs &#43; SUMMARIES only —
  never raw input/output tensors, never an end-user identity. api_key_id is a
  reference, not the secret. The summaries are statistical (see their docs).
  IDEMPOTENCY: EventEnvelope.id dedupes redeliveries at the transport layer;
  request_id is the BUSINESS dedupe handle (Billing won&#39;t double-meter a
  request_id it already recorded — the NATS→DB half of exactly-once-in-effect).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| request_id | [string](#string) |  | Gateway-minted request id (matches PredictResponse.request_id). The business idempotency key for Billing AND the join key for Monitor&#39;s ground-truth matching. SERVER-authoritative. |
| model_id | [string](#string) |  | The model that served (registry id) &#43; its public name (for dashboards). |
| model_name | [string](#string) |  |  |
| version | [string](#string) |  | The version that ACTUALLY served, post traffic-split. KEY for A/B analysis and per-version metering/monitoring. SERVER-authoritative. |
| api_key_id | [string](#string) |  | The API key that made the call (id, never the raw secret). Billing meters against this (→ team/rate-plan). SERVER-stamped from the authed request. |
| is_canary | [bool](#bool) |  | True if this prediction was served by a CANARY target (a non-stable version receiving a traffic slice). Lets Experiment Tracker bucket A/B outcomes and Monitor weight canary traffic distinctly. SERVER-authoritative. |
| latency | [google.protobuf.Duration](#google-protobuf-Duration) |  | Observed backend latency, precise typed value &#43; a denormalized whole-ms convenience for simple consumers/Grafana panels (derived from `latency`). |
| latency_ms | [int64](#int64) |  |  |
| token_count | [int64](#int64) |  | Token count for the call (prompt&#43;completion), when applicable — lets Billing meter METER_TYPE_INFERENCE_TOKENS off the single canonical event rather than a competing serving event. 0 for non-token models. |
| prediction_summary | [PredictionSummary](#forgepoint-events-v1-PredictionSummary) |  | Compact prediction summary for prediction-drift / A-B (NOT raw outputs). |
| feature_summary | [FeatureSummary](#forgepoint-events-v1-FeatureSummary) |  | Compact input feature summary for data-drift detection (NOT raw inputs). |
| completed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the inference completed (gateway clock). SERVER-authoritative; drives billing periods and time-windowed drift aggregation. |






<a name="forgepoint-events-v1-InferenceFailed"></a>

### InferenceFailed
InferenceFailed → fp.inference.failed
  PRODUCER:  inference-gateway.
  CONSUMERS: model-monitor (error RATE is its own degradation signal — a
             spiking failure rate is a kind of drift), notification (alert on
             sustained failures).
  WHY no summaries: on failure there may be no prediction to summarize and we
  avoid echoing a possibly-malformed input. We carry the failure CLASSIFICATION
  (which resilience pattern fired) instead, which is what drives the reaction.
  WHY typically NOT billed: Billing generally does not meter failed calls;
  carrying api_key_id still lets per-tenant error rates be computed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| request_id | [string](#string) |  | Gateway-minted request id (idempotency &#43; correlation). |
| model_id | [string](#string) |  | The model the client targeted (&#43; id if routing resolved it). |
| model_name | [string](#string) |  |  |
| version | [string](#string) |  | The version attempted, if routing got that far (empty if it failed before version selection, e.g. NO_ROUTE). |
| api_key_id | [string](#string) |  | The API key that made the call (id only) — per-tenant error rates / policy. |
| reason | [InferenceFailureReason](#forgepoint-events-v1-InferenceFailureReason) |  | Why it failed — the resilience-pattern outcome (drives Monitor/alerts). |
| error | [forgepoint.common.v1.ErrorDetail](#forgepoint-common-v1-ErrorDetail) |  | Structured error detail (reuses common.ErrorDetail so consumers handle it uniformly with the sync API). Human-readable message &#43; code, no PII. |
| failed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the failure occurred (gateway clock). |






<a name="forgepoint-events-v1-InvoiceGenerated"></a>

### InvoiceGenerated
InvoiceGenerated → fp.billing.invoice.generated
  PRODUCER:  billing (when a period closes and an invoice goes DRAFT→FINALIZED;
             via the outbox).
  CONSUMERS: notification (email/Slack the finalized invoice), external
             payment/accounting integrations.
  FAT-BUT-FLAT: carries the headline invoice facts (number, team, period,
  total) so Notification can render/send without a GetInvoice callback. The
  full line-item breakdown stays in the Invoice read model (GetInvoice) — a
  deliberate thin-tail: the alert needs the total, not every line.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invoice_id | [string](#string) |  | Invoice UUID &#43; the human-facing invoice number (safe to print/share). |
| invoice_number | [string](#string) |  |  |
| team | [string](#string) |  | Team billed &#43; the rate plan in effect for the period. |
| rate_plan_id | [string](#string) |  |  |
| period_start | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Billing period covered [start, end). |
| period_end | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| total_micros | [int64](#int64) |  | The invoice total: amount in micro-units &#43; ISO-4217 currency (rounded to the currency minor unit at finalization). Server-computed, authoritative. |
| currency_code | [string](#string) |  |  |
| finalized_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the invoice was finalized (producer clock). |






<a name="forgepoint-events-v1-MetricPoint"></a>

### MetricPoint
MetricPoint — one (key,value,step,ts) sample of an ML metric time-series.
Carried (repeated) on RunFinished as the run&#39;s headline/final metrics so a
consumer can rank/alert without a GetRun callback (&#34;fat event&#34;).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | Metric name, e.g. &#34;accuracy&#34;, &#34;val_auc&#34;. |
| value | [double](#double) |  | The measured value. |
| step | [int64](#int64) |  | Monotonic training step/epoch index (the X axis). Producer-supplied. |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Wall-clock time of measurement (producer-stamped on ingest). |






<a name="forgepoint-events-v1-ModelArchived"></a>

### ModelArchived
ModelArchived → fp.models.archived
  PRODUCER:  registry (after DeleteModel soft-deletes the model &#43; its versions).
  CONSUMERS: inference-gateway (stop routing to it), serving (tear down running
             instances), billing (stop metering).
  WHY model_name too: gateway/serving key routes/loads by name, so carrying it
  saves every consumer a lookup at exactly the moment the model is going away.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | The archived model&#39;s id &#43; name. |
| model_name | [string](#string) |  |  |
| archived_by | [string](#string) |  | Who archived it (auth claims) — audit. |
| archived_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the archive committed (producer clock). |






<a name="forgepoint-events-v1-ModelDeployed"></a>

### ModelDeployed
ModelDeployed → fp.pipelines.model.deployed
  PRODUCER:  pipeline-orchestrator — the DEPLOYMENT SAGA OWNS this. (Was MISSING
             from every proto though the gateway/serving consume it; added here,
             conflict #2.)
  CONSUMERS: inference-gateway (ADD a route/target → start splitting traffic to
             the version), serving (ensure the version is LOADED), experiment-tracker
             (record the deploy in the model&#39;s history).
  WHY THIS LIVES UNDER fp.pipelines.* (not fp.models.*): the FACT being announced
  is &#34;the deployment saga deployed a version&#34;, produced and owned by the saga.
  The model&#39;s own lifecycle facts (registered/promoted) live under fp.models.*;
  the act of DEPLOYING (a workflow outcome) lives under the workflow domain.
  This is the resolution of the prior fp.models.* vs fp.pipelines.* inconsistency.
  WHY endpoint &#43; weight_bps: the gateway needs the serving endpoint (resolved by
  the saga, NOT client-supplied — SSRF guard) and the initial canary weight to
  build the route target. traffic splitting is in basis points (0–10000).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | The deployed model &#43; the exact version now serving. |
| model_name | [string](#string) |  |  |
| version_id | [string](#string) |  |  |
| version | [string](#string) |  |  |
| endpoint | [string](#string) |  | The model-serving endpoint for this version (e.g. &#34;iris-v3.fp-models.svc:9090&#34;). Resolved by the orchestrator from the deploy; the gateway uses it as the route target&#39;s backend. Authoritative — never a client value. |
| weight_bps | [int32](#int32) |  | Initial traffic share for this version in basis points (0–10000). A canary typically deploys at a small weight (e.g. 1000 = 10%) and is dialed up by the saga&#39;s CANARY/PROMOTE steps via SetTrafficSplit. |
| execution_id | [string](#string) |  | The execution that performed the deploy (lineage back to the saga). |
| deployed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the deploy step completed (producer clock). |






<a name="forgepoint-events-v1-ModelDriftDetected"></a>

### ModelDriftDetected
ModelDriftDetected → fp.models.drift.detected
  PRODUCER:  model-monitor (a window breached a configured threshold). THE
             LOOP-CLOSER — Notification already subscribed to this before any
             producer existed; this is the producer.
  CONSUMERS: notification (alert a human), experiment-tracker (record drift in
             the model&#39;s history), pipeline-orchestrator (on CRITICAL &#43;
             auto_retrain, run the retrain pipeline → canary → promote).
  WHY THIS LIVES UNDER fp.models.* THOUGH MONITOR PRODUCES IT: the subject
  domain is the RESOURCE (a model), not the producing service. EventEnvelope.source
  = &#34;model-monitor&#34; records the actual producer (see OWNERSHIP NOTE up top).
  WHY headline scalars AND a full breakdown: model_name/drift_type/severity let
  a consumer route/filter/template an alert WITHOUT deserializing everything;
  the repeated DriftMetric breakdown &#43; report_id give Experiment Tracker the
  detail and a deep-link without a synchronous callback (&#34;fat-but-flat&#34;).
  GROUND-TRUTH JOIN: the report is over a window of request_ids the monitor
  observed on InferenceCompleted; delayed ground truth is matched back by those
  request_ids (SubmitGroundTruth), which is why InferenceCompleted carries
  request_id as a stable join key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model &#43; the exact serving version that drifted (version captured from the inference events, so a report is tied to the precise version — vital under canary traffic splitting). |
| model_version | [string](#string) |  |  |
| drift_type | [DriftType](#forgepoint-events-v1-DriftType) |  | Headline drift type &#43; severity, duplicated from the report for cheap routing and alert templating without deserializing the breakdown. |
| severity | [DriftSeverity](#forgepoint-events-v1-DriftSeverity) |  |  |
| report_id | [string](#string) |  | The drift report&#39;s stable id — deep-link handle &#43; business-level dedupe key (the report is itself idempotent on its window id). |
| metrics | [DriftMetric](#forgepoint-events-v1-DriftMetric) | repeated | Per-feature/per-output/per-metric breakdown — the actionable detail, so a reactor (Experiment Tracker) records it without calling back into Monitor. |
| window_start | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Window bounds [start,end) and sample count — statistical context (PSI over 12 samples is far weaker than over 12,000) and timeline placement. |
| window_end | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| sample_count | [int32](#int32) |  |  |
| auto_retrain | [bool](#bool) |  | Closed-loop control fields. auto_retrain = is self-healing armed for this model; retrain_pipeline_id = the Orchestrator pipeline to run (opaque id, NOT a pipeline-proto import). Carried on the event so a reactor can close the loop without a lookup. |
| retrain_pipeline_id | [string](#string) |  |  |
| retrain_context | [google.protobuf.Struct](#google-protobuf-Struct) |  | Free-form drift context passed as input to the retrain pipeline (e.g. {&#34;top_drifted_feature&#34;:&#34;income&#34;,&#34;psi&#34;:0.41}). Struct because each retrain DAG&#39;s expected inputs differ — a typed message would couple this event to every pipeline&#39;s input schema. Kept small &#43; PII-free (summaries only). |
| detected_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the drift was detected / report computed (producer clock). |






<a name="forgepoint-events-v1-ModelPromoted"></a>

### ModelPromoted
ModelPromoted → fp.models.promoted
  PRODUCER:  registry (after PromoteVersion commits the atomic stage swap).
  CONSUMERS: inference-gateway (update routing table to the new prod version),
             serving (reload/route to new weights), model-monitor (reset drift
             baseline to the new version&#39;s training distribution), billing
             (re-meter against the new version).
  THE SINGLE-PRODUCTION INVARIANT: promoting B to PRODUCTION atomically demotes
  the prior production version A to ARCHIVED. The event carries BOTH so a
  consumer sees the whole swap and can tear down the old deployment.
  WHY from_stage/to_stage: makes the event self-describing for audit and lets a
  consumer ignore transitions it doesn&#39;t care about (Monitor reacts only when
  to_stage == PRODUCTION).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | The model whose production version changed. |
| model_name | [string](#string) |  |  |
| version_id | [string](#string) |  | The version that was promoted (id &#43; label). |
| version | [string](#string) |  |  |
| from_stage | [ModelStage](#forgepoint-events-v1-ModelStage) |  | The transition this promotion performed (both ends, for self-description). |
| to_stage | [ModelStage](#forgepoint-events-v1-ModelStage) |  |  |
| demoted_version_id | [string](#string) |  | If a prior production version was auto-demoted (single-prod invariant), its id/label so consumers tear down the old deployment. Empty if none. |
| demoted_version | [string](#string) |  |  |
| promoted_by | [string](#string) |  | Who initiated the promotion (auth claims) — audit. |
| promoted_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the swap committed (producer clock). |






<a name="forgepoint-events-v1-ModelRegistered"></a>

### ModelRegistered
ModelRegistered → fp.models.registered
  PRODUCER:  registry (after RegisterModel commits to Postgres).
  CONSUMERS: pipeline-orchestrator (a registered model can become a pipeline
             input/target), experiment-tracker (link runs to the model).
  WHY THESE FIELDS: identity (model_id/model_name) &#43; ownership (owner_id/team)
  are everything a consumer needs to associate downstream work without a
  callback; framework/task_type let the UI/orchestrator route by model kind.
  We carry FLAT fields rather than embedding registry.Model to keep the event
  decoupled from the registry API (see DESIGN block).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | Registry model UUID (stable handle other services key off). |
| model_name | [string](#string) |  | Human-friendly model name within the team (the addressable handle). |
| framework | [string](#string) |  | ML framework descriptor (&#34;pytorch&#34;,&#34;onnx&#34;,...). Influences how serving loads. |
| task_type | [string](#string) |  | Task type (&#34;classification&#34;,&#34;llm&#34;,...). For UI grouping / routing. |
| owner_id | [string](#string) |  | Registrant user id and owning team — for attribution (billing/notification) and team-scoped consumers. Producer-derived from auth claims. |
| team | [string](#string) |  |  |
| registered_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When registration committed (producer clock). |






<a name="forgepoint-events-v1-ModelUndeployed"></a>

### ModelUndeployed
ModelUndeployed → fp.pipelines.model.undeployed
  PRODUCER:  pipeline-orchestrator (a saga removed a version from serving — a
             rollback compensation, a scale-to-zero, or an explicit teardown).
  CONSUMERS: inference-gateway (REMOVE the route target → stop traffic), serving
             (UNLOAD the model to free memory).
  WHY reason: distinguishes an intentional teardown from a rollback so consumers
  (and operators) don&#39;t alert on an expected removal.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | The model &#43; version being removed from serving. |
| model_name | [string](#string) |  |  |
| version_id | [string](#string) |  |  |
| version | [string](#string) |  |  |
| reason | [string](#string) |  | Why it was undeployed (&#34;rollback&#34;,&#34;superseded&#34;,&#34;scale_to_zero&#34;,&#34;teardown&#34;). Free-form but small; lets consumers suppress alerts on expected removals. |
| execution_id | [string](#string) |  | The execution that performed the undeploy (lineage). |
| undeployed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the undeploy completed (producer clock). |






<a name="forgepoint-events-v1-ModelVersionCreated"></a>

### ModelVersionCreated
ModelVersionCreated → fp.models.version.created
  PRODUCER:  registry (after CreateVersion commits; status=PENDING_UPLOAD).
  CONSUMERS: experiment-tracker (associate the training run that produced it).
  IMPORTANT: this fires at ROW CREATION, before the artifact is uploaded. A
  consumer that needs a USABLE artifact (serving, storage billing) must wait
  for ModelVersionReady instead — that distinction is exactly why both events
  exist (it was missing before; see conflict #3).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | Owning model id &#43; name (denormalized so consumers needn&#39;t join/look up). |
| model_name | [string](#string) |  |  |
| version_id | [string](#string) |  | Version UUID &#43; human label (e.g. &#34;1.2.0&#34;). |
| version | [string](#string) |  |  |
| created_by | [string](#string) |  | Who created it (auth claims) — for lineage/audit. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the version row was created (producer clock). Artifact NOT yet present. |






<a name="forgepoint-events-v1-ModelVersionReady"></a>

### ModelVersionReady
ModelVersionReady → fp.models.version.ready
  PRODUCER:  registry (when the artifact upload is confirmed &#43; checksum-verified,
             flipping VersionStatus PENDING_UPLOAD → READY).
  CONSUMERS: serving (a version is now loadable), billing (begin metering
             STORAGE_BYTES), pipeline-orchestrator (a deploy/promote saga
             waiting on artifact readiness can proceed).
  WHY ADDED: ModelVersionCreated only signals intent; READY is the transition
  that makes a version physically servable. Consumers that act on a real
  artifact need this edge, and several explicitly do (see conflict #3).
  WHY artifact_digest &#43; size_bytes: serving verifies it loaded the exact
  registered bytes (supply-chain integrity); billing meters storage on size —
  both server-measured, never client-asserted.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | Owning model id &#43; name. |
| model_name | [string](#string) |  |  |
| version_id | [string](#string) |  | Version UUID &#43; human label that just became READY. |
| version | [string](#string) |  |  |
| artifact_path | [string](#string) |  | Object-storage location of the artifact (e.g. &#34;s3://fp-models/&lt;m&gt;/&lt;v&gt;.onnx&#34;). |
| artifact_digest | [string](#string) |  | Content digest (e.g. &#34;sha256:...&#34;) verified after upload — integrity handle. |
| size_bytes | [int64](#int64) |  | Artifact size in bytes (server-measured) — billing meters storage on this. |
| ready_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the artifact became READY (producer clock). |






<a name="forgepoint-events-v1-NotificationDelivered"></a>

### NotificationDelivered
NotificationDelivered → fp.notifications.delivered
  CONSUMERS: experiment-tracker / dashboards (alert volume &#43; success rates).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notification_id | [string](#string) |  | The notification that was delivered &#43; who it went to. |
| recipient_user_id | [string](#string) |  |  |
| channel | [NotificationChannel](#forgepoint-events-v1-NotificationChannel) |  | Which channel succeeded. |
| event_type | [string](#string) |  | Provenance: the originating event&#39;s type (e.g. &#34;fp.pipelines.failed&#34;). |
| delivered_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When delivery succeeded (producer clock). |






<a name="forgepoint-events-v1-NotificationFailed"></a>

### NotificationFailed
NotificationFailed → fp.notifications.failed
  CONSUMERS: experiment-tracker / dashboards; on-call escalation / dead-letter.
  WHY publish a failure event: makes &#34;we failed to reach the user&#34; observable
  platform-wide and can feed an escalation flow.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notification_id | [string](#string) |  | The notification whose delivery failed &#43; who we tried to reach. |
| recipient_user_id | [string](#string) |  |  |
| channel | [NotificationChannel](#forgepoint-events-v1-NotificationChannel) |  | Which channel failed. |
| event_type | [string](#string) |  | Provenance: the originating event&#39;s type. |
| attempts | [int32](#int32) |  | Attempts made before giving up (exhausted retry budget). |
| error_message | [string](#string) |  | Last failure detail (e.g. &#34;503 from webhook&#34;, &#34;circuit breaker open&#34;). |
| failed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When we gave up (producer clock). |






<a name="forgepoint-events-v1-PipelineCompleted"></a>

### PipelineCompleted
PipelineCompleted → fp.pipelines.completed
  CONSUMERS: experiment-tracker (close out the run &#43; record duration),
             notification (optional success announce).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  |  |
| pipeline_id | [string](#string) |  |  |
| pipeline_type | [PipelineType](#forgepoint-events-v1-PipelineType) |  |  |
| duration | [google.protobuf.Duration](#google-protobuf-Duration) |  | Total wall-clock duration of the run (for SLO/latency dashboards). |
| completed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the run reached COMPLETED (producer clock). |






<a name="forgepoint-events-v1-PipelineFailed"></a>

### PipelineFailed
PipelineFailed → fp.pipelines.failed
  CONSUMERS: notification (turn into a page/alert).
  WHY compensation_failed: a clean rollback vs a STUCK SAGA (orphaned side
  effects) are different severities — Notification escalates the latter harder.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  |  |
| pipeline_id | [string](#string) |  |  |
| pipeline_type | [PipelineType](#forgepoint-events-v1-PipelineType) |  |  |
| failed_step_id | [string](#string) |  | The step whose failure ultimately failed the run. |
| error | [string](#string) |  | Failure summary for the alert. |
| compensation_failed | [bool](#bool) |  | True if compensation itself failed (a &#34;stuck saga&#34;). Escalate harder. |
| failed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the run reached terminal FAILED (producer clock). |






<a name="forgepoint-events-v1-PipelineStarted"></a>

### PipelineStarted
PipelineStarted → fp.pipelines.started
  CONSUMERS: notification (announce a run began), experiment-tracker (open a
             run record for the execution).
  WHY triggered_by: distinguishes a human-initiated run from an automated one
  (e.g. &#34;model-monitor&#34; for auto-retrain) — important for alert noise control.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  | The execution (run) id &#43; the pipeline (template) it instantiates. |
| pipeline_id | [string](#string) |  |  |
| pipeline_type | [PipelineType](#forgepoint-events-v1-PipelineType) |  | Saga vs DAG vs batch — lets consumers filter (e.g. only training DAGs). |
| triggered_by | [string](#string) |  | Who/what triggered it (user_id or a service identity like &#34;model-monitor&#34;). |
| started_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the run transitioned to RUNNING (producer clock). |






<a name="forgepoint-events-v1-PredictionSummary"></a>

### PredictionSummary
PredictionSummary — a compact, privacy-conscious summary of a prediction&#39;s
OUTPUT, carried on InferenceCompleted. WHY a summary, not the raw output
tensor: the event is consumed by Billing, Experiment Tracker, and Model
Monitor; none need (or should receive) raw outputs. Shipping full tensors on
every prediction would flood NATS and leak potentially sensitive data to three
services. Monitor gets enough distributional signal here for PREDICTION drift.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| top_label | [string](#string) |  | The predicted class label / argmax (classifier) or string-formatted scalar (regressor). Empty if not applicable. |
| top_score | [double](#double) |  | Confidence/probability of top_label (0.0–1.0); 0 for regression / no probs. |
| output_stats | [PredictionSummary.OutputStatsEntry](#forgepoint-events-v1-PredictionSummary-OutputStatsEntry) | repeated | Per-output simple stats (e.g. {&#34;score_mean&#34;:0.31,&#34;score_max&#34;:0.9}). Compact, model-agnostic numeric summary for prediction-drift math — NOT raw outputs. |
| output_cardinality | [int32](#int32) |  | Number of elements in the output (e.g. number of classes). Lets Monitor track output-shape changes across versions cheaply. |






<a name="forgepoint-events-v1-PredictionSummary-OutputStatsEntry"></a>

### PredictionSummary.OutputStatsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [double](#double) |  |  |






<a name="forgepoint-events-v1-QuotaExceeded"></a>

### QuotaExceeded
QuotaExceeded → fp.billing.quota.exceeded
  PRODUCER:  billing (when recorded usage pushes a team over its plan quota;
             written to the outbox in the SAME tx as the usage update).
  CONSUMERS: notification (alert the team), inference-gateway (flip the team&#39;s
             Redis quota cache to &#34;blocked&#34; so subsequent inferences are
             rejected/throttled — eventual consistency is acceptable for the
             gateway pre-flight).
  FAT enough to act: carries the limit AND current usage so the consumer can
  render &#34;1,012 / 1,000&#34; and the gateway can decide policy without a callback.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| team | [string](#string) |  | The team that exceeded quota &#43; the plan whose quota was hit. |
| rate_plan_id | [string](#string) |  |  |
| meter_type | [MeterType](#forgepoint-events-v1-MeterType) |  | Which meter&#39;s quota was exceeded. |
| quota_limit | [int64](#int64) |  | The plan&#39;s cap for that meter (the limit crossed) and the team&#39;s current usage (&gt;= limit), so the consumer shows the ratio without a callback. |
| current_usage | [int64](#int64) |  |  |
| occurred_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the threshold was crossed (event time). |






<a name="forgepoint-events-v1-RunCreated"></a>

### RunCreated
RunCreated → fp.experiments.run.created
  PRODUCER:  experiment-tracker (sync StartRun, or async materialization from a
             consumed platform event).
  CONSUMERS: notification (optional &#34;run started&#34; surface); live dashboards.
  WHY emit on create (not only finish): a live dashboard wants to show in-flight
  runs immediately.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_id | [string](#string) |  | The run &#43; the experiment it belongs to. |
| experiment_id | [string](#string) |  |  |
| model_version_id | [string](#string) |  | The model version it targets, if any (opaque Registry id — decoupled). |
| display_name | [string](#string) |  | Optional human label (e.g. &#34;lr=0.01 batch=64&#34;). |
| started_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the run started (producer clock). |






<a name="forgepoint-events-v1-RunFinished"></a>

### RunFinished
RunFinished → fp.experiments.run.finished
  PRODUCER:  experiment-tracker (run reached a terminal state).
  CONSUMERS: notification (alert on a finished/failed run), leaderboards/BFF
             cache invalidation.
  FAT EVENT: carries final_metrics so a consumer ranks/alerts WITHOUT a GetRun
  callback (terminal-run metrics are final, so staleness isn&#39;t a concern).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_id | [string](#string) |  | The run &#43; experiment. |
| experiment_id | [string](#string) |  |  |
| model_version_id | [string](#string) |  | The model version it produced/evaluated, if any (opaque Registry id). |
| status | [RunStatus](#forgepoint-events-v1-RunStatus) |  | Terminal status (FINISHED / FAILED / KILLED). |
| final_metrics | [MetricPoint](#forgepoint-events-v1-MetricPoint) | repeated | Denormalized headline metrics at finish (final/best per key). Lets a consumer rank/alert without calling back. |
| ended_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the run ended (producer clock). |






<a name="forgepoint-events-v1-StepCompleted"></a>

### StepCompleted
StepCompleted → fp.pipelines.step.completed
  CONSUMERS: experiment-tracker (record per-step progress/metrics).
  WHY output (Struct): a step&#39;s small, step-specific result (e.g. a TRAIN
  step&#39;s model artifact URI) so a consumer needn&#39;t call back. Struct because
  the shape varies per StepType.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  |  |
| pipeline_id | [string](#string) |  |  |
| step_id | [string](#string) |  | Template step id that completed &#43; its kind (so consumers route on it). |
| step_type | [StepType](#forgepoint-events-v1-StepType) |  |  |
| output | [google.protobuf.Struct](#google-protobuf-Struct) |  | Small step-specific output (e.g. {&#34;model_uri&#34;:&#34;...&#34;}). Struct, not typed, because it varies per step kind and per CUSTOM step. |
| completed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the step finished (producer clock). |






<a name="forgepoint-events-v1-StepFailed"></a>

### StepFailed
StepFailed → fp.pipelines.step.failed
  CONSUMERS: notification (surface the failing step for triage).
  Emitted BEFORE compensation runs — it is the CAUSE; CompensationTriggered
  marks the start of the UNDO phase.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  |  |
| pipeline_id | [string](#string) |  |  |
| step_id | [string](#string) |  |  |
| step_type | [StepType](#forgepoint-events-v1-StepType) |  |  |
| error | [string](#string) |  | Human-readable failure reason (triage/alerting; not end-user copy). |
| attempts | [int32](#int32) |  | How many attempts were made before giving up. |
| failed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the step failed (producer clock). |






<a name="forgepoint-events-v1-UsageRecorded"></a>

### UsageRecorded
UsageRecorded → fp.billing.usage.recorded
  PRODUCER:  billing (after a UsageRecord commits, via the outbox).
  CONSUMERS: experiment-tracker (attribute cost to a run/model), usage dashboards.
  DOC NOTE (conflict #6): the design doc&#39;s fp.billing.&gt; hierarchy explicitly
  lists quota.exceeded and invoice.generated; UsageRecorded was implied by the
  outbox example. We add fp.billing.usage.recorded under the sanctioned tree
  (recorded as a doc addition in event-contract.md).
  FAT-BUT-FLAT: carries the full priced fact (team, meter, quantity,
  server-computed cost) so a consumer needs no callback. cost is in MICRO units
  of the currency (1e-6; 1_000_000 == 1.00) — exact integer money math.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| record_id | [string](#string) |  | Ledger record id (UUID) — business dedupe handle for consumers. |
| team | [string](#string) |  | The team billed (producer-derived from the inference api_key / auth claims — never a client value; naming the billed team is account-takeover-for-money). |
| rate_plan_id | [string](#string) |  | The rate plan applied (pinned so the price is reproducible). |
| meter_type | [MeterType](#forgepoint-events-v1-MeterType) |  | What was metered &#43; how many units (request count / tokens / compute-secs / bytes). |
| quantity | [int64](#int64) |  |  |
| cost_micros | [int64](#int64) |  | Server-COMPUTED cost: amount in micro-units &#43; ISO-4217 currency. Clients never assert this; the server prices quantity against the plan. |
| currency_code | [string](#string) |  |  |
| model_id | [string](#string) |  | What produced the usage (attribution / per-model reports). |
| model_version | [string](#string) |  |  |
| source_request_id | [string](#string) |  | The originating inference request id (lineage &#43; the natural dedupe anchor: two records must never share a source_request_id). |
| occurred_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the metered action occurred (event time, not insert time). |






<a name="forgepoint-events-v1-UserCreated"></a>

### UserCreated
UserCreated → fp.auth.user.created
  PRODUCER:  auth (after CreateUser commits the new account to Postgres).
  CONSUMERS: notification (send a welcome / provisioning message), billing
             (open a usage account for the user&#39;s team), experiment-tracker /
             audit (record that an identity was provisioned). Consumers key off
             user_id (stable handle) and team (multi-tenancy scoping).
  SECURITY/PII: identity ATTRIBUTES only (id, email, name, team, role) — never
  the password hash or any credential. email is carried because notification
  needs an address to reach; it is not a secret. SERVER-authoritative: every
  field is producer-derived from the committed row, never a client assertion.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user_id | [string](#string) |  | Auth user UUID — the stable handle every other service keys off. |
| email | [string](#string) |  | Login email (the addressable identity). Carried so Notification can reach the user without a callback. NOT a secret; never the password hash. |
| name | [string](#string) |  | Display name (non-unique, human-friendly). |
| team | [string](#string) |  | Owning team — the multi-tenancy scope. Billing opens the account against it; team-scoped consumers filter on it. Producer-assigned, never client-supplied. |
| role | [string](#string) |  | The role granted at creation (role NAME, e.g. &#34;viewer&#34;). Flat RBAC: a user has exactly one role. Carried so an audit/consumer sees the initial grant without a CheckPermission round-trip. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the account was created (producer clock). |





 


<a name="forgepoint-events-v1-DriftMethod"></a>

### DriftMethod
DriftMethod mirrors monitor.v1.DriftMethod — the statistical test used, so a
drift score on an event is self-describing (PSI 0.3 != KS 0.3).

| Name | Number | Description |
| ---- | ------ | ----------- |
| DRIFT_METHOD_UNSPECIFIED | 0 |  |
| DRIFT_METHOD_PSI | 1 |  |
| DRIFT_METHOD_KL | 2 |  |
| DRIFT_METHOD_KS | 3 |  |



<a name="forgepoint-events-v1-DriftSeverity"></a>

### DriftSeverity
DriftSeverity mirrors monitor.v1.DriftSeverity — OK/WARNING/CRITICAL ladder.
The retrain policy branches on this (CRITICAL fires auto-retrain).

| Name | Number | Description |
| ---- | ------ | ----------- |
| DRIFT_SEVERITY_UNSPECIFIED | 0 |  |
| DRIFT_SEVERITY_OK | 1 |  |
| DRIFT_SEVERITY_WARNING | 2 |  |
| DRIFT_SEVERITY_CRITICAL | 3 |  |



<a name="forgepoint-events-v1-DriftType"></a>

### DriftType
DriftType mirrors monitor.v1.DriftType — which drift signal breached.

| Name | Number | Description |
| ---- | ------ | ----------- |
| DRIFT_TYPE_UNSPECIFIED | 0 |  |
| DRIFT_TYPE_DATA | 1 |  |
| DRIFT_TYPE_PREDICTION | 2 |  |
| DRIFT_TYPE_PERFORMANCE | 3 |  |



<a name="forgepoint-events-v1-InferenceFailureReason"></a>

### InferenceFailureReason
InferenceFailureReason mirrors inference.v1.FailureReason — WHY a prediction
failed, which resilience pattern fired. Drives Model Monitor&#39;s error-rate
signal and Notification routing (a capacity problem vs a backend-health
problem call for different reactions).

| Name | Number | Description |
| ---- | ------ | ----------- |
| INFERENCE_FAILURE_REASON_UNSPECIFIED | 0 |  |
| INFERENCE_FAILURE_REASON_NO_ROUTE | 1 |  |
| INFERENCE_FAILURE_REASON_RATE_LIMITED | 2 |  |
| INFERENCE_FAILURE_REASON_BULKHEAD_FULL | 3 |  |
| INFERENCE_FAILURE_REASON_CIRCUIT_OPEN | 4 |  |
| INFERENCE_FAILURE_REASON_UPSTREAM_ERROR | 5 |  |
| INFERENCE_FAILURE_REASON_TIMEOUT | 6 |  |
| INFERENCE_FAILURE_REASON_INVALID_INPUT | 7 |  |
| INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED | 8 |  |



<a name="forgepoint-events-v1-MeterType"></a>

### MeterType
MeterType mirrors billing.v1.MeterType — the billable axis a usage record
measures. Carried on UsageRecorded/QuotaExceeded so consumers (Experiment
Tracker cost attribution, the gateway&#39;s quota cache) need no callback.

| Name | Number | Description |
| ---- | ------ | ----------- |
| METER_TYPE_UNSPECIFIED | 0 |  |
| METER_TYPE_INFERENCE_REQUEST | 1 |  |
| METER_TYPE_INFERENCE_TOKENS | 2 |  |
| METER_TYPE_COMPUTE_SECONDS | 3 |  |
| METER_TYPE_STORAGE_BYTES | 4 |  |



<a name="forgepoint-events-v1-ModelStage"></a>

### ModelStage
ModelStage mirrors registry.v1.ModelStage — a model VERSION&#39;s lifecycle state.
Carried on ModelPromoted so a consumer (serving/monitor/billing) sees exactly
which transition happened (e.g. only react when TO == PRODUCTION).

| Name | Number | Description |
| ---- | ------ | ----------- |
| MODEL_STAGE_UNSPECIFIED | 0 |  |
| MODEL_STAGE_DEV | 1 |  |
| MODEL_STAGE_STAGING | 2 |  |
| MODEL_STAGE_PRODUCTION | 3 |  |
| MODEL_STAGE_ARCHIVED | 4 |  |



<a name="forgepoint-events-v1-NotificationChannel"></a>

### NotificationChannel
NotificationChannel mirrors notification.v1.NotificationChannel — the
transport a notification was delivered over. Carried on delivery events so
observers see which channel succeeded/failed.

| Name | Number | Description |
| ---- | ------ | ----------- |
| NOTIFICATION_CHANNEL_UNSPECIFIED | 0 |  |
| NOTIFICATION_CHANNEL_IN_APP | 1 |  |
| NOTIFICATION_CHANNEL_WEBHOOK | 2 |  |
| NOTIFICATION_CHANNEL_SLACK | 3 |  |
| NOTIFICATION_CHANNEL_EMAIL | 4 |  |



<a name="forgepoint-events-v1-PipelineType"></a>

### PipelineType
PipelineType mirrors pipeline.v1.PipelineType — saga vs DAG vs batch. Carried
on pipeline lifecycle events so a consumer can filter (e.g. Experiment Tracker
only ingests TRAINING_DAG completions for metrics).

| Name | Number | Description |
| ---- | ------ | ----------- |
| PIPELINE_TYPE_UNSPECIFIED | 0 |  |
| PIPELINE_TYPE_DEPLOYMENT_SAGA | 1 |  |
| PIPELINE_TYPE_TRAINING_DAG | 2 |  |
| PIPELINE_TYPE_BATCH_INFERENCE | 3 |  |



<a name="forgepoint-events-v1-RunStatus"></a>

### RunStatus
RunStatus mirrors experiment.v1.RunStatus — terminal state of a tracked run.
Carried on RunFinished so a consumer ranks/alerts without a GetRun callback.

| Name | Number | Description |
| ---- | ------ | ----------- |
| RUN_STATUS_UNSPECIFIED | 0 |  |
| RUN_STATUS_RUNNING | 1 |  |
| RUN_STATUS_FINISHED | 2 |  |
| RUN_STATUS_FAILED | 3 |  |
| RUN_STATUS_KILLED | 4 |  |



<a name="forgepoint-events-v1-StepType"></a>

### StepType
StepType mirrors pipeline.v1.StepType — the kind of work a saga/DAG step does.
Carried on step events so consumers can route on it (e.g. a REGISTER step
completing matters to the Registry/Experiment Tracker more than a VALIDATE).

| Name | Number | Description |
| ---- | ------ | ----------- |
| STEP_TYPE_UNSPECIFIED | 0 |  |
| STEP_TYPE_VALIDATE | 1 |  |
| STEP_TYPE_BUILD | 2 |  |
| STEP_TYPE_DEPLOY | 3 |  |
| STEP_TYPE_CANARY | 4 |  |
| STEP_TYPE_PROMOTE | 5 |  |
| STEP_TYPE_TRAIN | 6 |  |
| STEP_TYPE_EVALUATE | 7 |  |
| STEP_TYPE_REGISTER | 8 |  |
| STEP_TYPE_CUSTOM | 9 |  |


 

 

 



<a name="forgepoint_experiment_v1_experiment-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/experiment/v1/experiment.proto



<a name="forgepoint-experiment-v1-ArchiveExperimentRequest"></a>

### ArchiveExperimentRequest
ArchiveExperimentRequest soft-deletes an experiment: it sets archived_at and
hides it from default lists, but PRESERVES its runs and metrics. WHY soft (not
hard) delete: an experiment&#39;s runs are an audit trail and may be referenced by
model-lineage in the Registry (a run carries the model_version_id it produced).
Hard-deleting would orphan those references and destroy provenance. This is the
design&#39;s &#34;Delete/Archive where the design calls for it&#34; done safely.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | The experiment to archive. Must be visible to the caller&#39;s team. |






<a name="forgepoint-experiment-v1-ArchiveExperimentResponse"></a>

### ArchiveExperimentResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| experiment | [Experiment](#forgepoint-experiment-v1-Experiment) |  | The experiment after archiving (archived_at now set). |






<a name="forgepoint-experiment-v1-CompareRunsRequest"></a>

### CompareRunsRequest
CompareRunsRequest asks for the metric series of several runs side-by-side —
the data behind &#34;which model version performs better?&#34;. You pass the run IDs
and (optionally) restrict to specific metric keys to keep the payload small.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_ids | [string](#string) | repeated | The runs to compare. Bounded server-side (e.g., max 20 runs) — comparing hundreds of full curves at once is a payload/DoS hazard. |
| metric_keys | [string](#string) | repeated | Optional: only return these metric keys (e.g., [&#34;accuracy&#34;, &#34;val_auc&#34;]). Empty = return all metric keys present on the runs. Narrowing this is the main lever for keeping the response small. |






<a name="forgepoint-experiment-v1-CompareRunsResponse"></a>

### CompareRunsResponse
CompareRunsResponse returns one RunComparison per requested run (that exists
and is visible), preserving request order where possible.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| comparisons | [RunComparison](#forgepoint-experiment-v1-RunComparison) | repeated | One entry per compared run. |






<a name="forgepoint-experiment-v1-CreateExperimentRequest"></a>

### CreateExperimentRequest
CreateExperimentRequest carries ONLY client-owned fields. Note what is
ABSENT: id, owner_id, team, created_at — all SERVER-authoritative (id is
generated; owner_id/team come from the authenticated caller&#39;s claims;
created_at is set on write). This is the mass-assignment guard.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Unique-per-team experiment name. Required. |
| description | [string](#string) |  | Optional description of the experiment&#39;s goal. |
| tags | [CreateExperimentRequest.TagsEntry](#forgepoint-experiment-v1-CreateExperimentRequest-TagsEntry) | repeated | Optional organizational tags (key/value). Not metrics. |






<a name="forgepoint-experiment-v1-CreateExperimentRequest-TagsEntry"></a>

### CreateExperimentRequest.TagsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="forgepoint-experiment-v1-CreateExperimentResponse"></a>

### CreateExperimentResponse
CreateExperimentResponse wraps the created Experiment.
WHY wrap (not return Experiment directly): Buf RPC_RESPONSE_STANDARD_NAME
requires &#34;&lt;Rpc&gt;Response&#34; naming, and wrapping lets us add fields later
(e.g., a created-by-import flag) without mutating the shared Experiment type.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| experiment | [Experiment](#forgepoint-experiment-v1-Experiment) |  | The newly created experiment, with server-set id/owner/team/created_at. |






<a name="forgepoint-experiment-v1-DeleteRunRequest"></a>

### DeleteRunRequest
DeleteRunRequest removes a single run and ITS metrics/params. WHY this is
allowed to HARD-delete where ArchiveExperiment is not: a run is the unit of
experimental noise — a mis-launched job, a smoke test, a duplicate — and the
design&#39;s run leaderboard is unusable if garbage runs can&#39;t be pruned. SAFETY
RAIL: the server REJECTS deleting a run whose model_version_id is set and is
still referenced by a live (non-archived) model version in the Registry — that
would sever lineage; such a run must be Archived at the experiment level
instead. A run with no downstream model reference is safe to delete.
Only the run&#39;s owner or a team admin may delete (enforced from auth claims).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_id | [string](#string) |  | The run to delete. |
| idempotency_key | [string](#string) |  | Idempotency: a retried delete after a network blip must not error just because the run is already gone. WHY a key (not &#34;treat NOT_FOUND as success&#34;): a key distinguishes &#34;my earlier delete succeeded&#34; from &#34;someone else&#39;s run id typo&#34; — the server returns OK on replay of the SAME key, NOT_FOUND otherwise. |






<a name="forgepoint-experiment-v1-DeleteRunResponse"></a>

### DeleteRunResponse
DeleteRunResponse is a named-empty response (Buf forbids returning
google.protobuf.Empty and forbids reusing a domain message as a response).
Empty body = success; failures surface as gRPC status codes.






<a name="forgepoint-experiment-v1-Experiment"></a>

### Experiment
============================================================================
Experiment
============================================================================

WHY: an Experiment is a NAMED GROUPING of runs that share a goal — e.g.,
&#34;fraud-detector-v3 hyperparameter sweep&#34;. You compare runs WITHIN an
experiment. This is the MLflow &#34;experiment → runs&#34; hierarchy exactly.

OWNERSHIP / SECURITY: owner_id and team are SERVER-AUTHORITATIVE. They are
set from the authenticated caller&#39;s TokenClaims (injected by the auth
interceptor), never accepted on the create request. This prevents a caller
from creating an experiment &#34;owned&#34; by someone else (mass-assignment).
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Primary key, immutable. Server-generated. |
| name | [string](#string) |  | Unique-per-team human name, e.g., &#34;fraud-detector-v3-sweep&#34;. |
| description | [string](#string) |  | Optional free-text description of what this experiment is testing. |
| tags | [Experiment.TagsEntry](#forgepoint-experiment-v1-Experiment-TagsEntry) | repeated | Arbitrary key/value labels for filtering/organization (e.g., {&#34;project&#34;: &#34;fraud&#34;, &#34;owner_team&#34;: &#34;risk&#34;}). NOT for metrics. |
| owner_id | [string](#string) |  | The user who created this experiment. SERVER-set from auth context. |
| team | [string](#string) |  | The owning team (namespacing &#43; access). SERVER-set from auth context. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the experiment was created. SERVER-set, immutable. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the experiment was last mutated (name/description/tags edit, or archive). SERVER-set on every write. Lets clients cache/ETag and lets the UI sort by recency. |
| archived_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | SERVER-set archive timestamp. Unset = active. We SOFT-DELETE experiments (set this, hide from default lists) rather than hard-delete: runs/metrics are an audit trail and may be referenced by model lineage in the Registry. ArchiveExperiment sets this; it is never client-supplied. |






<a name="forgepoint-experiment-v1-Experiment-TagsEntry"></a>

### Experiment.TagsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="forgepoint-experiment-v1-GetExperimentRequest"></a>

### GetExperimentRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID of the experiment to fetch. |






<a name="forgepoint-experiment-v1-GetExperimentResponse"></a>

### GetExperimentResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| experiment | [Experiment](#forgepoint-experiment-v1-Experiment) |  | The requested experiment. NOT_FOUND if it doesn&#39;t exist or the caller&#39;s team has no access. |






<a name="forgepoint-experiment-v1-GetMetricHistoryRequest"></a>

### GetMetricHistoryRequest
============================================================================
GetMetricHistory  (the time-series read the rest of the proto refers to)
============================================================================

WHY this exists as its own RPC: GetRun deliberately returns only the
denormalized final_metrics (headline numbers), and CompareRuns is multi-run.
Several places in this contract say &#34;use a dedicated metric-history read for
the curve&#34; — this is that RPC. It returns the FULL time-series for ONE run,
PAGINATED, because a single run&#39;s &#34;loss&#34; curve can be millions of points and
must never be returned unbounded (that is both an OOM and a DoS hazard).

WHY paginated rather than server-streaming: the metrics table is RANGE-
partitioned by timestamp, so a cursor (page_token encoding the last
(key,step,timestamp) seen) maps cleanly onto an indexed keyset scan and is
trivially retryable. A server-stream would be fine too, but pagination reuses
the platform-wide common.Pagination contract and lets a BFF/UI fetch one
screenful at a time. (Streaming is reserved for genuinely push-shaped APIs,
e.g. a future WatchRun; a historical read is pull-shaped.)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_id | [string](#string) |  | The run whose metric history to read. |
| metric_keys | [string](#string) | repeated | Optional: restrict to these metric keys (e.g. [&#34;loss&#34;,&#34;val_auc&#34;]). Empty = all keys on the run. The main lever for keeping the response bounded. |
| min_step | [int64](#int64) |  | Optional inclusive step window [min_step, max_step]. Both 0 = no step bound. Lets a UI fetch &#34;epochs 100–200&#34; without scanning the whole curve. |
| max_step | [int64](#int64) |  |  |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor pagination. page_size defaults to 1000 here (curves are dense), max 5000 — a HIGHER cap than list RPCs because points are tiny and charts want many; still server-enforced so the page can&#39;t be unbounded. |






<a name="forgepoint-experiment-v1-GetMetricHistoryResponse"></a>

### GetMetricHistoryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| series | [MetricSeries](#forgepoint-experiment-v1-MetricSeries) | repeated | The requested points, grouped per metric key (chart-ready). Within a series, points are ordered by step then timestamp. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata. next_page_token encodes the last (key,step,timestamp) read so the next page resumes via an indexed keyset scan. |






<a name="forgepoint-experiment-v1-GetRunRequest"></a>

### GetRunRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID of the run to fetch. |






<a name="forgepoint-experiment-v1-GetRunResponse"></a>

### GetRunResponse
GetRunResponse returns run metadata &#43; params &#43; the denormalized headline
metrics &#43; any attached artifacts. It does NOT return the full metric
time-series (that can be huge); use GetMetricHistory for one run&#39;s curve, or
CompareRuns to overlay several runs.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run | [Run](#forgepoint-experiment-v1-Run) |  | The requested run. NOT_FOUND if missing or not visible to the caller. |






<a name="forgepoint-experiment-v1-ListExperimentsRequest"></a>

### ListExperimentsRequest
ListExperimentsRequest reuses the common PaginationRequest for the same
cursor-based shape every Forgepoint list RPC uses (see common.proto for the
cursor-vs-offset rationale). Page size is capped SERVER-SIDE (default 20,
max 100) to prevent a client from requesting an unbounded page.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| team_filter | [string](#string) |  | Optional NARROWING filter only — NOT an authorization scope. The server ALWAYS constrains results to the team(s) the caller&#39;s auth claims permit; team_filter can only narrow WITHIN that permitted set, never widen it. WHY the field still exists: an admin whose claims span several teams may want to view one team&#39;s experiments. A client cannot use this to read another team&#39;s data — the auth interceptor&#39;s claim, not this field, is the security boundary. (Mass-assignment/IDOR guard: tenancy derives from claims.) |
| include_archived | [bool](#bool) |  | Include soft-archived experiments. Default false (active only). WHY a flag, not a separate RPC: archive is a visibility toggle, not a different query. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20, max 100 (server-enforced). See common.PaginationRequest — the cap is enforced server-side, not trusted from the client (an unbounded page is a DoS lever). |






<a name="forgepoint-experiment-v1-ListExperimentsResponse"></a>

### ListExperimentsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| experiments | [Experiment](#forgepoint-experiment-v1-Experiment) | repeated | The page of experiments. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata (next_page_token &#43; total_count). |






<a name="forgepoint-experiment-v1-ListRunsRequest"></a>

### ListRunsRequest
ListRunsRequest lists runs, normally scoped to one experiment, with optional
status filtering. Paginated with the shared cursor type (page size capped
server-side: default 20, max 100).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| experiment_id | [string](#string) |  | Filter to runs in this experiment. Required in practice (you compare runs within an experiment); empty lists across the caller&#39;s team. |
| status_filter | [RunStatus](#forgepoint-experiment-v1-RunStatus) |  | Optional status filter, e.g., only RUN_STATUS_FINISHED for comparison. RUN_STATUS_UNSPECIFIED = no status filter (return all). |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20, max 100 (server-enforced). |






<a name="forgepoint-experiment-v1-ListRunsResponse"></a>

### ListRunsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| runs | [Run](#forgepoint-experiment-v1-Run) | repeated | The page of runs. Each carries final_metrics (headline numbers) so a list view can render leaderboards without fetching full metric series. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata. |






<a name="forgepoint-experiment-v1-LogMetricsRequest"></a>

### LogMetricsRequest
============================================================================
LogMetrics  (BATCH — the high-throughput ingestion RPC)
============================================================================

WHY a UNARY BATCH RPC (repeated MetricPoint) rather than client-streaming:
  A training job produces metrics in bursts (e.g., 500 points per epoch). We
  want each flush to be:
    - IDEMPOTENT &amp; retryable: a unary call with an idempotency_key can be
      safely re-sent after a timeout; a long-lived client stream that dies
      mid-flight leaves ambiguous partial state.
    - ONE TRANSACTION: the server writes the whole batch in a single
      multi-row INSERT (one round trip, one commit) — far cheaper than a
      point-per-message stream and aligned with the async batch-write design.
  Client-streaming would shine for an unbounded, never-ending feed, but
  training flushes are naturally chunked, so batch-unary is the better fit.
  (The truly unbounded, platform-wide metric feed is handled OFF this RPC by
  the NATS batch consumer — see the pattern header.)

CAP: the server bounds points-per-call (e.g., 1000) and returns
INVALID_ARGUMENT above it, so one request can&#39;t blow up memory or a single
transaction. Clients split larger flushes across calls.

SERVER-AUTHORITATIVE: each MetricPoint.timestamp is (re)stamped server-side
on ingest (partition key integrity); clients supply key/value/step only.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_id | [string](#string) |  | The run these metrics belong to. Must be RUNNING (logging to a terminal run is rejected — its metrics are final). |
| points | [MetricPoint](#forgepoint-experiment-v1-MetricPoint) | repeated | The batch of metric points. Capped server-side (e.g., 1000 per call). |
| idempotency_key | [string](#string) |  | Idempotency key for this batch. WHY it matters here specifically: metric ingestion is the highest-volume, most-retried mutation in the service. On a retry, the server uses this key to avoid double-writing the same batch (which would corrupt the curve with duplicate points). Mirrors the idempotent-consumer guarantee the async path gets from EventEnvelope.id. |






<a name="forgepoint-experiment-v1-LogMetricsResponse"></a>

### LogMetricsResponse
LogMetricsResponse reports how many points were durably accepted. WHY return
a count (not an empty ack): batch APIs should tell the caller what landed so
it can reconcile (e.g., if the server clamped/deduped). accepted_count may be
less than len(points) on idempotent replay (duplicates skipped).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| accepted_count | [int32](#int32) |  | Number of metric points durably written by THIS call (post-dedup). |






<a name="forgepoint-experiment-v1-LogParamsRequest"></a>

### LogParamsRequest
LogParamsRequest appends hyperparameters to a run after StartRun (e.g., a job
discovers a derived config value mid-setup). Params are write-once per key.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_id | [string](#string) |  | The run to attach params to. Must be RUNNING. |
| params | [Param](#forgepoint-experiment-v1-Param) | repeated | The params to record. Re-logging an existing key is rejected unless the value is identical (idempotent), preventing silent config drift. |






<a name="forgepoint-experiment-v1-LogParamsResponse"></a>

### LogParamsResponse
LogParamsResponse reports how many params were newly recorded.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| accepted_count | [int32](#int32) |  | Number of params newly written (excludes idempotent no-op duplicates). |






<a name="forgepoint-experiment-v1-MetricPoint"></a>

### MetricPoint
============================================================================
MetricPoint
============================================================================

WHY this exact shape — (key, value, step, timestamp): a metric in ML is a
TIME-SERIES, not a single number. &#34;loss&#34; isn&#39;t one value; it&#39;s a curve over
training steps. Each point pins:
  - key:   which metric (&#34;loss&#34;, &#34;accuracy&#34;, &#34;val_auc&#34;)
  - value: the measured number at this point
  - step:  the monotonic training step/epoch/batch index (the X axis)
  - timestamp: wall-clock time (server-set), for time-range queries

WHY BOTH step AND timestamp:
  step is the SEMANTIC X axis the user reasons about (&#34;loss at epoch 10&#34;).
  timestamp is the PHYSICAL axis for retention/partitioning and for
  &#34;accuracy over the last 30 days&#34;. The Postgres metrics table is RANGE-
  partitioned by timestamp (see Phase 7.3) so old partitions can be dropped
  cheaply — that&#39;s why timestamp is server-authoritative, not client-set.

WHY value is double: ML metrics are real numbers. double is the natural fit
and matches the DOUBLE PRECISION column in the storage schema.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | Metric name, e.g., &#34;loss&#34;, &#34;accuracy&#34;. Part of the time-series key. |
| value | [double](#double) |  | The measured value at this step. |
| step | [int64](#int64) |  | Monotonic step index (epoch/batch/iteration). The X axis of the curve. Client-supplied: the training job knows its own step counter. |
| timestamp | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Wall-clock time of measurement. SERVER-set on ingest so the partition key and retention are under platform control (a client can&#39;t backdate metrics into an old partition or the future). Echoed back on reads. |






<a name="forgepoint-experiment-v1-MetricSeries"></a>

### MetricSeries
============================================================================
MetricSeries
============================================================================

WHY: the columnar/grouped view of metrics for ONE key on ONE run — the shape
a chart wants (&#34;draw the loss curve&#34;). CompareRuns returns these so a client
can overlay the same metric across runs without re-grouping flat points.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | The metric name these points share, e.g., &#34;accuracy&#34;. |
| points | [MetricPoint](#forgepoint-experiment-v1-MetricPoint) | repeated | The ordered points (by step) for this metric on a single run. |






<a name="forgepoint-experiment-v1-Param"></a>

### Param
============================================================================
Param
============================================================================

WHY: a parameter is an INPUT to a run — a hyperparameter or config value set
BEFORE/at run start (learning_rate=0.01, optimizer=&#34;adam&#34;, batch_size=64).
Contrast with a metric, which is an OUTPUT measured DURING the run.

WHY value is a string (not a oneof of typed scalars):
  Params are heterogeneous (ints, floats, bools, enums, strings) and are
  used for display, grouping, and equality comparison — never for math. A
  single string column keeps the schema trivial and the API stable as new
  param types appear. MLflow makes the same call (params are strings). If we
  later need typed numeric params for range filtering, we add a typed field;
  the string stays as the canonical display form.

IMMUTABILITY: a param is write-once per key within a run. Re-logging the same
key is rejected (or treated as idempotent if value matches) so a run&#39;s
configuration can&#39;t silently change mid-flight.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | Parameter name, e.g., &#34;learning_rate&#34;, &#34;optimizer&#34;. Unique within a run. |
| value | [string](#string) |  | Stringified parameter value, e.g., &#34;0.01&#34;, &#34;adam&#34;, &#34;true&#34;. |






<a name="forgepoint-experiment-v1-Run"></a>

### Run
============================================================================
Run
============================================================================

WHY: a Run is ONE execution within an experiment — one training job, one set
of hyperparameters, one resulting metric curve. It is the unit you compare.

LINK TO MODEL VERSIONS (cross-service, decoupled):
  model_version_id ties the run that PRODUCED a model to that model&#39;s
  artifact in the Model Registry. We deliberately store it as an opaque
  string ID, NOT by importing forgepoint.registry.v1 — services are
  decoupled; the contract between them is the ID value plus async events, not
  a compile-time proto dependency. This keeps the experiment tracker
  buildable and deployable without the registry&#39;s proto, and lets either
  evolve independently. (Same reasoning the design doc uses for &#34;database
  per service&#34;: no cross-service hard coupling.)

WHY summary metrics live on the Run:
  The full metric history is a time-series (potentially millions of points)
  stored in a partitioned table and fetched on demand. But ListRuns and the
  comparison UI need the HEADLINE number per run (&#34;final accuracy = 0.94&#34;)
  without reading the whole series. final_metrics is a denormalized snapshot
  of the last/best value per metric key, written when the run finishes — a
  read-optimization, the CQRS-flavored &#34;projection&#34; of the series.

SECURITY: id, status, source, owner_id, started_at, ended_at, and
final_metrics are all SERVER-AUTHORITATIVE — none are accepted from a client
on StartRun/LogMetrics. A client only supplies params and metric points.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Primary key, immutable. Server-generated. |
| experiment_id | [string](#string) |  | The experiment this run belongs to. Set at StartRun; immutable. |
| display_name | [string](#string) |  | Optional human label for the run, e.g., &#34;lr=0.01 batch=64&#34;. If empty, the UI falls back to a short form of the id. |
| status | [RunStatus](#forgepoint-experiment-v1-RunStatus) |  | Current lifecycle state. SERVER-AUTHORITATIVE (see RunStatus). |
| source | [RunSource](#forgepoint-experiment-v1-RunSource) |  | Whether this run came from the sync API or the async event stream. SERVER-set based on which ingestion path created it. |
| model_version_id | [string](#string) |  | The model version this run produced (or evaluated), if any. Opaque ID into the Model Registry. Empty for runs that don&#39;t yield a registered model. See the cross-service note above on why this is a bare string. |
| owner_id | [string](#string) |  | The user who started this run. SERVER-set from auth context. |
| params | [Param](#forgepoint-experiment-v1-Param) | repeated | Hyperparameters / config for this run (the knobs). Typically set once at StartRun and appended to via LogParams. Returned on GetRun. |
| final_metrics | [MetricPoint](#forgepoint-experiment-v1-MetricPoint) | repeated | Denormalized headline metrics (final/best value per key), for cheap list and compare reads. SERVER-computed from the metric series; NOT the full history. Use CompareRuns / a metric-history read for the time-series. |
| started_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the run started. SERVER-set at StartRun, immutable. |
| ended_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the run reached a terminal state. SERVER-set; unset while RUNNING. |
| artifacts | [google.protobuf.Struct](#google-protobuf-Struct) |  | Free-form, JSON-shaped side-artifacts (confusion matrix, feature-importance, artifact manifest, eval report). Attached via SetRunArtifacts; size-capped server-side. Struct (not bytes) so it stays inspectable/renderable. Unset for runs that never attach any. NOT a metrics channel — metrics are typed. |






<a name="forgepoint-experiment-v1-RunComparison"></a>

### RunComparison
RunComparison is the per-run slice of a CompareRuns response: the run&#39;s
params (so the UI can show &#34;lr=0.01 vs lr=0.1&#34;) plus its metric series.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run | [Run](#forgepoint-experiment-v1-Run) |  | The run being compared (metadata, status, final_metrics). |
| series | [MetricSeries](#forgepoint-experiment-v1-MetricSeries) | repeated | The full metric series (per requested key) for this run — the curves. |






<a name="forgepoint-experiment-v1-SetRunArtifactsRequest"></a>

### SetRunArtifactsRequest
============================================================================
SetRunArtifacts  (the typed home for the google.protobuf.Struct import)
============================================================================

WHY this RPC exists (and why Struct): a run produces unstructured side-artifacts
whose shape varies and isn&#39;t worth a schema change per kind — a confusion
matrix, a feature-importance map, an artifact manifest, a small eval report.
google.protobuf.Struct round-trips to/from JSON, so callers attach arbitrary
JSON-shaped context WITHOUT us reaching for opaque bytes (which a UI can&#39;t
render) and WITHOUT bloating the hot LogMetrics path with free-form data.

SECURITY / SIZE: this is NOT a dumping ground. The server caps the serialized
Struct size (e.g. 256 KiB) and REJECTS larger payloads with INVALID_ARGUMENT —
large blobs belong in object storage with a URI referenced here, not inline on
NATS-adjacent state. Artifacts are write-once-ish: re-setting the same key
replaces it; keys are namespaced strings the run owns.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_id | [string](#string) |  | The run to attach artifacts to. |
| artifacts | [google.protobuf.Struct](#google-protobuf-Struct) |  | Free-form, JSON-shaped artifacts (e.g. {&#34;confusion_matrix&#34;: [[...]], &#34;manifest_uri&#34;: &#34;s3://...&#34;}). Server-size-capped. Struct (not bytes) so it is inspectable/renderable and stays human-readable. |
| idempotency_key | [string](#string) |  | Idempotency key — SetRunArtifacts is a mutation a client may retry; the same key replays to the same effect rather than re-applying. |






<a name="forgepoint-experiment-v1-SetRunArtifactsResponse"></a>

### SetRunArtifactsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run | [Run](#forgepoint-experiment-v1-Run) |  | The run after the attachment (carries the merged artifacts back for confirm). |






<a name="forgepoint-experiment-v1-StartRunRequest"></a>

### StartRunRequest
StartRunRequest opens a new run inside an experiment.
SERVER-AUTHORITATIVE and therefore ABSENT here: id, status (always created
RUNNING), source (set to API), owner_id (from claims), started_at,
final_metrics. The client only declares which experiment, an optional label,
the model version it targets, and its initial params.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| experiment_id | [string](#string) |  | The experiment this run belongs to. Required; must exist and be visible to the caller&#39;s team. |
| display_name | [string](#string) |  | Optional human label, e.g., &#34;lr=0.01 batch=64&#34;. |
| model_version_id | [string](#string) |  | Optional model version this run will produce/evaluate. Opaque Registry ID; see the Run.model_version_id note on why it&#39;s a bare string. |
| params | [Param](#forgepoint-experiment-v1-Param) | repeated | Initial hyperparameters for the run. More can be added via LogParams. |
| idempotency_key | [string](#string) |  | Idempotency key (client-generated UUID). WHY: StartRun is a mutation a client may retry after a network blip; without a dedup key a retry would create a DUPLICATE run. The server records (idempotency_key → run_id) and returns the SAME run on replay. This is the standard idempotent-create pattern (Stripe&#39;s Idempotency-Key header). Optional but strongly advised for automated callers. |






<a name="forgepoint-experiment-v1-StartRunResponse"></a>

### StartRunResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run | [Run](#forgepoint-experiment-v1-Run) |  | The newly started run (status = RUNNING, server-set fields populated). |






<a name="forgepoint-experiment-v1-UpdateExperimentRequest"></a>

### UpdateExperimentRequest
UpdateExperimentRequest edits the MUTABLE, client-owned fields of an
experiment (name/description/tags). Note what is ABSENT and therefore
NOT editable by a client: id (the target, but immutable as data), owner_id,
team, created_at, archived_at — all server-authoritative. You cannot
&#34;re-home&#34; an experiment to another owner/team via this RPC (mass-assignment
guard). WHY a field mask: a client editing only the description must not have
to round-trip name/tags and risk clobbering a concurrent edit.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | The experiment to update. The id selects the row; it is never changed. |
| name | [string](#string) |  | New name (applied only if &#34;name&#34; is in update_fields). |
| description | [string](#string) |  | New description (applied only if &#34;description&#34; is in update_fields). |
| tags | [UpdateExperimentRequest.TagsEntry](#forgepoint-experiment-v1-UpdateExperimentRequest-TagsEntry) | repeated | New tags — REPLACES the whole map (applied only if &#34;tags&#34; is in update_fields). WHY replace-not-merge: merge semantics make it impossible to DELETE a tag; a full replace is unambiguous and the client sends the desired final set. |
| update_fields | [string](#string) | repeated | Field mask of which of {name, description, tags} to apply. WHY a string list rather than google.protobuf.FieldMask: keeps the import surface minimal and the allowed set is tiny and validated server-side; an unknown field name is rejected with INVALID_ARGUMENT. |






<a name="forgepoint-experiment-v1-UpdateExperimentRequest-TagsEntry"></a>

### UpdateExperimentRequest.TagsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="forgepoint-experiment-v1-UpdateExperimentResponse"></a>

### UpdateExperimentResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| experiment | [Experiment](#forgepoint-experiment-v1-Experiment) |  | The experiment after the edit (updated_at refreshed). |






<a name="forgepoint-experiment-v1-UpdateRunStatusRequest"></a>

### UpdateRunStatusRequest
UpdateRunStatusRequest transitions a run to a terminal state (FINISHED /
FAILED / KILLED). WHY a dedicated RPC instead of a status field on some
&#34;UpdateRun&#34;: status is security-sensitive and state-machine-constrained, so
it gets its own narrow, auditable mutation. The server validates the
transition (e.g., you can&#39;t move a FINISHED run back to RUNNING) and stamps
ended_at itself.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run_id | [string](#string) |  | The run to transition. |
| status | [RunStatus](#forgepoint-experiment-v1-RunStatus) |  | The target terminal status. The server REJECTS illegal transitions and rejects RUN_STATUS_RUNNING/UNSPECIFIED here (you can&#39;t &#34;un-finish&#34; a run). |






<a name="forgepoint-experiment-v1-UpdateRunStatusResponse"></a>

### UpdateRunStatusResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| run | [Run](#forgepoint-experiment-v1-Run) |  | The run after the transition (ended_at now set, final_metrics computed). |





 


<a name="forgepoint-experiment-v1-RunSource"></a>

### RunSource
============================================================================
RunSource
============================================================================

WHY: a run can be created by a human-driven training job (SYNC path,
StartRun &#43; LogMetrics) OR materialized from the async event stream (e.g.,
the tracker observes fp.models.version.created and opens a run to attach
production metrics to). Recording the source makes the two ingestion paths
visible in the data — invaluable when debugging &#34;why does this run have no
params?&#34; (answer: it came from an event, not a training job).
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| RUN_SOURCE_UNSPECIFIED | 0 | Required zero value. |
| RUN_SOURCE_API | 1 | Created via the sync gRPC API (StartRun) — a first-party training job/SDK. |
| RUN_SOURCE_EVENT | 2 | Materialized by the async NATS batch consumer from a platform event. |



<a name="forgepoint-experiment-v1-RunStatus"></a>

### RunStatus
============================================================================
RunStatus
============================================================================

WHY a finite-state enum (not a free-form string): a run has a small, known
lifecycle, and downstream UI/queries switch on it (&#34;show me failed runs&#34;).
An enum makes the states self-documenting, prevents typos, and is the Buf-
blessed way to model a closed set.

LIFECYCLE (state machine):

  RUNNING ──► FINISHED   (job completed; metrics are final)
     │
     ├──────► FAILED     (job crashed / errored)
     │
     └──────► KILLED     (cancelled by a user or orchestrator)

RUNNING is the only non-terminal state. Status is SERVER-AUTHORITATIVE — a
run is created in RUNNING by StartRun, and only the server (via
UpdateRunStatus, or async on a PipelineFailed event) moves it to a terminal
state. Clients never set status on creation (mass-assignment guard).
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| RUN_STATUS_UNSPECIFIED | 0 | Required zero value (Buf ENUM_ZERO_VALUE_SUFFIX). Means &#34;not set&#34; — a run is never legitimately in this state; treat it as a serialization bug. |
| RUN_STATUS_RUNNING | 1 | The run is in progress. New MetricPoints are still expected. |
| RUN_STATUS_FINISHED | 2 | The run completed successfully. Its metrics are final and comparable. |
| RUN_STATUS_FAILED | 3 | The run errored out (training crashed, OOM, bad data). Metrics partial. |
| RUN_STATUS_KILLED | 4 | The run was cancelled by a user or the pipeline orchestrator. |


 

 


<a name="forgepoint-experiment-v1-ExperimentTrackerService"></a>

### ExperimentTrackerService
============================================================================
EXPERIMENT TRACKER SERVICE
============================================================================

WHY a single service boundary: experiments, runs, params, and metrics form
one cohesive aggregate (a run is meaningless outside its experiment; metrics
are meaningless outside their run). Splitting them would force inter-service
calls for every common flow. One service owns the fp_experiment database and
its time-partitioned metrics table.

RPC CATEGORIES:
  EXPERIMENTS: CreateExperiment, GetExperiment, ListExperiments,
               UpdateExperiment, ArchiveExperiment (soft delete)
  RUN LIFECYCLE: StartRun, UpdateRunStatus, GetRun, ListRuns, DeleteRun
  INGESTION (sync path): LogMetrics (batch), LogParams, SetRunArtifacts
  ANALYSIS / READ: GetMetricHistory (paginated curve), CompareRuns

NOT IN THIS PROTO (by design): the NATS batch consumer that ingests the
canonical subjects — fp.inference.completed / fp.models.version.created /
fp.pipelines.step.completed / fp.pipelines.completed / fp.features.written /
fp.billing.usage.recorded / fp.models.drift.detected /
fp.pipelines.model.deployed / fp.notifications.delivered|failed. That&#39;s the
async, high-volume path with back-pressure — it has no RPC surface; it&#39;s wired
in the service&#39;s events package (decoding events.* payloads). The gRPC API
above is the sync, first-party &#43; read/analysis surface. Keeping the hot loop
OFF gRPC is the entire point of the event-driven pattern.

SUBJECT-NAME FIXES baked into the list above (vs the old draft): plural
fp.pipelines.step.completed (was fp.pipeline.*) and fp.features.WRITTEN (was
the producerless fp.features.ingested) — reconciled against events.proto.

ASCII — Dual ingestion (the pattern in one picture):

  Training job ──gRPC StartRun/LogMetrics(batch)──► [Experiment Tracker] ──► Postgres
                                                         ▲   (time-partitioned
                                                         │    metrics table)
  Inference GW ─┐                                        │
  Pipeline Orch ┼─ NATS events ─► [batch consumer: buffer → flush every N/M]
  Registry ─────┘                  (NAK on overload = back-pressure)
============================================================================

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateExperiment | [CreateExperimentRequest](#forgepoint-experiment-v1-CreateExperimentRequest) | [CreateExperimentResponse](#forgepoint-experiment-v1-CreateExperimentResponse) | CreateExperiment creates a named grouping of runs. owner_id/team are set from the caller&#39;s auth claims (not the request). Requires experiments:write. |
| GetExperiment | [GetExperimentRequest](#forgepoint-experiment-v1-GetExperimentRequest) | [GetExperimentResponse](#forgepoint-experiment-v1-GetExperimentResponse) | GetExperiment fetches one experiment by ID (scoped to the caller&#39;s team). |
| ListExperiments | [ListExperimentsRequest](#forgepoint-experiment-v1-ListExperimentsRequest) | [ListExperimentsResponse](#forgepoint-experiment-v1-ListExperimentsResponse) | ListExperiments returns a paginated list (cursor-based, page size capped server-side at 100). team_filter only NARROWS within the caller&#39;s permitted teams; include_archived toggles soft-deleted rows. |
| UpdateExperiment | [UpdateExperimentRequest](#forgepoint-experiment-v1-UpdateExperimentRequest) | [UpdateExperimentResponse](#forgepoint-experiment-v1-UpdateExperimentResponse) | UpdateExperiment edits mutable fields (name/description/tags) via a field mask. owner_id/team/timestamps are server-authoritative and cannot be set. |
| ArchiveExperiment | [ArchiveExperimentRequest](#forgepoint-experiment-v1-ArchiveExperimentRequest) | [ArchiveExperimentResponse](#forgepoint-experiment-v1-ArchiveExperimentResponse) | ArchiveExperiment SOFT-deletes an experiment (sets archived_at, hides from default lists) while preserving its runs/metrics for lineage/audit. |
| StartRun | [StartRunRequest](#forgepoint-experiment-v1-StartRunRequest) | [StartRunResponse](#forgepoint-experiment-v1-StartRunResponse) | StartRun opens a new run (status RUNNING) in an experiment. Supports an idempotency_key so retries don&#39;t create duplicate runs. This is the design doc&#39;s StartRun/CreateRun. On success the server PUBLISHES events.RunCreated (fp.experiments.run.created). |
| UpdateRunStatus | [UpdateRunStatusRequest](#forgepoint-experiment-v1-UpdateRunStatusRequest) | [UpdateRunStatusResponse](#forgepoint-experiment-v1-UpdateRunStatusResponse) | UpdateRunStatus transitions a run to a terminal state (FINISHED/FAILED/ KILLED). Server validates the state-machine transition, stamps ended_at, and PUBLISHES events.RunFinished (fp.experiments.run.finished) carrying the run&#39;s final_metrics (fat event — consumers rank/alert with no callback). |
| GetRun | [GetRunRequest](#forgepoint-experiment-v1-GetRunRequest) | [GetRunResponse](#forgepoint-experiment-v1-GetRunResponse) | GetRun fetches a run&#39;s metadata, params, headline metrics, and artifacts (NOT the full metric time-series — use GetMetricHistory for the curve). |
| ListRuns | [ListRunsRequest](#forgepoint-experiment-v1-ListRunsRequest) | [ListRunsResponse](#forgepoint-experiment-v1-ListRunsResponse) | ListRuns returns a paginated list of runs, normally within one experiment, optionally filtered by status. Cursor-based; page size capped server-side. |
| DeleteRun | [DeleteRunRequest](#forgepoint-experiment-v1-DeleteRunRequest) | [DeleteRunResponse](#forgepoint-experiment-v1-DeleteRunResponse) | DeleteRun hard-deletes one run &#43; its metrics/params (pruning experimental noise). Rejected if the run&#39;s model_version_id is still referenced by a live Registry version (lineage guard); idempotent via idempotency_key. |
| LogMetrics | [LogMetricsRequest](#forgepoint-experiment-v1-LogMetricsRequest) | [LogMetricsResponse](#forgepoint-experiment-v1-LogMetricsResponse) | LogMetrics is the high-throughput BATCH ingestion RPC: a training job flushes many MetricPoints in one idempotent, transactional call. Timestamps are server-stamped; batch size capped server-side. See the message comment for why this is unary-batch rather than client-streaming. |
| LogParams | [LogParamsRequest](#forgepoint-experiment-v1-LogParamsRequest) | [LogParamsResponse](#forgepoint-experiment-v1-LogParamsResponse) | LogParams appends write-once hyperparameters to a RUNNING run. |
| SetRunArtifacts | [SetRunArtifactsRequest](#forgepoint-experiment-v1-SetRunArtifactsRequest) | [SetRunArtifactsResponse](#forgepoint-experiment-v1-SetRunArtifactsResponse) | SetRunArtifacts attaches free-form JSON-shaped artifacts (confusion matrix, manifest, eval report) to a run. Struct payload, server-size-capped. |
| GetMetricHistory | [GetMetricHistoryRequest](#forgepoint-experiment-v1-GetMetricHistoryRequest) | [GetMetricHistoryResponse](#forgepoint-experiment-v1-GetMetricHistoryResponse) | GetMetricHistory returns the FULL metric time-series for ONE run, PAGINATED (page size capped server-side, default 1000 / max 5000) with optional key and step-window filters. This is the &#34;metric-history read&#34; the rest of the proto refers to — the pull-shaped curve read GetRun deliberately omits. |
| CompareRuns | [CompareRunsRequest](#forgepoint-experiment-v1-CompareRunsRequest) | [CompareRunsResponse](#forgepoint-experiment-v1-CompareRunsResponse) | CompareRuns returns the metric series of several runs side-by-side — the data behind &#34;which run/model version wins?&#34;. Run count and payload bounded server-side. |

 



<a name="forgepoint_featurestore_v1_featurestore-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/featurestore/v1/featurestore.proto



<a name="forgepoint-featurestore-v1-DefineFeatureViewRequest"></a>

### DefineFeatureViewRequest
DefineFeatureViewRequest declares (or evolves) a FeatureView schema.

EVENT-SOURCING SEMANTICS: this is a COMMAND that, on success, appends a
FeatureViewDefined event. Calling it again for the same `name` with a changed
schema appends another FeatureViewDefined and bumps schema_version (additive
evolution preferred). The append, not an UPDATE, is the source of truth.

SECURITY: only schema-shaping inputs are accepted. id, owner_user_id,
owner_team, schema_version, created_at, updated_at are SERVER-assigned (the
owner from TokenClaims) — deliberately NOT in this request to block
mass-assignment.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Unique view name within the caller&#39;s team. First call creates it; a later call with the same name evolves the schema (new schema_version). |
| description | [string](#string) |  | Human-readable description for the catalog. |
| entity | [Entity](#forgepoint-featurestore-v1-Entity) |  | The Entity this view is keyed by (name &#43; join_key). Defining the entity inline keeps the view self-contained for M2; a first-class entity registry is a possible later refinement. |
| features | [FeatureSpec](#forgepoint-featurestore-v1-FeatureSpec) | repeated | The feature schema (specs). Server validates: non-empty, unique names, valid value_types, sane dimensions. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY (for &#34;exactly-once intent&#34;): DefineFeatureView mutates state by appending an event. A network retry after a server-side success (response lost) must NOT append a duplicate FeatureViewDefined / spuriously bump schema_version. The server records processed idempotency_keys; a repeat returns the SAME FeatureView. UUID v4 generated by the client per logical attempt (not per retry). |






<a name="forgepoint-featurestore-v1-DefineFeatureViewResponse"></a>

### DefineFeatureViewResponse
DefineFeatureViewResponse wraps the resulting FeatureView (with all
SERVER-assigned fields populated). Wrapped, not returned bare, per Buf
RPC_RESPONSE_STANDARD_NAME and for forward-compatibility (e.g., adding a
`created` bool to distinguish create vs evolve later).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view | [FeatureView](#forgepoint-featurestore-v1-FeatureView) |  | The defined/evolved view, including SERVER-assigned id, owner, version, timestamps. |






<a name="forgepoint-featurestore-v1-DeleteFeatureViewRequest"></a>

### DeleteFeatureViewRequest
DeleteFeatureViewRequest retires a FeatureView. WHY this exists (the design
/CLI need it) and WHY it is a SOFT delete that APPENDS an event rather than
DROP-ing rows:
  In an event-sourced system the log is the source of truth and is IMMUTABLE.
  &#34;Deleting&#34; therefore means appending a terminal FeatureViewDeleted event
  (the design doc&#39;s append-only model already lists Created/Updated/Deleted),
  which retires the view from the catalog and stops serving it — WITHOUT
  erasing history. Past training datasets assembled via GetHistoricalFeatures
  must remain reproducible (audit &#43; legal), so the historical events stay; the
  online (Redis) projection is torn down. A hard physical purge (GDPR erasure)
  is a separate, audited admin operation, deliberately NOT this RPC.

SECURITY: only the immutable id is accepted (no owner/team field — team
scoping is SERVER-derived from TokenClaims, so a caller cannot delete another
team&#39;s view). Idempotent: re-deleting an already-deleted view is a no-op that
returns the same result, both via the natural terminal state and the explicit
idempotency_key (a lost-response retry must not append a second delete event).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view_id | [string](#string) |  | The view to retire (immutable UUID). Name is intentionally NOT accepted for a destructive op — the caller must resolve to the stable id first, removing any rename-race ambiguity about WHICH view is being deleted. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY: appending FeatureViewDeleted is a mutation; a retried request (response lost in flight) must not append a duplicate terminal event. UUID v4 per logical attempt. Server dedupes and replays the result. |






<a name="forgepoint-featurestore-v1-DeleteFeatureViewResponse"></a>

### DeleteFeatureViewResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view | [FeatureView](#forgepoint-featurestore-v1-FeatureView) |  | The view as of its retirement: schema_version bumped by the delete event and deleted_at set to the retirement time. Returned so callers can confirm the terminal state without a follow-up GetFeatureView. |






<a name="forgepoint-featurestore-v1-Entity"></a>

### Entity
============================================================================
Entity
============================================================================

WHY: An Entity is the THING features describe and the KEY features are looked
up by — e.g., a &#34;user&#34;, a &#34;merchant&#34;, a &#34;device&#34;. Every FeatureView is keyed
by exactly one Entity, and every feature value belongs to a specific entity
instance (entity_id, e.g., user &#34;u_123&#34;). At inference the gateway says &#34;give
me features for user u_123&#34;; the entity tells the store how to interpret
&#34;u_123&#34;.

WHY MODEL IT EXPLICITLY (not just a string key): naming the join key is what
lets multiple FeatureViews compose at serving time — a model can pull the
&#34;user_credit&#34; view and the &#34;user_activity&#34; view and join them on the shared
`user` entity. This mirrors Feast&#39;s first-class Entity concept.

SECURITY: `name` and `join_key` are caller-provided identifiers (safe). There
are no owner/credential fields here — ownership lives on FeatureView and is
SERVER-assigned (see below) to avoid mass-assignment.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Stable, unique entity name within the team: &#34;user&#34;, &#34;merchant&#34;, &#34;device&#34;. Referenced by FeatureView.entity. Immutable once a view uses it. |
| join_key | [string](#string) |  | The logical join key column this entity is identified by, e.g., &#34;user_id&#34;. Documents how an entity_id maps onto upstream data for offline joins. |
| description | [string](#string) |  | Human-readable description for the catalog/UI. |






<a name="forgepoint-featurestore-v1-FeatureSpec"></a>

### FeatureSpec
============================================================================
FeatureSpec
============================================================================

WHY: One feature&#39;s schema — its name, type, and optional shape constraints.
A FeatureView holds a list of these. The server uses FeatureSpecs to VALIDATE
every WriteFeatures payload, which is the contract that prevents train/serve
skew and silent data corruption.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Feature name, unique within its FeatureView: &#34;txn_count_7d&#34;, &#34;embedding&#34;. Becomes a key in the FeatureValues map written/served for an entity. |
| value_type | [FeatureValueType](#forgepoint-featurestore-v1-FeatureValueType) |  | Declared value type. Server validates written values against this. |
| description | [string](#string) |  | Human-readable description (units, semantics) for the catalog/UI. |
| dimension | [int32](#int32) |  | For DOUBLE_LIST features: required vector length (e.g., 768 for an embedding). 0 = unconstrained/not-applicable. Lets the server reject wrong-dimension vectors before they reach training. |






<a name="forgepoint-featurestore-v1-FeatureValues"></a>

### FeatureValues
============================================================================
FeatureValues
============================================================================

WHY a map&lt;string, Value&gt;: a single entity&#39;s feature row is a set of
name→value pairs. We use google.protobuf.Value (the well-known type) so any
declared FeatureValueType — number, string, bool, list (DOUBLE_LIST), or
nested struct — maps onto one wire representation, while the FeatureView&#39;s
FeatureSpecs carry the authoritative type. This keeps the message generic
over feature types without an enormous oneof per feature.

  WHY NOT bytes: opaque bytes would lose the ability to inspect/validate/serve
  individual features and would force every consumer to agree on an encoding.
  WHY NOT a fixed message per view: views are user-defined at runtime; we
  can&#39;t generate a proto type per view.

The Timestamp here is the EVENT TIME — when these feature values became true
(NOT when they were ingested). Point-in-time correctness depends on event
time, so it is a first-class field, not server wall-clock. (Ingest/append
time is recorded separately by the server in the event log.)
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entity_id | [string](#string) |  | The entity instance these values describe, e.g., &#34;u_123&#34;. Interpreted against the FeatureView&#39;s Entity.join_key. |
| values | [FeatureValues.ValuesEntry](#forgepoint-featurestore-v1-FeatureValues-ValuesEntry) | repeated | name → value for each feature in this row. Keys must match FeatureSpec names in the view; the server validates value kinds against declared types. |
| event_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | EVENT TIME: when these values became valid in the real world. This is the timestamp point-in-time queries compare against (GetHistoricalFeatures as_of). Caller-supplied because only the producer knows the true event time (e.g., backfilling yesterday&#39;s features). If omitted on write, the server defaults it to ingest time and says so. |






<a name="forgepoint-featurestore-v1-FeatureValues-ValuesEntry"></a>

### FeatureValues.ValuesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [google.protobuf.Value](#google-protobuf-Value) |  |  |






<a name="forgepoint-featurestore-v1-FeatureVector"></a>

### FeatureVector
============================================================================
FeatureVector
============================================================================

WHY a distinct read-side type (vs reusing FeatureValues): a served feature
vector carries PROVENANCE the write side doesn&#39;t — which schema_version and
which event/log version produced it, and the projection&#39;s own timestamp.
Returning the version that served the value lets callers (and the Model
Monitor) detect staleness and reproduce reads. This separation (write command
shape vs read projection shape) is idiomatic CQRS, the natural companion to
event sourcing.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entity_id | [string](#string) |  | The entity instance this vector is for. |
| values | [FeatureVector.ValuesEntry](#forgepoint-featurestore-v1-FeatureVector-ValuesEntry) | repeated | name → value for the requested features. Missing requested features are simply absent here (see GetOnlineFeaturesResponse.missing_features). |
| as_of_version | [int64](#int64) |  | The event-log version that produced these values (the latest event applied for this entity, at or before as_of for historical reads). Lets the caller pin/repro a read and detect staleness against later writes. |
| event_time | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Event time of the values served (the producing event&#39;s event_time). |
| schema_version | [int64](#int64) |  | The FeatureView schema_version these values were validated against. |






<a name="forgepoint-featurestore-v1-FeatureVector-ValuesEntry"></a>

### FeatureVector.ValuesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [google.protobuf.Value](#google-protobuf-Value) |  |  |






<a name="forgepoint-featurestore-v1-FeatureView"></a>

### FeatureView
============================================================================
FeatureView
============================================================================

WHY: A FeatureView is the unit of definition and serving — a NAMED, VERSIONED
schema (a set of FeatureSpecs) over a single Entity. It is the design doc&#39;s
&#34;feature set&#34;. Consumers request features by (feature_view, entity_id); the
view tells the store which features exist and how to validate/serve them.

IMMUTABILITY &#43; VERSIONING (event-sourcing tie-in): a FeatureView&#39;s schema is
itself defined by an event (FeatureViewDefined). `schema_version` increments
when the schema changes (a new FeatureViewDefined event), so historical reads
can be interpreted against the schema that was in effect at the time. We
favor additive schema evolution (add features) over breaking changes.

SECURITY — SERVER-AUTHORITATIVE FIELDS (NOT accepted on define/write):
  id, owner_user_id, owner_team, schema_version, created_at, updated_at are
  all set by the SERVER from the authenticated principal / clock / sequence.
  The client never sends them on DefineFeatureViewRequest. This avoids
  mass-assignment (a caller forging owner_team to access another team&#39;s data
  or back-dating created_at). The auth interceptor&#39;s TokenClaims is the only
  trusted source of owner identity.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. SERVER-assigned. Stable handle used by all read/write RPCs. |
| name | [string](#string) |  | Unique, human-readable view name within the team: &#34;user_credit_features&#34;. |
| description | [string](#string) |  | Human-readable description for the catalog/UI. |
| entity | [Entity](#forgepoint-featurestore-v1-Entity) |  | The Entity this view is keyed by. Every feature row belongs to one entity instance (entity_id) of this entity&#39;s type. |
| features | [FeatureSpec](#forgepoint-featurestore-v1-FeatureSpec) | repeated | The schema: the features this view contains. Server validates writes against these specs. |
| schema_version | [int64](#int64) |  | Monotonic schema version. SERVER-assigned. Increments on each schema change (each FeatureViewDefined event for this name). Lets historical reads reason about which schema was in effect. |
| owner_user_id | [string](#string) |  | Owning user (UUID). SERVER-assigned from the authenticated caller. Never accepted from the client (mass-assignment guard). |
| owner_team | [string](#string) |  | Owning team label. SERVER-assigned from TokenClaims.team. Used for team-scoped authorization on reads/writes. Never client-supplied. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this view was first defined. SERVER-assigned. Immutable. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this view&#39;s schema was last changed. SERVER-assigned. |
| deleted_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | SOFT-DELETE marker. Unset (zero) while the view is live; SERVER-set to the retirement time when DeleteFeatureView appends the terminal FeatureViewDeleted event. WHY a timestamp, not a bool: it records WHEN the view was retired (audit) and doubles as the &#34;is deleted&#34; flag (presence = deleted). The view&#39;s history stays queryable via GetHistoricalFeatures for reproducibility; this only marks it retired from the live catalog/serving. Never client-supplied (mass-assignment guard). |






<a name="forgepoint-featurestore-v1-GetFeatureViewRequest"></a>

### GetFeatureViewRequest
GetFeatureViewRequest fetches a SINGLE view&#39;s current schema/metadata. WHY
this RPC was MISSING and is genuinely needed: every consumer-facing flow —
the `fp` CLI&#39;s `feature-view describe`, an SDK validating a payload before
WriteFeatures, the Web UI&#39;s view detail page — needs one view by handle, and
ListFeatureViews (page the whole catalog) is the wrong, expensive tool for
that. It is a pure read of the feature_views metadata projection.

EITHER handle works (a oneof so exactly one is supplied): the immutable id
(stable across renames — preferred for machine callers) or the name (the
human handle — convenient for the CLI). Team scoping is applied SERVER-side
from TokenClaims.team, so a caller cannot read another team&#39;s view by
guessing an id (authorization, not just existence).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view_id | [string](#string) |  | Immutable view UUID (FeatureView.id). Preferred for machine callers. |
| name | [string](#string) |  | View name, resolved within the caller&#39;s team. Convenient for humans/CLI. |






<a name="forgepoint-featurestore-v1-GetFeatureViewResponse"></a>

### GetFeatureViewResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view | [FeatureView](#forgepoint-featurestore-v1-FeatureView) |  | The requested view with all SERVER-assigned fields populated. |






<a name="forgepoint-featurestore-v1-GetHistoricalFeaturesRequest"></a>

### GetHistoricalFeaturesRequest
GetHistoricalFeaturesRequest performs a POINT-IN-TIME (&#34;as-of&#34;) read against
the offline projection of the event log: for each entity, the feature values
from the latest event with event_time &lt;= as_of. This is the reproducibility
workhorse — training jobs use it to assemble the exact feature snapshot that
existed when labels were observed, eliminating LABEL LEAKAGE (using features
computed AFTER the prediction time).

WHY UNARY (not server-streaming) for M2: training feature retrieval is
typically a bounded set of (entity, timestamp) lookups assembled into a
dataset, and the result is paginated. A large-scale &#34;point-in-time JOIN&#34;
producing millions of rows is a batch/offline job (write to object storage,
return a URI) rather than a synchronous stream — a deliberate later
refinement, called out here so the unary choice is defensible.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view_id | [string](#string) |  | Target view (UUID). |
| entity_ids | [string](#string) | repeated | Entity instances to retrieve as-of the timestamp. Server CAPS the batch at MAX_BATCH_SIZE=1000 entities; over-cap → INVALID_ARGUMENT, to bound the point-in-time scan. Larger training pulls page via `pagination` below. |
| as_of | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | POINT-IN-TIME cutoff. For each entity, return the values from the latest event with event_time &lt;= as_of. REQUIRED — a missing as_of would make the query non-reproducible (it would mean &#34;now&#34;, which changes over time). |
| feature_names | [string](#string) | repeated | Optional feature-name projection. Empty = all features in the view. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination over the entity result set (large training pulls page through results). page_size defaults to 20, capped at MAX_PAGE_SIZE=100 server-side (clamped). |






<a name="forgepoint-featurestore-v1-GetHistoricalFeaturesResponse"></a>

### GetHistoricalFeaturesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| vectors | [FeatureVector](#forgepoint-featurestore-v1-FeatureVector) | repeated | One vector per entity that had at least one event at or before `as_of`. Each vector&#39;s as_of_version is the log version of the event that satisfied the point-in-time read (so the snapshot is exactly reproducible). |
| missing_entity_ids | [string](#string) | repeated | Requested entities with NO event at or before `as_of` (they did not exist yet at that time). Explicit, so training code can drop or default them. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | next_page_token &#43; total_count for the entity result set. |






<a name="forgepoint-featurestore-v1-GetOnlineFeaturesRequest"></a>

### GetOnlineFeaturesRequest
GetOnlineFeaturesRequest reads the LATEST feature values for entities from the
low-latency online projection (Redis hash feature:{view}:{entity}). This is
the inference hot path: the Inference Gateway calls it per prediction, so it
must be fast and is served from the cache projection, never by replaying the
log.

PATTERN NOTE: this is the &#34;read model&#34; of CQRS over the event log. It is
eventually consistent with WriteFeatures (see eventual-consistency note in
the file header); as_of_version in the response lets callers detect lag.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view_id | [string](#string) |  | Target view (UUID). |
| entity_ids | [string](#string) | repeated | Entity instances to fetch (e.g., [&#34;u_123&#34;,&#34;u_456&#34;]). Batched to amortize round-trips for models that score many entities at once. Server CAPS the batch at MAX_BATCH_SIZE=1000 entities; over-cap → INVALID_ARGUMENT (bounds hot-path latency/payload). The cap is the contract. |
| feature_names | [string](#string) | repeated | Optional projection: only return these feature names. Empty = all features in the view. Lets a model fetch just the columns it needs (smaller payload, less Redis work). |






<a name="forgepoint-featurestore-v1-GetOnlineFeaturesResponse"></a>

### GetOnlineFeaturesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| vectors | [FeatureVector](#forgepoint-featurestore-v1-FeatureVector) | repeated | One vector per found entity (order not guaranteed). Each carries its as_of_version/event_time/schema_version provenance. |
| missing_entity_ids | [string](#string) | repeated | Entity ids requested but absent in the online view (never written, or evicted). Returned explicitly (rather than silently dropped) so the caller can decide: use a default, fall back to GetHistoricalFeatures, or error. |






<a name="forgepoint-featurestore-v1-ListFeatureViewsRequest"></a>

### ListFeatureViewsRequest
ListFeatureViewsRequest pages the FeatureView catalog (a read projection over
FeatureViewDefined events). Reuses common.v1.PaginationRequest for a uniform
cursor-based shape across the platform (see common.proto for the WHY).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name_filter | [string](#string) |  | Optional: case-insensitive substring filter on view name. Empty = no filter. Team scoping is applied SERVER-side from TokenClaims.team (NOT a client field) so a caller cannot enumerate another team&#39;s catalog. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20; the server CAPS it at MAX_PAGE_SIZE=100 (oversized requests are CLAMPED, not rejected) to bound response size and prevent enumeration/DoS. The cap is the contract. |






<a name="forgepoint-featurestore-v1-ListFeatureViewsResponse"></a>

### ListFeatureViewsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_views | [FeatureView](#forgepoint-featurestore-v1-FeatureView) | repeated | The page of feature views (catalog projection). Schemas are included so a UI/SDK can render the catalog without N follow-up calls. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | next_page_token &#43; total_count. total_count may be -1 when expensive. |






<a name="forgepoint-featurestore-v1-RebuildViewsRequest"></a>

### RebuildViewsRequest
RebuildViewsRequest replays the append-only log to REGENERATE the materialized
views (Redis online &#43; Postgres offline). This is THE event-sourcing party
trick the design doc calls out: &#34;can replay ALL events from scratch to rebuild
both views&#34;. WHY it must be an explicit RPC, not just an internal job: it is
the documented RECOVERY path after a Redis flush, a projection-logic bugfix
(replay corrects every entity), or a schema migration of a read model — an
operator/CLI needs to invoke it deliberately. The log is never touched; only
the caches are rebuilt FROM it (proving the log is the true source of truth).

SECURITY: this is an ADMIN operation guarded by a distinct, higher-privilege
scope (&#34;features:admin&#34;), NOT the ordinary &#34;features:write&#34; — a full replay is
expensive and operationally sensitive, so it must not be reachable with a
routine write token. Scope is SERVER-enforced from TokenClaims.

LONG-RUNNING / STREAMING CHOICE: a full replay can take minutes over a large
log, so this is modeled as a SERVER-STREAMING RPC that emits incremental
RebuildViewsResponse progress frames (events replayed so far / total, current
view). NOTE the message is named RebuildViewsResponse (not RebuildProgress) to
satisfy Buf&#39;s RPC_RESPONSE_STANDARD_NAME rule: a unary OR streaming response
type must be &lt;RpcName&gt;Response. The frames it carries are still &#34;progress&#34;
frames semantically — the name is the contract, the doc explains the role. WHY
server-streaming and not a unary &#34;kick off &#43; poll&#34;: the operator/CLI wants
live progress and a single connection whose closure signals completion, with
no polling loop or separate job-status endpoint. (Contrast the data-plane read
RPCs, which stay unary because they&#39;re bounded and latency-sensitive — the
streaming choice here is justified by DURATION, not data volume.)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view_id | [string](#string) |  | Optional: restrict the rebuild to one view (its UUID). Empty = rebuild ALL views (full recovery). Scoping to one view makes a targeted bugfix replay cheap instead of reprocessing the entire platform&#39;s feature history. |
| target | [RebuildTarget](#forgepoint-featurestore-v1-RebuildTarget) |  | Which projection(s) to rebuild. Lets an operator rebuild only the cache that was lost/corrupted (e.g. ONLINE after a Redis flush) instead of both. |






<a name="forgepoint-featurestore-v1-RebuildViewsResponse"></a>

### RebuildViewsResponse
RebuildViewsResponse is one streamed frame of a RebuildViews replay (a
&#34;progress&#34; frame). The stream emits these periodically; the final frame has
done=true and the connection closes cleanly (its closure is the completion
signal — no poll needed). Named &lt;RpcName&gt;Response to satisfy Buf&#39;s
RPC_RESPONSE_STANDARD_NAME rule even though it is a streamed progress frame.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events_replayed | [int64](#int64) |  | Events replayed so far and the total to replay (so the CLI renders a bar). total_events may be -1 early if counting the log up front is itself costly. |
| total_events | [int64](#int64) |  |  |
| current_feature_view_id | [string](#string) |  | The view currently being rebuilt (empty when rebuilding the whole fleet and between views). Lets the operator see progress per view. |
| done | [bool](#bool) |  | True on the FINAL frame — the rebuild is complete and the stream will close. |






<a name="forgepoint-featurestore-v1-WriteFeaturesRequest"></a>

### WriteFeaturesRequest
WriteFeaturesRequest appends new feature values — the primary EVENT-SOURCING
write. It records FeaturesWritten event(s) into the append-only log; it does
NOT update rows in place. Subsequent reads see these via the materialized
views the server updates from the new events.

WHY BATCH (repeated FeatureValues): feature pipelines write thousands of rows
per run. One row per RPC would be chatty and lose write atomicity. A batch
appends as one logical, ordered set of events (one log version range),
matching how producers actually emit features.

SECURITY: the caller may write only to views their team owns (enforced
server-side from TokenClaims). No owner/version/append-time fields are
accepted — those are SERVER-assigned. event_time inside FeatureValues IS
caller-supplied (it is domain data the producer owns, not a security field).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_view_id | [string](#string) |  | Target view (UUID, from FeatureView.id). The view&#39;s schema validates the payload. Using the immutable id (not name) avoids ambiguity if a view is later renamed. |
| features | [FeatureValues](#forgepoint-featurestore-v1-FeatureValues) | repeated | The feature rows to append. Each entry&#39;s `values` are validated against the view&#39;s FeatureSpecs (names &#43; types &#43; dimensions). A type mismatch fails the WHOLE batch (INVALID_ARGUMENT) so partial-corrupt writes can&#39;t slip in. Batch size is CAPPED at MAX_BATCH_SIZE=1000 rows; an over-cap request is REJECTED with INVALID_ARGUMENT (NOT truncated — silently dropping feature rows would corrupt counts and history). Split larger pipelines into batches. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY: a retried WriteFeatures (lost response) must not append the same rows twice — duplicate feature events would corrupt counts and point-in-time history. The server deduplicates on this key (UUID v4 per logical batch) and returns the original result on replay. This is how an event-sourced write achieves at-least-once delivery with exactly-once EFFECT. |






<a name="forgepoint-featurestore-v1-WriteFeaturesResponse"></a>

### WriteFeaturesResponse
WriteFeaturesResponse confirms the append and returns the assigned log
version range, so producers can correlate, audit, and (if needed) wait for
the online projection to catch up to written_through_version.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| written_count | [int32](#int32) |  | Number of feature rows (events) appended. |
| written_through_version | [int64](#int64) |  | The highest event-log version assigned by this append. Reads observing an as_of_version &gt;= this value are guaranteed to include this batch — useful for read-your-writes checks against the eventually-consistent online view. |





 


<a name="forgepoint-featurestore-v1-FeatureValueType"></a>

### FeatureValueType
============================================================================
FeatureValueType
============================================================================

WHY an enum (not a free-form string): the FeatureView schema declares the
expected type of each feature. The server validates incoming WriteFeatures
payloads against these types — rejecting, e.g., a string where a DOUBLE is
declared. Catching type drift at the boundary prevents silently corrupt
features from poisoning training data, which is one of the worst, hardest-to-
debug failure modes in ML (&#34;the model got worse and nobody knows why&#34;).

WHY THESE TYPES: they cover the ML feature primitives. Embeddings/vectors are
represented as DOUBLE_LIST. Free-form/structured blobs fall back to STRUCT.
Buf STANDARD requires the zero value to be *_UNSPECIFIED and every value to be
prefixed with the enum name.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| FEATURE_VALUE_TYPE_UNSPECIFIED | 0 | Default/unset — an invalid schema declaration. Server rejects on define. |
| FEATURE_VALUE_TYPE_INT64 | 1 | 64-bit signed integer (counts, IDs-as-features, ordinals). |
| FEATURE_VALUE_TYPE_DOUBLE | 2 | IEEE-754 double (most numeric ML features: amounts, ratios, scores). |
| FEATURE_VALUE_TYPE_STRING | 3 | UTF-8 string (categoricals before encoding, raw text features). |
| FEATURE_VALUE_TYPE_BOOL | 4 | Boolean flag feature. |
| FEATURE_VALUE_TYPE_TIMESTAMP | 5 | Wall-clock time feature (e.g., &#34;last_login_at&#34;). Encoded as Timestamp. |
| FEATURE_VALUE_TYPE_DOUBLE_LIST | 6 | Vector of doubles — the canonical shape for embeddings. Validation may also assert a fixed dimensionality declared in FeatureSpec.dimension. |
| FEATURE_VALUE_TYPE_STRUCT | 7 | Arbitrary nested structure (google.protobuf.Struct). Escape hatch for structured features that don&#39;t fit the primitives. Use sparingly — typed features are validated and indexable; structs are opaque. |



<a name="forgepoint-featurestore-v1-RebuildTarget"></a>

### RebuildTarget
RebuildTarget selects which materialized projection(s) a rebuild regenerates.
Buf STANDARD: _UNSPECIFIED zero value &#43; enum-name prefix on every value.

| Name | Number | Description |
| ---- | ------ | ----------- |
| REBUILD_TARGET_UNSPECIFIED | 0 | Unset — server treats as REBUILD_TARGET_ALL (rebuild everything) but the caller SHOULD set this explicitly; the zero value exists only to satisfy proto3/Buf, not as a meaningful &#34;no target&#34;. |
| REBUILD_TARGET_ALL | 1 | Rebuild both the Redis online view and the Postgres offline view. |
| REBUILD_TARGET_ONLINE | 2 | Rebuild only the Redis online (latest-value) projection — e.g. after a flush. |
| REBUILD_TARGET_OFFLINE | 3 | Rebuild only the Postgres offline (point-in-time) projection. |


 

 


<a name="forgepoint-featurestore-v1-FeatureStoreService"></a>

### FeatureStoreService
============================================================================
EVENT PAYLOADS — NOW CANONICAL (forgepoint.events.v1), NOT redefined here
============================================================================

The Feature Store PRODUCES two events. Their payload messages used to live in
THIS file; they have been MOVED to the platform&#39;s single canonical event
package and DELETED here. Producers pack the canonical messages into
common.v1.EventEnvelope.data (a google.protobuf.Any); consumers unpack the
SAME message by its Any type_url — one contract, both ends of every pipe.

  WHAT THE FEATURE STORE PUBLISHES (producer = &#34;feature-store&#34;):
    fp.features.view.defined  → forgepoint.events.v1.FeatureViewDefined
    fp.features.written       → forgepoint.events.v1.FeaturesWritten

  WHO CONSUMES (informational — defined by the consumers, not us):
    FeatureViewDefined → experiment-tracker (correlate which feature schema
                         versions fed a training run), audit subscribers.
    FeaturesWritten    → experiment-tracker (which feature versions fed a
                         run), model-monitor (scope drift checks / cache
                         invalidation to the changed entities).

WHY MOVED (the bug this fixes — conflict #4 in events.proto):
  The platform&#39;s async nervous system had DRIFTED. The design doc and the
  experiment-tracker subscribed to &#34;fp.features.ingested&#34; / &#34;FeaturesIngested&#34;,
  while this service published &#34;FeaturesWritten&#34; on &#34;fp.features.written&#34;.
  Same fact, two names — the subscription would NEVER fire. Centralizing the
  payloads in forgepoint.events.v1 makes that class of mismatch impossible:
  producer and consumer literally import the same generated Go type. (The
  experiment-tracker fix repoints its subscription to fp.features.written.)

  It also DECOUPLES the event schema from this service&#39;s API schema. The old
  local messages were fine until you ask: should the Experiment Tracker
  compile-depend on featurestore.proto just to read an event? No. The event is
  a separately-versioned PUBLISHED CONTRACT (schema-registry thinking); it
  imports only well-known types &#43; common, never a service&#39;s API proto.

THIN-BY-DESIGN (unchanged, and codified in the canonical message):
  events.FeaturesWritten carries the affected entity_ids &#43; a count &#43;
  written_through_version — NOT the feature VALUES. Events are for
  NOTIFICATION/correlation; a consumer that needs the actual values calls
  GetOnlineFeatures/GetHistoricalFeatures (the read models below). This keeps
  NATS small and stops the bus becoming a second source of truth. (This is the
  &#34;thin event &#43; callback&#34; side of the fat-vs-thin tradeoff — chosen here
  because feature rows are large and only some consumers want the values.)

PUBLISHER FIELD MAPPING (a few lines in the handler at the publish boundary —
the price of decoupling, and the only thing the rename touches):
  DefineFeatureView success →  events.FeatureViewDefined{
      feature_view_id   = view.id
      feature_view_name = view.name        // canonical field is feature_view_name
      schema_version    = view.schema_version
      owner_team        = view.owner_team
      defined_at        = view.updated_at  // RENAMED: old local occurred_at → defined_at
  }
  WriteFeatures success →      events.FeaturesWritten{
      feature_view_id          = req.feature_view_id
      feature_view_name        = view.name
      entity_ids               = affected ids (MAY be truncated; count is authoritative)
      written_count            = rows appended
      written_through_version  = resp.written_through_version
      written_at               = append commit time  // RENAMED: old occurred_at → written_at
  }
The canonical messages live in proto/forgepoint/events/v1/events.proto under
the &#34;FEATURE-STORE EVENTS&#34; section — read there for field-level docs.

============================================================================
FEATURE STORE SERVICE
============================================================================

WHY one FeatureStoreService (not split into Catalog/Write/Serve services):
  definition (schema), ingestion (append), and serving (project) all operate
  on the SAME event log and views and share the same team-ownership boundary.
  Splitting them would force inter-service calls and a shared event store
  across service boundaries — violating database-per-service. They live
  behind one boundary; CQRS happens INSIDE this service (write model = event
  log; read models = Redis online &#43; Postgres offline), not across services.

RPC CATEGORIES:
  DEFINITION (catalog):  DefineFeatureView, GetFeatureView, ListFeatureViews
  WRITE (commands):      WriteFeatures                 → appends events
  READ (projections):    GetOnlineFeatures (latest),
                         GetHistoricalFeatures (point-in-time)
  ADMIN (lifecycle):     DeleteFeatureView (soft retire → FeatureViewDeleted),
                         RebuildViews (replay log → regenerate projections)

SERVER-ENFORCED LIMITS (in the CONTRACT, not just prose — see each RPC):
  * page_size (ListFeatureViews, GetHistoricalFeatures): default 20, hard cap
    MAX_PAGE_SIZE = 100 (oversized requests are CLAMPED, not rejected).
  * batch size (entity_ids on GetOnline/GetHistorical; features on Write):
    hard cap MAX_BATCH_SIZE = 1000 entities/rows; OVER-cap requests are
    REJECTED with INVALID_ARGUMENT (a too-large write must fail loudly, not be
    silently truncated — truncation would drop feature rows). proto3 has no
    native numeric bound, so these caps are documented as named constants and
    enforced server-side; the constants are the contract.

STREAMING CHOICE:
  DATA-PLANE RPCs are UNARY; the long-running ADMIN replay is SERVER-STREAMING.
    - WriteFeatures: producers emit in BATCHES (one RPC = one atomic append
      with one idempotency key). Client-streaming would blur the idempotency/
      atomicity boundary (when is the append &#34;committed&#34;?). Bounded batches &#43;
      idempotency key is simpler and safer.
    - GetOnlineFeatures: a single fast key→value projection read; sub-ms
      budget on the inference hot path — streaming overhead is unwarranted.
    - GetHistoricalFeatures: bounded, PAGINATED point-in-time pulls. Truly
      huge training joins are an offline batch job (export to object storage),
      not a synchronous server stream — a deliberate later refinement.
    - RebuildViews: SERVER-STREAMING — a full log replay runs for minutes, so
      we stream RebuildViewsResponse frames for live feedback and use stream-close as the
      completion signal (no poll loop). Note this is justified by DURATION, not
      data volume — the contrast with the unary reads is the point.
  So the only streaming RPC is the admin replay, deliberately, and every choice
  is documented so it is defensible.

EVENT-SOURCING RECAP:
  * Source of truth = append-only feature_events log. Views are caches.
  * Reads never replay on the hot path; they read projections.
  * Reproducibility = GetHistoricalFeatures(as_of) over event_time.
  * Recovery = RebuildViews replays the log to rebuild Redis &#43; Postgres views.
  * Soft delete = DeleteFeatureView appends a terminal event; history is kept.
  * Exactly-once EFFECT under retries = idempotency_key on mutating RPCs.
  * Eventual consistency online = surfaced via as_of_version in reads.
  * Async contract = publishes forgepoint.events.v1 FeatureViewDefined /
    FeaturesWritten (canonical, decoupled from this API proto).
============================================================================

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| DefineFeatureView | [DefineFeatureViewRequest](#forgepoint-featurestore-v1-DefineFeatureViewRequest) | [DefineFeatureViewResponse](#forgepoint-featurestore-v1-DefineFeatureViewResponse) | DefineFeatureView creates a FeatureView or evolves its schema (append a FeatureViewDefined event; bump schema_version). Owner/team/timestamps are SERVER-assigned. Idempotent via idempotency_key. On commit, publishes forgepoint.events.v1.FeatureViewDefined on fp.features.view.defined. Requires &#34;features:write&#34;. |
| GetFeatureView | [GetFeatureViewRequest](#forgepoint-featurestore-v1-GetFeatureViewRequest) | [GetFeatureViewResponse](#forgepoint-featurestore-v1-GetFeatureViewResponse) | GetFeatureView returns a SINGLE view&#39;s current schema/metadata by id or name (a read of the catalog projection), scoped to the caller&#39;s team. Requires &#34;features:read&#34;. |
| ListFeatureViews | [ListFeatureViewsRequest](#forgepoint-featurestore-v1-ListFeatureViewsRequest) | [ListFeatureViewsResponse](#forgepoint-featurestore-v1-ListFeatureViewsResponse) | ListFeatureViews returns a paginated catalog of feature views (a projection over FeatureViewDefined events), scoped to the caller&#39;s team. page_size defaults to 20 and is CAPPED at MAX_PAGE_SIZE=100 server-side (clamped). Requires &#34;features:read&#34;. |
| WriteFeatures | [WriteFeaturesRequest](#forgepoint-featurestore-v1-WriteFeaturesRequest) | [WriteFeaturesResponse](#forgepoint-featurestore-v1-WriteFeaturesResponse) | WriteFeatures appends a batch of feature rows to the event log (the core event-sourcing write). Values are validated against the view schema; a type mismatch fails the whole batch. Batch size is capped at MAX_BATCH_SIZE=1000 rows (over-cap → INVALID_ARGUMENT). Idempotent via idempotency_key. On commit, publishes forgepoint.events.v1.FeaturesWritten on fp.features.written. Requires &#34;features:write&#34;. |
| GetOnlineFeatures | [GetOnlineFeaturesRequest](#forgepoint-featurestore-v1-GetOnlineFeaturesRequest) | [GetOnlineFeaturesResponse](#forgepoint-featurestore-v1-GetOnlineFeaturesResponse) | GetOnlineFeatures returns the LATEST feature values for entities from the low-latency online projection (the inference hot path). entity_ids batch is capped at MAX_BATCH_SIZE=1000. Eventually consistent with writes; response carries as_of_version for staleness detection. Requires &#34;features:read&#34;. |
| GetHistoricalFeatures | [GetHistoricalFeaturesRequest](#forgepoint-featurestore-v1-GetHistoricalFeaturesRequest) | [GetHistoricalFeaturesResponse](#forgepoint-featurestore-v1-GetHistoricalFeaturesResponse) | GetHistoricalFeatures returns POINT-IN-TIME (&#34;as-of&#34;) feature values from the offline projection — for each entity, the latest event with event_time &lt;= as_of. Powers reproducible training and prevents label leakage. entity_ids batch capped at MAX_BATCH_SIZE=1000; result paginated with page_size capped at MAX_PAGE_SIZE=100. Requires &#34;features:read&#34;. |
| DeleteFeatureView | [DeleteFeatureViewRequest](#forgepoint-featurestore-v1-DeleteFeatureViewRequest) | [DeleteFeatureViewResponse](#forgepoint-featurestore-v1-DeleteFeatureViewResponse) | DeleteFeatureView soft-retires a view by appending a terminal FeatureViewDeleted event (the log/history is preserved for reproducibility; the online projection is torn down). Idempotent via idempotency_key. Requires &#34;features:write&#34;. |
| RebuildViews | [RebuildViewsRequest](#forgepoint-featurestore-v1-RebuildViewsRequest) | [RebuildViewsResponse](#forgepoint-featurestore-v1-RebuildViewsResponse) stream | RebuildViews replays the append-only log to regenerate the materialized projections (Redis online &#43; Postgres offline) — the event-sourcing recovery path. SERVER-STREAMING: emits RebuildViewsResponse progress frames until done. This is an ADMIN op requiring the elevated &#34;features:admin&#34; scope (a full replay is expensive and sensitive — not reachable with a routine write token). |

 



<a name="forgepoint_inference_v1_inference-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/inference/v1/inference.proto



<a name="forgepoint-inference-v1-BatchPredictItem"></a>

### BatchPredictItem
BatchPredictItem is one row in a batch — its inputs plus a client-supplied
id so the client can correlate each result back to its request (order is
preserved, but an explicit id is robust to partial failures / reordering).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item_id | [string](#string) |  | Client-chosen correlation id for this item (e.g., a row key). Echoed back in the matching BatchPredictResult. Opaque to the gateway. |
| inputs | [BatchPredictItem.InputsEntry](#forgepoint-inference-v1-BatchPredictItem-InputsEntry) | repeated | Named input tensors for this single prediction (same shape rules as PredictRequest.inputs). |






<a name="forgepoint-inference-v1-BatchPredictItem-InputsEntry"></a>

### BatchPredictItem.InputsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TensorData](#forgepoint-inference-v1-TensorData) |  |  |






<a name="forgepoint-inference-v1-BatchPredictRequest"></a>

### BatchPredictRequest
BatchPredictRequest carries MANY independent inputs in one call.

WHY a unary batch RPC in addition to streaming: for a bounded, modest batch
(a few hundred rows) a single request/response is simpler for clients than
managing a stream, and lets the gateway fan out to the backend efficiently.
For UNBOUNDED or very large batches, use StreamPredict (server-streaming)
so results flow back incrementally without buffering everything in memory.
We offer BOTH and document when to reach for which (see service comments).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The public model name. Required. One model per batch (a batch is N rows for the SAME model, not a mix). |
| items | [BatchPredictItem](#forgepoint-inference-v1-BatchPredictItem) | repeated | The batch items. Each is one independent prediction&#39;s named inputs. HARD CAP: the gateway rejects more than 256 items server-side with INVALID_ARGUMENT, to bound memory/latency of a single unary call (this is a MAX, not a tunable default — it is part of the contract, not a hint). For larger jobs use StreamPredict (cap 10000) so results stream incrementally. |
| version_override | [string](#string) |  | OPTIONAL privileged version override applied to the WHOLE batch (debug / canary probing). Empty = weighted split, decided once per item. |
| idempotency_key | [string](#string) |  | Idempotency key for the whole batch (safe retry without double-billing). |






<a name="forgepoint-inference-v1-BatchPredictResponse"></a>

### BatchPredictResponse
BatchPredictResponse returns one result per input item.

WHY per-item status instead of failing the whole batch: in a batch, item 7
being malformed shouldn&#39;t discard the other 255 valid predictions. Each
result carries its own success/error (PARTIAL SUCCESS semantics), which is
far more useful for bulk scoring jobs than all-or-nothing.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| results | [BatchPredictResult](#forgepoint-inference-v1-BatchPredictResult) | repeated | One result per request item, correlated by item_id. Same length/order as the request items (barring explicit reordering, item_id is authoritative). |
| request_id | [string](#string) |  | Unique ID for the whole batch call. SERVER-authoritative. |






<a name="forgepoint-inference-v1-BatchPredictResult"></a>

### BatchPredictResult
BatchPredictResult is the outcome for a single batch item — either outputs
or a structured error, never silently dropped.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item_id | [string](#string) |  | The client&#39;s item_id this result corresponds to. |
| outputs | [BatchPredictResult.OutputsEntry](#forgepoint-inference-v1-BatchPredictResult-OutputsEntry) | repeated | The output tensors for this item. Empty if `error` is set. |
| served_version | [string](#string) |  | Which version served THIS item. Items in one batch can be served by different versions (weighted split is per-item). SERVER-authoritative. |
| error | [forgepoint.common.v1.ErrorDetail](#forgepoint-common-v1-ErrorDetail) |  | Per-item error if this prediction failed. Nil/unset on success. Reuses the common structured error type so clients handle it uniformly with single-predict errors. PARTIAL-SUCCESS marker for this item. |
| failure_reason | [forgepoint.events.v1.InferenceFailureReason](#forgepoint-events-v1-InferenceFailureReason) |  | The gateway&#39;s resilience classification for THIS item&#39;s failure (which pattern fired). INFERENCE_FAILURE_REASON_UNSPECIFIED on success. WHY surface it on a batch item and not on unary Predict: a unary failure rides a gRPC status code (RESOURCE_EXHAUSTED, UNAVAILABLE, ...) that already carries the class; a batch is PARTIAL-SUCCESS over a single OK envelope, so each item must carry its own machine-readable cause here for a bulk-scoring client to decide per-item retry policy (retry a TIMEOUT, never an INVALID_INPUT). WHY the events enum (not a private mirror): this is the SAME failure taxonomy the gateway publishes on events.InferenceFailed, so reusing events.InferenceFailureReason makes the sync result and the async event speak one vocabulary with no divergent-copy / mapping risk. |






<a name="forgepoint-inference-v1-BatchPredictResult-OutputsEntry"></a>

### BatchPredictResult.OutputsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TensorData](#forgepoint-inference-v1-TensorData) |  |  |






<a name="forgepoint-inference-v1-CircuitState"></a>

### CircuitState
============================================================================
CircuitState
============================================================================

WHY expose circuit state at all: the breaker is internal machinery, but
OPERATORS and dashboards need to see it (&#34;why is v3 getting no traffic?
because its breaker is OPEN&#34;). Exposing it read-only turns an invisible
failure mode into an observable one. The metric
fp_gateway_circuit_breaker_state mirrors this for Prometheus.

THE 3-STATE MACHINE:

  ┌─────────┐  failures ≥ threshold   ┌──────┐
  │ CLOSED  │ ──────────────────────► │ OPEN │
  │ (normal)│                          │(fail │
  └─────────┘ ◄──── successes ≥ ─────┐ │ fast)│
       ▲       success_threshold     │ └──────┘
       │                             │     │ after open_timeout
       │                        ┌──────────┐│
       └────── any failure ─────│ HALF_OPEN│◄ (send 1 probe)
                                └──────────┘

  CLOSED: requests flow; count failures. Too many → OPEN.
  OPEN: reject immediately (fail fast) for open_timeout; don&#39;t hammer a
        sick backend. After the timeout → HALF_OPEN.
  HALF_OPEN: allow a few probe requests. If they succeed → CLOSED (recovered);
        if any fails → back to OPEN (still sick).
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model &#43; version (backend) this breaker guards. |
| version | [string](#string) |  |  |
| state | [CircuitBreakerState](#forgepoint-inference-v1-CircuitBreakerState) |  | Current breaker state. |
| consecutive_failures | [int32](#int32) |  | Consecutive failures observed in the current window (drives the trip decision). SERVER-authoritative observability field. |
| last_transition_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the breaker last changed state. With state=OPEN this anchors the open_timeout countdown to HALF_OPEN. SERVER-authoritative. |






<a name="forgepoint-inference-v1-DeleteRouteRequest"></a>

### DeleteRouteRequest
DeleteRouteRequest removes a model from the routing table entirely. After
this, predict calls for the model fail with NOT_FOUND. Normally driven by a
ModelUndeployed event; the RPC is the manual override.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model to remove from routing. |
| idempotency_key | [string](#string) |  | Idempotency key — deleting an already-deleted route is a safe no-op. |






<a name="forgepoint-inference-v1-DeleteRouteResponse"></a>

### DeleteRouteResponse
DeleteRouteResponse is intentionally empty. WHY a named empty message vs
google.protobuf.Empty: Buf RPC_RESPONSE_STANDARD_NAME requires &#34;&lt;Rpc&gt;Response&#34;
naming, and a named type is forward-compatible (we could later add e.g.
drained_request_count without changing the RPC signature).






<a name="forgepoint-inference-v1-GetCircuitStateRequest"></a>

### GetCircuitStateRequest
GetCircuitStateRequest inspects one backend&#39;s breaker. WHY model&#43;version:
breakers are PER BACKEND (per version), not per model — v2 can be tripped
while v1 stays healthy, which is exactly what protects you during a bad
canary.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  |  |
| version | [string](#string) |  |  |






<a name="forgepoint-inference-v1-GetCircuitStateResponse"></a>

### GetCircuitStateResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| circuit_state | [CircuitState](#forgepoint-inference-v1-CircuitState) |  | The current breaker state for the requested backend. |






<a name="forgepoint-inference-v1-GetModelInfoRequest"></a>

### GetModelInfoRequest
GetModelInfoRequest asks for one model&#39;s public contract.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The public model name to describe. Required. (No version/api_key/team here: the version set is what the gateway routes; the principal is from the token.) |






<a name="forgepoint-inference-v1-GetModelInfoResponse"></a>

### GetModelInfoResponse
GetModelInfoResponse is the caller-facing description of a model.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_info | [ModelInfo](#forgepoint-inference-v1-ModelInfo) |  | The model contract (schema &#43; routable versions &#43; serving status). |






<a name="forgepoint-inference-v1-GetRouteRequest"></a>

### GetRouteRequest
GetRouteRequest fetches the current routing-table entry for one model.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model whose route to fetch. |






<a name="forgepoint-inference-v1-GetRouteResponse"></a>

### GetRouteResponse
GetRouteResponse wraps the Route. WHY wrap (not return Route directly): Buf
RPC_RESPONSE_STANDARD_NAME, plus forward-compat (we can later add e.g. a
resolved_at or source field without touching the shared Route domain type).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| route | [Route](#forgepoint-inference-v1-Route) |  | The current route, including all targets and their weights. |






<a name="forgepoint-inference-v1-ListCircuitStatesRequest"></a>

### ListCircuitStatesRequest
ListCircuitStatesRequest lists breaker states across backends, for
dashboards/operators. Optional model filter; paginated.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name_filter | [string](#string) |  | Optional: only breakers for this model. Empty = all backends. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination (page_size default 20, max 100 server-side). |






<a name="forgepoint-inference-v1-ListCircuitStatesResponse"></a>

### ListCircuitStatesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| circuit_states | [CircuitState](#forgepoint-inference-v1-CircuitState) | repeated | The page of breaker states. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata. |






<a name="forgepoint-inference-v1-ListRoutesRequest"></a>

### ListRoutesRequest
ListRoutesRequest lists all routing-table entries (the whole routing table),
paginated. Reuses common pagination for table-wide consistency.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20, capped at 100 server-side (see common.proto). Prevents a client from pulling the entire routing table in one unbounded response. |






<a name="forgepoint-inference-v1-ListRoutesResponse"></a>

### ListRoutesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| routes | [Route](#forgepoint-inference-v1-Route) | repeated | The page of routes. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata: next_page_token &#43; total_count. |






<a name="forgepoint-inference-v1-ModelInfo"></a>

### ModelInfo
============================================================================
ModelInfo
============================================================================

The CALLER VIEW of a model: enough to build a valid request and understand
what will answer it, and NOTHING server-internal. Contrast with Route (the
operator view) which carries endpoints/status. Populated by the gateway from
the routing table it built off ModelDeployed/ModelPromoted events plus the
input/output schema it learned from the serving backend&#39;s signature.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The public model name clients call. |
| is_serving | [bool](#bool) |  | Whether the model is currently routable (has at least one ACTIVE target). False = predicts will fail FAILURE_REASON_NO_ROUTE. Lets a UI grey it out. |
| inputs | [TensorSpec](#forgepoint-inference-v1-TensorSpec) | repeated | The model&#39;s input tensor signature (names/dtypes/shapes) for client-side request validation at the edge. |
| outputs | [TensorSpec](#forgepoint-inference-v1-TensorSpec) | repeated | The model&#39;s output tensor signature, so a client knows what keys/shapes to expect back in PredictResponse.outputs. |
| versions | [VersionInfo](#forgepoint-inference-v1-VersionInfo) | repeated | The currently routable versions and their traffic weights (caller-safe projection — no endpoints). Mirrors the live split a Predict would use. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the routing entry backing this info last changed (deploy/promote event or admin write). SERVER-authoritative; lets a client reason about staleness. |






<a name="forgepoint-inference-v1-PredictRequest"></a>

### PredictRequest
PredictRequest is the hot-path message: one inference call.

WHY the client may NOT pick the served version freely: if a client could
pin every request to &#34;v-stable&#34;, canary versions would never receive the
traffic they need to be evaluated — defeating the whole traffic-split
pattern. So we model TWO knobs with different trust levels:
  - model_name (required): WHICH model. Client-chosen, always honored.
  - version_override (optional): pin a specific version. This is a
    PRIVILEGED escape hatch (debugging a specific version, the canary
    executor probing the new version). The gateway authorizes it; ordinary
    traffic leaves it empty and is split by weight. Documented as override
    so it&#39;s obvious it bypasses canary splitting.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The public model name to predict with (e.g., &#34;fraud-detector&#34;). Required. |
| inputs | [PredictRequest.InputsEntry](#forgepoint-inference-v1-PredictRequest-InputsEntry) | repeated | The named input tensors, keyed by the model&#39;s input names (e.g., {&#34;features&#34;: TensorData{...}}). The gateway validates names/shapes against the model&#39;s declared input schema before forwarding. |
| version_override | [string](#string) |  | OPTIONAL privileged override: force a specific version, bypassing weighted traffic splitting. Empty = let the gateway split by weight (the norm). Requires elevated permission; used by the canary executor and for debug. |
| idempotency_key | [string](#string) |  | Idempotency key for safe retries. WHY on a PREDICT (a read-ish op)? Predictions can have side effects downstream — every call emits an InferenceCompleted event that Billing meters. If a client retries after a timeout, a duplicate key lets the gateway recognize the retry and avoid DOUBLE-BILLING / double-emitting. The gateway caches recent keys (short TTL in Redis). Empty = no idempotency guarantee (gateway treats as unique). |






<a name="forgepoint-inference-v1-PredictRequest-InputsEntry"></a>

### PredictRequest.InputsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TensorData](#forgepoint-inference-v1-TensorData) |  |  |






<a name="forgepoint-inference-v1-PredictResponse"></a>

### PredictResponse
PredictResponse returns the model outputs plus the metadata that makes the
gateway&#39;s decisions OBSERVABLE to the caller.

WHY echo served_version: the client asked for a model, not a version, but it
MUST be told which version actually answered — for A/B analysis (&#34;which
variant produced this?&#34;), for debugging, and so the client can correlate
with the InferenceCompleted event. This is SERVER-authoritative: it reflects
what the gateway chose, never what the client requested.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| outputs | [PredictResponse.OutputsEntry](#forgepoint-inference-v1-PredictResponse-OutputsEntry) | repeated | The named output tensors, keyed by the model&#39;s output names (e.g., {&#34;probabilities&#34;: TensorData{...}}). |
| served_version | [string](#string) |  | Which model version actually served this request. SERVER-authoritative — the result of weighted traffic splitting (or version_override). |
| latency | [google.protobuf.Duration](#google-protobuf-Duration) |  | End-to-end latency the gateway observed for the backend call. WHY Duration not int64 ms: google.protobuf.Duration is the typed well-known unit, has sub-ms precision, and avoids &#34;is this ms or µs?&#34; ambiguity. The event&#39;s latency_ms is a denormalized convenience for metering/dashboards. |
| request_id | [string](#string) |  | Unique ID the gateway assigned to this request (UUID). Returned to the client AND carried on the InferenceCompleted event, so the client can join its logs to platform events/traces. SERVER-authoritative. |






<a name="forgepoint-inference-v1-PredictResponse-OutputsEntry"></a>

### PredictResponse.OutputsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TensorData](#forgepoint-inference-v1-TensorData) |  |  |






<a name="forgepoint-inference-v1-Route"></a>

### Route
============================================================================
Route
============================================================================

WHY a Route per model (not per version): clients call by MODEL NAME (e.g.,
&#34;fraud-detector&#34;); the gateway hides which versions exist behind it. A Route
is the routing-table row for one model — the list of candidate versions and
how traffic splits among them. This indirection is the whole point of a
gateway: the client&#39;s contract (&#34;predict with fraud-detector&#34;) is stable
while we shift traffic between v1/v2/v3 underneath.

HOW IT&#39;S POPULATED: the routing table is driven by EVENTS, not API writes in
the common case. The gateway consumes fp.pipelines.model.deployed (the saga
promotes a version) and adds/updates targets. The admin RPCs (UpsertRoute /
SetTrafficSplit) are the manual override / break-glass control plane for
operators and the canary executor.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The public model name clients call (e.g., &#34;fraud-detector&#34;). Unique key of the routing table. |
| targets | [RouteTarget](#forgepoint-inference-v1-RouteTarget) | repeated | The candidate versions and their traffic weights. weight_bps across all ACTIVE targets must sum to 10000. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this route was last changed (deploy event or admin write). SERVER-authoritative timestamp — never accepted on write. |






<a name="forgepoint-inference-v1-RouteTarget"></a>

### RouteTarget
============================================================================
RouteTarget
============================================================================

WHY weights in BASIS POINTS (per-10,000) not percent: integer basis points
let us express fine-grained canaries (e.g., 0.5% = 50 bps) exactly, with no
float rounding when the weights must sum to a fixed total. This is the same
reason finance uses bps. The set of a model&#39;s RouteTargets must have
weight_bps summing to 10000 (100%); the gateway enforces this on write.

WHY status here: a target can exist but be temporarily DRAINING (taking no
new traffic while in-flight requests finish) during a rollback. Status is
SERVER-authoritative — clients propose weights, the gateway owns lifecycle.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  | The model version this target points to (e.g., &#34;v3&#34;, &#34;2024-06-01-a&#34;). Combined with the parent Route&#39;s model_name, this identifies a backend. |
| endpoint | [string](#string) |  | The model-serving endpoint for this version (e.g., a K8s Service DNS name like &#34;iris-v3.fp-models.svc:9090&#34;). SERVER-authoritative: it is resolved from the ModelDeployed event the gateway consumed, NOT supplied by API clients — accepting an arbitrary endpoint from a client would be an SSRF hole (the gateway would forward to anywhere). Read-only in responses. |
| weight_bps | [int32](#int32) |  | Traffic share for this version in basis points (0–10000). All targets of a model must sum to 10000. E.g., stable=9000 (90%), canary=1000 (10%). |
| status | [TargetStatus](#forgepoint-inference-v1-TargetStatus) |  | Lifecycle status of this target. SERVER-authoritative. |






<a name="forgepoint-inference-v1-SetTrafficSplitRequest"></a>

### SetTrafficSplitRequest
SetTrafficSplitRequest adjusts ONLY the weights of an existing route — the
canary &#34;dial&#34;. WHY a dedicated RPC separate from UpsertRoute: shifting
traffic (90/10 → 50/50 → 0/100) is the single most common, highest-stakes
operation in a rollout, and it&#39;s done repeatedly by the canary executor as
metrics pass thresholds. A narrow, purpose-built RPC is safer (you can&#39;t
accidentally drop a target) and clearer in audit logs (&#34;traffic shifted&#34;)
than a full route replace.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model whose traffic to re-weight. |
| weights | [TrafficWeight](#forgepoint-inference-v1-TrafficWeight) | repeated | New weights per version. Each entry must reference a version already present in the route. weight_bps across all referenced ACTIVE targets must sum to 10000. The gateway rejects unknown versions and bad sums. |
| idempotency_key | [string](#string) |  | Idempotency key — re-applying the same split (retry) is a no-op, not a double shift. |






<a name="forgepoint-inference-v1-SetTrafficSplitResponse"></a>

### SetTrafficSplitResponse
SetTrafficSplitResponse returns the updated route so the caller confirms the
new weights took effect exactly as intended.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| route | [Route](#forgepoint-inference-v1-Route) |  |  |






<a name="forgepoint-inference-v1-StreamPredictRequest"></a>

### StreamPredictRequest
StreamPredictRequest is a single request that yields a STREAM of results.

WHY SERVER-STREAMING (1 request → N responses) and not bidi here: the
client knows all its inputs up front (it&#39;s a batch job), so it sends them
once; the gateway streams results back AS each prediction completes. This
gives incremental delivery (client starts processing result 1 while result
500 is still computing) and bounded gateway memory (no need to hold the
whole response set). Use this for LARGE batches; use unary BatchPredict for
small ones.

WHEN BIDI WOULD BE RIGHT (and why we DON&#39;T use it): bidirectional streaming
fits an OPEN-ENDED feed where the client keeps sending inputs over a long-
lived connection (e.g., a sensor stream). Forgepoint&#39;s online predict is
request/response and its batch is &#34;submit N, get N back&#34; — neither needs
the client to keep pushing after the initial request. We deliberately avoid
bidi&#39;s added complexity (flow control on both directions, harder retries).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The public model name. Required. |
| items | [BatchPredictItem](#forgepoint-inference-v1-BatchPredictItem) | repeated | All batch items to score. The gateway streams a StreamPredictResponse per item as each completes. WHY there is STILL a cap even though responses stream: the REQUEST is unary — the whole `items` list must be received and held before the first result streams back, so an unbounded list is a memory DoS on the request side. The gateway caps this at 10000 items server-side (10x the unary BatchPredict cap; see service docs) and rejects above it with INVALID_ARGUMENT. The bulkhead independently bounds in-flight concurrency to the backend. For truly unbounded feeds, submit multiple stream calls. |
| version_override | [string](#string) |  | OPTIONAL privileged version override for the whole stream. |
| idempotency_key | [string](#string) |  | Idempotency key for the streamed batch. |






<a name="forgepoint-inference-v1-StreamPredictResponse"></a>

### StreamPredictResponse
StreamPredictResponse is ONE message in the response stream — the result for
a single item, delivered as soon as it&#39;s ready.

RPC_RESPONSE_STANDARD_NAME note: a server-streaming RPC&#39;s response type must
still be named &#34;&lt;Rpc&gt;Response&#34;; the streaming is expressed by `stream` on
the RPC, not by the type name. Each streamed message is one of these.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [BatchPredictResult](#forgepoint-inference-v1-BatchPredictResult) |  | The result for one item (outputs or per-item error), correlated by item_id. Reuses BatchPredictResult so streamed and batched results share one shape — clients write one result handler for both paths. |
| request_id | [string](#string) |  | Unique ID of the overall stream call, repeated on each message so a client can attribute every streamed result to the same call. SERVER-authoritative. |






<a name="forgepoint-inference-v1-TensorData"></a>

### TensorData
============================================================================
TensorData
============================================================================

WHY raw bytes &#43; shape &#43; dtype instead of repeated float / repeated double:
  - PERFORMANCE: a 1000-feature vector as `repeated float` is 1000 protobuf
    varint-tagged fields; as packed bytes it&#39;s a single length-delimited
    blob the runtime can memcpy straight into a tensor. This is how every
    serious inference protocol (KServe v2, TF Serving, Triton) does it.
  - ZERO-COPY: the gateway forwards the bytes to model-serving without
    deserializing the numbers it doesn&#39;t need to inspect — it only reads
    shape/dtype for validation. Cheaper gateway = lower added latency.
  - DTYPE FLEXIBILITY: one message type carries float/int/string tensors;
    no separate FloatTensor / IntTensor messages.

TRADEOFF: bytes are opaque to JSON/grpcurl — you can&#39;t eyeball the numbers.
For the external HTTP API the gateway accepts/returns JSON arrays and
converts; this binary form is the efficient internal/gRPC representation.

WHY NOT repeated float:
wire size &#43; zero-copy forwarding. shape carries dimensions (e.g., [1, 4]
for one iris sample of 4 features); data length must equal product(shape) *
sizeof(dtype), which the gateway validates.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shape | [int64](#int64) | repeated | Tensor dimensions, outermost first. E.g., [1, 4] = batch of 1, 4 features. An empty shape denotes a scalar. The gateway validates that len(data) == product(shape) * elem_size(dtype). |
| dtype | [DataType](#forgepoint-inference-v1-DataType) |  | Element type of the bytes in `data`. Required (non-UNSPECIFIED). |
| data | [bytes](#bytes) |  | The raw tensor contents, densely packed in row-major (C) order for the given dtype. Opaque to the gateway except for length validation. |






<a name="forgepoint-inference-v1-TensorSpec"></a>

### TensorSpec
============================================================================
TensorSpec
============================================================================

WHY a SPEC (no bytes) distinct from TensorData (carries bytes): GetModelInfo
publishes the SHAPE of the contract — &#34;input &#39;features&#39; is FLOAT32 [1,4]&#34; —
so a client (or the HTTP edge) can validate a request locally before sending
it. A -1 in a dimension means &#34;dynamic&#34; (e.g., a variable batch axis), the
usual convention in ONNX/Triton model signatures.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The tensor&#39;s name in the model signature (e.g., &#34;features&#34;). The key a client uses in PredictRequest.inputs / reads from PredictResponse.outputs. |
| dtype | [DataType](#forgepoint-inference-v1-DataType) |  | Element type the model expects/produces for this tensor. |
| shape | [int64](#int64) | repeated | Declared dimensions, outermost first; -1 marks a dynamic axis (e.g., a variable batch size). Empty = scalar. |






<a name="forgepoint-inference-v1-TrafficWeight"></a>

### TrafficWeight
TrafficWeight is a (version, weight) pair — the minimal payload for the
traffic dial. Deliberately NOT RouteTarget: SetTrafficSplit must not let a
caller smuggle in endpoint/status changes, so it accepts only the two
fields it is allowed to change.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  | An existing version in the route. |
| weight_bps | [int32](#int32) |  | New traffic share in basis points (0–10000). Setting 0 effectively drains a version of new traffic without removing it from the route. |






<a name="forgepoint-inference-v1-UpsertRouteRequest"></a>

### UpsertRouteRequest
UpsertRouteRequest is the BREAK-GLASS manual control to create/replace a
route. In normal operation routes are driven by ModelDeployed events; this
RPC is for operators and the canary executor to override.

SECURITY — what is and isn&#39;t accepted (anti mass-assignment):
  - The caller supplies model_name and the desired targets&#39; version &#43;
    weight_bps ONLY.
  - endpoint is NOT honored from the request — the gateway resolves each
    version&#39;s endpoint from its deploy record. Accepting a client endpoint
    would let a caller make the gateway forward to an arbitrary host (SSRF).
  - status and updated_at are SERVER-authoritative and ignored on input.
  - The gateway validates that ACTIVE targets&#39; weight_bps sum to 10000.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model whose route to create or replace. |
| targets | [RouteTarget](#forgepoint-inference-v1-RouteTarget) | repeated | Desired targets. Only `version` and `weight_bps` are read from each; `endpoint`, `status` are server-resolved/ignored (see message doc). |
| idempotency_key | [string](#string) |  | Idempotency key — UpsertRoute is a mutation; a retried create/replace with the same key must not produce a divergent table or duplicate audit events. |






<a name="forgepoint-inference-v1-UpsertRouteResponse"></a>

### UpsertRouteResponse
UpsertRouteResponse returns the route AS STORED (with server-resolved
endpoints, status, and updated_at), so the caller sees the authoritative
result of its write, not just an echo of its input.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| route | [Route](#forgepoint-inference-v1-Route) |  |  |






<a name="forgepoint-inference-v1-VersionInfo"></a>

### VersionInfo
============================================================================
VersionInfo
============================================================================

A caller-safe projection of one routable version: its label and current
traffic share. Deliberately OMITS the backend endpoint and breaker internals
(those are operator-only, exposed via GetRoute/GetCircuitState) — a public
caller has no business seeing the serving pod&#39;s address.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  | The version label (e.g., &#34;v3&#34;). What shows up as served_version. |
| weight_bps | [int32](#int32) |  | This version&#39;s current traffic share in basis points (0–10000). Lets a UI show &#34;v3 is taking 10% canary traffic&#34; without operator scope. |
| is_stable | [bool](#bool) |  | True if this version is the current stable (non-canary) target. Lets a caller/UI distinguish the canary from the stable variant. |





 


<a name="forgepoint-inference-v1-CircuitBreakerState"></a>

### CircuitBreakerState
============================================================================
CircuitBreakerState
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| CIRCUIT_BREAKER_STATE_UNSPECIFIED | 0 | Required zero value. |
| CIRCUIT_BREAKER_STATE_CLOSED | 1 | Normal operation: requests pass through, failures are counted. |
| CIRCUIT_BREAKER_STATE_OPEN | 2 | Tripped: requests fail fast without hitting the backend. |
| CIRCUIT_BREAKER_STATE_HALF_OPEN | 3 | Probing: a limited number of trial requests determine recovery. |



<a name="forgepoint-inference-v1-DataType"></a>

### DataType
============================================================================
DataType
============================================================================

WHY an explicit dtype enum: a model input is just bytes on the wire; the
receiver must know how to interpret those bytes (4-byte float vs 8-byte
float vs int). Carrying the dtype with the tensor makes the payload
self-describing — the gateway can validate against the model&#39;s declared
input schema before forwarding, catching client mistakes (sending int32
where float32 is expected) at the edge with a clear INVALID_ARGUMENT
instead of a cryptic failure deep in the ONNX runtime.

WHY only a small numeric set: Forgepoint serves tiny CPU ONNX models
(tabular/iris-class). float32 covers ~all feature vectors; we add a few
common types for completeness. STRING is included for categorical/text
inputs that some models embed. Adding more later is backward compatible
(appending enum values never breaks the wire format).
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| DATA_TYPE_UNSPECIFIED | 0 | Required zero value (Buf ENUM_ZERO_VALUE_SUFFIX). Treat as &#34;unset&#34; — the gateway rejects tensors with an unspecified dtype. |
| DATA_TYPE_FLOAT32 | 1 | IEEE-754 32-bit float. The default for ML feature vectors. |
| DATA_TYPE_FLOAT64 | 2 | IEEE-754 64-bit float. For models that need double precision. |
| DATA_TYPE_INT32 | 3 | Signed 32-bit integer (e.g., categorical indices, token IDs). |
| DATA_TYPE_INT64 | 4 | Signed 64-bit integer. |
| DATA_TYPE_STRING | 5 | UTF-8 strings (categorical/text features). Encoded length-prefixed in the bytes payload; the runtime decodes per the model&#39;s string handling. |
| DATA_TYPE_BOOL | 6 | Boolean (1 byte per element). For binary flags. |



<a name="forgepoint-inference-v1-TargetStatus"></a>

### TargetStatus
============================================================================
TargetStatus
============================================================================

WHY a status enum vs a bare bool &#34;active&#34;: a canary rollout has more than
two states. DRAINING (let in-flight finish, send no new traffic) is what
makes a graceful rollback possible — flipping straight to removed would
kill in-flight predictions. This mirrors Envoy/K8s endpoint draining.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| TARGET_STATUS_UNSPECIFIED | 0 | Required zero value. |
| TARGET_STATUS_ACTIVE | 1 | Receiving traffic per its weight_bps. |
| TARGET_STATUS_DRAINING | 2 | No new traffic; in-flight requests allowed to complete. Used during rollback/promotion before full removal. Effective weight is treated as 0. |
| TARGET_STATUS_UNHEALTHY | 3 | Quarantined by the circuit breaker (backend unhealthy). The gateway sets this automatically; it is not a client-settable state. |


 

 


<a name="forgepoint-inference-v1-InferenceGatewayService"></a>

### InferenceGatewayService
============================================================================
INFERENCE GATEWAY SERVICE
============================================================================

WHY one service with both a hot DATA plane (Predict*) and a CONTROL plane
(route/breaker admin): they&#39;re cohesive — the control plane configures
exactly what the data plane enforces, and both live in the same process that
holds the in-memory routing table and breaker state. Splitting them would
force the data plane to fetch routing config over the network on the hot
path (latency) or duplicate it. Envoy follows the same shape: one proxy
process, an xDS control surface plus the data path.

RPC CATEGORIES:
  DATA PLANE (hot path):   Predict, BatchPredict, StreamPredict
  MODEL METADATA (read):   GetModelInfo
  ROUTING CONTROL PLANE:   GetRoute, ListRoutes, UpsertRoute,
                           SetTrafficSplit, DeleteRoute
  RESILIENCE OBSERVABILITY: GetCircuitState, ListCircuitStates

NOTE ON THE EXTERNAL HTTP API: the platform exposes a thin JSON edge that
maps 1:1 onto these RPCs (gRPC is the canonical internal contract; the HTTP
adapter converts TensorData bytes ↔ JSON arrays):
  POST /v1/models/{model_name}/predict                      → Predict
  POST /v1/models/{model_name}/versions/{version}/predict   → Predict (the
       {version} path segment becomes version_override, which the edge only
       forwards for callers holding the elevated scope; otherwise 403).
  GET  /v1/models/{model_name}/info                         → GetModelInfo

AUTH &amp; TENANCY (where the trust boundary is): every RPC runs behind the shared
auth interceptor (ValidateToken &#43; CheckPermission). The caller&#39;s api_key_id,
team, and scopes are taken FROM THE VERIFIED TOKEN — never from request
fields. That is why no *Request here carries an api_key/team/owner: a
client-supplied principal would be account-takeover-for-billing (the inference
event meters against api_key_id). Data-plane calls require an inference scope;
control-plane RPCs require an elevated deploy/admin scope (only operators and
the canary executor reshape routes); version_override on Predict requires that
same elevation. The pre-flight quota check keys off the token&#39;s team against
the Redis cache flipped by events.QuotaExceeded — also never a request field.
============================================================================

----- DATA PLANE -------------------------------------------------------

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Predict | [PredictRequest](#forgepoint-inference-v1-PredictRequest) | [PredictResponse](#forgepoint-inference-v1-PredictResponse) | Predict routes a single inference request to a model-serving backend via the full resilience stack (rate limit → route → traffic split → circuit breaker → bulkhead → forward with retry) and returns the outputs plus the version that served and the latency observed. Emits InferenceCompleted on success / InferenceFailed on failure (async, off the response path). |
| BatchPredict | [BatchPredictRequest](#forgepoint-inference-v1-BatchPredictRequest) | [BatchPredictResponse](#forgepoint-inference-v1-BatchPredictResponse) | BatchPredict scores many inputs for one model in a single request, returning one result per item with PARTIAL-SUCCESS semantics (a bad item doesn&#39;t fail the batch). Use for BOUNDED batches (capped at 256 items server-side); for large/unbounded batches use StreamPredict. |
| StreamPredict | [StreamPredictRequest](#forgepoint-inference-v1-StreamPredictRequest) | [StreamPredictResponse](#forgepoint-inference-v1-StreamPredictResponse) stream | StreamPredict scores a (potentially large) batch and SERVER-STREAMS one result per item as each completes — incremental delivery with bounded gateway RESPONSE memory (it never buffers the whole result set). The single request carries all items (capped at 10000 server-side — the request list is still buffered in full, so it is bounded too); results flow back until the stream ends. Choose this over BatchPredict when the batch is large or you want to start consuming results before the batch finishes. |
| GetModelInfo | [GetModelInfoRequest](#forgepoint-inference-v1-GetModelInfoRequest) | [GetModelInfoResponse](#forgepoint-inference-v1-GetModelInfoResponse) | GetModelInfo returns the client-facing metadata a caller needs to construct a valid predict request WITHOUT knowing internal versions: the input/output tensor schema (names/shapes/dtypes for edge validation), the currently routable versions and their traffic weights, and whether the model is serving at all. Backs the external GET /v1/models/{model_name}/info. WHY a dedicated read RPC and not &#34;just call GetRoute&#34;: GetRoute is the OPERATOR view (raw routing table, elevated scope); GetModelInfo is the CALLER view (the public contract of the model, inference scope) and deliberately omits server-internal fields like backend endpoints and breaker internals. |
| GetRoute | [GetRouteRequest](#forgepoint-inference-v1-GetRouteRequest) | [GetRouteResponse](#forgepoint-inference-v1-GetRouteResponse) | GetRoute returns the current routing-table entry (versions &#43; weights) for one model. Read-only; used by operators, the UI, and the canary executor to inspect current splits. |
| ListRoutes | [ListRoutesRequest](#forgepoint-inference-v1-ListRoutesRequest) | [ListRoutesResponse](#forgepoint-inference-v1-ListRoutesResponse) | ListRoutes returns the whole routing table, paginated (page_size capped at 100 server-side). For the operator dashboard / `fp` CLI. |
| UpsertRoute | [UpsertRouteRequest](#forgepoint-inference-v1-UpsertRouteRequest) | [UpsertRouteResponse](#forgepoint-inference-v1-UpsertRouteResponse) | UpsertRoute creates or replaces a model&#39;s route (break-glass manual control; routes are normally driven by ModelDeployed events). Endpoints are server-resolved (never accepted from the client) and weights must sum to 10000. Requires elevated permission. Idempotent via idempotency_key. |
| SetTrafficSplit | [SetTrafficSplitRequest](#forgepoint-inference-v1-SetTrafficSplitRequest) | [SetTrafficSplitResponse](#forgepoint-inference-v1-SetTrafficSplitResponse) | SetTrafficSplit adjusts ONLY the version weights of an existing route — the canary dial (e.g., 90/10 → 50/50 → 0/100). The most common rollout operation; narrow and audit-friendly by design. Requires elevated permission. Idempotent. |
| DeleteRoute | [DeleteRouteRequest](#forgepoint-inference-v1-DeleteRouteRequest) | [DeleteRouteResponse](#forgepoint-inference-v1-DeleteRouteResponse) | DeleteRoute removes a model from routing entirely (subsequent predicts get NOT_FOUND). Normally driven by ModelUndeployed; this is the override. Requires elevated permission. Idempotent (deleting twice is a no-op). |
| GetCircuitState | [GetCircuitStateRequest](#forgepoint-inference-v1-GetCircuitStateRequest) | [GetCircuitStateResponse](#forgepoint-inference-v1-GetCircuitStateResponse) | GetCircuitState reports the breaker state for one backend (model&#43;version): CLOSED / OPEN / HALF_OPEN plus failure count and last transition. Turns an invisible failure mode into an observable one for dashboards/operators. |
| ListCircuitStates | [ListCircuitStatesRequest](#forgepoint-inference-v1-ListCircuitStatesRequest) | [ListCircuitStatesResponse](#forgepoint-inference-v1-ListCircuitStatesResponse) | ListCircuitStates lists breaker states across backends (optional model filter), paginated — the at-a-glance health view of all routed backends. |

 



<a name="forgepoint_monitor_v1_monitor-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/monitor/v1/monitor.proto



<a name="forgepoint-monitor-v1-ConfigureMonitorRequest"></a>

### ConfigureMonitorRequest
ConfigureMonitorRequest creates or updates a model&#39;s monitor (upsert on
model_name). This is the ONLY write path for monitor config.

SECURITY — MASS-ASSIGNMENT GUARD: this request deliberately accepts ONLY the
operator-owned fields. It does NOT accept id, owner_team, state,
baseline_version, baseline_captured_at, created_at, or updated_at — all of
those are SERVER-authoritative on the Monitor (see Monitor&#39;s security note).
  - owner_team is derived from the caller&#39;s AUTH CLAIMS, never a request field
    (a client that could name its own team could create a monitor against
    another tenant&#39;s model — cross-tenant write).
  - Accepting id/state/baseline_* would let a client point a monitor at a
    hand-picked baseline or fake its state to force/suppress retrains.
We model the writable surface explicitly as scalar fields rather than embedding
a Monitor, precisely so unwritable fields cannot even be expressed on the wire.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model to monitor. Identifies the monitor for upsert (one monitor per model name, WITHIN the caller&#39;s team). Required. The server additionally verifies the caller&#39;s team owns this model (via Registry) before creating — model_name here is the WHICH, not the authorization input. |
| window_duration | [google.protobuf.Duration](#google-protobuf-Duration) |  | Window shape (see Monitor.window_duration / window_size / min_samples). The same server-side BOUNDS apply as on the Monitor fields: window_size in [0, 1_000_000]; 0 &lt;= min_samples &lt;= window_size (when window_size&gt;0); window_duration must be positive when set. Out-of-range → INVALID_ARGUMENT. |
| window_size | [int32](#int32) |  |  |
| min_samples | [int32](#int32) |  |  |
| thresholds | [ThresholdConfig](#forgepoint-monitor-v1-ThresholdConfig) | repeated | Per-drift-type thresholds to apply. Replaces the existing set on update (PUT-style, not PATCH-style) — simpler to reason about than field-merging, and a full set is what an operator edits in the UI anyway. |
| auto_retrain | [bool](#bool) |  | The closed-loop switch. When true, retrain_pipeline_id must be set. |
| retrain_pipeline_id | [string](#string) |  | Pipeline to trigger on auto-retrain (opaque Orchestrator pipeline id). |
| enabled | [bool](#bool) |  | Pause/resume without losing config: false moves the monitor to PAUSED (events still windowed, nothing scored/fired); true resumes scoring. Distinct from auto_retrain — you can be enabled&#43;alert-only, or paused. |
| idempotency_key | [string](#string) |  | Idempotency key for safe retries of this mutation. WHY it matters even for an upsert: a retried ConfigureMonitor after a network blip must not bump updated_at twice or race two concurrent edits. The server records the key (short TTL) and returns the first result for a duplicate. Empty = treated as a unique request. Same discipline as every mutating RPC in the platform. |






<a name="forgepoint-monitor-v1-ConfigureMonitorResponse"></a>

### ConfigureMonitorResponse
ConfigureMonitorResponse returns the resulting monitor config.
WHY wrap (not return Monitor directly): Buf RPC_RESPONSE_STANDARD_NAME, and a
wrapper lets us later add fields (e.g., a `created` bool to distinguish insert
from update) without touching the shared Monitor domain message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| monitor | [Monitor](#forgepoint-monitor-v1-Monitor) |  | The monitor after the upsert. Includes SERVER-authoritative fields (id, state, baseline_*) the request could not set. |
| created | [bool](#bool) |  | True if this call created a new monitor, false if it updated an existing one. Cheap signal for the UI/CLI to print &#34;created&#34; vs &#34;updated&#34;. |






<a name="forgepoint-monitor-v1-DeleteMonitorRequest"></a>

### DeleteMonitorRequest
DeleteMonitorRequest tears down a model&#39;s monitor. WHY this RPC is needed (the
design/CLI imply lifecycle management of monitors): when a model is retired you
must be able to STOP monitoring it — otherwise the consumer keeps windowing a
dead model&#39;s (absent) traffic and the fleet view shows zombie rows. This is a
soft-delete by default (drift history is retained for audit; see purge flag).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model whose monitor to delete. Required. Team ownership enforced by auth. |
| purge_reports | [bool](#bool) |  | If true, ALSO purge this monitor&#39;s persisted drift reports (a hard delete for GDPR/right-to-erasure or test cleanup). Default false = soft-delete the config but KEEP the drift_reports history for audit (&#34;why did it retrain last month?&#34;). |
| idempotency_key | [string](#string) |  | Idempotency key — a retried delete after a network blip must be a safe no-op (deleting an already-deleted monitor returns success, not NOT_FOUND-on-retry). The server records the key (short TTL). Empty = treated as unique. |






<a name="forgepoint-monitor-v1-DeleteMonitorResponse"></a>

### DeleteMonitorResponse
DeleteMonitorResponse is a named-empty ack (Buf forbids google.protobuf.Empty
and reusing a domain message). WHY return how many reports were purged: a
hard-delete caller wants confirmation of the erasure scope for its own audit.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| purged_report_count | [int32](#int32) |  | Number of drift reports purged (0 when purge_reports=false, i.e. soft delete). |






<a name="forgepoint-monitor-v1-DriftMetric"></a>

### DriftMetric
============================================================================
DriftMetric
============================================================================

WHY a per-feature/per-output metric (not just one number per report): drift
is rarely uniform — usually ONE feature moved (a sensor recalibrated, an
upstream join changed units) while the rest are stable. Reporting the score
per feature is what makes a drift report ACTIONABLE: &#34;income_drift PSI=0.41,
everything else &lt;0.05&#34; tells an engineer exactly where to look. Aggregating to
a single model-level number would throw that away.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The thing whose distribution was measured: a feature name for data drift (e.g., &#34;income&#34;), an output name/class for prediction drift (e.g., &#34;fraud&#34;), or a metric name for performance decay (e.g., &#34;accuracy&#34;, &#34;f1&#34;). |
| method | [DriftMethod](#forgepoint-monitor-v1-DriftMethod) |  | The statistical method used to produce `score` (PSI/KL/KS). Travels with the score because thresholds are method-specific (see the DriftMethod reference). |
| score | [double](#double) |  | The computed drift score under `method`. Higher = more drift. SERVER-computed. |
| baseline_value | [double](#double) |  | The baseline (training-time) summary value for context, e.g., baseline mean. Lets the UI render &#34;was 5.1, now 7.8&#34; without a second lookup. |
| current_value | [double](#double) |  | The current-window summary value, paired with baseline_value above. |
| severity | [DriftSeverity](#forgepoint-monitor-v1-DriftSeverity) |  | Severity for THIS metric, derived from `score` vs the monitor&#39;s thresholds. The report&#39;s overall severity is the max across its metrics. |






<a name="forgepoint-monitor-v1-DriftReport"></a>

### DriftReport
============================================================================
DriftReport
============================================================================

WHY a first-class, persisted report (not just an event): the report is the
durable record of a single window&#39;s verdict. It backs the paginated history
(ListDriftReports), the Web UI time series, and the audit trail (&#34;why did the
model retrain at 03:14?&#34;). The canonical events.ModelDriftDetected event
carries a *flattened copy* of this report&#39;s fields &#43; its report_id (it does NOT
embed monitor.DriftReport — events are decoupled from API types); this
persisted report is the source of truth a consumer deep-links back to.

IDEMPOTENCY — window_id is the natural key: each closed window has a stable
id, and a report is persisted idempotently on it (Task 16.3: &#34;idempotent on
window id&#34;). If the stream redelivers events or the scorer runs twice for the
same window, we get ONE report, not duplicates — and exactly one event fires.
This is the same idempotency-key discipline every consumer in the platform uses.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Report identity. SERVER-assigned. |
| monitor_id | [string](#string) |  | The monitor that produced this report. |
| model_name | [string](#string) |  | The model &#43; the serving version that was live during this window. Version is captured from the inference events, so a report is always tied to the exact version that drifted (critical when a canary is splitting traffic). |
| model_version | [string](#string) |  |  |
| drift_type | [DriftType](#forgepoint-monitor-v1-DriftType) |  | The dominant drift type for this report (the type whose breach triggered it). A window can show multiple signals; this is the headline. |
| severity | [DriftSeverity](#forgepoint-monitor-v1-DriftSeverity) |  | Overall severity = max severity across `metrics`. This is what the policy and consumers branch on. SERVER-computed. |
| metrics | [DriftMetric](#forgepoint-monitor-v1-DriftMetric) | repeated | Per-feature / per-output / per-performance-metric breakdown. The actionable detail — see DriftMetric. Empty only for a degenerate OK report. |
| window_id | [string](#string) |  | The stable id of the window this report scored. The IDEMPOTENCY KEY for persistence and event emission (see message-level note). SERVER-assigned. |
| sample_count | [int32](#int32) |  | How many inference samples were in the scored window. Surfaces statistical confidence (&#34;PSI=0.4 over 12 samples&#34; is far weaker than over 12,000). |
| window_start | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Window bounds [start, end). Lets the UI place the report on a timeline and correlate with deploys/incidents. |
| window_end | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the report was computed/persisted. SERVER-assigned. |






<a name="forgepoint-monitor-v1-GetDriftReportRequest"></a>

### GetDriftReportRequest
GetDriftReportRequest fetches a single drift report by id.
WHY by report id (not model&#43;window): report id is the stable, shareable handle
the event and the UI deep-link both reference (&#34;open report &lt;id&gt;&#34;).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| report_id | [string](#string) |  | The drift report&#39;s UUID (DriftReport.id). Required. |






<a name="forgepoint-monitor-v1-GetDriftReportResponse"></a>

### GetDriftReportResponse
GetDriftReportResponse wraps the report.
WHY wrap: RPC_RESPONSE_STANDARD_NAME &#43; forward compatibility.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| report | [DriftReport](#forgepoint-monitor-v1-DriftReport) |  |  |






<a name="forgepoint-monitor-v1-GetModelHealthRequest"></a>

### GetModelHealthRequest
GetModelHealthRequest reads the at-a-glance health verdict for one model.
WHY a distinct RPC from GetMonitorStatus (design doc lists GetModelHealth
explicitly, and `fp monitor &lt;model&gt;` wants a one-line answer): see ModelHealth.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model whose health to read. Required. Authorization (caller&#39;s team owns the model) is enforced by the auth interceptor, not by this field. |






<a name="forgepoint-monitor-v1-GetModelHealthResponse"></a>

### GetModelHealthResponse
GetModelHealthResponse wraps the health rollup.
WHY wrap: RPC_RESPONSE_STANDARD_NAME &#43; forward compatibility.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| health | [ModelHealth](#forgepoint-monitor-v1-ModelHealth) |  |  |






<a name="forgepoint-monitor-v1-GetMonitorStatusRequest"></a>

### GetMonitorStatusRequest
GetMonitorStatusRequest reads the live status for one model&#39;s monitor.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model whose monitor status to read. Required. |






<a name="forgepoint-monitor-v1-GetMonitorStatusResponse"></a>

### GetMonitorStatusResponse
GetMonitorStatusResponse wraps the live MonitorStatus.
WHY wrap: RPC_RESPONSE_STANDARD_NAME &#43; forward compatibility.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [MonitorStatus](#forgepoint-monitor-v1-MonitorStatus) |  |  |






<a name="forgepoint-monitor-v1-GroundTruthLabel"></a>

### GroundTruthLabel
============================================================================
GroundTruthLabel
============================================================================

WHY a dedicated message for delayed labels: performance decay is the only
drift signal that needs the TRUTH, and the truth arrives LATE — a fraud label
might be confirmed days after the prediction. SubmitGroundTruth lets an
upstream system backfill the actual outcome for a past prediction, keyed by
the inference request_id, so the monitor can compute rolling accuracy/F1.

WHY request_id as the join key: the Inference Gateway stamps a SERVER-
authoritative request_id on every PredictResponse and carries it on the
InferenceCompleted event. That id is the only thing tying &#34;a prediction we
saw&#34; to &#34;the outcome you now know&#34;, without shipping PII or raw features.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| request_id | [string](#string) |  | The inference request this label is the outcome for. Must match a request_id the monitor previously observed on the inference stream (else it&#39;s buffered or dropped per retention policy). This is the join key — NOT a user/PII id. |
| actual_label | [string](#string) |  | The actual observed outcome (e.g., &#34;fraud&#34; / &#34;not_fraud&#34;, or a regression target as a string). Compared against the recorded prediction for that id. SECURITY/PII: this is a LABEL, not a free-form blob — the server caps its length (e.g. 256 bytes) and it must not be used to smuggle PII; the monitor stores only the label↔request_id pairing it needs for rolling accuracy, the same PII-free trust boundary as the inference event. |
| observed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the true outcome became known (NOT when the prediction was made). Lets the monitor measure label latency and weight recent labels for rolling F1. |






<a name="forgepoint-monitor-v1-ListDriftReportsRequest"></a>

### ListDriftReportsRequest
ListDriftReportsRequest returns a model&#39;s drift history, newest first.
Reuses the common cursor-based PaginationRequest (consistency across the
platform; see common.proto for the WHY on cursor vs offset).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | Filter to one model&#39;s reports. Required — drift history is always read per-model (a global firehose of every model&#39;s reports has no use case and would be an expensive unbounded scan). |
| min_severity | [DriftSeverity](#forgepoint-monitor-v1-DriftSeverity) |  | Optional: return only reports at/above this severity (e.g., only CRITICAL). DRIFT_SEVERITY_UNSPECIFIED = no severity filter (return all). |
| since | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Optional time-range filter on window_end. Zero values = unbounded on that side. Lets the UI scope to &#34;last 7 days&#34; without paging through everything. |
| until | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  |  |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20; the server CAPS it at 100 regardless of a larger client request (DoS / accidental-firehose guard, matching the platform-wide max documented in common.proto). |






<a name="forgepoint-monitor-v1-ListDriftReportsResponse"></a>

### ListDriftReportsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reports | [DriftReport](#forgepoint-monitor-v1-DriftReport) | repeated | The page of reports, newest window_end first. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | next_page_token &#43; total_count. |






<a name="forgepoint-monitor-v1-ListMonitorsRequest"></a>

### ListMonitorsRequest
ListMonitorsRequest returns the monitors the caller&#39;s team owns — the FLEET
view the Web UI&#39;s &#34;Monitoring&#34; page and `fp monitor` (no arg) render. WHY this
RPC exists: every other read here is per-model; an operator also needs &#34;show me
every monitored model and which are unhealthy&#34; without knowing names up front.

SECURITY: the result is ALWAYS scoped to the caller&#39;s team by the auth
interceptor (derived from auth claims) — there is deliberately NO team field on
this request, because a client-supplied team would be a cross-tenant read.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| min_severity | [DriftSeverity](#forgepoint-monitor-v1-DriftSeverity) |  | Optional: return only monitors whose latest overall severity is at/above this (e.g. CRITICAL) — drives the &#34;unhealthy only&#34; filter. UNSPECIFIED = all. |
| state | [MonitorState](#forgepoint-monitor-v1-MonitorState) |  | Optional: return only monitors in this lifecycle state (e.g. ACTIVE). MONITOR_STATE_UNSPECIFIED = any state. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20; the server CAPS it at 100 regardless of a larger client request (platform-wide cap, see common.proto). |






<a name="forgepoint-monitor-v1-ListMonitorsResponse"></a>

### ListMonitorsResponse
ListMonitorsResponse is a page of fleet entries — each pairs the monitor config
with its current health so the UI renders the whole table from ONE call (no
N&#43;1 GetModelHealth per row).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [ListMonitorsResponse.Entry](#forgepoint-monitor-v1-ListMonitorsResponse-Entry) | repeated | The page of fleet entries. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | next_page_token &#43; total_count. |






<a name="forgepoint-monitor-v1-ListMonitorsResponse-Entry"></a>

### ListMonitorsResponse.Entry
One entry per monitored model in this page.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| monitor | [Monitor](#forgepoint-monitor-v1-Monitor) |  | The monitor&#39;s configuration. |
| health | [ModelHealth](#forgepoint-monitor-v1-ModelHealth) |  | Its current at-a-glance health (overall severity, per-type lights, state). |






<a name="forgepoint-monitor-v1-ModelHealth"></a>

### ModelHealth
============================================================================
ModelHealth
============================================================================

WHY a dedicated health rollup distinct from MonitorStatus: the design doc and
the `fp monitor &lt;model&gt;` CLI ask a SIMPLER question than MonitorStatus answers
— &#34;is this model healthy right now, in one line?&#34;. MonitorStatus exposes the
raw live machinery (window fill, latest report, lifecycle state) for an
operator drilling in; ModelHealth is the AT-A-GLANCE verdict a dashboard tile,
a CLI summary, or a Rollouts AnalysisTemplate (Phase 22) consumes. Splitting
them keeps GetModelHealth a cheap, cacheable read and lets the health verdict
evolve (e.g., fold error-rate from InferenceFailed in later) without bloating
the status view.

SERVER-AUTHORITATIVE: every field here is computed by the monitor. A client
cannot assert its own model &#34;healthy&#34;.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model this health verdict is for. |
| model_version | [string](#string) |  | The version currently judged (the live production/served version the monitor is scoring). Empty if no traffic has been seen yet. |
| overall_severity | [DriftSeverity](#forgepoint-monitor-v1-DriftSeverity) |  | The headline verdict: the MAX severity across all active drift signals (data / prediction / performance) in the most recent scored window. OK means healthy; WARNING/CRITICAL surface the worst signal. This is the single value a dashboard tile colors on. SERVER-computed. |
| severity_by_type | [ModelHealth.SeverityByTypeEntry](#forgepoint-monitor-v1-ModelHealth-SeverityByTypeEntry) | repeated | The most recent severity per drift type, so a UI can render a 3-light panel (data / prediction / performance) without fetching every report. Keyed by the DriftType enum&#39;s integer value (proto3 map keys cannot be enums). A type absent from the map is simply not configured/scored for this model. |
| state | [MonitorState](#forgepoint-monitor-v1-MonitorState) |  | Live lifecycle state (mirrors Monitor.state) — distinguishes &#34;healthy&#34; from &#34;not scoring yet&#34; (WARMING_UP), which a bare severity of OK would conflate. |
| latest_report_id | [string](#string) |  | The most recent drift report id, a deep-link handle for &#34;see why&#34;. Empty until the first window closes. |
| last_event_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the monitor last consumed an inference event for this model. A stale value means the model is getting no traffic (its own kind of alert). |
| drift_events_total | [int64](#int64) |  | Count of CRITICAL reports in this monitor&#39;s lifetime — a cheap trend KPI. |






<a name="forgepoint-monitor-v1-ModelHealth-SeverityByTypeEntry"></a>

### ModelHealth.SeverityByTypeEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [int32](#int32) |  |  |
| value | [DriftSeverity](#forgepoint-monitor-v1-DriftSeverity) |  |  |






<a name="forgepoint-monitor-v1-Monitor"></a>

### Monitor
============================================================================
Monitor
============================================================================

WHY this is the central config aggregate: one Monitor governs one model&#39;s
drift detection — its window shape, its thresholds, its baseline reference,
and its auto-retrain switch. It is the read model returned by ConfigureMonitor
and embedded in MonitorStatus.

SECURITY — what is SERVER-AUTHORITATIVE here (NOT client-writable on configure):
  - id, created_at, updated_at: identity &amp; audit, owned by the server.
  - state: lifecycle, advanced by the monitor as baselines/windows fill.
  - baseline_version / baseline_captured_at: the training-time distribution
    reference is resolved by the monitor from the Model Registry / Experiment
    Tracker — a client cannot point a monitor at an arbitrary baseline (that
    would let someone choose a baseline that conveniently &#34;matches&#34; current
    traffic to suppress a real drift signal).
  What IS client-controlled (via ConfigureMonitorRequest): model identity,
  window shape, thresholds, and the auto-retrain toggle. See that request
  message for the exact accepted fields (mass-assignment guard).
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Primary key, immutable, SERVER-assigned. |
| model_name | [string](#string) |  | The model this monitor watches (e.g., &#34;fraud-detector&#34;). Stable name from the Model Registry; combined with the live serving version at event time. |
| owner_team | [string](#string) |  | SERVER-AUTHORITATIVE. The team that owns this monitor, derived from the creator&#39;s auth claims on ConfigureMonitor — NEVER from a request field. WHY it is server-set and not in the request: tenancy is an authz fact, and a client that could name its own team could create/read monitors for another tenant&#39;s models (a cross-tenant data leak). All reads (List/Get/Status) are additionally scoped to the caller&#39;s team by the auth interceptor; this field only RECORDS the owner for display/audit, it is not the access-control input. |
| window_duration | [google.protobuf.Duration](#google-protobuf-Duration) |  | Window length over which drift is computed. Two ways to bound a sliding window — by TIME (this field) or by COUNT (window_size below). The monitor uses whichever is set; if both are set, a window closes when EITHER bound is hit (whichever comes first), which keeps low-traffic models from waiting forever and high-traffic models from accumulating unbounded memory. |
| window_size | [int32](#int32) |  | Max number of inference events per window (count-based bound). See above for how it interacts with window_duration. 0 = unbounded by count (time only). BOUNDS (server-validated on write): must be &gt;= 0 and &lt;= 1_000_000. The upper cap is a MEMORY guard — the live window is held in Redis, so an unbounded or absurd window_size would let a single monitor pin gigabytes of RAM. Values above the cap are rejected with INVALID_ARGUMENT (not silently clamped, so the operator notices). int32 (max ~2.1B) comfortably covers the 1M cap with no overflow risk in window-fill arithmetic. |
| min_samples | [int32](#int32) |  | Minimum samples before the monitor will SCORE a window. Below this the monitor stays WARMING_UP — comparing a near-empty window to a large baseline yields noise, so we refuse to emit a verdict until the sample is sound. BOUNDS (server-validated): 0 &lt;= min_samples &lt;= window_size when window_size&gt;0 (you cannot require more samples than the window can hold, else it never scores). Rejected with INVALID_ARGUMENT otherwise. |
| thresholds | [ThresholdConfig](#forgepoint-monitor-v1-ThresholdConfig) | repeated | Per-drift-type thresholds (data / prediction / performance). A monitor may configure any subset; an unconfigured type is simply not scored. |
| auto_retrain | [bool](#bool) |  | The auto-retrain switch — the heart of the closed loop. When true and a CRITICAL breach fires, the monitor calls the Pipeline Orchestrator to run the model&#39;s training pipeline. When false, a breach only alerts (human in the loop). Defaults to false: self-healing must be opted into, because an accidental retrain storm is expensive and disruptive. |
| retrain_pipeline_id | [string](#string) |  | The training pipeline to trigger on auto-retrain (Pipeline Orchestrator&#39;s pipeline id). Required when auto_retrain=true; the monitor validates that it is set before enabling the loop. It is an OPAQUE identifier here — Model Monitor does not import the pipeline proto (services are decoupled; the loop is closed via the event bus / a gRPC client, not a compile-time dependency). |
| state | [MonitorState](#forgepoint-monitor-v1-MonitorState) |  | SERVER-AUTHORITATIVE. Lifecycle state (pending baseline → warming → active). |
| baseline_version | [string](#string) |  | SERVER-AUTHORITATIVE. The model version whose training-time distribution is the comparison baseline (e.g., &#34;v7&#34;). Resolved from Registry/Experiment Tracker, not chosen by the client. |
| baseline_captured_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | SERVER-AUTHORITATIVE. When the baseline distribution was captured. Lets the UI show &#34;baseline is 40 days old — consider refreshing&#34;. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | SERVER-AUTHORITATIVE. When this monitor was created. Immutable. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | SERVER-AUTHORITATIVE. Last config change (bumped on every ConfigureMonitor). |






<a name="forgepoint-monitor-v1-MonitorStatus"></a>

### MonitorStatus
============================================================================
MonitorStatus
============================================================================

WHY a separate &#34;live status&#34; view distinct from the Monitor config: config is
what you SET; status is what&#39;s HAPPENING right now — the in-flight window&#39;s
fill level, the freshest scores, and the latest report. Splitting them keeps
ConfigureMonitor focused on intent and GetMonitorStatus focused on
observability (it reads the live Redis window, not just Postgres config).
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| monitor | [Monitor](#forgepoint-monitor-v1-Monitor) |  | The monitor&#39;s configuration (so a status call is self-contained for a UI). |
| state | [MonitorState](#forgepoint-monitor-v1-MonitorState) |  | Live lifecycle state, mirrored from monitor.state for at-a-glance reads. |
| current_window_samples | [int32](#int32) |  | How many samples are in the CURRENT (still-open) window. Compared against monitor.min_samples this tells you &#34;scoring in 230 more requests&#34;. |
| latest_report | [DriftReport](#forgepoint-monitor-v1-DriftReport) |  | The most recent completed drift report, if any. Null until the first window closes. The cheapest &#34;is my model healthy?&#34; read. |
| last_event_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the monitor last consumed an inference event. A stale value means the model is getting no traffic (its own kind of alert) or the consumer stalled. |
| drift_events_total | [int64](#int64) |  | Count of CRITICAL reports in this monitor&#39;s lifetime — a cheap health KPI and the source of the `model_drift_detected_total` metric. |






<a name="forgepoint-monitor-v1-ResetBaselineRequest"></a>

### ResetBaselineRequest
ResetBaselineRequest manually re-pins a monitor&#39;s drift baseline to a specific
model version&#39;s training-time distribution. WHY a manual path EXISTS even
though ModelPromoted auto-resets it: (a) an operator who sees &#34;baseline is 40
days old&#34; (Monitor.baseline_captured_at) can refresh it without a promotion;
(b) after a known legitimate distribution shift (a new feature pipeline) you
re-baseline to stop alerting on expected drift. The closed-loop auto-reset
handles the common case; this is the operator escape hatch.

SECURITY — the baseline is STILL server-resolved: the caller names a VERSION,
not a distribution. The monitor fetches that version&#39;s training-time summary
from Registry/Experiment-Tracker itself — a client cannot upload a hand-crafted
baseline that conveniently matches current traffic to suppress real drift.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model whose baseline to reset. Required. Team ownership enforced by auth. |
| baseline_version | [string](#string) |  | The model version whose training-time distribution becomes the new baseline (e.g. &#34;v8&#34;). Empty = reset to the CURRENT production version (the common &#34;refresh to latest&#34; case). The server validates the version exists and is owned by the caller&#39;s team before resolving its baseline. |
| idempotency_key | [string](#string) |  | Idempotency key — re-pinning to the same version twice must be a no-op, not a second baseline-fetch that bumps updated_at. Empty = treated as unique. |






<a name="forgepoint-monitor-v1-ResetBaselineResponse"></a>

### ResetBaselineResponse
ResetBaselineResponse returns the monitor after the re-baseline, so the caller
sees the new baseline_version / baseline_captured_at without a follow-up read.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| monitor | [Monitor](#forgepoint-monitor-v1-Monitor) |  | The monitor with its refreshed SERVER-authoritative baseline_* fields. |






<a name="forgepoint-monitor-v1-StreamDriftEventsRequest"></a>

### StreamDriftEventsRequest
StreamDriftEventsRequest opens a SERVER-STREAMING subscription to live drift
events as they fire. WHY server-streaming (not client polling ListDriftReports
in a loop): drift is push-shaped — a dashboard or `fp monitor --watch` wants
to be NOTIFIED the instant a window breaches, not to poll every N seconds and
add latency &#43; load. One long-lived gRPC stream replaces a polling storm. This
is the same reason Pipeline Orchestrator uses a streaming WatchExecution.

WHY a SEPARATE stream from the NATS event bus: NATS fan-out (the canonical
events.ModelDriftDetected on fp.models.drift.detected) is for SERVICE-to-service
reactions (Notification, Orchestrator). This gRPC stream is for INTERACTIVE
clients (UI, CLI) that hold an authenticated request scope and want a filtered,
backpressure-aware feed — without granting them a NATS connection. Two
transports, two audiences, ONE underlying drift report.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | Optional filter: stream only this model&#39;s drift events. Empty = all models the caller is authorized to see (authorization is enforced by the auth interceptor, not by this field — never trust a client-supplied scope). |
| min_severity | [DriftSeverity](#forgepoint-monitor-v1-DriftSeverity) |  | Optional: only stream events at/above this severity (e.g., CRITICAL only, to drive a pager). DRIFT_SEVERITY_UNSPECIFIED = all severities. |






<a name="forgepoint-monitor-v1-StreamDriftEventsResponse"></a>

### StreamDriftEventsResponse
StreamDriftEventsResponse is ONE drift event in the stream. Each message
carries the full report so a client renders without a follow-up GetDriftReport.
WHY a wrapper message (not streaming DriftReport directly): keeps room to add
stream-control fields later (e.g., a heartbeat flag to keep idle connections
alive, or a resume cursor) without breaking the stream contract.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| report | [DriftReport](#forgepoint-monitor-v1-DriftReport) |  | The drift report that just fired. For OK/heartbeat frames this may be unset if heartbeat is later added; today every frame carries a report. |






<a name="forgepoint-monitor-v1-SubmitGroundTruthRequest"></a>

### SubmitGroundTruthRequest
SubmitGroundTruthRequest backfills delayed true outcomes so the monitor can
compute performance decay. Accepts a BATCH because labels typically arrive in
bulk from an upstream labeling/ETL job, and per-label RPCs would be chatty.

SECURITY: the monitor only accepts labels for request_ids it has actually
observed on the inference stream — a caller cannot inject labels for arbitrary
or fabricated ids to skew a model&#39;s measured accuracy. Unknown ids are
reported back, not silently trusted.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model these labels belong to (scopes the request_id lookups). Required. |
| labels | [GroundTruthLabel](#forgepoint-monitor-v1-GroundTruthLabel) | repeated | The batch of (request_id → actual outcome) labels. CONTRACT CAP: at most 1000 labels per call; a larger batch is rejected with INVALID_ARGUMENT (a bound in the contract, not just prose — it protects the server from an unbounded single request and bounds the transactional write). Callers with more labels page through multiple calls (each idempotent on its own key). |
| idempotency_key | [string](#string) |  | Idempotency key — re-submitting the same labeling batch after a retry must not double-count outcomes in the rolling accuracy window. The server records the key (short TTL) and treats a duplicate as a no-op returning the first result. Empty = treated as unique. |






<a name="forgepoint-monitor-v1-SubmitGroundTruthResponse"></a>

### SubmitGroundTruthResponse
SubmitGroundTruthResponse reports how the batch was applied — NOT just an
empty ack. WHY: ground truth is fuzzy (some ids won&#39;t match an observed
prediction; some may be late past the retention window), and the caller needs
to know what stuck so it can retry or alert on its side.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| accepted | [int32](#int32) |  | How many labels were matched to an observed prediction and recorded. |
| unmatched_request_ids | [string](#string) | repeated | request_ids that did not match any observed prediction (unknown or aged out of the monitor&#39;s retention window). The caller can investigate/resubmit. |






<a name="forgepoint-monitor-v1-ThresholdConfig"></a>

### ThresholdConfig
============================================================================
ThresholdConfig
============================================================================

WHY per-(drift-type) thresholds, not one global number: data drift, prediction
drift, and performance decay live on different scales and warrant different
sensitivities. A PSI of 0.2 on features might be &#34;warn&#34;, while a 5-point
accuracy drop is already &#34;critical&#34;. So a monitor carries a SET of these,
keyed by drift type, rather than a single scalar.

WARN vs CRITICAL: two thresholds give the severity ladder its two rungs.
warn_score ≤ critical_score is enforced server-side; crossing critical is
what arms the auto-retrain action.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| drift_type | [DriftType](#forgepoint-monitor-v1-DriftType) |  | Which drift signal this threshold governs (data / prediction / performance). |
| method | [DriftMethod](#forgepoint-monitor-v1-DriftMethod) |  | The statistical method to apply for this drift type (PSI/KL/KS). The monitor uses this to compute the score the thresholds are compared against. |
| warn_score | [double](#double) |  | Score at/above which severity becomes WARNING (alert, no retrain). For performance decay this is interpreted as a *drop* magnitude vs baseline. |
| critical_score | [double](#double) |  | Score at/above which severity becomes CRITICAL (fires ModelDriftDetected and, if enabled, auto-retrain). Must be &gt;= warn_score (validated on write). |





 


<a name="forgepoint-monitor-v1-DriftMethod"></a>

### DriftMethod
============================================================================
DriftMethod
============================================================================

WHY surface the statistical METHOD: PSI, KL-divergence, and the KS-test
answer &#34;did the distribution move?&#34; differently, so a score is meaningless
without the test that produced it. Exposing the method on every metric makes
the report self-describing — a reader (or the Web UI) knows a score of 0.3 means
&#34;PSI=0.3 (significant)&#34; vs &#34;KS=0.3 (a p-value-ish distance)&#34;. Different
methods have different threshold conventions, so the method must travel with
the score.

METHOD REFERENCE:
  - PSI (Population Stability Index): bins both distributions, sums
    (curr% - base%) * ln(curr%/base%) per bin. Rule of thumb: &lt;0.1 stable,
    0.1–0.25 moderate shift, &gt;0.25 significant. Cheap, interpretable, the
    industry default for tabular features. Asymmetric handling of empty bins
    needs care (add epsilon) — a classic gotcha.
  - KL-divergence: relative entropy of curr vs base. Unbounded, asymmetric;
    sensitive to bins where base≈0. Good for prediction-probability drift.
  - KS-test (Kolmogorov–Smirnov): max gap between the two empirical CDFs.
    Non-parametric, distribution-free, yields a p-value; great for continuous
    features but weak on heavy multimodality.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| DRIFT_METHOD_UNSPECIFIED | 0 |  |
| DRIFT_METHOD_PSI | 1 | Population Stability Index — binned, interpretable, the tabular default. |
| DRIFT_METHOD_KL | 2 | Kullback–Leibler divergence — relative entropy, asymmetric. |
| DRIFT_METHOD_KS | 3 | Kolmogorov–Smirnov two-sample test — max CDF gap, yields a p-value. |



<a name="forgepoint-monitor-v1-DriftSeverity"></a>

### DriftSeverity
============================================================================
DriftSeverity
============================================================================

WHY a severity ladder (not just a boolean breached/ok): a single threshold
hides nuance the control loop needs. A WARNING means &#34;watch this&#34; (alert a
human, do NOT auto-retrain); CRITICAL means &#34;act now&#34; (fire the retrain). The
policy maps a numeric score against the monitor&#39;s configured thresholds to a
severity, and consumers branch on severity rather than re-deriving it from
raw scores. This mirrors Prometheus/Alertmanager severity labels.

SERVER-AUTHORITATIVE: severity is computed by the monitor from the score and
the configured thresholds — never supplied by a client.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| DRIFT_SEVERITY_UNSPECIFIED | 0 |  |
| DRIFT_SEVERITY_OK | 1 | Score is within configured limits. No action. Reports may still be stored for the time series even when OK (so the UI can show &#34;stable for 14 days&#34;). |
| DRIFT_SEVERITY_WARNING | 2 | Score crossed the warn threshold but not the critical one. Alert a human; do NOT auto-retrain. The leading-indicator zone. |
| DRIFT_SEVERITY_CRITICAL | 3 | Score crossed the critical threshold. This is what fires ModelDriftDetected and (if enabled) the auto-retrain control action. |



<a name="forgepoint-monitor-v1-DriftType"></a>

### DriftType
============================================================================
DriftType
============================================================================

WHY an enum (not a free string): the three drift signals are a CLOSED set
with distinct math and distinct meaning to consumers (Notification renders a
different alert per type; the retrain policy may weight performance decay
higher than a transient prediction-drift blip). An enum makes the set
exhaustive and lint-checkable; a string would let &#34;data_drift&#34; and
&#34;DataDrift&#34; diverge across services.

Buf STANDARD requires the zero value suffixed _UNSPECIFIED and every value
prefixed with the enum name (DRIFT_TYPE_).
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| DRIFT_TYPE_UNSPECIFIED | 0 | Default / unknown. A well-formed report always sets a concrete type; _UNSPECIFIED surfaces serialization bugs rather than silently defaulting. |
| DRIFT_TYPE_DATA | 1 | Input feature distribution shifted vs the training baseline (PSI/KL/KS). |
| DRIFT_TYPE_PREDICTION | 2 | Output/prediction distribution shifted over time (label-free early signal). |
| DRIFT_TYPE_PERFORMANCE | 3 | Actual accuracy/F1 dropped vs the registered baseline (needs ground truth). |



<a name="forgepoint-monitor-v1-MonitorState"></a>

### MonitorState
============================================================================
MonitorState
============================================================================

WHY a lifecycle state on the monitor itself: a monitor can exist but not yet
be usable. Drift math is meaningless until we have a baseline AND enough
samples in the current window to be statistically sound (comparing 3 requests
to a 50k-row baseline produces noise, not signal). State lets the monitor
say &#34;I&#39;m collecting, ask me later&#34; instead of emitting garbage drift scores.

SERVER-AUTHORITATIVE: the monitor advances its own state as baselines load
and windows fill. Clients can PAUSE/RESUME via ConfigureMonitor.enabled, but
they cannot fake ACTIVE to force premature scoring.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| MONITOR_STATE_UNSPECIFIED | 0 |  |
| MONITOR_STATE_PENDING_BASELINE | 1 | Configured but waiting for a training baseline (fetched from Registry / Experiment Tracker). No scoring yet. |
| MONITOR_STATE_WARMING_UP | 2 | Baseline loaded but the current window has not reached min_samples. Accumulating; not yet scoring. |
| MONITOR_STATE_ACTIVE | 3 | Baseline present, window full enough — actively computing drift and able to fire the control loop. |
| MONITOR_STATE_PAUSED | 4 | Explicitly paused by an operator (ConfigureMonitor.enabled=false). Events are still consumed/windowed but no drift is scored and nothing is fired. |


 

 


<a name="forgepoint-monitor-v1-MonitorService"></a>

### MonitorService
============================================================================
MODEL MONITOR SERVICE
============================================================================

WHY a single MonitorService (not split Config/Report/Stream services): the
config, the reports it produces, and the live stream are one cohesive bounded
context — &#34;the drift monitoring of models&#34;. Splitting them would force
inter-service calls for flows that are naturally one boundary (configuring a
monitor and reading its reports). One service, one Postgres&#43;Redis store.

NOTE ON THE CONTROL LOOP: the most important behavior — consuming
`fp.inference.completed`, windowing, scoring, and firing the retrain — is NOT
an RPC. It is an event-driven loop running inside the service (NATS consumer &#43;
scorer &#43; policy). This gRPC surface is the CONTROL &amp; OBSERVABILITY plane over
that loop: configure it, read what it found, watch it live, feed it truth.

RPC CATEGORIES:
  CONFIG:        ConfigureMonitor, DeleteMonitor, ResetBaseline
  OBSERVABILITY: GetModelHealth, GetMonitorStatus, ListMonitors,
                 GetDriftReport, ListDriftReports, StreamDriftEvents
  FEEDBACK:      SubmitGroundTruth (delayed labels → performance decay)

AUTH: every RPC is guarded by the shared auth interceptor (see auth.proto).
Drift scores and config are operationally sensitive; reads require monitoring
read scope, writes (ConfigureMonitor, DeleteMonitor, ResetBaseline,
SubmitGroundTruth) require write scope. EVERY RPC is additionally TEAM-SCOPED
by the interceptor from the caller&#39;s auth claims — no request carries a team or
tenancy field (those would be cross-tenant footguns; see per-request notes).
============================================================================

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ConfigureMonitor | [ConfigureMonitorRequest](#forgepoint-monitor-v1-ConfigureMonitorRequest) | [ConfigureMonitorResponse](#forgepoint-monitor-v1-ConfigureMonitorResponse) | ConfigureMonitor upserts a model&#39;s monitor: window shape, per-drift-type thresholds, and the auto-retrain switch. The ONLY config write path. SERVER-authoritative fields (id, owner_team, state, baseline_*) are not accepted here (mass-assignment guard). |
| DeleteMonitor | [DeleteMonitorRequest](#forgepoint-monitor-v1-DeleteMonitorRequest) | [DeleteMonitorResponse](#forgepoint-monitor-v1-DeleteMonitorResponse) | DeleteMonitor stops monitoring a model (soft-delete by default; optionally purges drift history). Idempotent on idempotency_key. |
| ResetBaseline | [ResetBaselineRequest](#forgepoint-monitor-v1-ResetBaselineRequest) | [ResetBaselineResponse](#forgepoint-monitor-v1-ResetBaselineResponse) | ResetBaseline manually re-pins the drift baseline to a model version&#39;s training distribution (operator escape hatch; auto-reset happens on ModelPromoted). The baseline is server-resolved from the named version. |
| GetModelHealth | [GetModelHealthRequest](#forgepoint-monitor-v1-GetModelHealthRequest) | [GetModelHealthResponse](#forgepoint-monitor-v1-GetModelHealthResponse) | GetModelHealth returns the at-a-glance health verdict for one model (overall severity &#43; per-type lights &#43; state). The one-line &#34;is this model healthy?&#34; read for dashboards and `fp monitor &lt;model&gt;`. |
| GetMonitorStatus | [GetMonitorStatusRequest](#forgepoint-monitor-v1-GetMonitorStatusRequest) | [GetMonitorStatusResponse](#forgepoint-monitor-v1-GetMonitorStatusResponse) | GetMonitorStatus returns the live status for a model: lifecycle state, current-window fill, the latest report, and lifetime drift count. The operator drill-in view (hits the live Redis window). |
| ListMonitors | [ListMonitorsRequest](#forgepoint-monitor-v1-ListMonitorsRequest) | [ListMonitorsResponse](#forgepoint-monitor-v1-ListMonitorsResponse) | ListMonitors returns the caller team&#39;s monitored-model FLEET (config &#43; current health per row), paginated, with optional severity/state filters. Powers the Web UI monitoring table and `fp monitor` (no arg). Page size capped at 100. |
| GetDriftReport | [GetDriftReportRequest](#forgepoint-monitor-v1-GetDriftReportRequest) | [GetDriftReportResponse](#forgepoint-monitor-v1-GetDriftReportResponse) | GetDriftReport fetches a single persisted drift report by id (the handle the ModelDriftDetected event and UI deep-links reference). |
| ListDriftReports | [ListDriftReportsRequest](#forgepoint-monitor-v1-ListDriftReportsRequest) | [ListDriftReportsResponse](#forgepoint-monitor-v1-ListDriftReportsResponse) | ListDriftReports returns a model&#39;s drift history (newest first), paginated, with optional severity and time-range filters. Page size is capped server- side at 100. |
| SubmitGroundTruth | [SubmitGroundTruthRequest](#forgepoint-monitor-v1-SubmitGroundTruthRequest) | [SubmitGroundTruthResponse](#forgepoint-monitor-v1-SubmitGroundTruthResponse) | SubmitGroundTruth backfills delayed true outcomes (batched) so the monitor can compute PERFORMANCE DECAY — the lagging, ground-truth confirmation of drift. Returns how many labels matched an observed prediction. |
| StreamDriftEvents | [StreamDriftEventsRequest](#forgepoint-monitor-v1-StreamDriftEventsRequest) | [StreamDriftEventsResponse](#forgepoint-monitor-v1-StreamDriftEventsResponse) stream | StreamDriftEvents opens a long-lived SERVER-STREAM of drift events as they fire, optionally filtered by model and min severity. Powers live dashboards and `fp monitor --watch` without polling. (Server-streaming: one request, many responses, until the client cancels or the deadline elapses.) |

 



<a name="forgepoint_notification_v1_notification-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/notification/v1/notification.proto



<a name="forgepoint-notification-v1-ChannelPreference"></a>

### ChannelPreference
============================================================================
ChannelPreference
============================================================================

WHY: A user&#39;s choice for ONE channel — is it enabled, what&#39;s the minimum
severity that should reach it, and the channel-specific config (the webhook
URL, the Slack URL, the email address). This is the per-channel building block
of NotificationPreferences.

SECURITY — secrets/PII handling (masking-on-read):
  The `target` here holds potentially sensitive routing data (a webhook URL
  may embed a token; an email is PII). The platform MAY return it on
  GetPreferences for the OWNING user (so they can see/edit their own settings)
  but it is scoped to that user only and must never appear in another user&#39;s
  responses or in any Notification/DeliveryAttempt. For high-secret deployments
  the server is free to return a masked target (e.g. &#34;https://hooks.slack…/***&#34;)
  on reads while accepting the full value on UpdatePreferences. We document
  this rather than hard-code masking so the storage layer owns the policy.

SECURITY — SSRF (the headline risk for THIS service):
  `target` for WEBHOOK/SLACK is a CLIENT-SUPPLIED URL that the delivery layer
  will make an outbound HTTP request to. A naive implementation is a textbook
  Server-Side Request Forgery primitive: a user could point a &#34;webhook&#34; at
  http://169.254.169.254/ (the cloud metadata endpoint → IAM credential theft),
  at http://localhost:port internal admin APIs, at an RFC1918 service inside the
  cluster, or at file://. The CONTRACT therefore REQUIRES the server to validate
  every webhook/slack target BEFORE storing it (on UpdatePreferences/TestChannel)
  AND to re-resolve&#43;re-check at delivery time (TOCTOU — DNS can be rebound
  between store and POST). The mandated validation, enforced server-side:
    1. Scheme allowlist: https only (http rejected; no file/gopher/ftp/etc).
    2. Host allowlist for SLACK: must be hooks.slack.com (Slack incoming-webhook
       host) — a Slack channel cannot point anywhere else.
    3. DNS resolution &#43; IP DENYLIST for WEBHOOK: reject if the resolved IP is
       loopback (127/8, ::1), link-local (169.254/16, fe80::/10 — blocks the
       metadata endpoint), private (10/8, 172.16/12, 192.168/16, fc00::/7),
       unspecified/multicast/reserved. Resolve and pin the IP, then connect to
       the PINNED ip (defeats DNS-rebinding TOCTOU).
    4. No credentials in the userinfo component; cap redirects (or disable them)
       so a 302 can&#39;t bounce the request to a denied address.
  A target that fails validation is REJECTED at write time (INVALID_ARGUMENT
  with a field-level ErrorDetail), so a malicious URL never reaches storage.
  This is documented in the contract — not left to the implementer&#39;s memory —
  precisely because it is the one place this service makes attacker-influenced
  outbound calls.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| channel | [NotificationChannel](#forgepoint-notification-v1-NotificationChannel) |  | Which channel this preference configures. Server-validated against the closed NotificationChannel enum; a value the delivery layer can&#39;t service is rejected. |
| enabled | [bool](#bool) |  | Master on/off for this channel. Disabled channels are skipped entirely (the corresponding DeliveryAttempt is recorded as SUPPRESSED, not FAILED). |
| min_severity | [NotificationSeverity](#forgepoint-notification-v1-NotificationSeverity) |  | Minimum severity that should be delivered on this channel. An event below this threshold is SUPPRESSED for this channel. E.g. set EMAIL&#39;s threshold to ERROR to avoid being emailed about routine INFO events. UNSPECIFIED means &#34;no floor&#34; (deliver everything this channel is enabled for). |
| target | [string](#string) |  | Channel-specific routing target. Semantics depend on `channel`: WEBHOOK/SLACK → the HTTPS URL to POST to. EMAIL → the destination email address. IN_APP → ignored (the inbox target is the user themselves). SUBJECT TO THE SSRF VALIDATION &#43; masking-on-read rules in the SECURITY notes above. Server-validated on every write; never client-trusted at delivery time. |






<a name="forgepoint-notification-v1-DeliveryAttempt"></a>

### DeliveryAttempt
============================================================================
DeliveryAttempt
============================================================================

WHY a separate message from Notification: one Notification fans out to N
channels, and each channel can be retried K times. Flattening attempt detail
into Notification would lose that structure. DeliveryAttempt is the typed view
of one row in the `delivery_log` table — the audit trail of &#34;did it actually
go out, and if not, why&#34;.

This is what makes failures debuggable: a webhook that returned 503 three
times then opened the circuit breaker leaves three RETRYING/FAILED attempts
with the response codes recorded. (Returned by GetNotification, which includes
the per-channel attempt history.)

SECURITY: response_code/error_message describe OUR delivery outcome, not the
downstream&#39;s secrets. We deliberately do NOT echo the target URL or any auth
header here — only enough to debug delivery health.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| channel | [NotificationChannel](#forgepoint-notification-v1-NotificationChannel) |  | Which transport this attempt used. |
| status | [DeliveryStatus](#forgepoint-notification-v1-DeliveryStatus) |  | Outcome of this attempt (see DeliveryStatus). |
| attempt | [int32](#int32) |  | 1-based attempt number within the retry budget (e.g. 1, 2, 3 for the 3-attempt exponential-backoff policy in the plan). 0 if not yet attempted. |
| response_code | [int32](#int32) |  | For HTTP-based channels (WEBHOOK/SLACK): the response status code, e.g. 200, 503. 0 for non-HTTP channels or when no response was received (timeout). |
| error_message | [string](#string) |  | Human-readable failure detail for logs/debugging (e.g. &#34;connection refused&#34;, &#34;circuit breaker open&#34;). Empty on success. NOT for end-user display. |
| attempted_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this delivery attempt was made. Server clock. |






<a name="forgepoint-notification-v1-GetNotificationRequest"></a>

### GetNotificationRequest
GetNotificationRequest fetches one inbox entry in full, including its
per-channel delivery attempt history (see GetNotificationResponse).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | The Notification.id to fetch. The server verifies the notification belongs to the authenticated caller before returning it (returns NOT_FOUND, not PERMISSION_DENIED, for someone else&#39;s id — avoids leaking existence). |






<a name="forgepoint-notification-v1-GetNotificationResponse"></a>

### GetNotificationResponse
GetNotificationResponse wraps the Notification plus its delivery audit trail.
WHY include delivery_attempts here but not on the list view: the attempt
history is the &#34;detail view&#34; payload — heavier, and only wanted when you open
a single notification to ask &#34;did my Slack alert actually go out?&#34;. Keeping it
off the list keeps list responses small.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notification | [Notification](#forgepoint-notification-v1-Notification) |  | The requested notification. |
| delivery_attempts | [DeliveryAttempt](#forgepoint-notification-v1-DeliveryAttempt) | repeated | Per-channel delivery attempts for this notification (the delivery_log rows), ordered oldest attempt first. Empty for purely IN_APP notifications that required no external delivery. |






<a name="forgepoint-notification-v1-GetPreferencesRequest"></a>

### GetPreferencesRequest
GetPreferencesRequest fetches the authenticated caller&#39;s notification
preferences. WHY no user_id field: same authority rule as the inbox — prefs
are always the caller&#39;s own, resolved from TokenClaims. No cross-user reads.






<a name="forgepoint-notification-v1-GetPreferencesResponse"></a>

### GetPreferencesResponse
GetPreferencesResponse wraps the caller&#39;s preferences.
WHY a wrapper around NotificationPreferences instead of returning it directly:
Buf STANDARD&#39;s RPC_RESPONSE_STANDARD_NAME requires &#34;&lt;Rpc&gt;Response&#34; naming, and
a wrapper lets us add response-level fields later (e.g. server-default hints,
or a &#34;preferences have never been set&#34; flag) without touching the shared
NotificationPreferences domain message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| preferences | [NotificationPreferences](#forgepoint-notification-v1-NotificationPreferences) |  | The caller&#39;s current preferences. If the user has never configured any, the server returns sensible defaults (IN_APP enabled, all else disabled) rather than an error, so the settings UI always has something to render. |






<a name="forgepoint-notification-v1-ListDeliveryAttemptsRequest"></a>

### ListDeliveryAttemptsRequest
============================================================================
ListDeliveryAttempts (the delivery-log audit view)
============================================================================

WHY THIS RPC EXISTS: the design doc &#43; implementation plan (Phase 9) call for a
`GetDeliveryLog` surface — the operator/owner view over the `delivery_log`
table: &#34;did my alerts actually go out, and which failed?&#34;. GetNotification
returns the attempts for ONE notification; this is the cross-notification
query the plan asks for (&#34;show me all my FAILED webhook deliveries in the last
hour&#34;). It is the delivery-health counterpart to the inbox list. Folding it
into GetNotification would not answer the fleet question, so it is its own RPC.

SECURITY / AUTHORITY: scoped to the AUTHENTICATED caller&#39;s own notifications —
there is no recipient_user_id field (same anti-IDOR rule as the inbox). The
status/channel filters are query predicates over the caller&#39;s own rows only.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| channel | [NotificationChannel](#forgepoint-notification-v1-NotificationChannel) |  | Optional: restrict to attempts on a single channel (e.g. only WEBHOOK). UNSPECIFIED = all channels. |
| status | [DeliveryStatus](#forgepoint-notification-v1-DeliveryStatus) |  | Optional: restrict to a single outcome (e.g. only FAILED, to triage broken endpoints). UNSPECIFIED = all statuses. |
| notification_id | [string](#string) |  | Optional: restrict to attempts for one of the caller&#39;s notifications. Empty = across all of the caller&#39;s notifications. If set, the server verifies the notification belongs to the caller (NOT_FOUND otherwise — no existence leak). |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination (shared shape, see common.proto). Server caps page_size at 100 and defaults it to 20, enforced server-side regardless of the requested value — a client cannot demand an unbounded page. |






<a name="forgepoint-notification-v1-ListDeliveryAttemptsResponse"></a>

### ListDeliveryAttemptsResponse
ListDeliveryAttemptsResponse returns one page of the caller&#39;s delivery log,
newest attempt first. Each row carries enough to debug delivery health without
echoing the target URL or any secret (see DeliveryAttempt&#39;s SECURITY note).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| attempts | [DeliveryAttempt](#forgepoint-notification-v1-DeliveryAttempt) | repeated | The page of delivery attempts (the delivery_log rows), newest first. |
| notification_ids | [string](#string) | repeated | Which notification each attempt belongs to, positionally aligned 1:1 with `attempts` (attempts[i] is for notification_ids[i]). WHY a parallel slice rather than embedding the id on DeliveryAttempt: DeliveryAttempt is also returned by GetNotification where the notification id is already known/implied, so we keep the id off the attempt message and surface it here where the fleet view needs it. Lets the UI link each row back to its inbox entry. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata: next_page_token &#43; total_count. See common.proto. |






<a name="forgepoint-notification-v1-ListNotificationsRequest"></a>

### ListNotificationsRequest
ListNotificationsRequest fetches the authenticated caller&#39;s inbox page.
WHY there is no `user_id` field: the inbox is ALWAYS scoped to the caller,
resolved from TokenClaims by the auth interceptor. Accepting a user_id here
would be a mass-assignment / IDOR hole (one user reading another&#39;s inbox), so
it is deliberately absent. Admin &#34;read any inbox&#34; tooling, if ever needed,
would be a separate explicitly-authorized RPC — not a field on this one.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| unread_only | [bool](#bool) |  | Optional: if true, return only unread notifications. Default false = all. |
| min_severity | [NotificationSeverity](#forgepoint-notification-v1-NotificationSeverity) |  | Optional: return only notifications at or above this severity. UNSPECIFIED = no severity floor. |
| event_type_filter | [string](#string) |  | Optional: restrict to a single event type (exact match, e.g. &#34;fp.pipelines.failed&#34;). Empty = all event types. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination (shared shape, see common.proto). Server caps page_size at 100 and defaults it to 20 (documented in common.proto). The server enforces the cap regardless of what the client sends, so a client cannot demand an unbounded page. |






<a name="forgepoint-notification-v1-ListNotificationsResponse"></a>

### ListNotificationsResponse
ListNotificationsResponse returns one page of the caller&#39;s inbox, newest first.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notifications | [Notification](#forgepoint-notification-v1-Notification) | repeated | The page of notifications. Ordered by created_at descending (newest first) so the most recent alerts are at the top of the inbox. |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata: next_page_token &#43; total_count. See common.proto. |
| unread_count | [int32](#int32) |  | Convenience: total UNREAD count across the whole inbox (not just this page), for the UI&#39;s badge. Cheap to compute with an indexed COUNT and saves the UI a second round-trip. -1 if not computed. |






<a name="forgepoint-notification-v1-MarkReadRequest"></a>

### MarkReadRequest
MarkReadRequest marks one or more of the caller&#39;s notifications as read.
WHY accept a repeated id (batch) rather than a single id: the UI almost always
marks read in bulk (&#34;mark all on screen as read&#34; / &#34;mark this thread read&#34;),
and a batch RPC avoids N round-trips. A single-id mark is just a list of one.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ids | [string](#string) | repeated | The notification ids to mark read. All must belong to the caller; ids that don&#39;t (or don&#39;t exist) are ignored rather than failing the whole batch, so a partially-stale client list still makes progress. Max 1000 ids per call (server-enforced) to bound the write. |
| mark_all | [bool](#bool) |  | Optional escape hatch: if true, mark the caller&#39;s ENTIRE inbox read and ignore `ids`. WHY a flag rather than a separate MarkAllRead RPC: it&#39;s the same operation with a different selector; one RPC keeps the surface small. |






<a name="forgepoint-notification-v1-MarkReadResponse"></a>

### MarkReadResponse
MarkReadResponse reports how many notifications were actually flipped to read.
WHY return a count instead of an empty response: the UI uses it to update the
unread badge without a follow-up List call, and it confirms how many of a
batch were valid (since unknown/foreign ids are silently skipped).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| marked_count | [int32](#int32) |  | Number of notifications transitioned from unread → read by this call. |
| unread_count | [int32](#int32) |  | The caller&#39;s remaining unread count after this operation (for the badge). |






<a name="forgepoint-notification-v1-Notification"></a>

### Notification
============================================================================
Notification
============================================================================

WHY this is the central read model: a Notification is the materialized,
human-facing record produced when an inbound platform event matched a user&#39;s
preferences. It is what the inbox (List/Get) returns and what MarkRead
mutates. Think of it as one row in GitHub&#39;s notification center.

PROVENANCE FIELDS (event_id, event_type, source_service): we deliberately keep
a back-pointer to the originating EventEnvelope. WHY: it makes notifications
debuggable and traceable end-to-end (&#34;this Slack alert came from event
&lt;uuid&gt; of type fp.pipelines.failed produced by the pipeline service&#34;). It also
powers idempotency on the consumer — see the consumer note in the service doc.

SECURITY / AUTHORITY: every field here is SERVER-AUTHORITATIVE. The recipient,
severity, status, timestamps, and provenance are all derived from the event
and the matching engine — there is no client write path that sets them. The
inbox is read-mostly; the only mutation a client can perform is MarkRead.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Stable identifier for this inbox entry. Used by GetNotification and MarkRead. NOT the same as the originating event_id (see below). |
| recipient_user_id | [string](#string) |  | The user this notification was delivered to. Server-set from the matched preference&#39;s owner — NEVER taken from a request body. The List RPC scopes results to the authenticated caller (from TokenClaims), so one user can never read another user&#39;s inbox. |
| title | [string](#string) |  | Short, human-readable headline for the inbox row. E.g. &#34;Pipeline &#39;nightly-retrain&#39; failed at step &#39;train&#39;&#34;. |
| body | [string](#string) |  | Longer human-readable body. Rendered server-side from the event payload so the inbox doesn&#39;t need per-event-type rendering logic on the client. |
| severity | [NotificationSeverity](#forgepoint-notification-v1-NotificationSeverity) |  | Server-derived severity (see NotificationSeverity). Drives sorting/badging and preference-based suppression. Client cannot set this. |
| channels | [NotificationChannel](#forgepoint-notification-v1-NotificationChannel) | repeated | The channels this notification was (or will be) delivered over. A single notification commonly includes IN_APP plus zero or more fan-out channels depending on the recipient&#39;s preferences. See DeliveryAttempt for the per-channel outcome. |
| read | [bool](#bool) |  | Whether the user has read this notification in the inbox. Flipped by MarkRead. Server-owned; defaults false on creation. |
| event_id | [string](#string) |  | PROVENANCE — the EventEnvelope.id (common.proto) that triggered this. Doubles as the idempotency anchor: re-processing the same event_id for the same recipient must NOT create a duplicate inbox row (idempotent consumer). |
| event_type | [string](#string) |  | PROVENANCE — the EventEnvelope.type, e.g. &#34;fp.pipelines.failed&#34;. This is the exact value the matching engine pattern-matched against. Surfaced so the UI can group/filter (&#34;show me all drift alerts&#34;). |
| source_service | [string](#string) |  | PROVENANCE — the producing service (&#34;pipeline&#34;, &#34;billing&#34;, &#34;model-monitor&#34;). From EventEnvelope.source. Useful for filtering and for support triage. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the notification was created (i.e. when the matching event was processed). Server clock. Immutable. |
| read_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the user marked it read (unset while unread). Server clock. |






<a name="forgepoint-notification-v1-NotificationPreferences"></a>

### NotificationPreferences
============================================================================
NotificationPreferences
============================================================================

WHY a single per-user preferences aggregate: a user configures their channels
together (&#34;enable Slack for errors, email for criticals only&#34;). Returning and
updating them as one document keeps the API and the UI simple (one settings
page = one GetPreferences &#43; one UpdatePreferences), and makes the update
atomic — no partial half-saved state across separate per-channel RPCs.

This aggregate is also WHERE THE CHOREOGRAPHY ROUTING LIVES from the user&#39;s
point of view: the matching engine, on each inbound event, looks up the
recipient&#39;s preferences to decide which channels fire and at what severity
floor. (The mapping from event type → recipient set is the platform&#39;s concern;
preferences answer &#34;given that this user is a recipient, how do they want it?&#34;)
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user_id | [string](#string) |  | The user these preferences belong to. Server-set from the authenticated caller on read/write — NEVER taken from the request body (prevents one user from editing another&#39;s preferences via mass-assignment). |
| channels | [ChannelPreference](#forgepoint-notification-v1-ChannelPreference) | repeated | Per-channel settings. Typically one entry per NotificationChannel the user has configured. IN_APP is implicitly always-on even if absent here. |
| muted_event_patterns | [string](#string) | repeated | Optional event-type patterns the user wants to MUTE entirely, across all channels. Same wildcard grammar as the matching engine (e.g. &#34;fp.inference.*&#34; to silence all inference chatter). An empty list means &#34;mute nothing&#34;. WHY here and not a separate RPC: muting is just another preference; keeping it in the aggregate preserves the atomic single-document update. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When these preferences were last updated. Server clock. Lets the UI show &#34;last changed&#34; and supports optimistic-concurrency checks if added later. |






<a name="forgepoint-notification-v1-TestChannelRequest"></a>

### TestChannelRequest
============================================================================
TestChannel (owner self-test — does NOT violate choreography)
============================================================================

WHY THIS RPC EXISTS: configuring a webhook/Slack URL is error-prone (wrong URL,
expired token, firewall). Every real notification product (PagerDuty, GitHub,
Grafana) lets you &#34;send a test&#34; to verify a channel works before relying on it.

WHY IT DOESN&#39;T BREAK CHOREOGRAPHY: choreography forbids a PRODUCER addressing
us to make us notify a THIRD party. This is the opposite: the OWNER asks us to
deliver a synthetic test to THEIR OWN configured channel. No platform producer
is involved, no other user is targeted, and it drives no business workflow. It
is a control-plane self-check, exactly like GetPreferences — so it belongs on
the gRPC surface, not the event bus.

SECURITY: the test is delivered to the channel CONFIG of the AUTHENTICATED
caller (resolved from their stored preferences / TokenClaims) — the request
does NOT carry a free-form target, so it cannot be turned into an SSRF probe of
arbitrary URLs. The stored target was already SSRF-validated on write, and the
delivery path re-validates with a pinned-IP connect (the same guard as a real
delivery). The caller can only test channels they own.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| channel | [NotificationChannel](#forgepoint-notification-v1-NotificationChannel) |  | Which of the caller&#39;s configured channels to send the synthetic test over. Must correspond to an enabled ChannelPreference the caller owns (else FAILED_PRECONDITION). IN_APP testing is a no-op success (the inbox is always reachable). NEVER carries a URL — the destination is the stored, already- validated target, so this RPC is not an SSRF vector. |
| idempotency_key | [string](#string) |  | Caller-supplied idempotency key (typically a UUID). WHY on a test-send: a client that retries after a timeout must not double-fire the test (spamming the user&#39;s Slack). Same key → the server returns the first attempt&#39;s result instead of sending again. The Stripe idempotency-key pattern. Optional but recommended for automated callers / CLI retries. |






<a name="forgepoint-notification-v1-TestChannelResponse"></a>

### TestChannelResponse
TestChannelResponse reports the synthetic delivery&#39;s outcome so the settings UI
can show &#34;✓ delivered&#34; / &#34;✗ 401 from Slack&#34; inline next to the channel.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [DeliveryStatus](#forgepoint-notification-v1-DeliveryStatus) |  | Outcome of the test delivery (DELIVERED on success; FAILED with detail below on error; SUPPRESSED if the channel was disabled). |
| response_code | [int32](#int32) |  | For HTTP channels: the response status code observed (0 if none / non-HTTP). |
| error_message | [string](#string) |  | Human-readable failure detail on error (e.g. &#34;401 from Slack&#34;, &#34;connection refused&#34;, &#34;target failed SSRF revalidation&#34;). Empty on success. Mirrors DeliveryAttempt.error_message — for the operator, not end-user copy. |






<a name="forgepoint-notification-v1-UpdatePreferencesRequest"></a>

### UpdatePreferencesRequest
UpdatePreferencesRequest replaces the caller&#39;s preferences.
WHY full-document replace (PUT semantics) rather than a partial patch: the
settings UI loads the whole preferences document, the user edits it, and saves
it back — sending the full desired state is the simplest correct model and
avoids ambiguous &#34;is an absent channel a delete or untouched?&#34; patch
semantics. If granular patching is ever needed, a field_mask can be added.

SECURITY: the request carries the desired ChannelPreference set but NOT a
user_id — the target user is ALWAYS the authenticated caller (anti
mass-assignment). updated_at is also ignored if present; the server stamps it.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| channels | [ChannelPreference](#forgepoint-notification-v1-ChannelPreference) | repeated | The full desired per-channel preference set. Replaces the stored set wholesale. An omitted channel is treated as &#34;not configured&#34; (i.e. removed), matching PUT semantics. The server validates EVERY target before persisting: email must be well-formed; webhook/slack URLs must pass the full SSRF guard documented on ChannelPreference.target (https-only, Slack-host allowlist, private/link-local/loopback IP denylist with pinned-IP connect). A target that fails validation rejects the whole update with INVALID_ARGUMENT &#43; a field-level ErrorDetail (so a malicious URL never reaches storage), rather than silently dropping one channel. |
| muted_event_patterns | [string](#string) | repeated | The full desired mute list (event-type patterns). Replaces the stored list. Empty = mute nothing. Server-bounded: at most 100 patterns (a runaway mute list is both a storage-abuse vector and a sign of misuse — real muting needs a handful of patterns, not thousands). |
| idempotency_key | [string](#string) |  | Caller-supplied idempotency key (typically a UUID). WHY on a settings write: a flaky client that retries an UpdatePreferences after a timeout must not risk a confusing double-apply or clobber a concurrent edit. Same key → server returns the result of the first apply instead of re-applying. This is the Stripe idempotency-key pattern; here it mainly guards retry safety on a last-write-wins document. Optional but recommended for automated callers. |






<a name="forgepoint-notification-v1-UpdatePreferencesResponse"></a>

### UpdatePreferencesResponse
UpdatePreferencesResponse returns the stored preferences after the update, so
the client can confirm exactly what was persisted (including the server&#39;s
fresh updated_at and any normalization the server applied to targets).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| preferences | [NotificationPreferences](#forgepoint-notification-v1-NotificationPreferences) |  | The preferences as now stored. Reflects server normalization/defaults. |





 


<a name="forgepoint-notification-v1-DeliveryStatus"></a>

### DeliveryStatus
============================================================================
DeliveryStatus
============================================================================

WHY a per-notification delivery status enum: a single logical notification can
fan out to several channels, each of which can independently succeed, fail, or
still be retrying. The inbox needs to surface &#34;did my Slack alert actually go
out?&#34; The status is owned entirely by the delivery layer (server-authoritative)
and reported back on Notification reads; it is never accepted on a write.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| DELIVERY_STATUS_UNSPECIFIED | 0 |  |
| DELIVERY_STATUS_PENDING | 1 | Accepted into the system, not yet attempted on a fan-out channel. |
| DELIVERY_STATUS_RETRYING | 2 | Currently being retried (within the exponential-backoff window). |
| DELIVERY_STATUS_DELIVERED | 3 | Successfully delivered on the channel (e.g. webhook returned 2xx). |
| DELIVERY_STATUS_FAILED | 4 | Permanently failed after exhausting retries / circuit-breaker open. |
| DELIVERY_STATUS_SUPPRESSED | 5 | Suppressed by the user&#39;s preferences (e.g. severity below their threshold, or quiet hours). Recorded so the inbox can show &#34;we chose not to page you&#34;. |



<a name="forgepoint-notification-v1-NotificationChannel"></a>

### NotificationChannel
============================================================================
NotificationChannel
============================================================================

WHY an enum (not a free string): the set of delivery transports is small,
closed, and security-sensitive (each channel has its own config &#43; secret
handling). An enum gives us exhaustive switch handling in Go and prevents a
caller from inventing a channel the delivery layer can&#39;t service.

CHANNELS:
  IN_APP  — the platform web inbox (List/Get/MarkRead). Always available, no
            external config, no secrets. This is the default &#34;channel&#34; for
            every Notification row even when a fan-out channel also fires.
  WEBHOOK — HTTP POST of the event payload to a user-configured URL. Delivery
            uses retries &#43; a per-URL circuit breaker (see implementation plan
            Task 9.4) so a dead endpoint doesn&#39;t get hammered.
  SLACK   — POST to a Slack Incoming Webhook URL (a specialization of WEBHOOK
            with Slack&#39;s message JSON shape).
  EMAIL   — SMTP delivery. May be mocked in non-prod (per the plan).

Buf STANDARD requires: zero value suffixed _UNSPECIFIED; every value prefixed
with the enum name (ENUM_VALUE_PREFIX &#43; ENUM_ZERO_VALUE_SUFFIX).
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| NOTIFICATION_CHANNEL_UNSPECIFIED | 0 | Default/unknown. A well-formed preference never uses this; it exists so the wire&#39;s zero value is explicitly &#34;not set&#34; rather than a meaningful channel. |
| NOTIFICATION_CHANNEL_IN_APP | 1 | Platform web inbox (the List/Get/MarkRead surface in this proto). |
| NOTIFICATION_CHANNEL_WEBHOOK | 2 | Generic HTTP POST to a caller-supplied URL. |
| NOTIFICATION_CHANNEL_SLACK | 3 | Slack Incoming Webhook (Slack-shaped JSON over an HTTP POST). |
| NOTIFICATION_CHANNEL_EMAIL | 4 | Email via SMTP. |



<a name="forgepoint-notification-v1-NotificationSeverity"></a>

### NotificationSeverity
============================================================================
NotificationSeverity
============================================================================

WHY: A coarse priority lets the inbox UI sort/badge notifications and lets
preferences gate noise (e.g. &#34;only EMAIL me for ERROR and above&#34;). The
severity is derived SERVER-SIDE from the event type — a `*.failed` or
`*.drift.detected` event maps to ERROR/WARNING; informational lifecycle
events map to INFO. Clients never set this (it&#39;s not on any write request).

We keep this intentionally small (INFO/WARNING/ERROR/CRITICAL) — the classic
syslog-lite ladder — because a finer scale invites bikeshedding and is hard
to map consistently from arbitrary event types.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| NOTIFICATION_SEVERITY_UNSPECIFIED | 0 |  |
| NOTIFICATION_SEVERITY_INFO | 1 | Routine lifecycle signal (e.g. pipeline started). Low attention. |
| NOTIFICATION_SEVERITY_WARNING | 2 | Something to look at soon (e.g. mild drift, approaching quota). |
| NOTIFICATION_SEVERITY_ERROR | 3 | A failure occurred (pipeline failed, delivery failed, quota exceeded). |
| NOTIFICATION_SEVERITY_CRITICAL | 4 | Page-someone-now class (e.g. production model drift past the hard threshold, canary failure during a promotion). Highest attention. |


 

 


<a name="forgepoint-notification-v1-NotificationService"></a>

### NotificationService
============================================================================
NOTIFICATION SERVICE
============================================================================

WHY a single small NotificationService: as explained at the top, this is the
CONTROL PLANE only. The real engine — subscribe to `fp.&gt;`, match preferences,
deliver with retries &#43; circuit breaker, write the delivery log — runs on the
NATS path and exposes NOTHING synchronous. There is intentionally no
&#34;send&#34;/&#34;trigger&#34; RPC: a producer calling us would break choreography.

RPC CATEGORIES:
  INBOX (read model):   ListNotifications, GetNotification, MarkRead
  DELIVERY LOG (audit): ListDeliveryAttempts
  PREFERENCES (config): GetPreferences, UpdatePreferences, TestChannel
  (TestChannel is a control-plane self-check, not a &#34;send&#34; RPC — see its doc.)

WHY NO STREAMING RPC HERE (a deliberate choice):
  A live &#34;watch my inbox&#34; stream is tempting, but the natural realtime path
  for notifications is the very NATS stream this service already consumes and
  the fan-out channels (web push, Slack) it already delivers over. The web UI
  gets realtime via the BFF bridging `fp.notifications.&gt;` to Server-Sent
  Events — NOT via a gRPC server-stream on this service. So the gRPC surface
  stays request/response (inbox &#43; settings), and we avoid maintaining
  long-lived per-client streams on a service whose whole point is to be a
  stateless event reactor. Contrast: the Pipeline Orchestrator DOES expose a
  WatchExecution server-stream, because there the authoritative state lives in
  that service and a client genuinely needs to follow one execution&#39;s lifecycle.

IDEMPOTENCY (on BOTH paths):
  - Async consumer: events can be redelivered (NATS at-least-once). The
    consumer dedupes on EventEnvelope.id (carried into Notification.event_id)
    per recipient, so a redelivered event never creates a duplicate inbox row
    or a duplicate delivery. This is the &#34;idempotent consumer&#34; platform rule.
  - Sync mutation: UpdatePreferences takes an idempotency_key so client retries
    are safe. MarkRead is naturally idempotent (read→read is a no-op), so it
    needs no key.
============================================================================

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListNotifications | [ListNotificationsRequest](#forgepoint-notification-v1-ListNotificationsRequest) | [ListNotificationsResponse](#forgepoint-notification-v1-ListNotificationsResponse) | ListNotifications returns a paginated page of the AUTHENTICATED caller&#39;s inbox (newest first), with optional unread/severity/event-type filters and an unread_count for the UI badge. Scoped to the caller — never another user. |
| GetNotification | [GetNotificationRequest](#forgepoint-notification-v1-GetNotificationRequest) | [GetNotificationResponse](#forgepoint-notification-v1-GetNotificationResponse) | GetNotification fetches one of the caller&#39;s notifications in full, including its per-channel delivery attempt history (the delivery_log). Returns NOT_FOUND for ids that don&#39;t exist OR belong to another user (so existence of another user&#39;s notification isn&#39;t leaked). |
| MarkRead | [MarkReadRequest](#forgepoint-notification-v1-MarkReadRequest) | [MarkReadResponse](#forgepoint-notification-v1-MarkReadResponse) | MarkRead marks one, many, or all of the caller&#39;s notifications as read and returns the new unread count. Naturally idempotent (marking a read item read is a no-op), so no idempotency key is required. Foreign/unknown ids are skipped rather than failing the batch. |
| ListDeliveryAttempts | [ListDeliveryAttemptsRequest](#forgepoint-notification-v1-ListDeliveryAttemptsRequest) | [ListDeliveryAttemptsResponse](#forgepoint-notification-v1-ListDeliveryAttemptsResponse) | ListDeliveryAttempts returns a paginated page of the caller&#39;s delivery log (the GetDeliveryLog surface the plan calls for) — the cross-notification view for &#34;did my alerts go out, which failed?&#34;, with optional channel/status/ notification filters. Scoped to the caller; secrets/targets are never echoed. |
| GetPreferences | [GetPreferencesRequest](#forgepoint-notification-v1-GetPreferencesRequest) | [GetPreferencesResponse](#forgepoint-notification-v1-GetPreferencesResponse) | GetPreferences returns the caller&#39;s notification preferences (per-channel settings &#43; mute patterns), falling back to sensible defaults if the user has never configured any. Always the caller&#39;s own preferences. |
| UpdatePreferences | [UpdatePreferencesRequest](#forgepoint-notification-v1-UpdatePreferencesRequest) | [UpdatePreferencesResponse](#forgepoint-notification-v1-UpdatePreferencesResponse) | UpdatePreferences replaces the caller&#39;s preferences wholesale (PUT semantics) and returns the stored result. Accepts an optional idempotency key so retries are safe. The target user is always the caller — the request body cannot reassign ownership. Every webhook/slack target is SSRF-validated server-side before persisting (see ChannelPreference.target). |
| TestChannel | [TestChannelRequest](#forgepoint-notification-v1-TestChannelRequest) | [TestChannelResponse](#forgepoint-notification-v1-TestChannelResponse) | TestChannel sends a synthetic test notification to one of the CALLER&#39;S OWN configured channels so the settings UI can confirm it works. This is a control-plane self-check (the owner testing their own delivery config), NOT a producer-facing &#34;send&#34; RPC — it carries no target URL and addresses no other user, so it neither violates choreography nor opens an SSRF vector. Accepts an idempotency key so a retry doesn&#39;t double-fire the test. |

 



<a name="forgepoint_pipeline_v1_pipeline-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/pipeline/v1/pipeline.proto



<a name="forgepoint-pipeline-v1-CancelExecutionRequest"></a>

### CancelExecutionRequest
CancelExecutionRequest requests a graceful stop of an in-flight run.

WHY cancel returns the Execution (not an empty response): cancellation is
ASYNCHRONOUS — it transitions the run to COMPENSATING (undo completed steps)
and only later to CANCELLED. Returning the Execution lets the caller see the
immediate transition (e.g., RUNNING → COMPENSATING) and then Watch it settle.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  | UUID of the execution to cancel. |
| reason | [string](#string) |  | Optional human-readable reason, recorded for audit (&#34;superseded by retrain&#34;). |






<a name="forgepoint-pipeline-v1-CancelExecutionResponse"></a>

### CancelExecutionResponse
CancelExecutionResponse wraps the (now COMPENSATING/CANCELLED) Execution.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution | [Execution](#forgepoint-pipeline-v1-Execution) |  |  |






<a name="forgepoint-pipeline-v1-CreatePipelineRequest"></a>

### CreatePipelineRequest
CreatePipelineRequest authors a new pipeline template.

SECURITY — anti-mass-assignment: we deliberately ACCEPT ONLY the
user-authorable fields (name, type, steps). We do NOT accept id, created_by,
created_at, or team — those are server-authoritative and derived from the
authenticated principal. This prevents a caller from forging ownership or
timestamps. (Same discipline as auth.proto&#39;s CreateUserRequest.)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Pipeline name. Must be unique within the caller&#39;s team. |
| type | [PipelineType](#forgepoint-pipeline-v1-PipelineType) |  | Execution strategy (saga / DAG / batch). |
| steps | [StepDefinition](#forgepoint-pipeline-v1-StepDefinition) | repeated | The step graph. The server validates: known StepTypes, depends_on refers to real step ids, no cycles (DAG), compensation_step_id refers to real steps. BATCH-SIZE CAP (contract, not just prose): the server REJECTS a definition with more than 256 steps (INVALID_ARGUMENT). A pipeline graph this large is almost always a mistake, and an unbounded graph is a DoS vector (the engine persists a checkpoint row per step and topo-sorts the whole set). 256 is far above any real ML workflow yet bounds the work the server commits to. |
| idempotency_key | [string](#string) |  | Caller-supplied idempotency key (typically a UUID) for the CREATE itself. WHY mutating-create idempotency: a client retry after a network blip must not create two pipelines with the same name (which would then fail the uniqueness check confusingly, or — worse, under a race — both succeed). Same key → the SAME PipelineDefinition is returned, never a duplicate. Empty = no dedup (interactive one-off). Mirrors TriggerExecution&#39;s idempotency_key and the Stripe idempotency-key pattern used across the platform. |






<a name="forgepoint-pipeline-v1-CreatePipelineResponse"></a>

### CreatePipelineResponse
CreatePipelineResponse wraps the created PipelineDefinition.
WHY wrap (not return PipelineDefinition directly): Buf STANDARD
(RPC_RESPONSE_STANDARD_NAME) requires &#34;&lt;Rpc&gt;Response&#34;; wrapping is also
forward-compatible (we can add e.g. validation_warnings later without
touching the shared PipelineDefinition domain message).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipeline | [PipelineDefinition](#forgepoint-pipeline-v1-PipelineDefinition) |  | The newly created pipeline. id/created_by/created_at populated by server. |






<a name="forgepoint-pipeline-v1-DeletePipelineRequest"></a>

### DeletePipelineRequest
DeletePipelineRequest removes a pipeline TEMPLATE.

WHY a dedicated RPC &#43; WHY soft-delete: operators need to retire obsolete
templates (the design/CLI call for a Delete). We SOFT-DELETE (archive) rather
than hard-delete: past Executions reference this pipeline_id for audit/lineage,
and hard-deleting would dangle those foreign keys and erase the history of what
ran. An archived template is hidden from ListPipelines and cannot be triggered,
but GetExecution on its historical runs still resolves the pipeline name.

SAFETY: the server REJECTS deletion while the pipeline has a non-terminal
Execution (PENDING/RUNNING/COMPENSATING) — you cannot retire a template whose
saga is mid-flight (FAILED_PRECONDITION). Cancel those runs first.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipeline_id | [string](#string) |  | UUID of the pipeline to archive. |






<a name="forgepoint-pipeline-v1-DeletePipelineResponse"></a>

### DeletePipelineResponse
DeletePipelineResponse is an intentionally-empty, dedicated response.
WHY a named empty message (not google.protobuf.Empty): Buf STANDARD
(RPC_RESPONSE_STANDARD_NAME) requires a &lt;Rpc&gt;Response type, and a named message
is forward-compatible — we can add fields later (e.g. archived_at) without a
breaking signature change. google.protobuf.Empty can never grow a field.






<a name="forgepoint-pipeline-v1-Execution"></a>

### Execution
============================================================================
Execution
============================================================================

WHY: A single run of a PipelineDefinition — the saga/DAG &#34;process&#34; with its
own status, timeline, and step records. This is the primary object clients
poll (GetExecution), stream (WatchExecution), and list (ListExecutions).

SECURITY: every field here is SERVER-AUTHORITATIVE. Clients never write an
Execution directly; they only TriggerExecution (which references a pipeline
id &#43; input) and the server constructs/owns the Execution. status, timestamps,
current_step, and triggered_by cannot be set by the client.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Server-assigned, immutable. |
| pipeline_id | [string](#string) |  | The PipelineDefinition this run instantiates. |
| status | [ExecutionStatus](#forgepoint-pipeline-v1-ExecutionStatus) |  | The saga/DAG state machine value (see ExecutionStatus). |
| current_step | [string](#string) |  | The StepDefinition.id currently executing (or last executed). For a DAG with parallelism this is the most-recently-scheduled step — a coarse &#34;where are we&#34; pointer; the full picture is in `step_executions`. |
| step_executions | [StepExecution](#forgepoint-pipeline-v1-StepExecution) | repeated | Per-step runtime records. Populated on GetExecution/WatchExecution so the caller can render the full timeline without N extra calls. |
| triggered_by | [string](#string) |  | SERVER-AUTHORITATIVE: who/what triggered this run. May be a user_id (manual trigger) or a service identity (e.g., &#34;model-monitor&#34; for auto-retrain). Derived from the authenticated caller, never from request input. |
| started_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | SERVER-AUTHORITATIVE: when the run started. |
| completed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | SERVER-AUTHORITATIVE: when the run reached a terminal state. Unset while PENDING/RUNNING/COMPENSATING. |
| input | [google.protobuf.Struct](#google-protobuf-Struct) |  | The input payload the run was triggered with (e.g., the model_id to deploy, or a drift report for auto-retrain). Echoed back for traceability. Struct because input shape varies by pipeline. SECURITY: no secrets/PII here. |
| error | [string](#string) |  | Human-readable failure reason when status is FAILED. Empty otherwise. Summarizes the failing step&#39;s error for quick triage. |






<a name="forgepoint-pipeline-v1-GetExecutionRequest"></a>

### GetExecutionRequest
GetExecutionRequest fetches the current state of one run (point-in-time poll).
WHY a poll RPC alongside the WatchExecution stream: not every caller wants a
long-lived stream (e.g., the CLI `fp pipeline status &lt;id&gt;` does a single
fetch; a load balancer health probe can&#39;t hold a stream). Get is the cheap,
stateless read; Watch is for live UIs.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  | UUID of the execution to fetch. |






<a name="forgepoint-pipeline-v1-GetExecutionResponse"></a>

### GetExecutionResponse
GetExecutionResponse wraps the Execution (incl. its step_executions).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution | [Execution](#forgepoint-pipeline-v1-Execution) |  |  |






<a name="forgepoint-pipeline-v1-GetPipelineRequest"></a>

### GetPipelineRequest
GetPipelineRequest fetches ONE pipeline TEMPLATE by id.
WHY this RPC exists (it was missing): the CLI (`fp pipeline get &lt;id&gt;`), the web
UI&#39;s pipeline editor, and any client that wants to inspect a template&#39;s full
step graph before triggering it need a by-id read. ListPipelines is for
browsing; this is the point lookup. The server scopes the read to the caller&#39;s
team/RBAC (a caller cannot fetch another team&#39;s pipeline by guessing its id).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipeline_id | [string](#string) |  | UUID of the pipeline definition to fetch. |






<a name="forgepoint-pipeline-v1-GetPipelineResponse"></a>

### GetPipelineResponse
GetPipelineResponse wraps the PipelineDefinition (incl. its full step graph).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipeline | [PipelineDefinition](#forgepoint-pipeline-v1-PipelineDefinition) |  |  |






<a name="forgepoint-pipeline-v1-ListExecutionsRequest"></a>

### ListExecutionsRequest
ListExecutionsRequest returns a paginated, filterable list of runs.

PAGINATION: reuses forgepoint.common.v1.PaginationRequest for consistency
across every list RPC on the platform (cursor-based; see common.proto for the
WHY). SECURITY: the server CAPS page_size (default 20, max 100) regardless of
the requested value, to prevent a client from requesting an unbounded page.

TENANCY (anti-IDOR): there is deliberately NO team field on this request. The
result set is ALWAYS scoped to the caller&#39;s team, derived SERVER-SIDE from the
auth token claims — never from a client-supplied tenancy field. A client cannot
list another team&#39;s executions by passing a different team. The pipeline_id /
status_filter below only NARROW within that authorized scope.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipeline_id | [string](#string) |  | Optional: only runs of this pipeline. Empty = all pipelines the caller&#39;s team owns. A pipeline_id from another team yields an empty page (scope-filtered, not an error — we don&#39;t confirm/deny existence of out-of-scope ids). |
| status_filter | [ExecutionStatus](#forgepoint-pipeline-v1-ExecutionStatus) |  | Optional: only runs in this status (e.g., FAILED for a triage dashboard). UNSPECIFIED = any status. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20, capped at 100 server-side. |






<a name="forgepoint-pipeline-v1-ListExecutionsResponse"></a>

### ListExecutionsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executions | [Execution](#forgepoint-pipeline-v1-Execution) | repeated | The page of executions. NOTE: list items omit the per-step detail (step_executions) to keep the page lightweight — fetch GetExecution for the full timeline. (Documented contract; the server populates summary fields.) |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata: next_page_token &#43; total_count. |






<a name="forgepoint-pipeline-v1-ListPipelinesRequest"></a>

### ListPipelinesRequest
ListPipelinesRequest returns a paginated list of pipeline TEMPLATES (not
runs). Same pagination/cap discipline as ListExecutions (page_size default 20,
capped 100 server-side). TENANCY: like ListExecutions, results are scoped to
the caller&#39;s team from auth claims — there is no client-supplied team field.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type_filter | [PipelineType](#forgepoint-pipeline-v1-PipelineType) |  | Optional: only pipelines of this type (e.g., only TRAINING_DAG). UNSPECIFIED = any type. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination. page_size defaults to 20, capped at 100 server-side. |






<a name="forgepoint-pipeline-v1-ListPipelinesResponse"></a>

### ListPipelinesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipelines | [PipelineDefinition](#forgepoint-pipeline-v1-PipelineDefinition) | repeated | The page of pipeline definitions matching the request (within team/RBAC scope). |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | Pagination metadata. |






<a name="forgepoint-pipeline-v1-PipelineDefinition"></a>

### PipelineDefinition
============================================================================
PipelineDefinition
============================================================================

WHY: The reusable template a user authors once and triggers many times. It is
the &#34;program&#34;; an Execution is a &#34;process&#34; running that program.

SECURITY — SERVER-AUTHORITATIVE FIELDS:
  `id`, `created_by`, `created_at` are set by the SERVER, never accepted on
  CreatePipeline (see CreatePipelineRequest). Accepting created_by from the
  client would be a mass-assignment / identity-spoofing hole — a caller could
  author a pipeline &#34;as&#34; another user. created_by is derived from the
  authenticated principal (TokenClaims) in the auth interceptor.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Server-assigned, immutable. Primary key. |
| name | [string](#string) |  | Human-readable pipeline name (e.g., &#34;fraud-model-deploy&#34;). Unique per team. |
| type | [PipelineType](#forgepoint-pipeline-v1-PipelineType) |  | Saga vs DAG vs batch — selects the execution strategy. |
| steps | [StepDefinition](#forgepoint-pipeline-v1-StepDefinition) | repeated | The ordered/graph set of steps. For a saga this is effectively a sequence; for a DAG the depends_on edges define the real shape. |
| created_by | [string](#string) |  | SERVER-AUTHORITATIVE: user_id of the authenticated creator (from token claims, not request input). Used for audit and ownership/RBAC checks. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | SERVER-AUTHORITATIVE: when the definition was created. Immutable. |
| team | [string](#string) |  | Team that owns this pipeline (namespacing for list/RBAC). Server-derived from the creator&#39;s token claims, not free-form client input. |






<a name="forgepoint-pipeline-v1-StepDefinition"></a>

### StepDefinition
============================================================================
StepDefinition
============================================================================

WHY a &#34;definition&#34; vs an &#34;execution&#34;: a StepDefinition is the STATIC template
(part of a PipelineDefinition) — it describes what a step IS. A StepExecution
(below) is a RUNTIME instance — what happened when a step RAN. Same
definition can produce many executions (one per pipeline run). This
template/instance split is the same one Airflow draws between a DAG and a
DagRun, or Temporal between a Workflow and a WorkflowExecution.

THE TWO EDGES THAT POWER BOTH MODES:
  - `depends_on`            → DAG edges (topological sort, parallelism).
  - `compensation_step_id`  → saga undo pointer (what to run to roll back).

A single message carries both so one engine can run either mode; a saga
typically uses linear depends_on &#43; compensation pointers, a DAG uses rich
depends_on &#43; no compensation.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | Stable identifier for this step WITHIN its pipeline (e.g., &#34;deploy&#34;, &#34;train-fold-1&#34;). Referenced by other steps&#39; depends_on / compensation_step_id. Unique within a PipelineDefinition; NOT a global UUID. |
| name | [string](#string) |  | Human-readable name for UI/logs. Not required to be unique. |
| type | [StepType](#forgepoint-pipeline-v1-StepType) |  | What kind of work this step performs → selects the StepExecutor. |
| depends_on | [string](#string) | repeated | DAG edges: IDs of steps that must COMPLETE before this step may start. Empty = a root step (can start immediately). The engine topologically sorts on these and runs independent steps in parallel. A cycle here is rejected at CreatePipeline (DAGs are acyclic by definition). |
| compensation_step_id | [string](#string) |  | SAGA compensation pointer: the id of the step to run to UNDO this step if a later step fails. Empty = nothing to compensate (e.g., a read-only VALIDATE step). Example: a DEPLOY step&#39;s compensation_step_id points at a &#34;destroy-instance&#34; step. Compensation runs in REVERSE completion order. |
| config | [google.protobuf.Struct](#google-protobuf-Struct) |  | Free-form, step-specific configuration (e.g., canary traffic %, training hyperparameters, target model_id/version). Struct (not a typed message) because config shape varies per StepType and per CUSTOM step — typing every variant would bloat this proto and couple it to executor internals. SECURITY (untrusted input — executors MUST validate, never trust shape): - SSRF: config may name a target model_id/version, but it must NOT supply a raw serving URL/host/IP for the engine to call. The DEPLOY/CANARY executors RESOLVE the serving endpoint server-side (from the model version &#43; the K8s Service in fp-models) and only ever talk to that allowlisted, internally-constructed address. A client-supplied endpoint would be a server-side request forgery vector — rejected at validation. - Resource caps: numeric config (e.g. parallel fold count, retry budget) is clamped to engine maxima so a config can&#39;t request unbounded fan-out. - No secrets here: config is echoed in GetExecution responses and step events. |
| timeout | [google.protobuf.Duration](#google-protobuf-Duration) |  | Optional per-step timeout. If the step runs longer, the engine fails it (which, in a saga, triggers compensation). Zero/unset = use engine default. |
| max_retries | [int32](#int32) |  | Optional max retry attempts for transient failures before the step is declared FAILED. Zero = no retries (fail fast). Retries are how a saga tolerates flaky downstreams without immediately rolling everything back. |






<a name="forgepoint-pipeline-v1-StepExecution"></a>

### StepExecution
============================================================================
StepExecution
============================================================================

WHY: The runtime record of ONE step running within ONE execution. Persisted
to `step_executions` — this is THE durability checkpoint. On crash recovery
the engine reads the latest row per execution to resume from the last
completed step. Surfacing it in the API lets the UI render a live, per-step
timeline and lets operators see exactly where a saga is stuck.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4 of this step-run (distinct from StepDefinition.id, which is the template step id). One StepDefinition can yield many StepExecutions. |
| execution_id | [string](#string) |  | The Execution this step-run belongs to. |
| step_id | [string](#string) |  | The StepDefinition.id (template id) this run corresponds to. Lets the UI line up the runtime row with the static graph node. |
| status | [StepStatus](#forgepoint-pipeline-v1-StepStatus) |  | Current lifecycle state of this step (incl. compensation sub-states). |
| started_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this step started running. Unset while PENDING. |
| completed_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this step reached a terminal state. Unset while not terminal. |
| output | [google.protobuf.Struct](#google-protobuf-Struct) |  | Step OUTPUT, passed as input to dependent steps (DAG data flow) — e.g., a TRAIN step emits a model artifact URI consumed by REGISTER. Struct because the shape is step-specific. SECURITY: never put secrets here; outputs flow to other steps and into responses. |
| error | [string](#string) |  | Human-readable error message when status is FAILED / COMPENSATION_FAILED. For debugging/audit, NOT for end-user display. Empty on success. |
| attempt | [int32](#int32) |  | How many times this step has been attempted (&gt;= 1 once it has run). Surfaces retry behavior to operators (&#34;it succeeded on attempt 3&#34;). |






<a name="forgepoint-pipeline-v1-TriggerExecutionRequest"></a>

### TriggerExecutionRequest
TriggerExecutionRequest starts a new run of an existing pipeline.

WHY &#34;Trigger&#34; and not &#34;Create&#34;: the client does not construct an Execution;
it asks the orchestrator to START one. The orchestrator owns the resulting
Execution&#39;s entire lifecycle (status, steps, timestamps).

IDEMPOTENCY: triggering a deployment saga twice (e.g., a
client retry after a network blip) would deploy the same model twice and
could leave orphaned serving instances. The idempotency_key makes the trigger
EXACTLY-ONCE from the caller&#39;s view: the server stores key → execution_id; a
repeat with the same key returns the SAME Execution instead of starting a new
one. This is the Stripe idempotency-key pattern, and it pairs with the
idempotent NATS consumers used elsewhere on the platform.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipeline_id | [string](#string) |  | The pipeline template to run. |
| input | [google.protobuf.Struct](#google-protobuf-Struct) |  | Run input (e.g., {&#34;model_id&#34;: &#34;...&#34;, &#34;version&#34;: &#34;...&#34;} for a deploy saga, or a drift report for auto-retrain). Struct because shape is pipeline- specific. SECURITY: validated by the engine; no secrets/PII. |
| idempotency_key | [string](#string) |  | Caller-supplied idempotency key (typically a UUID). Same key → same Execution returned, never a duplicate run. Strongly recommended for any automated trigger (CI, Model Monitor auto-retrain). Empty = no dedup (each call starts a fresh run — use only for interactive one-offs). |






<a name="forgepoint-pipeline-v1-TriggerExecutionResponse"></a>

### TriggerExecutionResponse
TriggerExecutionResponse returns the freshly-created Execution (status
PENDING/RUNNING). Wrapped per Buf STANDARD naming and for forward compat.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution | [Execution](#forgepoint-pipeline-v1-Execution) |  | The started execution. Clients typically follow up with WatchExecution. |






<a name="forgepoint-pipeline-v1-UpdatePipelineRequest"></a>

### UpdatePipelineRequest
UpdatePipelineRequest edits an existing pipeline TEMPLATE (name and/or steps).

WHY this RPC exists (the CLI/UI need to edit a template without recreating it):
without Update, the only way to change a pipeline is delete-and-recreate, which
changes its id and orphans its execution history. Update keeps the id (and
therefore the lineage of past runs) stable.

SECURITY — anti-mass-assignment (same discipline as Create): we accept ONLY the
user-authorable fields. id selects the target; name/type/steps are the editable
payload. We do NOT accept created_by, created_at, or team — those stay
server-authoritative and immutable; allowing a client to rewrite created_by
would let it forge ownership on an existing record.

IMMUTABILITY NOTE: `type` (saga vs DAG) is NOT editable — changing a pipeline&#39;s
execution model out from under in-flight executions is unsafe, so the server
rejects a type change (create a new pipeline instead). It is omitted from this
request deliberately.

CONCURRENCY: editing a template does NOT affect already-running executions —
each Execution captures the step graph it started with (template/instance
split). Only future TriggerExecutions see the new definition.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipeline_id | [string](#string) |  | UUID of the pipeline to update (selects the target; not itself mutable). |
| name | [string](#string) |  | New human-readable name. Must remain unique within the caller&#39;s team. |
| steps | [StepDefinition](#forgepoint-pipeline-v1-StepDefinition) | repeated | The replacement step graph. Same validation &#43; 256-step cap as CreatePipeline. This is a FULL REPLACE of the steps (not a partial patch) — simpler to reason about than field-level merge for a graph, and the whole graph is re-validated. |






<a name="forgepoint-pipeline-v1-UpdatePipelineResponse"></a>

### UpdatePipelineResponse
UpdatePipelineResponse wraps the updated PipelineDefinition.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pipeline | [PipelineDefinition](#forgepoint-pipeline-v1-PipelineDefinition) |  |  |






<a name="forgepoint-pipeline-v1-WatchExecutionRequest"></a>

### WatchExecutionRequest
WatchExecutionRequest opens a live stream of updates for one execution.
One request → many responses (server-streaming). See the service comment for
WHY server-streaming is the right call here.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution_id | [string](#string) |  | UUID of the execution to watch. |
| include_current_state | [bool](#bool) |  | If true, the server first emits the CURRENT full state as one or more updates, THEN streams subsequent changes. This avoids a get-then-watch race (a state change slipping between the snapshot read and the stream open). If false, only changes occurring AFTER the stream opens are sent. |






<a name="forgepoint-pipeline-v1-WatchExecutionResponse"></a>

### WatchExecutionResponse
WatchExecutionResponse is ONE event in the WatchExecution stream.

NAMING: buf&#39;s RPC_RESPONSE_STANDARD_NAME lint rule requires the response of
rpc WatchExecution to be named WatchExecutionResponse. Because this is a
SERVER-STREAMING RPC, each message on the stream is one of these — so the
&#34;Response&#34; here is a single streamed update (a delta), not a one-shot reply.

WHY a dedicated stream message (not just re-sending Execution): a stream is a
sequence of DELTAS over time, and the consumer needs to know WHAT changed and
WHEN. We send the full Execution snapshot (simple, idempotent for the UI to
render) PLUS metadata: which step changed and a monotonic sequence number for
gap detection / reconnection. This mirrors how the BFF bridges this stream to
Server-Sent Events for the web UI (see implementation plan, M4).

WHY NOT a oneof of fine-grained events (StepStarted/StepFinished/...): a
full-snapshot-per-update is far simpler for clients — they just replace their
view. The cost is slightly larger messages; for a handful of steps this is
negligible and the simplicity is worth it. (Tradeoff explicitly chosen.)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| execution | [Execution](#forgepoint-pipeline-v1-Execution) |  | The full current execution snapshot at the moment of this update. Clients can render directly from this without tracking prior deltas. |
| changed_step | [StepExecution](#forgepoint-pipeline-v1-StepExecution) |  | The StepExecution that changed and caused this update, if any. Empty for execution-level transitions (e.g., RUNNING → COMPLETED) that aren&#39;t tied to a single step. Lets a UI highlight &#34;the deploy step just finished&#34;. |
| sequence | [uint64](#uint64) |  | Monotonic, per-execution sequence number. Lets a reconnecting client detect gaps (&#34;I last saw seq 7, the first message after reconnect is seq 10 — I missed 8 and 9, do a GetExecution to resync&#34;). Ordering/gap-detection is the streaming equivalent of an event log offset (Kafka offset / NATS sequence). |
| emitted_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the server emitted this update. |





 


<a name="forgepoint-pipeline-v1-ExecutionStatus"></a>

### ExecutionStatus
============================================================================
ExecutionStatus
============================================================================

WHY: This is the saga state machine, surfaced as an enum. The orchestrator
transitions an Execution through these states and persists each transition.

STATE MACHINE:

  PENDING ──► RUNNING ──► COMPLETED            (happy path)
                 │
                 ├──► (step fails) ──► COMPENSATING ──► FAILED
                 │                         (undo completed steps in reverse)
                 │
                 └──► (CancelExecution) ──► COMPENSATING ──► CANCELLED

  - PENDING:      durably created, not yet scheduled.
  - RUNNING:      executing steps (sequential saga, or parallel DAG levels).
  - COMPENSATING: a step failed (or user cancelled); running compensation
                  steps in REVERSE completion order.
  - COMPLETED:    all steps succeeded.
  - FAILED:       a step failed AND compensation finished. Terminal.
  - CANCELLED:    user-requested stop; compensation done. Terminal.

WHY SEPARATE FAILED vs CANCELLED: both are terminal and both run
compensation, but the CAUSE matters for alerting and audit. FAILED pages an
on-call human (SagaStuck/PipelineFailed alert → Notification service);
CANCELLED is an expected operator action and should NOT page.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| EXECUTION_STATUS_UNSPECIFIED | 0 |  |
| EXECUTION_STATUS_PENDING | 1 |  |
| EXECUTION_STATUS_RUNNING | 2 |  |
| EXECUTION_STATUS_COMPENSATING | 3 |  |
| EXECUTION_STATUS_COMPLETED | 4 |  |
| EXECUTION_STATUS_FAILED | 5 |  |
| EXECUTION_STATUS_CANCELLED | 6 |  |



<a name="forgepoint-pipeline-v1-PipelineType"></a>

### PipelineType
============================================================================
PipelineType
============================================================================

WHY: One service, two execution models. The type tells the engine HOW to run
the steps — sequentially with compensation (saga) or as a parallel DAG.

WHY NOT TWO SEPARATE SERVICES: because the durable state,
crash recovery, persistence, and observability are identical for both; only
the scheduling differs. Splitting would duplicate the hard 80% (durability)
to vary the easy 20% (scheduling). The type field selects the strategy at
runtime — Strategy pattern at the message level.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| PIPELINE_TYPE_UNSPECIFIED | 0 | Proto3 requires a zero value; treat it as &#34;unset / invalid&#34;. Buf STANDARD (ENUM_ZERO_VALUE_SUFFIX) requires the _UNSPECIFIED suffix. |
| PIPELINE_TYPE_DEPLOYMENT_SAGA | 1 | Sequential saga with compensation. Used for model DEPLOYMENT: validate → deploy → canary → promote, with rollback on failure. |
| PIPELINE_TYPE_TRAINING_DAG | 2 | Parallel DAG. Used for model TRAINING: fetch → preprocess → (train fold 1..N in parallel) → aggregate → evaluate → register. |
| PIPELINE_TYPE_BATCH_INFERENCE | 3 | Batch inference DAG (scoring a dataset). Also DAG-scheduled. |



<a name="forgepoint-pipeline-v1-StepStatus"></a>

### StepStatus
============================================================================
StepStatus
============================================================================

WHY: Each step has its own lifecycle, independent of the overall execution.
This is the per-checkpoint status persisted in `step_executions` — the row
the orchestrator reads on crash recovery to know where to resume.

COMPENSATION SUB-STATES (the saga-specific bit):
  When a saga rolls back, a previously COMPLETED step moves through:
    COMPLETED ──► COMPENSATING ──► COMPENSATED   (undo succeeded)
                               └─► COMPENSATION_FAILED (undo FAILED — danger!)

  COMPENSATION_FAILED is the worst case in any saga: we could not undo a
  side effect (e.g., failed to destroy a serving instance). This is a
  &#34;stuck saga&#34; requiring human intervention; the engine surfaces it loudly
  rather than silently leaving orphaned resources. The honest posture:
  &#34;compensation is best-effort and idempotent, and a failed compensation
  is an alert, not a silent state&#34;.

WHY SKIPPED EXISTS: in a DAG, if a parent fails, its not-yet-started
dependents are marked SKIPPED (never ran) — distinct from FAILED (ran and
errored). Keeping them separate makes the execution graph readable.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| STEP_STATUS_UNSPECIFIED | 0 |  |
| STEP_STATUS_PENDING | 1 | Created/persisted, not yet started (write-ahead checkpoint before run). |
| STEP_STATUS_RUNNING | 2 | Currently executing. |
| STEP_STATUS_COMPLETED | 3 | Finished successfully. |
| STEP_STATUS_FAILED | 4 | Ran and errored. |
| STEP_STATUS_SKIPPED | 5 | A dependency failed, so this step never ran (DAG failure propagation). |
| STEP_STATUS_COMPENSATING | 6 | Currently running this step&#39;s compensation (undo) action. |
| STEP_STATUS_COMPENSATED | 7 | Compensation (undo) succeeded — the side effect was reversed. |
| STEP_STATUS_COMPENSATION_FAILED | 8 | Compensation FAILED — side effect could NOT be reversed. Needs a human. |



<a name="forgepoint-pipeline-v1-StepType"></a>

### StepType
============================================================================
StepType
============================================================================

WHY: Each step maps to a concrete StepExecutor implementation in the engine
(ExecutorRegistry: StepType → executor). The proto enumerates the KINDS of
work the platform knows how to do; CUSTOM is the escape hatch.

WHY AN ENUM AND NOT A FREE STRING: an enum is self-documenting, validated at
the edge (unknown step types are rejected at CreatePipeline), and lets the
executor registry be exhaustive. Tradeoff: adding a new step kind is a proto
change. CUSTOM &#43; a config map covers the long tail without a proto bump.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| STEP_TYPE_UNSPECIFIED | 0 |  |
| STEP_TYPE_VALIDATE | 1 | Saga (deployment) steps ------------------------------------------------- Verify the model exists and is in a deployable stage (calls Model Registry). |
| STEP_TYPE_BUILD | 2 | Build/prepare a serving artifact or image. |
| STEP_TYPE_DEPLOY | 3 | Create the K8s serving Deployment for a model version. |
| STEP_TYPE_CANARY | 4 | Shift a small traffic % to the new version and watch metrics (Inference GW). |
| STEP_TYPE_PROMOTE | 5 | Promote to 100% traffic once the canary passes. |
| STEP_TYPE_TRAIN | 6 | DAG (training) steps ---------------------------------------------------- Launch a K8s training Job. |
| STEP_TYPE_EVALUATE | 7 | Evaluate trained model metrics against thresholds. |
| STEP_TYPE_REGISTER | 8 | Register the resulting model version in the Model Registry. |
| STEP_TYPE_CUSTOM | 9 | Escape hatch: arbitrary step whose behavior is driven entirely by `config`. Used for one-off / experimental steps without changing this proto. |


 

 


<a name="forgepoint-pipeline-v1-PipelineOrchestratorService"></a>

### PipelineOrchestratorService
============================================================================
PIPELINE ORCHESTRATOR SERVICE
============================================================================

WHY ONE SERVICE WITH THESE RPCs: the service has two responsibilities —
DEFINING workflows (CreatePipeline / ListPipelines) and RUNNING &#43; OBSERVING
them (Trigger / Get / Watch / Cancel / List executions). They share the same
durable store and domain model, so they live behind one service boundary.

STREAMING CHOICE: only WatchExecution is server-
streaming; everything else is unary. WHY:
  - WatchExecution: the orchestrator is the SINGLE source of truth for saga
    state and PUSHES transitions as they happen. One client request →
    many server messages = SERVER-STREAMING. Polling GetExecution in a loop
    would be wasteful (most polls see no change) and laggy. Long-lived push
    is the natural fit, and the BFF bridges it to SSE for the browser.
  - Everything else is a single request/response → unary. We do NOT use
    client-streaming or bidi anywhere here: the client never sends a stream of
    data to the orchestrator; it issues discrete commands.

LEADER ELECTION (deployment note): although the gRPC
API can be served by many replicas, only the ELECTED LEADER actually drives
sagas (runs steps, writes checkpoints). This prevents two pods from executing
the same saga concurrently and double-applying side effects. Read RPCs
(Get/List/Watch) can be served by any replica from the shared DB.

CALL FLOW (closing the loop):
  Model Monitor detects drift
       │ gRPC
       ▼
  PipelineOrchestratorService.TriggerExecution(training pipeline, drift report)
       │  saga/DAG: retrain → evaluate → register → deploy → canary → promote
       ▼
  emits fp.pipelines.* events → Notification &#43; Experiment Tracker
============================================================================

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreatePipeline | [CreatePipelineRequest](#forgepoint-pipeline-v1-CreatePipelineRequest) | [CreatePipelineResponse](#forgepoint-pipeline-v1-CreatePipelineResponse) | CreatePipeline registers a new pipeline TEMPLATE (saga or DAG). The server validates the step graph (known types, valid depends_on, no cycles, valid compensation pointers) and assigns id/created_by/created_at server-side. |
| ListPipelines | [ListPipelinesRequest](#forgepoint-pipeline-v1-ListPipelinesRequest) | [ListPipelinesResponse](#forgepoint-pipeline-v1-ListPipelinesResponse) | ListPipelines returns a paginated list of pipeline templates within the caller&#39;s team/RBAC scope, optionally filtered by type. |
| GetPipeline | [GetPipelineRequest](#forgepoint-pipeline-v1-GetPipelineRequest) | [GetPipelineResponse](#forgepoint-pipeline-v1-GetPipelineResponse) | GetPipeline fetches one pipeline TEMPLATE by id (full step graph), scoped to the caller&#39;s team/RBAC. Point lookup for the CLI/UI before triggering a run. |
| UpdatePipeline | [UpdatePipelineRequest](#forgepoint-pipeline-v1-UpdatePipelineRequest) | [UpdatePipelineResponse](#forgepoint-pipeline-v1-UpdatePipelineResponse) | UpdatePipeline edits a template&#39;s name/steps in place (id and execution history preserved). Re-validates the step graph; type is immutable. |
| DeletePipeline | [DeletePipelineRequest](#forgepoint-pipeline-v1-DeletePipelineRequest) | [DeletePipelineResponse](#forgepoint-pipeline-v1-DeletePipelineResponse) | DeletePipeline soft-deletes (archives) a template so it can no longer be listed or triggered, while preserving past executions&#39; lineage. Rejected if a non-terminal execution of the pipeline is still in flight. |
| TriggerExecution | [TriggerExecutionRequest](#forgepoint-pipeline-v1-TriggerExecutionRequest) | [TriggerExecutionResponse](#forgepoint-pipeline-v1-TriggerExecutionResponse) | TriggerExecution starts a new run of a pipeline. Supports an idempotency_key so retries don&#39;t double-trigger (exactly-once from the caller&#39;s view). The orchestrator owns the resulting Execution&#39;s lifecycle. |
| GetExecution | [GetExecutionRequest](#forgepoint-pipeline-v1-GetExecutionRequest) | [GetExecutionResponse](#forgepoint-pipeline-v1-GetExecutionResponse) | GetExecution fetches the current state of a run (point-in-time), including its per-step timeline. Cheap, stateless poll — use Watch for live updates. |
| WatchExecution | [WatchExecutionRequest](#forgepoint-pipeline-v1-WatchExecutionRequest) | [WatchExecutionResponse](#forgepoint-pipeline-v1-WatchExecutionResponse) stream | WatchExecution opens a SERVER-STREAMING feed of live updates for one run. The orchestrator pushes a WatchExecutionResponse on every state transition until the run reaches a terminal state (COMPLETED/FAILED/CANCELLED), then closes the stream. With include_current_state=true the server first replays the current state, avoiding a get-then-watch race. |
| CancelExecution | [CancelExecutionRequest](#forgepoint-pipeline-v1-CancelExecutionRequest) | [CancelExecutionResponse](#forgepoint-pipeline-v1-CancelExecutionResponse) | CancelExecution requests a graceful stop. The run transitions to COMPENSATING (undo completed steps in reverse) and then CANCELLED. Returns the Execution so the caller sees the immediate transition. |
| ListExecutions | [ListExecutionsRequest](#forgepoint-pipeline-v1-ListExecutionsRequest) | [ListExecutionsResponse](#forgepoint-pipeline-v1-ListExecutionsResponse) | ListExecutions returns a paginated, filterable list of runs (by pipeline and/or status). page_size is capped server-side. List items omit per-step detail; fetch GetExecution for the full timeline. |

 



<a name="forgepoint_registry_v1_registry-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/registry/v1/registry.proto



<a name="forgepoint-registry-v1-ConfirmVersionUploadRequest"></a>

### ConfirmVersionUploadRequest
----------------------------------------------------------------------------
ConfirmVersionUpload (COMMAND / write path) — the PENDING_UPLOAD → READY edge
----------------------------------------------------------------------------

WHY this RPC exists (the missing READY trigger):
  CreateVersion lands a version at status=PENDING_UPLOAD and hands back a
  presigned PUT URL; the bytes then flow DIRECTLY to MinIO/S3, bypassing this
  service. Something must tell the registry &#34;the upload finished, go verify
  it&#34; so the status can advance to READY (or FAILED). That trigger is this RPC.
  It is what publishes events.ModelVersionReady (fp.models.version.ready) — the
  edge serving/billing/pipeline-orchestrator consume (conflict #3). Before this
  RPC existed, the proto&#39;s prose claimed &#34;the storage layer flips status to
  READY&#34; but exposed no surface to do it — a real gap this fix closes.

WHO CALLS IT: typically the uploader (CLI/CI) after a successful PUT, OR an
  S3/MinIO bucket-notification webhook bridged into this RPC. Either way the
  server RE-VERIFIES the object server-side (existence &#43; checksum &#43; size) —
  it does NOT trust the caller&#39;s claim that the upload succeeded.

SECURITY / MASS-ASSIGNMENT (critical): the caller does NOT supply
  artifact_path, artifact_digest, or size_bytes. Those are MEASURED by the
  server from the stored object. Accepting a client-asserted digest would let
  a caller register a &#34;verified&#34; artifact whose bytes don&#39;t match — defeating
  the entire content-integrity guarantee — and a client-asserted size would let
  a tenant under-report storage to dodge billing. The ONLY input is which
  version to confirm. The expected_digest field below is an OPTIONAL assertion
  the server CHECKS AGAINST (cross-check), never one it stores blindly.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version_id | [string](#string) |  | The PENDING_UPLOAD version whose artifact upload just completed. Required. |
| expected_digest | [string](#string) |  | OPTIONAL client-asserted content digest (&#34;sha256:...&#34;). If set, the server computes the object&#39;s digest and FAILS the confirmation (status→FAILED) on mismatch — a cross-check, not a stored value. If empty, the server simply records the digest it computes. Never trusted as the source of truth. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY: confirmation triggers the ModelVersionReady event and billing&#39;s storage metering. A retry with the same key is a no-op returning the current version state — so a duplicate webhook delivery cannot double-fire ModelVersionReady or double-meter storage. Optional but recommended. |






<a name="forgepoint-registry-v1-ConfirmVersionUploadResponse"></a>

### ConfirmVersionUploadResponse
ConfirmVersionUploadResponse returns the version with status now READY (on a
successful verify) or FAILED (on checksum/size mismatch or missing object).
On READY, the server has populated artifact_path/artifact_digest/size_bytes —
all server-measured — and has published events.ModelVersionReady.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [ModelVersion](#forgepoint-registry-v1-ModelVersion) |  | The version reflecting its post-verification status (READY or FAILED) and, when READY, the server-measured artifact_path/artifact_digest/size_bytes. |






<a name="forgepoint-registry-v1-CreateVersionRequest"></a>

### CreateVersionRequest
CreateVersionRequest creates a new version of an existing model. The artifact
itself is NOT in this message — bytes go to object storage via the presigned
URL returned in the response (see GetUploadURL note). This keeps large blobs
off the gRPC path (gRPC messages are capped ~4MB by default and models can be
gigabytes).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | Target model. SERVER-AUTHORITATIVE binding for the version&#39;s model_id — validated to exist and belong to the caller&#39;s team. |
| version | [string](#string) |  | OPTIONAL explicit version label. Empty = the server auto-assigns the next monotonic version (the safe default — prevents accidental clobbering and version races). When provided, it must be unique for the model. We allow it because some teams pin meaningful versions (e.g., a git SHA or semver from CI). stage/status are NOT settable here. |
| description | [string](#string) |  | Changelog/notes for this version. |
| metrics | [google.protobuf.Struct](#google-protobuf-Struct) |  | Training/eval metrics (free-form struct). Stored as JSONB. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY: a retried CreateVersion must not mint a duplicate version (which would waste a version number and a storage slot). Same semantics as RegisterModel.idempotency_key — repeat with the same key returns the original version &#43; its (still valid) upload URL. |
| upload_url_ttl_seconds | [int64](#int64) |  | How long the returned presigned upload URL should remain valid, in seconds. Optional; server clamps to a sane max (e.g., 3600s) to limit the window an upload credential is usable. 0 = server default. |






<a name="forgepoint-registry-v1-CreateVersionResponse"></a>

### CreateVersionResponse
CreateVersionResponse returns the new version (status=PENDING_UPLOAD,
stage=DEV) AND a presigned URL the client PUTs the artifact bytes to.
WHY bundle the URL here instead of a separate round-trip: the create-then-
upload flow is always paired, so returning both avoids a second RPC and a
race where the version exists but the client lost the URL.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [ModelVersion](#forgepoint-registry-v1-ModelVersion) |  | The created version. status will be VERSION_STATUS_PENDING_UPLOAD until the upload is confirmed; stage is MODEL_STAGE_DEV. |
| upload_url | [string](#string) |  | Presigned PUT URL for the artifact. Single-use, time-limited. The bytes never traverse this gRPC service — they go straight to MinIO/S3. After the PUT completes, the client (or an S3/MinIO bucket-notification bridge) calls ConfirmVersionUpload, which server-side verifies the object and flips status to READY (publishing events.ModelVersionReady). If this URL expires before the upload finishes, GetUploadURL re-issues a fresh one for the same version. |
| upload_url_expires_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When upload_url stops being valid. Client must complete the PUT before this (or call GetUploadURL for a fresh URL). |






<a name="forgepoint-registry-v1-DeleteModelRequest"></a>

### DeleteModelRequest
----------------------------------------------------------------------------
DeleteModel (COMMAND / write path) — soft delete / archive-all
----------------------------------------------------------------------------

WHY soft delete, not hard delete:
  Models have downstream lineage (experiments, billing records, served
  deployments). Hard-deleting would orphan those references and destroy audit
  history. DeleteModel sets the model&#39;s archived_at, archives all its
  versions, and removes it from active read lists — but the rows survive for
  compliance and potential restore. This is the same posture as &#34;soft delete&#34;
  in Stripe/most SaaS registries.
----------------------------------------------------------------------------


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | The model to archive (soft-delete). Required. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY: deleting an already-deleted model is naturally idempotent, but the key also guards against a retry re-emitting the ModelArchived event (which would double-notify). Optional but recommended. |






<a name="forgepoint-registry-v1-DeleteModelResponse"></a>

### DeleteModelResponse
DeleteModelResponse is a NAMED EMPTY message (not google.protobuf.Empty) to
satisfy Buf RPC_RESPONSE_STANDARD_NAME and stay forward-compatible: if we
later want to return archived_at or a count of archived versions, we add
fields here without changing the RPC signature. See auth.proto&#39;s
RevokeAPIKeyResponse for the same rationale.






<a name="forgepoint-registry-v1-GetDownloadURLRequest"></a>

### GetDownloadURLRequest
----------------------------------------------------------------------------
GetDownloadURL (QUERY-ish / storage) — fetch artifact bytes out of band
----------------------------------------------------------------------------

WHY a presigned DOWNLOAD URL rather than streaming bytes through gRPC:
  Symmetric with the upload flow — artifacts can be gigabytes; routing them
  through this control-plane service would blow the gRPC message limit and
  waste CPU/bandwidth. Serving and the CLI fetch the artifact directly from
  MinIO/S3 using a short-lived, scoped credential. This RPC does NOT mutate
  state, so it is safe to retry and needs no idempotency key.
----------------------------------------------------------------------------


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version_id | [string](#string) |  | The version whose artifact to download. Must be READY. |
| ttl_seconds | [int64](#int64) |  | Requested validity of the URL in seconds; server clamps to a max. 0 = server default. Short TTLs limit how long a leaked URL is usable. |






<a name="forgepoint-registry-v1-GetDownloadURLResponse"></a>

### GetDownloadURLResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| download_url | [string](#string) |  | Presigned GET URL for the artifact. Time-limited, scoped to this object. |
| expires_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the URL expires. |
| artifact_digest | [string](#string) |  | Content digest of what will be downloaded, so the client can verify integrity after fetching (matches ModelVersion.artifact_digest). |






<a name="forgepoint-registry-v1-GetModelRequest"></a>

### GetModelRequest
GetModelRequest targets a model by id OR name (one of). We allow name lookup
because callers (serving, CLI) usually know the human name, not the UUID.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | The model&#39;s UUID. Takes precedence if both are set. |
| name | [string](#string) |  | The model&#39;s name within the caller&#39;s team. Used when id is empty. |






<a name="forgepoint-registry-v1-GetModelResponse"></a>

### GetModelResponse
GetModelResponse wraps the Model from the READ projection.
CONSISTENCY: served from Redis and therefore EVENTUALLY CONSISTENT — a model
registered milliseconds ago may briefly 404 here until the projection catches
up. Callers needing read-your-writes should react to the ModelRegistered
event instead of polling.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model | [Model](#forgepoint-registry-v1-Model) |  |  |






<a name="forgepoint-registry-v1-GetUploadURLRequest"></a>

### GetUploadURLRequest
----------------------------------------------------------------------------
GetUploadURL (STORAGE) — (re)issue a presigned upload URL for a version
----------------------------------------------------------------------------

WHY a standalone RPC when CreateVersion already returns an upload URL:
  The create-then-PUT flow can break between the two steps — the presigned URL
  expires (TTL elapsed), the client crashed before uploading, or a CI runner
  was preempted. Without a way to RE-ISSUE the URL, the only recovery would be
  to mint a NEW version (wasting a version number and a storage slot). This RPC
  re-issues a fresh presigned PUT for an EXISTING version that is still
  PENDING_UPLOAD, making the upload step resumable. It is idempotent/read-only
  with respect to registry STATE (it grants a credential; it does not change
  the version row), so no idempotency key is needed.

SECURITY: rejected with FAILED_PRECONDITION if the version is already READY
  (no overwriting a verified artifact) or FAILED. The storage KEY is computed
  server-side from the version&#39;s identity — never client-supplied — so this
  cannot be used to obtain a write credential for an arbitrary object (path
  traversal / cross-model overwrite guard).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version_id | [string](#string) |  | The PENDING_UPLOAD version to (re)issue an upload URL for. Required. |
| ttl_seconds | [int64](#int64) |  | Requested URL validity in seconds; server clamps to a max (e.g. 3600s) to bound how long the write credential is usable. 0 = server default. |






<a name="forgepoint-registry-v1-GetUploadURLResponse"></a>

### GetUploadURLResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| upload_url | [string](#string) |  | Fresh presigned PUT URL for the artifact. Single-use, time-limited. Bytes go straight to MinIO/S3 — never through this control-plane service. |
| upload_url_expires_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When upload_url stops being valid. |






<a name="forgepoint-registry-v1-GetVersionRequest"></a>

### GetVersionRequest
GetVersionRequest fetches one version, either by its version id, or by the
(model_id, version-label) pair — whichever the caller has on hand.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | The version&#39;s UUID. Takes precedence if set. |
| model_id | [string](#string) |  | Alternative lookup: the owning model id... |
| version | [string](#string) |  | ...plus the version label (e.g. &#34;1.2.0&#34;). Used together when id is empty. |






<a name="forgepoint-registry-v1-GetVersionResponse"></a>

### GetVersionResponse
GetVersionResponse wraps the ModelVersion from the read projection.
CONSISTENCY: EVENTUALLY CONSISTENT (Redis).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [ModelVersion](#forgepoint-registry-v1-ModelVersion) |  |  |






<a name="forgepoint-registry-v1-ListModelsRequest"></a>

### ListModelsRequest
ListModelsRequest lists models in the caller&#39;s team (team scoping is applied
server-side from auth claims — there is intentionally no team field here, so
a caller cannot list another team&#39;s models).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task_type_filter | [string](#string) |  | Optional filter: only models whose task_type equals this value. Empty = all. |
| framework_filter | [string](#string) |  | Optional filter: only models using this framework. Empty = all. |
| include_archived | [bool](#bool) |  | When true, ARCHIVED models are included. Default false hides them — the common case is &#34;show me active models&#34;. Mirrors the projection&#39;s choice to drop archived models from the default models:list set. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination (reused from common.proto). page_size defaults to 20 and is CAPPED AT 100 server-side — a request for more is silently clamped to 100 to bound Redis ZRANGE work and response size (DoS guard). |






<a name="forgepoint-registry-v1-ListModelsResponse"></a>

### ListModelsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| models | [Model](#forgepoint-registry-v1-Model) | repeated | The page of models, ordered newest-first by created_at (the projection&#39;s sorted-set score). EVENTUALLY CONSISTENT (Redis read model). |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | next_page_token &#43; total_count. total_count may be -1 if computing the exact count is expensive (see common.proto PaginationResponse). |






<a name="forgepoint-registry-v1-ListVersionsRequest"></a>

### ListVersionsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_id | [string](#string) |  | The model whose versions to list. Required. |
| stage_filter | [ModelStage](#forgepoint-registry-v1-ModelStage) |  | Optional filter: only versions in this stage (e.g., only PRODUCTION). MODEL_STAGE_UNSPECIFIED (the default/zero) means &#34;any stage&#34;. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination; 100-item server-side cap. |






<a name="forgepoint-registry-v1-ListVersionsResponse"></a>

### ListVersionsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| versions | [ModelVersion](#forgepoint-registry-v1-ModelVersion) | repeated | Versions ordered newest-first. EVENTUALLY CONSISTENT (Redis read model). |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  |  |






<a name="forgepoint-registry-v1-Model"></a>

### Model
============================================================================
Model — the named, versioned thing. The aggregate root.
============================================================================

WHY a Model has no inline stage/version fields except a denormalized pointer:
  A Model is the stable identity (&#34;fraud-detector&#34;); its versions carry the
  mutable lifecycle. We DO denormalize one read-optimized pointer onto the
  read projection — latest_version / production_version — because the single
  most common query is &#34;give me the prod version of model X&#34; and we don&#39;t
  want clients to ListVersions and scan. That denormalization is a READ-MODEL
  concern (Redis), surfaced here so GetModelResponse can include it.

SECURITY: id, owner_id, team, created_at, updated_at, archived_at are all
SERVER-AUTHORITATIVE — populated by the service, never accepted from clients.
owner_id and team come from the caller&#39;s validated TokenClaims, not the body.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Primary key, immutable. Server-generated on RegisterModel. |
| name | [string](#string) |  | Unique, human-friendly model name within a team namespace, e.g. &#34;fraud-detector&#34;. Used as the addressable handle by serving/gateway. Client-supplied at registration; immutable afterward (rename = new model). |
| description | [string](#string) |  | Free-text description. Client-supplied at registration; mutable afterward via UpdateModel (see below). Name is NOT mutable (rename = new model). |
| owner_id | [string](#string) |  | The user_id of the registrant. SERVER-AUTHORITATIVE: derived from the caller&#39;s auth claims, NOT from the request body. Identifies accountability and is used by billing/notification to attribute cost and route alerts. |
| team | [string](#string) |  | Team namespace (e.g., &#34;ml-platform&#34;). SERVER-AUTHORITATIVE: copied from the caller&#39;s TokenClaims.team. Scopes visibility and billing. Never client-set, so a caller cannot register a model into another team&#39;s namespace. |
| framework | [string](#string) |  | ML framework, e.g. &#34;pytorch&#34;, &#34;tensorflow&#34;, &#34;sklearn&#34;, &#34;onnx&#34;. Client-supplied descriptive metadata; influences how serving loads it. |
| task_type | [string](#string) |  | Task type, e.g. &#34;classification&#34;, &#34;regression&#34;, &#34;embedding&#34;, &#34;llm&#34;. Client-supplied descriptive metadata; used for UI grouping and routing. |
| tags | [Model.TagsEntry](#forgepoint-registry-v1-Model-TagsEntry) | repeated | Arbitrary key/value tags for search and organization, e.g. {&#34;domain&#34;: &#34;fraud&#34;, &#34;pii&#34;: &#34;false&#34;}. Drives SearchByTag&#39;s Redis tag sets. Client-supplied (a map field is order-independent and dedupes keys, which is exactly the semantics we want for tags — unlike repeated ModelTag). |
| production_version | [string](#string) |  | READ-MODEL CONVENIENCE (eventually consistent): the version string of this model&#39;s current PRODUCTION version, or &#34;&#34; if none is in production. Lets a client learn the serving version from a single GetModel without a second call. Populated only on the read path (Redis projection). |
| latest_version | [string](#string) |  | READ-MODEL CONVENIENCE (eventually consistent): the most recently created version string regardless of stage. &#34;&#34; if the model has no versions yet. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the model was first registered. SERVER-AUTHORITATIVE, immutable. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Last time the model row (or its denormalized pointers) changed. SERVER-AUTHORITATIVE. |
| archived_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | Set when the model is soft-deleted/archived; null/zero while active. SERVER-AUTHORITATIVE. DeleteModel sets this rather than hard-deleting, so lineage and audit survive (and the read projection can hide it from lists). |






<a name="forgepoint-registry-v1-Model-TagsEntry"></a>

### Model.TagsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="forgepoint-registry-v1-ModelVersion"></a>

### ModelVersion
============================================================================
ModelVersion — an immutable, point-in-time snapshot of a model&#39;s artifact.
============================================================================

WHY versions are immutable except for stage/status transitions:
  The artifact (weights) and metrics captured at training time must never
  change — that&#39;s the whole point of a registry (reproducibility, rollback).
  The ONLY mutable aspects are the lifecycle stage (DEV→STAGING→PROD→ARCHIVED,
  via PromoteVersion) and status (PENDING_UPLOAD→READY/FAILED, set when the
  upload completes). Everything else is write-once.

SECURITY: id, model_id binding, stage, status, artifact_path, artifact_digest,
size_bytes, created_by, created_at are SERVER-AUTHORITATIVE. A client cannot
hand us a version that is already PRODUCTION or claim an arbitrary digest.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | UUID v4. Primary key for the version row. Server-generated. |
| model_id | [string](#string) |  | FK to the owning Model.id. SERVER-AUTHORITATIVE: bound from the path/request target, not a forgeable body field — you create a version *of a model*. |
| version | [string](#string) |  | Human-facing version label, e.g. &#34;1&#34;, &#34;1.2.0&#34;, &#34;2024-06-17-a&#34;. Unique per model. SERVER-AUTHORITATIVE by default: the service assigns a monotonic version unless the client explicitly requests one (see CreateVersionRequest). |
| description | [string](#string) |  | Free-text changelog/notes for this version. Client-supplied at creation. |
| artifact_path | [string](#string) |  | Object-storage location of the artifact (e.g. &#34;s3://fp-models/&lt;model&gt;/&lt;ver&gt;/ model.onnx&#34;). SERVER-AUTHORITATIVE: computed by the storage layer from the presigned-upload flow. Clients never set where bytes land — preventing path traversal / overwriting another model&#39;s artifact. |
| artifact_digest | [string](#string) |  | Content digest (e.g. &#34;sha256:...&#34;), verified server-side after upload. SERVER-AUTHORITATIVE. Enables content-addressable integrity &#43; dedupe and lets serving confirm it loaded exactly the registered bytes. |
| size_bytes | [int64](#int64) |  | Artifact size in bytes. SERVER-AUTHORITATIVE (measured on upload). Surfaced for UI and for Billing to meter storage. A client-supplied size would let a caller under-report storage usage — hence server-measured only. |
| metrics | [google.protobuf.Struct](#google-protobuf-Struct) |  | Training/eval metrics as a free-form struct, e.g. {&#34;accuracy&#34;: 0.97, &#34;auc&#34;: 0.99, &#34;loss&#34;: 0.03}. google.protobuf.Struct (not a fixed message) because metric sets vary per model/task and we don&#39;t want a schema change every time a new metric appears. Client-supplied at creation; stored in Postgres JSONB. |
| stage | [ModelStage](#forgepoint-registry-v1-ModelStage) |  | Lifecycle stage. SERVER-AUTHORITATIVE: starts at MODEL_STAGE_DEV; only PromoteVersion may advance it. Never accepted on CreateVersion (else a caller could create straight into PRODUCTION, bypassing review). |
| status | [VersionStatus](#forgepoint-registry-v1-VersionStatus) |  | Artifact readiness. SERVER-AUTHORITATIVE: PENDING_UPLOAD on creation, flipped to READY/FAILED by the storage layer once the upload is confirmed. |
| created_by | [string](#string) |  | The user_id that created this version. SERVER-AUTHORITATIVE from auth claims. |
| created_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this version was created. SERVER-AUTHORITATIVE, immutable. |






<a name="forgepoint-registry-v1-PromoteVersionRequest"></a>

### PromoteVersionRequest
----------------------------------------------------------------------------
PromoteVersion (COMMAND / write path) — the stage state machine
----------------------------------------------------------------------------

WHY one PromoteVersion RPC instead of separate Promote/Demote/Archive RPCs:
  The lifecycle is a single state machine; modeling it as &#34;set target stage,
  server validates the transition&#34; keeps one authoritative transition
  validator rather than scattering rules across four RPCs. The server rejects
  illegal transitions (e.g., DEV→PRODUCTION skipping STAGING, or promoting a
  non-READY version) with FAILED_PRECONDITION.

THE SINGLE-PRODUCTION INVARIANT:
  Promoting version B of model M to PRODUCTION must ATOMICALLY demote the
  current production version A of M to ARCHIVED — there is never a moment with
  two production versions. This happens in ONE Postgres transaction; the
  resulting ModelPromoted event carries BOTH the newly-promoted version and
  the demoted one so downstream consumers (serving must reload, billing must
  re-meter, monitor must re-baseline) see the swap atomically.
----------------------------------------------------------------------------


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version_id | [string](#string) |  | The version to transition. SERVER-AUTHORITATIVE binding to its model. |
| target_stage | [ModelStage](#forgepoint-registry-v1-ModelStage) |  | The desired target stage. The server validates the transition is legal from the version&#39;s current stage and that the version is READY when targeting STAGING/PRODUCTION. This is the ONLY place stage is client-influenced — and even here it&#39;s a *request* the server may reject, not a direct write. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY: promotion triggers side effects (serving reload, billing re-meter). A duplicate promote with the same key is a no-op returning the current state, so a client retry can&#39;t re-fire those side effects or emit a second ModelPromoted event. |






<a name="forgepoint-registry-v1-PromoteVersionResponse"></a>

### PromoteVersionResponse
PromoteVersionResponse returns the version in its new stage, plus the version
that was demoted (if any) so the caller sees the full atomic swap result.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [ModelVersion](#forgepoint-registry-v1-ModelVersion) |  | The promoted version reflecting its new stage. |
| demoted_version | [ModelVersion](#forgepoint-registry-v1-ModelVersion) |  | The previously-production version that was auto-demoted to ARCHIVED by this promotion, or unset if there was no prior production version. Lets the caller (and audit log) record the exact swap. |






<a name="forgepoint-registry-v1-RegisterModelRequest"></a>

### RegisterModelRequest
RegisterModelRequest carries ONLY client-owned fields. Note the deliberate
absence of id, owner_id, team, timestamps, production_version, latest_version
— all server-authoritative (see mass-assignment note in the header).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Desired unique model name within the caller&#39;s team. Validated for format and uniqueness server-side; collision → ALREADY_EXISTS. |
| description | [string](#string) |  | Optional human description. |
| framework | [string](#string) |  | Framework descriptor (&#34;pytorch&#34;, &#34;onnx&#34;, ...). |
| task_type | [string](#string) |  | Task type descriptor (&#34;classification&#34;, &#34;llm&#34;, ...). |
| tags | [RegisterModelRequest.TagsEntry](#forgepoint-registry-v1-RegisterModelRequest-TagsEntry) | repeated | Initial tags. Optional; more can be added later. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY (write-safety). RegisterModel is a non-idempotent create: a retried request (network blip, client retry) must NOT create a second model. The server stores this client-generated UUID with the created row; a repeat with the same key returns the original Model instead of erroring. WHY surface it in the API rather than rely on name-uniqueness: name collision would surface as ALREADY_EXISTS (an error the client must special-case), whereas an idempotency key makes the retry a clean success returning the same resource — the Stripe-style contract. Empty = the server treats the call as one-shot (still protected by name uniqueness). |






<a name="forgepoint-registry-v1-RegisterModelRequest-TagsEntry"></a>

### RegisterModelRequest.TagsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="forgepoint-registry-v1-RegisterModelResponse"></a>

### RegisterModelResponse
RegisterModelResponse wraps the created Model (write path returns the truth
from Postgres, so it is immediately consistent for the registrant — the
eventual-consistency caveat applies only to the READ RPCs below).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model | [Model](#forgepoint-registry-v1-Model) |  | The newly registered model. production_version/latest_version are empty (no versions yet); owner_id/team/timestamps are server-populated. |






<a name="forgepoint-registry-v1-SearchByTagRequest"></a>

### SearchByTagRequest
----------------------------------------------------------------------------
SearchByTag (QUERY / read path — paginated)
----------------------------------------------------------------------------

WHY a dedicated RPC rather than a tag filter on ListModels:
  Tag search hits a DIFFERENT Redis structure — the per-tag set
  models:tag:{key}:{value} — which the projection maintains specifically so
  tag lookups are O(members) set reads, not scans. A separate RPC makes that
  distinct read shape explicit and keeps ListModels&#39; filter surface small.
----------------------------------------------------------------------------


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  | Tag key to match, e.g. &#34;domain&#34;. |
| value | [string](#string) |  | Tag value to match, e.g. &#34;fraud&#34;. (key,value) together select the models:tag:{key}:{value} set in the projection. |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor-based pagination; same 100-item server-side cap as ListModels. |






<a name="forgepoint-registry-v1-SearchByTagResponse"></a>

### SearchByTagResponse
SearchByTagResponse reuses the same paginated shape as ListModels for client
uniformity. (Each list RPC still has its OWN response type to satisfy Buf&#39;s
RPC_RESPONSE_STANDARD_NAME — we don&#39;t share ListModelsResponse.)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| models | [Model](#forgepoint-registry-v1-Model) | repeated |  |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  |  |






<a name="forgepoint-registry-v1-UpdateModelRequest"></a>

### UpdateModelRequest
----------------------------------------------------------------------------
UpdateModel (COMMAND / write path) — mutate the small mutable surface
----------------------------------------------------------------------------

WHY a dedicated UpdateModel (the design/CLI need it, and the old code only
hinted at it):
  A Model&#39;s IDENTITY (id, name, owner_id, team) and its lineage timestamps are
  immutable/server-owned, but two fields legitimately change over a model&#39;s
  life: its human DESCRIPTION and its TAGS (re-classify &#34;domain&#34;, flip a
  &#34;deprecated&#34; flag). Without an UpdateModel the only way to fix a typo&#39;d
  description would be to re-register — losing the id and all version lineage.

SECURITY / MASS-ASSIGNMENT (the whole reason this message is so small):
  Only description and tags are accepted. name is OMITTED on purpose (renaming
  an addressable handle out from under serving/gateway is a footgun — a rename
  is modeled as a new model). id selects the target but is never itself
  mutated. owner_id/team/stage/status/timestamps/artifact fields are absent so
  a caller can NEVER reassign ownership, jump teams, or backdate via this RPC.

PARTIAL-UPDATE SEMANTICS (&#34;how do you patch in proto3?&#34;):
  proto3 scalars have no presence, so &#34;field omitted&#34; vs &#34;field set to empty&#34;
  are indistinguishable for a bare string. We make the contract explicit with
  update_mask-style booleans (update_description / replace_tags) rather than a
  full google.protobuf.FieldMask, because the mutable surface is tiny and two
  flags are clearer to read than a path-string mask. This avoids the classic
  PATCH bug where an unset field silently clears stored data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | The model to update. Required. Server-authoritative target binding (the model must belong to the caller&#39;s team — enforced from auth claims). |
| description | [string](#string) |  | New description. Applied ONLY when update_description is true (so an unset description doesn&#39;t blank an existing one — see partial-update note above). |
| update_description | [bool](#bool) |  | When true, `description` (even if empty) replaces the stored description. When false, the description is left untouched. |
| tags | [UpdateModelRequest.TagsEntry](#forgepoint-registry-v1-UpdateModelRequest-TagsEntry) | repeated | Replacement tag set. Applied ONLY when replace_tags is true. Tags are REPLACED wholesale (not merged) — the simplest, least-surprising semantics for a map; a caller that wants to add one tag sends the full desired set. |
| replace_tags | [bool](#bool) |  | When true, `tags` replaces the stored tag map (an empty map clears all tags). When false, tags are left untouched. |
| idempotency_key | [string](#string) |  | IDEMPOTENCY KEY: an update is naturally idempotent (applying the same patch twice yields the same state), but the key still guards against a retry re-emitting any change event / double-bumping updated_at. Optional. |






<a name="forgepoint-registry-v1-UpdateModelRequest-TagsEntry"></a>

### UpdateModelRequest.TagsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="forgepoint-registry-v1-UpdateModelResponse"></a>

### UpdateModelResponse
UpdateModelResponse returns the updated Model (write path → Postgres truth, so
immediately consistent for the caller; the READ RPCs remain eventually
consistent). NOTE: UpdateModel publishes NO canonical lifecycle event —
description/tag edits are not in the events.proto contract (no consumer reacts
to them), so there is nothing to emit. The Redis projection is refreshed by an
internal projection refresh, not a published domain event.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model | [Model](#forgepoint-registry-v1-Model) |  |  |





 


<a name="forgepoint-registry-v1-ModelStage"></a>

### ModelStage
============================================================================
ModelStage — the lifecycle state of a *version*, not the model.
============================================================================

WHY stage lives on the VERSION, not the Model:
  &#34;Production&#34; is a property of a specific set of weights, not of the model
  name. Model &#34;fraud-detector&#34; always exists; what changes over time is
  WHICH version is serving production traffic. Putting the stage on
  ModelVersion lets exactly one version per model occupy PRODUCTION while
  older versions sit in ARCHIVED and newer candidates wait in STAGING.

THE TRANSITION RULES (enforced server-side):
  DEV ──► STAGING ──► PRODUCTION ──► ARCHIVED
   └──────────────────────────────────► ARCHIVED (abandon a candidate)
  Promoting a version to PRODUCTION auto-demotes the *current* production
  version of the same model to ARCHIVED (single-prod invariant). This is the
  atomic swap that PromoteVersion performs and emits as ModelPromoted.

WHY an enum, not a free string:
  Closed set, validated at the wire boundary, prevents typos like &#34;prod&#34; vs
  &#34;production&#34; silently fragmenting the read projection&#39;s stage index.
  Buf STANDARD requires the _UNSPECIFIED zero value &#43; ENUM_NAME prefix on
  every value (so the int 0 never accidentally means &#34;DEV&#34;).

CANONICAL-EVENT MAPPING (the decoupling point):
  This enum is the SERVICE/API enum. The event bus carries a MIRROR enum,
  events.v1.ModelStage, with byte-identical values (UNSPECIFIED=0, DEV=1,
  STAGING=2, PRODUCTION=3, ARCHIVED=4). They are kept numerically aligned on
  purpose, but the handler still maps registry.ModelStage &lt;-&gt; events.ModelStage
  explicitly at the publish/consume boundary rather than casting — so that if
  the API enum ever evolves independently (a new internal stage that isn&#39;t a
  published fact), the event contract is insulated. This is the same &#34;event
  schema is decoupled from API schema&#34; discipline events.proto enforces.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| MODEL_STAGE_UNSPECIFIED | 0 | Default zero value. Means &#34;stage not set / unknown&#34;. A freshly created version starts at MODEL_STAGE_DEV explicitly — the server never leaves a version in UNSPECIFIED. Required by Buf ENUM_ZERO_VALUE_SUFFIX. |
| MODEL_STAGE_DEV | 1 | Newly created version. Artifact may still be uploading. Not eligible for serving. This is where every CreateVersion lands. |
| MODEL_STAGE_STAGING | 2 | Validated candidate undergoing pre-production checks (shadow traffic, canary eval). Promotable to PRODUCTION. |
| MODEL_STAGE_PRODUCTION | 3 | The version currently serving production traffic. INVARIANT: at most one PRODUCTION version per model at any time (enforced by PromoteVersion). |
| MODEL_STAGE_ARCHIVED | 4 | Retired version. Kept for lineage/audit and rollback, but not served and hidden from default list views. Terminal state. |



<a name="forgepoint-registry-v1-VersionStatus"></a>

### VersionStatus
============================================================================
VersionStatus — readiness of a version&#39;s artifact, orthogonal to stage.
============================================================================

WHY separate from ModelStage:
  Stage answers &#34;where in the lifecycle is this version?&#34; (a human/policy
  decision). Status answers &#34;is the artifact physically present and usable?&#34;
  (a system fact). A version can be in STAGING (stage) yet PENDING_UPLOAD
  (status) because the weights haven&#39;t finished uploading to MinIO. Conflating
  them would mean we couldn&#39;t represent &#34;promoted but artifact still arriving&#34;.
  Serving must check BOTH: stage == PRODUCTION AND status == READY.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| VERSION_STATUS_UNSPECIFIED | 0 | Required zero value. Unknown/unset status. |
| VERSION_STATUS_PENDING_UPLOAD | 1 | Version row created; artifact upload not yet confirmed. CreateVersion returns a version in this state alongside a presigned upload URL. |
| VERSION_STATUS_READY | 2 | Artifact present in object storage and validated (checksum verified). Only READY versions are eligible to be promoted/served. The PENDING_UPLOAD -&gt; READY transition is driven by ConfirmVersionUpload (below) and is the moment that publishes events.ModelVersionReady (fp.models.version.ready) — the edge serving/billing/pipeline-orchestrator wait on (see conflict #3 in events.proto). ModelVersionCreated fires earlier, at row creation; it is NOT a signal the artifact is usable. |
| VERSION_STATUS_FAILED | 3 | Upload failed, checksum mismatch, or validation error. Not promotable. Set by ConfirmVersionUpload when verification fails (no ModelVersionReady is emitted in that case — there is nothing servable to announce). |


 

 


<a name="forgepoint-registry-v1-RegistryService"></a>

### RegistryService
============================================================================
REGISTRY SERVICE
============================================================================

WHY a single RegistryService spanning both command and query RPCs:
  CQRS separates the read/write MODELS and STORES, not necessarily the API
  surface. Exposing both through one gRPC service keeps the client experience
  simple (one stub) while the IMPLEMENTATION routes commands to the Postgres
  write repo and queries to the Redis read repo. If read and write needed
  independent scaling/deploys we could split them into two services later —
  the proto is structured (clear COMMAND vs QUERY grouping below) so that
  split would be mechanical. This is the pragmatic &#34;logical CQRS, physical
  monolith-of-one-service&#34; choice MLflow&#39;s registry also makes.

STREAMING CHOICE — why everything here is UNARY:
  None of these operations is a long-lived stream. Registrations and
  promotions are discrete commands; lookups return a bounded object; lists are
  PAGINATED (cursor) rather than server-streamed because pagination gives the
  client backpressure, resumability (the page_token survives a disconnect),
  and cacheability that a server stream does not. Contrast with the Pipeline
  Orchestrator&#39;s WatchExecution, which IS server-streaming because execution
  progress is a genuine open-ended event feed. Picking unary&#43;pagination here
  is the correct call (&#34;why not stream
  ListModels?&#34; → backpressure &#43; resumable cursor &#43; simpler caching).

RPC GROUPS:
  COMMANDS (Postgres write, then publish a canonical events.v1 NATS event):
    RegisterModel, UpdateModel, CreateVersion, ConfirmVersionUpload,
    PromoteVersion, DeleteModel
  QUERIES (Redis read projection, eventually consistent):
    GetModel, ListModels, SearchByTag, GetVersion, ListVersions
  STORAGE (object store, presigned URLs — bytes bypass this service):
    GetUploadURL (re-issue), GetDownloadURL  (the FIRST upload URL is returned
    inline by CreateVersion; GetUploadURL re-issues an expired one)
============================================================================

--- COMMANDS (write path → Postgres → NATS event) ---

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| RegisterModel | [RegisterModelRequest](#forgepoint-registry-v1-RegisterModelRequest) | [RegisterModelResponse](#forgepoint-registry-v1-RegisterModelResponse) | RegisterModel creates a new model (the identity, no versions yet). Writes to Postgres and publishes events.ModelRegistered (fp.models.registered). owner_id/team are taken from the caller&#39;s auth claims, never the request. Idempotent via idempotency_key. |
| UpdateModel | [UpdateModelRequest](#forgepoint-registry-v1-UpdateModelRequest) | [UpdateModelResponse](#forgepoint-registry-v1-UpdateModelResponse) | UpdateModel mutates the small mutable surface (description and/or tags) of an existing model, using explicit update_description/replace_tags flags so an omitted field never silently clears stored data. name/owner/team/timestamps are NOT mutable here (mass-assignment guard). Publishes NO canonical event — these edits are not a published platform fact — but refreshes the read model. |
| CreateVersion | [CreateVersionRequest](#forgepoint-registry-v1-CreateVersionRequest) | [CreateVersionResponse](#forgepoint-registry-v1-CreateVersionResponse) | CreateVersion cuts a new immutable version of an existing model. Writes the version row (status=PENDING_UPLOAD, stage=DEV), returns a presigned upload URL for the artifact, and publishes events.ModelVersionCreated (fp.models.version.created). The artifact bytes never flow through this RPC. |
| ConfirmVersionUpload | [ConfirmVersionUploadRequest](#forgepoint-registry-v1-ConfirmVersionUploadRequest) | [ConfirmVersionUploadResponse](#forgepoint-registry-v1-ConfirmVersionUploadResponse) | ConfirmVersionUpload drives the PENDING_UPLOAD → READY (or FAILED) transition after the artifact has been PUT directly to object storage. The server RE-VERIFIES the object (existence &#43; server-measured digest &#43; size — never a client-asserted value), and on success flips status to READY and publishes events.ModelVersionReady (fp.models.version.ready) carrying the server-measured artifact_path/artifact_digest/size_bytes that serving/billing/orchestrator need. This is the READY edge that was previously unreachable (conflict #3). |
| PromoteVersion | [PromoteVersionRequest](#forgepoint-registry-v1-PromoteVersionRequest) | [PromoteVersionResponse](#forgepoint-registry-v1-PromoteVersionResponse) | PromoteVersion advances a version through the stage state machine (DEV→STAGING→PRODUCTION→ARCHIVED), enforcing legal transitions and the single-production invariant in ONE Postgres transaction, then publishes events.ModelPromoted (fp.models.promoted). This is what drives serving/gateway/monitor/billing reactions. |
| DeleteModel | [DeleteModelRequest](#forgepoint-registry-v1-DeleteModelRequest) | [DeleteModelResponse](#forgepoint-registry-v1-DeleteModelResponse) | DeleteModel soft-deletes (archives) a model and all its versions, removes it from active read lists, and publishes events.ModelArchived (fp.models.archived). Rows are retained for lineage/audit — not a hard delete. |
| GetModel | [GetModelRequest](#forgepoint-registry-v1-GetModelRequest) | [GetModelResponse](#forgepoint-registry-v1-GetModelResponse) | GetModel returns a single model by id or name from the Redis read model. May briefly miss a just-registered model (projection lag). |
| ListModels | [ListModelsRequest](#forgepoint-registry-v1-ListModelsRequest) | [ListModelsResponse](#forgepoint-registry-v1-ListModelsResponse) | ListModels returns a paginated, team-scoped page of models from the read model, newest-first, with optional task_type/framework filters. page_size is capped at 100 server-side. |
| SearchByTag | [SearchByTagRequest](#forgepoint-registry-v1-SearchByTagRequest) | [SearchByTagResponse](#forgepoint-registry-v1-SearchByTagResponse) | SearchByTag returns models matching a (key,value) tag, served from the projection&#39;s per-tag Redis set. Paginated, 100-item cap. |
| GetVersion | [GetVersionRequest](#forgepoint-registry-v1-GetVersionRequest) | [GetVersionResponse](#forgepoint-registry-v1-GetVersionResponse) | GetVersion returns a single version by id, or by (model_id, version) label, from the read model. |
| ListVersions | [ListVersionsRequest](#forgepoint-registry-v1-ListVersionsRequest) | [ListVersionsResponse](#forgepoint-registry-v1-ListVersionsResponse) | ListVersions returns a paginated, newest-first list of a model&#39;s versions from the read model, optionally filtered by stage. 100-item cap. |
| GetUploadURL | [GetUploadURLRequest](#forgepoint-registry-v1-GetUploadURLRequest) | [GetUploadURLResponse](#forgepoint-registry-v1-GetUploadURLResponse) | GetUploadURL re-issues a fresh presigned PUT URL for an existing version that is still PENDING_UPLOAD (e.g. the original URL from CreateVersion expired, or the client crashed before uploading). Makes the upload step resumable without minting a duplicate version. Rejected if the version is already READY/FAILED. |
| GetDownloadURL | [GetDownloadURLRequest](#forgepoint-registry-v1-GetDownloadURLRequest) | [GetDownloadURLResponse](#forgepoint-registry-v1-GetDownloadURLResponse) | GetDownloadURL returns a short-lived presigned URL to fetch a READY version&#39;s artifact directly from MinIO/S3, plus the expected digest for integrity verification. Read-only and safe to retry. |

 



<a name="forgepoint_serving_v1_serving-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## forgepoint/serving/v1/serving.proto



<a name="forgepoint-serving-v1-GetModelInfoRequest"></a>

### GetModelInfoRequest
GetModelInfoRequest asks for the static schema/identity of the served model.
WHY it still takes a name/version even though the pod serves one model:
  Symmetry with the gateway&#39;s interface and forward-compat — keeps the
  request shape stable if a future multi-model variant ever appears. Empty
  fields mean &#34;the model this pod serves&#34;.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | Optional logical name filter; empty = this pod&#39;s model. |
| version | [string](#string) |  | Optional version filter; empty = this pod&#39;s version. |






<a name="forgepoint-serving-v1-GetModelInfoResponse"></a>

### GetModelInfoResponse
GetModelInfoResponse wraps ModelInfo.
WHY wrap (not return ModelInfo directly): Buf STANDARD RPC_RESPONSE_STANDARD_NAME
requires a &lt;Rpc&gt;Response message, and wrapping lets us add fields later (e.g.
warmup status) without mutating the shared ModelInfo domain type.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_info | [ModelInfo](#forgepoint-serving-v1-ModelInfo) |  | The static description of the served model. |






<a name="forgepoint-serving-v1-GetModelStatusRequest"></a>

### GetModelStatusRequest
GetModelStatusRequest asks for the current lifecycle status.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | Model name; empty = this pod&#39;s model. |
| version | [string](#string) |  | Version; empty = this pod&#39;s version. |






<a name="forgepoint-serving-v1-GetModelStatusResponse"></a>

### GetModelStatusResponse
GetModelStatusResponse wraps ModelStatus (Buf naming &#43; forward-compat).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [ModelStatus](#forgepoint-serving-v1-ModelStatus) |  | Current lifecycle status. |






<a name="forgepoint-serving-v1-GetServingMetricsRequest"></a>

### GetServingMetricsRequest
GetServingMetricsRequest takes no selectors today (one model per pod) but is
a named message so the metric surface can grow (e.g. per-window stats).






<a name="forgepoint-serving-v1-GetServingMetricsResponse"></a>

### GetServingMetricsResponse
GetServingMetricsResponse wraps the metrics snapshot used for autoscaling and
load-aware routing. WHY a snapshot RPC in addition to /metrics scraping: see
ServingMetrics docs — the gateway reads this for least-inflight routing.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metrics | [ServingMetrics](#forgepoint-serving-v1-ServingMetrics) |  | Point-in-time serving metrics. |






<a name="forgepoint-serving-v1-HealthCheckRequest"></a>

### HealthCheckRequest
HealthCheckRequest optionally scopes the check to a model (future multi-model).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | Empty = check this pod&#39;s model. Mirrors gRPC&#39;s standard health protocol &#34;service&#34; selector so existing health tooling maps cleanly. |






<a name="forgepoint-serving-v1-HealthCheckResponse"></a>

### HealthCheckResponse
HealthCheckResponse reports serving status &#43; the signals behind the verdict.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [HealthStatus](#forgepoint-serving-v1-HealthStatus) |  | The readiness verdict K8s readinessProbe keys on. |
| model_state | [ModelState](#forgepoint-serving-v1-ModelState) |  | Current model lifecycle state (richer context behind `status`). |
| last_inference_latency | [google.protobuf.Duration](#google-protobuf-Duration) |  | Most recent inference latency observed; the livenessProbe compares this to its configured threshold to decide alive vs wedged. |






<a name="forgepoint-serving-v1-ListLoadedModelsRequest"></a>

### ListLoadedModelsRequest
============================================================================
ListLoadedModels (control plane — fleet/pod introspection)
============================================================================

WHY this RPC exists even though it&#39;s one-model-per-pod TODAY:
  The per-version-Deployment controller and admin/`fp` CLI need a uniform way
  to ask a pod &#34;what do you currently have resident, and in what state?&#34; —
  without N separate GetModelStatus calls and without assuming the pod holds
  exactly one model. On today&#39;s single-model pod this returns 0 or 1 entry;
  it is also the forward-compatible seam if a future multi-model pod variant
  (the rejected Triton-style server) ever appears. The reconcile loop reads
  this to detect drift between desired (the Deployment spec / consumed
  ModelDeployed events) and actual (resident) models.

PAGINATION (reuse common.v1, mandated for every list): even though a serving
pod holds a tiny number of models, the contract requires pagination on ALL
list RPCs for uniformity and so the same SDK list helpers work everywhere.
PAGE-SIZE CAP IS PART OF THE CONTRACT: server clamps page_size to [1, 100]
(default 20) — a request for more returns at most 100. This is the same cap
common.PaginationRequest documents; stated here so it is a contract promise,
not an implementation accident.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pagination | [forgepoint.common.v1.PaginationRequest](#forgepoint-common-v1-PaginationRequest) |  | Cursor &#43; page-size envelope (common.v1). page_size is server-clamped to [1,100], default 20; page_token is the opaque cursor from a prior response. |
| state_filter | [ModelState](#forgepoint-serving-v1-ModelState) |  | Optional state filter: when set, only models in this lifecycle state are returned (e.g. list only READY models the pod can actually serve). Empty/ UNSPECIFIED = all states. A FILTER, not an authority field. |






<a name="forgepoint-serving-v1-ListLoadedModelsResponse"></a>

### ListLoadedModelsResponse
ListLoadedModelsResponse returns the resident models&#39; live status plus the
pagination cursor. Returns ModelStatus (the live view), not ModelInfo, because
the fleet view cares about state/health, not full I/O schema (fetch that per
model via GetModelInfo when needed).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| models | [ModelStatus](#forgepoint-serving-v1-ModelStatus) | repeated | The resident models&#39; live lifecycle status (0..page_size entries). |
| pagination | [forgepoint.common.v1.PaginationResponse](#forgepoint-common-v1-PaginationResponse) |  | next_page_token (empty = last page) &#43; total_count (resident model count). |






<a name="forgepoint-serving-v1-LoadModelRequest"></a>

### LoadModelRequest
LoadModelRequest tells the pod to fetch an artifact from object storage and
load it into the ONNX runtime. Used by the operator/controller that manages
per-version Deployments (and at startup the pod self-loads from env vars).

WHY load-by-reference (URI/digest), not by uploading bytes here:
  The artifact lives in MinIO/S3 (the registry put it there). Streaming
  megabytes of weights through a control RPC would be wasteful and couple the
  control plane to artifact size. We pass a pointer; the pod pulls it. This
  mirrors KServe&#39;s storageUri model.

SECURITY: the caller specifies WHERE to load from but the pod only accepts
URIs within its configured allowed bucket/prefix (validated server-side) —
a caller can&#39;t point a serving pod at an arbitrary attacker-controlled URL
(SSRF / supply-chain guard). expected_digest lets the pod verify integrity.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | Logical model name to register this artifact under on the pod. |
| version | [string](#string) |  | Version label for the artifact (must match the Deployment&#39;s intended ver). |
| artifact_uri | [string](#string) |  | Object-storage URI of the ONNX artifact, e.g. &#34;s3://fp-models/fraud/v3.onnx&#34;. Validated against the pod&#39;s allow-listed bucket/prefix before fetching. |
| expected_digest | [string](#string) |  | Optional expected sha256 digest. If set, the pod aborts the load (FAILED) when the fetched bytes don&#39;t match — integrity/supply-chain protection. |
| idempotency_key | [string](#string) |  | Idempotency key so a retried LoadModel (e.g. controller re-reconcile) does not re-download/re-init an already-loaded identical model. If the requested (name, version, digest) is already READY, the pod returns the current status without reloading. |






<a name="forgepoint-serving-v1-LoadModelResponse"></a>

### LoadModelResponse
LoadModelResponse returns the resulting model status (which may still be
DOWNLOADING/LOADING — load is asynchronous; poll GetModelStatus for READY).
WHY return status, not empty: the caller wants to know whether the load was
accepted and the current state, without an immediate second RPC.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [ModelStatus](#forgepoint-serving-v1-ModelStatus) |  | Current lifecycle status after accepting the load request. |






<a name="forgepoint-serving-v1-ModelInfo"></a>

### ModelInfo
============================================================================
ModelInfo
============================================================================

WHY: The static description of the model this pod serves — its identity and
I/O schema. Returned by GetModelInfo so clients/SDKs can introspect and
build correct requests without out-of-band documentation.

SECURITY/AUTHORITY NOTE: every field here is SERVER-authoritative. It is
derived from the loaded artifact&#39;s metadata (set by the registry/training
pipeline), never accepted from a client. There is no &#34;create model&#34; RPC that
takes a ModelInfo — a serving pod serves exactly the model it was deployed
with. This prevents a caller from spoofing model identity.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Logical model name (matches the registry), e.g. &#34;fraud-detector&#34;. |
| version | [string](#string) |  | The exact version this pod serves, e.g. &#34;v3&#34; or a semver/sha. Because it&#39;s one-Deployment-per-version, a given pod&#39;s version is fixed for its lifetime. |
| input_schema | [TensorSpec](#forgepoint-serving-v1-TensorSpec) | repeated | Input tensors the model expects. Clients key Predict.inputs by these names. |
| output_schema | [TensorSpec](#forgepoint-serving-v1-TensorSpec) | repeated | Output tensors the model produces. Predict.outputs is keyed by these names. |
| loaded_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When this model finished loading into memory on this pod (READY transition). Server-set; useful for cold-start observability and cache-warmth checks. |
| artifact_digest | [string](#string) |  | Opaque digest (e.g. sha256) of the artifact bytes pulled from MinIO. Lets the gateway/registry confirm the pod is serving the exact artifact it expects (supply-chain integrity), without exposing the artifact itself. |






<a name="forgepoint-serving-v1-ModelStatus"></a>

### ModelStatus
ModelStatus is the live lifecycle view of the served model — used by probes,
the operator&#39;s reconcile loop, and admin tooling.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | The model&#39;s name (echoed for callers managing several pods). |
| version | [string](#string) |  | The model&#39;s version. |
| state | [ModelState](#forgepoint-serving-v1-ModelState) |  | Current lifecycle state (the state machine in ModelState&#39;s docs). |
| message | [string](#string) |  | Human-readable detail — especially the reason on FAILED (&#34;digest mismatch&#34;, &#34;unsupported ONNX op: X&#34;, &#34;OOM loading 2.1GB model&#34;). Empty when healthy. |
| updated_at | [google.protobuf.Timestamp](#google-protobuf-Timestamp) |  | When the pod last transitioned state. Lets the operator detect a stuck DOWNLOADING/LOADING (cold-start watchdog). |






<a name="forgepoint-serving-v1-PredictRequest"></a>

### PredictRequest
PredictRequest carries the input tensors for one inference call.

WHY model_name &#43; version here even though it&#39;s one-model-per-pod:
  Defense in depth. The gateway routes to the right pod, but the pod
  re-validates that the request targets the model it actually serves —
  if they mismatch it returns FAILED_PRECONDITION. This catches gateway
  routing bugs and stale connections instead of silently serving the wrong
  model. version may be empty to mean &#34;whatever this pod serves&#34;.

SECURITY (mass-assignment &#43; spoofing): the request body carries NO
auth/owner/principal/billing/api-key fields, and there is nothing here a
caller could set to attribute, charge, or authorize the call. Identity is
established by the gateway&#39;s auth interceptor and propagated as gRPC context
metadata, NEVER trusted from the request body — the serving pod is
intentionally auth-agnostic (see the Sidecar rationale up top). Because the
pod emits NO event, there is also no billed-principal field to spoof here
(metering happens entirely off the gateway&#39;s events.InferenceCompleted).

INPUT-SIZE BOUND (DoS guard, in the contract not just prose): a Predict call
may carry AT MOST 64 named input tensors (map entries), and the server
rejects any single TensorData whose `data` exceeds the configured max tensor
bytes (default 16 MiB) with INVALID_ARGUMENT. These caps are server-enforced
(a map cardinality cap cannot be expressed in proto3), preventing a hostile
or buggy client from OOM-ing a pod with a giant/fan-out request.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | Target model logical name. Validated against the pod&#39;s loaded model. |
| version | [string](#string) |  | Target version. Empty = &#34;this pod&#39;s version&#34;. If set and it doesn&#39;t match, the pod rejects rather than mis-serving. |
| inputs | [PredictRequest.InputsEntry](#forgepoint-serving-v1-PredictRequest-InputsEntry) | repeated | Named input tensors, keyed by TensorSpec.name from the model&#39;s input_schema. WHY a map (not repeated): models name their inputs; a map makes the binding explicit and order-independent, matching ONNX&#39;s named-input API exactly. SERVER-CAPPED at 64 entries (see INPUT-SIZE BOUND above). |
| idempotency_key | [string](#string) |  | Client-supplied idempotency key. WHY it still matters even though Predict emits NO event now: Predict is effectively pure (in→out math), but a NETWORK RETRY after a successful inference whose response was lost still pays the full compute cost again. The pod keeps a short-lived result cache keyed by this id so a retry returns the prior result without re-running the ONNX session — pure latency/compute savings, not a correctness/billing concern (billing is the gateway&#39;s, off events.InferenceCompleted). Optional: empty = recompute every time. Recommended for any client that retries. |
| correlation_id | [string](#string) |  | Optional correlation id propagated from the gateway for distributed tracing. Echoed back in PredictResponse so the gateway can stitch this pod&#39;s span to the request it will later report on events.InferenceCompleted. Purely a trace/correlation handle — carries no authority. |






<a name="forgepoint-serving-v1-PredictRequest-InputsEntry"></a>

### PredictRequest.InputsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TensorData](#forgepoint-serving-v1-TensorData) |  |  |






<a name="forgepoint-serving-v1-PredictResponse"></a>

### PredictResponse
PredictResponse returns the model&#39;s output tensors plus serving metadata.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| outputs | [PredictResponse.OutputsEntry](#forgepoint-serving-v1-PredictResponse-OutputsEntry) | repeated | Named output tensors, keyed by TensorSpec.name from output_schema. |
| model_version | [string](#string) |  | The model version that actually produced this result. Echoed so the caller (and the gateway, for canary attribution) records which version answered — critical when traffic is split across versions. |
| inference_latency | [google.protobuf.Duration](#google-protobuf-Duration) |  | Server-measured inference wall time for THIS call (not including network). Lets the gateway record per-call latency and the client display it. |
| correlation_id | [string](#string) |  | Echo of the request&#39;s correlation_id for distributed tracing. The gateway uses it to link this pod&#39;s inference span to the request it will report on events.InferenceCompleted. |
| from_cache | [bool](#bool) |  | True if this response was served from the pod&#39;s result cache (an idempotent retry) rather than freshly computed. Purely an observability/latency signal for the caller (no event is emitted either way — metering is the gateway&#39;s). |






<a name="forgepoint-serving-v1-PredictResponse-OutputsEntry"></a>

### PredictResponse.OutputsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TensorData](#forgepoint-serving-v1-TensorData) |  |  |






<a name="forgepoint-serving-v1-ServingMetrics"></a>

### ServingMetrics
============================================================================
ServingMetrics
============================================================================

WHY this is a first-class RPC response and not just /metrics scraping:
  The HPA-driving signal (inflight_requests) is part of the SERVICE CONTRACT
  in this pattern. Exposing it via gRPC lets the gateway make fast
  load-aware routing decisions (least-inflight) AND lets a metrics adapter
  read a stable, documented shape. The Prometheus /metrics endpoint is the
  transport for the HPA; this message is the canonical schema.

WHY INFLIGHT REQUESTS AND NOT CPU FOR AUTOSCALING:
  Inference latency is dominated by request queuing once the CPU is busy;
  inflight (concurrency/queue depth) crosses the danger threshold BEFORE CPU
  saturates, giving the HPA earlier, more stable scaling signals. This is the
  crux of &#34;HPA on custom metrics&#34;.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| inflight_requests | [int32](#int32) |  | Requests currently being processed (the primary custom HPA metric). HPA targets an average inflight-per-pod; when actual exceeds target, scale out. |
| total_requests | [int64](#int64) |  | Total predictions served since process start. Monotonic counter; used to derive predictions/sec (RED &#34;Rate&#34;) at the scrape layer. |
| failed_requests | [int64](#int64) |  | Total failed predictions since start (RED &#34;Errors&#34;). Combined with total_requests gives an error rate for alerting. |
| p50_latency | [google.protobuf.Duration](#google-protobuf-Duration) |  | Rolling p50 inference latency. WHY Duration not ms-int: Duration is the proto well-known type for elapsed time (nanosecond precision, self- describing) and matches our other time fields&#39; style. |
| p99_latency | [google.protobuf.Duration](#google-protobuf-Duration) |  | Rolling p99 inference latency (RED &#34;Duration&#34;, tail). Also the liveness signal: if p99 exceeds a configured threshold the liveness probe fails and K8s restarts the pod. |
| model_memory_bytes | [int64](#int64) |  | Resident model memory estimate in bytes. Capacity-planning signal for the operator (how many model versions fit per node). Server-computed. |






<a name="forgepoint-serving-v1-StreamPredictRequest"></a>

### StreamPredictRequest
============================================================================
StreamPredict (optional streaming data plane)
============================================================================

WHY a bidirectional stream in addition to unary Predict:
  Two real use cases:
    1. HIGH-THROUGHPUT batch scoring: a client pushes a long sequence of rows
       and reads results as they complete, amortizing TCP/gRPC handshake and
       HTTP/2 framing over many predictions instead of one RPC per row.
    2. ONLINE/low-latency feeds: a long-lived connection (e.g. a feature
       stream) sends inputs continuously and receives predictions, avoiding
       per-request connection setup.

WHY BIDIRECTIONAL (stream→stream) not server-streaming:
  The client decides when to send the next input (it may be reacting to prior
  outputs or rate-limiting itself), so inputs must be a stream too. Responses
  are NOT guaranteed 1:1-ordered with requests in general, which is why each
  carries its own idempotency_key/correlation_id to pair them.

TRADEOFF: streaming complicates the HPA inflight accounting (a stream holds a
connection but may be idle) and load balancing (L7 gRPC LBs balance streams,
not messages). For that reason unary Predict remains the PRIMARY path the
gateway uses; StreamPredict is opt-in for batch/online clients. Documented
because streaming is not free.

Buf note: streaming RPCs still require dedicated Request/Response messages
(RPC_REQUEST_RESPONSE_UNIQUE). We reuse the same tensor shapes but in
stream-specific wrappers so the unary and streaming contracts can evolve
independently.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| inputs | [StreamPredictRequest.InputsEntry](#forgepoint-serving-v1-StreamPredictRequest-InputsEntry) | repeated | One inference&#39;s inputs, same semantics (and the same server-side 64-entry / max-tensor-bytes caps) as PredictRequest.inputs. The server additionally bounds total in-flight stream messages per connection (default 256) to keep HPA inflight accounting and per-pod memory bounded — a slow consumer cannot make the pod buffer unboundedly. |
| idempotency_key | [string](#string) |  | Per-message idempotency key. It serves TWO purposes on a stream: (1) it PAIRS each response to its request (responses are not 1:1-ordered with requests), and (2) it makes a retried/duplicated stream message a cache hit instead of a recompute (same pure latency/compute saving as unary — NOT a billing concern; the pod emits no event). Recommended on every message; if empty, that message is always recomputed and the client must rely on send order to pair its response. |
| correlation_id | [string](#string) |  | Optional per-message correlation id for tracing individual predictions within the stream. |






<a name="forgepoint-serving-v1-StreamPredictRequest-InputsEntry"></a>

### StreamPredictRequest.InputsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TensorData](#forgepoint-serving-v1-TensorData) |  |  |






<a name="forgepoint-serving-v1-StreamPredictResponse"></a>

### StreamPredictResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| outputs | [StreamPredictResponse.OutputsEntry](#forgepoint-serving-v1-StreamPredictResponse-OutputsEntry) | repeated | The outputs for one inference. |
| model_version | [string](#string) |  | The version that produced this result. |
| inference_latency | [google.protobuf.Duration](#google-protobuf-Duration) |  | Inference wall time for this single prediction. |
| idempotency_key | [string](#string) |  | Echoes the request message&#39;s idempotency_key so the client can pair this response with the request it answers (responses are not order-guaranteed). |
| correlation_id | [string](#string) |  | Echoes the request message&#39;s correlation id for tracing. |






<a name="forgepoint-serving-v1-StreamPredictResponse-OutputsEntry"></a>

### StreamPredictResponse.OutputsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TensorData](#forgepoint-serving-v1-TensorData) |  |  |






<a name="forgepoint-serving-v1-TensorData"></a>

### TensorData
TensorData is a single named tensor on the wire (input feature or output).
See the TENSOR PRIMITIVES block above for the full byte-layout contract.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shape | [int64](#int64) | repeated | Dimensions, row-major. e.g. [1, 4] = batch of 1, four features. A leading batch dimension is conventional but not required by this service. |
| data | [bytes](#bytes) |  | Raw little-endian, row-major element bytes. Length MUST equal product(shape) * sizeof(dtype) (or the length-prefixed encoding for STRING). The server rejects mismatches with INVALID_ARGUMENT. |
| dtype | [DataType](#forgepoint-serving-v1-DataType) |  | How to interpret `data`. Must match the model&#39;s expected input dtype; the server validates against the loaded model&#39;s schema. |






<a name="forgepoint-serving-v1-TensorSpec"></a>

### TensorSpec
============================================================================
TensorSpec
============================================================================

WHY: A model&#39;s input/output schema is part of its public contract. Clients
(and the gateway) need to know &#34;this model wants a tensor named &#39;input&#39; of
shape [-1, 4] float32&#34; to build valid requests and validate before sending.
This is the served-side mirror of the registry&#39;s model schema.

DYNAMIC DIMENSIONS: a shape entry of -1 means &#34;dynamic&#34; (typically the batch
dimension). e.g. [-1, 4] = any number of 4-feature rows. This mirrors ONNX&#39;s
own dynamic-axis convention, so SDKs can map it 1:1.
============================================================================


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | The tensor&#39;s name as the ONNX graph expects it (e.g. &#34;input&#34;, &#34;float_input&#34;). Predict requests key their `inputs` map by this name. |
| shape | [int64](#int64) | repeated | Expected dimensions; -1 marks a dynamic axis (usually batch). Informational contract for clients; the runtime enforces the concrete shape at inference. |
| dtype | [DataType](#forgepoint-serving-v1-DataType) |  | Expected element type for this tensor. |






<a name="forgepoint-serving-v1-UnloadModelRequest"></a>

### UnloadModelRequest
UnloadModelRequest frees a model from memory (e.g. before shutdown or to
reclaim RAM). After unload the pod reports MODEL_STATE_UNLOADED and Predict
returns FAILED_PRECONDITION until reloaded. Admin/controller-only — the
caller&#39;s authority comes from the auth interceptor, never from this body.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| model_name | [string](#string) |  | Model to unload. Empty fields = the pod&#39;s single loaded model. |
| version | [string](#string) |  | Version to unload; empty = the pod&#39;s current version. |
| reason | [string](#string) |  | Optional reason for audit/observability (&#34;undeployed&#34;, &#34;shutdown&#34;, &#34;evicted&#34;). Free-form but small; lets operators distinguish an intentional unload from an eviction in logs. NOT published as an event (serving emits none) — the authoritative teardown fact is the orchestrator&#39;s events.ModelUndeployed.reason that TRIGGERED this call. |
| idempotency_key | [string](#string) |  | Idempotency key so a retried UnloadModel (controller re-reconcile after the model is already UNLOADED) is a no-op returning the current status rather than an error. Optional. |






<a name="forgepoint-serving-v1-UnloadModelResponse"></a>

### UnloadModelResponse
UnloadModelResponse is intentionally minimal but named (Buf RPC_RESPONSE_
STANDARD_NAME) and forward-compatible.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [ModelStatus](#forgepoint-serving-v1-ModelStatus) |  | Status after unload (expected MODEL_STATE_UNLOADED). |





 


<a name="forgepoint-serving-v1-DataType"></a>

### DataType
DataType is the element type of a tensor&#39;s raw byte buffer.
WHY an enum (not a free string like &#34;float32&#34;): an enum is validated at the
proto layer, gives generated constants, and prevents typos like &#34;flaot32&#34;.
Suffix/prefix conventions satisfy Buf STANDARD (ENUM_ZERO_VALUE_SUFFIX &#43;
ENUM_VALUE_PREFIX): zero value is *_UNSPECIFIED, every value is DATA_TYPE_*.

| Name | Number | Description |
| ---- | ------ | ----------- |
| DATA_TYPE_UNSPECIFIED | 0 | Unset/unknown. A request with this dtype is rejected (INVALID_ARGUMENT) — forces clients to be explicit about element type. |
| DATA_TYPE_FLOAT32 | 1 | 32-bit IEEE-754 float. The default for most CPU ONNX models (features and logits). 4 bytes/element. |
| DATA_TYPE_FLOAT64 | 2 | 64-bit IEEE-754 float. Some sklearn-exported models use doubles. 8 bytes. |
| DATA_TYPE_INT32 | 3 | 32-bit signed integer. Categorical/index inputs. 4 bytes. |
| DATA_TYPE_INT64 | 4 | 64-bit signed integer. Label outputs, token IDs. 8 bytes. |
| DATA_TYPE_BOOL | 5 | 8-bit boolean (0/1). Mask inputs. 1 byte/element. |
| DATA_TYPE_STRING | 6 | UTF-8 string elements. WHY length-prefixed, not fixed-width: strings are variable length, so for STRING tensors `data` holds, per element, a 4-byte little-endian length followed by that many UTF-8 bytes. Used by NLP models that take raw text. Kept last so adding it didn&#39;t renumber. |



<a name="forgepoint-serving-v1-HealthStatus"></a>

### HealthStatus
HealthStatus expresses the serving-readiness verdict.

| Name | Number | Description |
| ---- | ------ | ----------- |
| HEALTH_STATUS_UNSPECIFIED | 0 | Unknown/unset. |
| HEALTH_STATUS_SERVING | 1 | Model loaded and inference latency within threshold — accept traffic. |
| HEALTH_STATUS_NOT_SERVING | 2 | Process alive but not ready (model still loading, or latency degraded). K8s should keep the pod but route traffic away (readiness fail). |



<a name="forgepoint-serving-v1-ModelState"></a>

### ModelState
============================================================================
ModelState
============================================================================

WHY an enum, not a bool &#34;loaded&#34;: a model in a serving pod moves through a
lifecycle (downloading the artifact from MinIO → loading into ONNX → ready),
and can fail or be unloaded. A single bool can&#39;t express &#34;currently
downloading&#34; vs &#34;load failed&#34;. The gateway&#39;s readiness routing and the
admin UI need the distinction.

STATE MACHINE:

  UNSPECIFIED
       │ LoadModel / startup
       ▼
  DOWNLOADING ──(MinIO fetch ok)──► LOADING ──(ONNX init ok)──► READY
       │                               │                          │
       │ (fetch err)                   │ (init err)               │ UnloadModel
       ▼                               ▼                          ▼
     FAILED ◄───────────────────────FAILED                    UNLOADED

READY is the only state where Predict succeeds; all others return
FAILED_PRECONDITION so the gateway can route elsewhere.
============================================================================

| Name | Number | Description |
| ---- | ------ | ----------- |
| MODEL_STATE_UNSPECIFIED | 0 | Unknown/unset. |
| MODEL_STATE_DOWNLOADING | 1 | Pulling the model artifact from object storage (MinIO/S3) to local disk. |
| MODEL_STATE_LOADING | 2 | Artifact on disk; initializing the ONNX runtime session (allocating the in-memory inference graph). Brief but non-zero — this is &#34;cold start&#34;. |
| MODEL_STATE_READY | 3 | Model is loaded in memory and serving predictions. Readiness probe passes. |
| MODEL_STATE_FAILED | 4 | Load failed (bad artifact, OOM, unsupported op). See ModelStatus.message for the reason. Pod stays unready; K8s/operator decides on restart. |
| MODEL_STATE_UNLOADED | 5 | Model was deliberately unloaded (admin UnloadModel) to free memory. Distinct from FAILED so operators don&#39;t alert on an intentional unload. |


 

 


<a name="forgepoint-serving-v1-ModelServingService"></a>

### ModelServingService
============================================================================
NO EVENT-PAYLOAD MESSAGES HERE — BY DESIGN
============================================================================

A serving pod publishes NOTHING on the NATS bus. The three former event
messages (ModelLoadedEvent, PredictionCompletedEvent, ModelUnloadedEvent)
were DELETED when the platform adopted the canonical event contract
(proto/forgepoint/events/v1/events.proto). See the EVENTING block at the top
of this file for the full rationale. In short:
  - The single inference/metering event is the gateway&#39;s
    events.InferenceCompleted (the gateway owns latency/served-version/
    principal/canary) — a serving pod must not emit a competing one.
  - The authoritative deploy lifecycle is the orchestrator saga&#39;s
    events.ModelDeployed / events.ModelUndeployed — a serving pod loading or
    unloading a model is an implementation detail of that saga, not a second
    source of truth.
Serving CONSUMES events.{ModelVersionReady, ModelDeployed, ModelUndeployed,
ModelPromoted, ModelArchived} (unmarshalled in Go at the consume boundary by
the per-version-Deployment controller) and reacts via the LoadModel /
UnloadModel control-plane RPCs below. Because this proto declares and
references no event type, it does NOT import events.proto — importing without
use would violate the decoupling rule and Buf&#39;s import hygiene.

============================================================================
MODEL SERVING SERVICE
============================================================================

WHY one ModelServingService split into a data plane and a control plane:
  - DATA PLANE (Predict, StreamPredict): the hot path, latency-critical,
    called only by the Inference Gateway, scaled by the HPA. Minimal,
    stateless, auth-agnostic — see the Sidecar rationale at the top.
  - CONTROL PLANE (LoadModel, UnloadModel, GetModelStatus, GetModelInfo,
    GetServingMetrics, HealthCheck): low-frequency lifecycle/observability
    calls made by the operator/controller, probes, and metric adapters.

  They share one service (not two) because they target the SAME pod and the
  same in-memory model; a separate AdminService would just add a second
  listener for no isolation benefit on a single-model pod. Authorization is
  enforced by the interceptor chain (the gateway&#39;s identity may Predict;
  only the operator&#39;s identity may LoadModel/UnloadModel).

RPC CATEGORIES:
  DATA PLANE:    Predict, StreamPredict
  INTROSPECTION: GetModelInfo, ListLoadedModels
  LIFECYCLE:     LoadModel, UnloadModel, GetModelStatus
  AUTOSCALING:   GetServingMetrics
  PROBES:        HealthCheck

NONE of these RPCs publish an event — the data plane returns inline and the
control plane mutates pod-local state; the platform&#39;s record of what served
(events.InferenceCompleted) and what is deployed (events.ModelDeployed/
Undeployed) is produced by the gateway and orchestrator respectively.

ASCII — where each RPC sits:

  ┌───────────────── Inference Gateway ─────────────────┐
  │  Predict / StreamPredict  ──────►  (data plane, HPA-scaled) │
  │  GetServingMetrics  ──────►  least-inflight routing         │
  └────────────────────────────────────────────────────┘
  ┌──────────── Operator / Controller ─────────────────┐
  │  LoadModel / UnloadModel / GetModelStatus / ListLoadedModels ►│
  │  (driven by consumed ModelDeployed/Undeployed/Promoted/...)  │
  └────────────────────────────────────────────────────┘
  ┌──────────────────── Kubelet ───────────────────────┐
  │  HealthCheck (readiness=READY, liveness=latency&lt;thr)────────►│
  └────────────────────────────────────────────────────┘
============================================================================

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Predict | [PredictRequest](#forgepoint-serving-v1-PredictRequest) | [PredictResponse](#forgepoint-serving-v1-PredictResponse) | Predict runs a single synchronous inference. The hot path; called by the Inference Gateway after it has authed, rate-limited, and chosen a version. NO side effect on the bus: the pod returns the result and the GATEWAY publishes events.InferenceCompleted (billing &#43; monitor consume that single canonical event). Idempotent on idempotency_key purely to make gateway retries cheap (result cache, not a billing concern). |
| StreamPredict | [StreamPredictRequest](#forgepoint-serving-v1-StreamPredictRequest) stream | [StreamPredictResponse](#forgepoint-serving-v1-StreamPredictResponse) stream | StreamPredict is a bidirectional stream for high-throughput batch scoring or long-lived online feeds. Opt-in (unary Predict is the primary path). Each request message carries its own idempotency_key; responses are not 1:1-ordered with requests, so clients pair them via that key. |
| GetModelInfo | [GetModelInfoRequest](#forgepoint-serving-v1-GetModelInfoRequest) | [GetModelInfoResponse](#forgepoint-serving-v1-GetModelInfoResponse) | GetModelInfo returns the served model&#39;s identity &#43; I/O schema so clients can build valid Predict requests. All fields server-authoritative. |
| ListLoadedModels | [ListLoadedModelsRequest](#forgepoint-serving-v1-ListLoadedModelsRequest) | [ListLoadedModelsResponse](#forgepoint-serving-v1-ListLoadedModelsResponse) | ListLoadedModels returns the live status of every model resident on the pod (0..1 today; forward-compatible with multi-model). Paginated (common.v1; page_size server-clamped to [1,100]). The reconcile loop uses it to compare desired (consumed ModelDeployed events) vs actual (resident) models. |
| LoadModel | [LoadModelRequest](#forgepoint-serving-v1-LoadModelRequest) | [LoadModelResponse](#forgepoint-serving-v1-LoadModelResponse) | LoadModel pulls an artifact from object storage and loads it into the ONNX runtime. Admin/controller-only — typically driven by a consumed events.ModelDeployed / events.ModelVersionReady. Load is async — poll GetModelStatus for READY. Idempotent: re-loading an already-READY identical (name,version,digest) is a no-op returning current status. SSRF-guarded: artifact_uri is validated against the pod&#39;s allow-listed bucket/prefix. |
| UnloadModel | [UnloadModelRequest](#forgepoint-serving-v1-UnloadModelRequest) | [UnloadModelResponse](#forgepoint-serving-v1-UnloadModelResponse) | UnloadModel frees the model from memory. Admin/controller-only — typically driven by a consumed events.ModelUndeployed / events.ModelArchived. After unload Predict returns FAILED_PRECONDITION. Publishes NO event (the authoritative teardown fact is the orchestrator&#39;s events.ModelUndeployed). |
| GetModelStatus | [GetModelStatusRequest](#forgepoint-serving-v1-GetModelStatusRequest) | [GetModelStatusResponse](#forgepoint-serving-v1-GetModelStatusResponse) | GetModelStatus returns the live lifecycle state (DOWNLOADING/LOADING/READY/ FAILED/UNLOADED). Used by the operator&#39;s reconcile loop and admin tooling. |
| GetServingMetrics | [GetServingMetricsRequest](#forgepoint-serving-v1-GetServingMetricsRequest) | [GetServingMetricsResponse](#forgepoint-serving-v1-GetServingMetricsResponse) | GetServingMetrics returns the inflight/latency snapshot that drives the HPA (custom-metrics autoscaling) and the gateway&#39;s least-inflight routing. |
| HealthCheck | [HealthCheckRequest](#forgepoint-serving-v1-HealthCheckRequest) | [HealthCheckResponse](#forgepoint-serving-v1-HealthCheckResponse) | HealthCheck reports serving readiness (model loaded) &#43; the latency signal behind liveness. Wired to K8s readiness/liveness probes for this per-model Deployment. |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

