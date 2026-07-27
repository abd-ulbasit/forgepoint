// Module for ALL generated proto/gRPC code (github.com/abd-ulbasit/forgepoint/gen/go).
//
// WHY a dedicated module for generated code:
//   - The go_package option in every .proto resolves to
//     github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/<svc>/v1, so a module
//     rooted here owns that import path for every service.
//   - Generated code changes on every `make proto`; isolating it in its own module
//     keeps its (protobuf/grpc) dependency set separate from hand-written services.
//   - All services depend on this module via the workspace (go.work) — one place to
//     bump protobuf/grpc versions for the whole platform.
module github.com/abd-ulbasit/forgepoint/gen/go

go 1.25.0

require (
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.39.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
)
