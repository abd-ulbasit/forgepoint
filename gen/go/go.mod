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
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
)
