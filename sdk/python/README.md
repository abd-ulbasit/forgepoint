# Forgepoint Python SDK

A typed, secure, ergonomic Python client for the [Forgepoint](../../README.md) ML
platform. It wraps Forgepoint's gRPC APIs (defined API-first in
[`proto/`](../../proto)) with a single high-level `Client`: secure-by-default
channels, transparent JWT auth, a streaming pipeline watcher, and typed
exceptions instead of raw gRPC errors.

```python
import os
import forgepoint

with forgepoint.Client(target=os.environ["FORGEPOINT_TARGET"]) as fp:
    fp.login(email="me@corp.com", password=os.environ["FP_PASSWORD"])
    model = fp.register_model(name="fraud-detector", framework="onnx")
    for m in fp.list_models().models:
        print(m.name, m.production_version)
```

---

## Install

```bash
# From the repo, editable (dev) install:
cd sdk/python
python -m pip install -e .

# With dev tooling (ruff, mypy, grpcio-tools):
python -m pip install -e '.[dev]'
```

Runtime dependencies are just `grpcio` and `protobuf`. Python 3.9+.

The package ships PEP 561 inline types (a `py.typed` marker and `.pyi` stubs for
all generated messages), so `mypy` and editors get full type information.

---

## Endpoint / how to reach the services

The SDK speaks **gRPC** directly to Forgepoint services. There are two common
ways to get a `target`:

### 1. Port-forward a service (local dev against the cluster)

Each service exposes a gRPC port. Forward it and point the SDK at `localhost`:

```bash
# Example: forward the auth + registry + pipeline + monitor services.
# (Run each in its own terminal, or use distinct local ports.)
kubectl -n fp-system port-forward svc/auth      50051:50051
kubectl -n fp-system port-forward svc/registry  50052:50051
```

Because a `Client` holds one channel, target **one** service endpoint per client
for cross-service flows you would run one client per address, or route everything
through a single gateway address. For local TLS-less dev, opt into an insecure
channel **explicitly** (only allowed for localhost):

```python
fp = forgepoint.Client(target="localhost:50051", insecure=True)
```

### 2. The platform endpoint (TLS, default)

In a real deployment the services sit behind a TLS endpoint (mesh ingress / API
gateway). Just pass the host:port — the channel is TLS by default:

```python
fp = forgepoint.Client(target="api.forgepoint.example:443")
```

> **The BFF is HTTP/JSON, not gRPC.** Forgepoint also has a BFF
> (`services/bff`) that exposes a REST surface (`POST /api/v1/login`,
> `GET /api/v1/models`, …) for the web UI. This SDK targets the **gRPC** services
> directly, which is the richer, typed surface (it includes server-streaming
> `watch_execution`, which the BFF bridges to SSE). Use the BFF from a browser;
> use this SDK from Python.

### Configuration via environment (12-factor)

`target`, `token`, and the insecure opt-out can all come from the environment, so
nothing is hardcoded:

| Env var | Maps to | Example |
|---|---|---|
| `FORGEPOINT_TARGET` | `Client(target=...)` | `api.forgepoint.example:443` |
| `FORGEPOINT_TOKEN` | `Client(token=...)` | `<a JWT or API key>` |
| `FORGEPOINT_INSECURE` | `Client(insecure=...)` | `1` (localhost only) |

```python
fp = forgepoint.Client()  # reads FORGEPOINT_TARGET / FORGEPOINT_TOKEN
```

---

## Authentication flow

```
┌────────────┐  login(email, password)   ┌──────────────┐
│  Client    │ ────────────────────────► │ AuthService  │
│            │ ◄──────────────────────── │  .Login      │
│            │      access_token (JWT)   └──────────────┘
│  token     │
│  holder    │   every later RPC: metadata "authorization: Bearer <jwt>"
│   ▲        │ ────────────────────────► any service (registry, pipeline, …)
└───┼────────┘
    │ written once by login(); read on every call by the auth interceptor
```

1. `fp.login(email, password)` calls `AuthService.Login`, which returns a signed
   JWT. The SDK stores it in an in-memory **token holder**.
2. A gRPC **client interceptor** wraps the channel and attaches
   `authorization: Bearer <jwt>` metadata to **every** subsequent RPC — you never
   pass the token around. Adding a new SDK method is authenticated automatically.
3. The Forgepoint services validate that token (the server-side auth interceptor
   in `pkg/grpcutil`) and enforce RBAC.

You can also supply a token directly (e.g. a long-lived **API key**) instead of
logging in:

```python
fp = forgepoint.Client(target=..., token=os.environ["FORGEPOINT_TOKEN"])
# or later:
fp.set_token(my_api_key)
```

### Security properties

- **Secure by default.** The channel is TLS unless you pass `insecure=True`, and
  that opt-out is **refused for non-localhost targets** — so a dev shortcut can't
  silently ship plaintext credentials to prod.
- **The token is never logged, printed, or put in a URL.** It lives only in the
  token holder and is sent only as request metadata. `repr(client)` and the token
  holder's `repr` redact it.
- **No hardcoded secrets or endpoints.** `target` and `token` are inputs and are
  env-overridable.

### Typed errors

gRPC status codes are mapped to typed exceptions so you `except` by meaning:

| gRPC status | SDK exception |
|---|---|
| `UNAUTHENTICATED` | `forgepoint.UnauthenticatedError` |
| `PERMISSION_DENIED` | `forgepoint.PermissionDeniedError` |
| `NOT_FOUND` | `forgepoint.NotFoundError` |
| `ALREADY_EXISTS` | `forgepoint.AlreadyExistsError` |
| `INVALID_ARGUMENT` | `forgepoint.InvalidArgumentError` |
| `DEADLINE_EXCEEDED` | `forgepoint.DeadlineExceededError` |
| `UNAVAILABLE` | `forgepoint.UnavailableError` |

All derive from `forgepoint.ForgepointError`. Each keeps the original
`grpc.StatusCode` on `.code` and the server message on `.details`.

```python
try:
    fp.get_model(name="fraud-detector")
except forgepoint.NotFoundError:
    ...
except forgepoint.UnauthenticatedError:
    fp.login(email, password)  # token expired — re-auth and retry
```

---

## Client API surface

| Area | Method | Underlying RPC |
|---|---|---|
| **Auth** | `login(email, password) -> str` | `AuthService.Login` |
| | `create_user(email, name, password, team) -> User` | `AuthService.CreateUser` |
| **Registry** | `register_model(name, ...) -> Model` | `RegistryService.RegisterModel` |
| | `get_model(id= / name=) -> Model` | `RegistryService.GetModel` |
| | `list_models(...) -> ListModelsResponse` | `RegistryService.ListModels` |
| | `list_versions(model_id, ...) -> ListVersionsResponse` | `RegistryService.ListVersions` |
| **Pipeline** | `start_execution(pipeline_id, input=, ...) -> Execution` (alias `start`) | `…OrchestratorService.TriggerExecution` |
| | `get_execution(execution_id) -> Execution` | `…OrchestratorService.GetExecution` |
| | `watch_execution(execution_id) -> Iterator[...]` (generator) | `…OrchestratorService.WatchExecution` (server-stream) |
| **Monitor** | `list_drift_reports(model_name, ...) -> ListDriftReportsResponse` | `MonitorService.ListDriftReports` |
| **Lifecycle** | `close()`, `with Client(...) as c:` | — |

Methods take/return the generated proto messages (which carry `.pyi` types).
Convenience enums are re-exported: `forgepoint.ModelStage`,
`forgepoint.VersionStatus`, `forgepoint.ExecutionStatus`,
`forgepoint.PipelineType`, `forgepoint.DriftSeverity`.

---

## Examples

Runnable scripts live in [`examples/`](examples). They all read `target` and
credentials from the environment — no secrets in the source.

```bash
export FORGEPOINT_TARGET=localhost:50051
export FORGEPOINT_INSECURE=1          # localhost dev only
export FP_EMAIL=me@corp.com
export FP_PASSWORD=...                 # not echoed; read from env

python examples/01_login.py
python examples/02_register_and_list.py
python examples/03_watch_execution.py
```

- **`01_login.py`** — authenticate and print the logged-in user (never the token).
- **`02_register_and_list.py`** — login → register a model → list models →
  list its versions.
- **`03_watch_execution.py`** — login → trigger a pipeline execution → stream its
  progress with the `watch_execution` generator until it reaches a terminal state.

---

## Regenerating the gRPC stubs

The generated protobuf/gRPC code under `src/forgepoint/_proto/` is produced by
**buf** from the platform protos, using an **isolated** template that writes
**only** into this SDK (it never touches `gen/go`):

```bash
# From the repo root:
cd proto && buf generate --template ../sdk/python/buf.gen.yaml
```

This runs three remote buf plugins — `protocolbuffers/python` (messages),
`grpc/python` (service stubs), and `protocolbuffers/pyi` (type stubs) — into
`src/forgepoint/_proto/forgepoint/…`. See
[`buf.gen.yaml`](buf.gen.yaml) for why `out:` is relative to `proto/` and how the
generated absolute imports are made resolvable (the `__path__` extension shim in
`src/forgepoint/_proto/__init__.py`, which extends the parent package's `__path__`
rather than touching `sys.path`).

> Do **not** edit anything under `_proto/` by hand — it is regenerated.

---

## API reference docs

Full message/service reference (generated from the same protos) lives in
[`../../docs/api/`](../../docs/api). See
[`docs/api/proto-reference.md`](../../docs/api/proto-reference.md).
