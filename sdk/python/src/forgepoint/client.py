"""The high-level Forgepoint client.

:class:`Client` is the single entry point of the SDK. It owns one gRPC channel,
the per-service stubs built on it, the shared token holder, and ergonomic typed
methods for the core platform flows (auth, registry, pipeline, monitor). Design
goals, in priority order:

  1. SECURE BY DEFAULT — TLS channel unless the caller *explicitly* opts out for
     localhost (``insecure=True``). No silent plaintext.
  2. ERGONOMIC — ``client.login(...)`` then ``client.register_model(...)``; the
     JWT plumbing, metadata, and error translation are invisible.
  3. TYPED — methods take/return the generated proto messages (which carry .pyi
     stubs), with thin helpers where a Pythonic shape reads better.
  4. SAFE — typed exceptions (not raw grpc.RpcError), context-manager cleanup,
     and a token that is never logged/printed/URL-embedded.

WHY ONE CHANNEL, MANY STUBS
---------------------------
All Forgepoint services *can* sit behind one gRPC endpoint (e.g. the in-cluster
mesh address, or a single port-forward to one service at a time). gRPC stubs are
cheap wrappers over a channel, so we build one channel and instantiate every
service stub on it. The auth interceptor wraps the channel ONCE, so all stubs are
authenticated uniformly. (If different services live at different addresses, make
one Client per address — the object is light.)
"""

from __future__ import annotations

import os
from typing import Iterator, Mapping, Optional, Sequence

import grpc

# Importing the _proto package runs its __path__ extension shim so the generated
# absolute imports (`from forgepoint.auth.v1 import auth_pb2`) resolve. MUST
# precede the stub imports below.
from . import _proto  # noqa: F401  (import for its __path__ side effect)
from ._interceptors import _AuthInterceptor, _TokenHolder
from .errors import (
    AuthenticationRequiredError,
    ConfigurationError,
    from_rpc_error,
)

# Generated stubs + message modules. These resolve thanks to the _proto shim.
from forgepoint.auth.v1 import auth_pb2, auth_pb2_grpc
from forgepoint.registry.v1 import registry_pb2, registry_pb2_grpc
from forgepoint.pipeline.v1 import pipeline_pb2, pipeline_pb2_grpc
from forgepoint.monitor.v1 import monitor_pb2, monitor_pb2_grpc
from forgepoint.common.v1 import common_pb2

# Environment variable names used as fallbacks for target/token. WHY env-first:
# 12-factor — endpoint and credentials are runtime config, never hardcoded. The
# examples and CLI read these so no secret/endpoint is baked into source.
ENV_TARGET = "FORGEPOINT_TARGET"
ENV_TOKEN = "FORGEPOINT_TOKEN"
ENV_INSECURE = "FORGEPOINT_INSECURE"  # "1"/"true" to allow a plaintext channel

# Hosts for which an insecure (plaintext) channel is *permitted* when the caller
# opts in. We still require the explicit insecure flag; this list just refuses to
# let the opt-out apply to a non-local host by accident.
_LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1", "[::1]")


def _is_local_target(target: str) -> bool:
    """True if the gRPC target's host is loopback (localhost/127.0.0.1/::1).

    ONLY the literal hosts ``localhost``, ``127.0.0.1``, and ``::1`` qualify for
    the plaintext (``insecure=True``) opt-out. Any other host — including other
    loopback-range addresses like ``127.0.0.2`` or the unspecified ``0.0.0.0`` —
    is refused by design, so a plaintext channel can never carry the Bearer token
    to anything but a definitively-local endpoint.
    """
    host = target
    # Strip a scheme if present (grpc targets are usually host:port, but be lenient).
    if "://" in host:
        host = host.split("://", 1)[1]
    # IPv6 in brackets, e.g. [::1]:50051
    if host.startswith("["):
        return host.split("]", 1)[0] + "]" in _LOCAL_HOSTS or host.startswith("[::1]")
    host = host.rsplit(":", 1)[0] if ":" in host else host
    return host in _LOCAL_HOSTS


class Client:
    """Typed, secure, ergonomic client for the Forgepoint platform.

    Example::

        import forgepoint
        with forgepoint.Client(target="api.forgepoint.example:443") as fp:
            fp.login(email="me@corp.com", password=os.environ["FP_PASSWORD"])
            model = fp.register_model(name="fraud-detector", framework="onnx")
            for m in fp.list_models().models:
                print(m.name, m.production_version)

    Args:
        target: gRPC ``host:port`` (e.g. ``"api.forgepoint:443"``). Falls back to
            the ``FORGEPOINT_TARGET`` env var. Required (here or in env).
        token: An existing bearer JWT/API key. Optional — you can instead call
            :meth:`login`. Falls back to ``FORGEPOINT_TOKEN``.
        insecure: Opt into a PLAINTEXT channel. Honored ONLY for the literal
            loopback hosts ``localhost``, ``127.0.0.1``, and ``::1`` — any other
            host (e.g. ``127.0.0.2`` or ``0.0.0.0``) is refused by design.
            Defaults to the ``FORGEPOINT_INSECURE`` env var (off). Use for local
            dev against a port-forwarded service with no TLS.
        tls_root_certs: Optional PEM bytes for a custom CA (e.g. a self-signed
            cluster cert). Ignored when ``insecure``.
        default_timeout: Default per-RPC deadline in seconds. ``None`` = no
            client-side deadline (rely on the server). Recommended to set one.
        channel_options: Extra gRPC channel options (advanced).
    """

    def __init__(
        self,
        target: Optional[str] = None,
        token: Optional[str] = None,
        *,
        insecure: Optional[bool] = None,
        tls_root_certs: Optional[bytes] = None,
        default_timeout: Optional[float] = 30.0,
        channel_options: Optional[Sequence[tuple[str, object]]] = None,
    ) -> None:
        target = target or os.environ.get(ENV_TARGET)
        if not target:
            raise ConfigurationError(
                "no target: pass target=... or set the FORGEPOINT_TARGET env var "
                "(e.g. 'api.forgepoint.example:443' or 'localhost:50051')"
            )
        self._target = target
        self._default_timeout = default_timeout

        # Token holder is shared by reference with the auth interceptor, so a
        # later login() transparently authenticates already-built stubs.
        self._token_holder = _TokenHolder(token or os.environ.get(ENV_TOKEN))

        if insecure is None:
            insecure = os.environ.get(ENV_INSECURE, "").lower() in ("1", "true", "yes")

        self._channel = self._build_channel(
            target, insecure=insecure, tls_root_certs=tls_root_certs, options=channel_options
        )

        # One interceptor instance authenticates every stub (unary + streaming).
        intercepted = grpc.intercept_channel(self._channel, _AuthInterceptor(self._token_holder))

        # Build the per-service stubs on the intercepted channel.
        self._auth = auth_pb2_grpc.AuthServiceStub(intercepted)
        self._registry = registry_pb2_grpc.RegistryServiceStub(intercepted)
        self._pipeline = pipeline_pb2_grpc.PipelineOrchestratorServiceStub(intercepted)
        self._monitor = monitor_pb2_grpc.MonitorServiceStub(intercepted)

    # ------------------------------------------------------------------ #
    # Channel construction (secure-by-default)                            #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _build_channel(
        target: str,
        *,
        insecure: bool,
        tls_root_certs: Optional[bytes],
        options: Optional[Sequence[tuple[str, object]]],
    ) -> grpc.Channel:
        """Create a secure channel by default; plaintext only on explicit opt-out.

        SECURITY (interview point): the *default* path is ``secure_channel`` with
        TLS. ``insecure_channel`` is reachable ONLY when the caller passes
        ``insecure=True`` AND the target is loopback. Refusing an insecure channel
        to a remote host prevents the classic "I set insecure for local dev and
        shipped it to prod" credential-leak footgun — a plaintext channel would
        send the Bearer token in the clear.
        """
        opts = list(options or ())
        if insecure:
            if not _is_local_target(target):
                raise ConfigurationError(
                    f"insecure=True is only allowed for localhost targets, not {target!r}. "
                    "Refusing to send credentials over a plaintext channel to a remote host."
                )
            # Plaintext: acceptable ONLY for a local, TLS-terminating dev setup.
            return grpc.insecure_channel(target, options=opts)

        # Default: TLS. With no custom root certs, grpc uses the system trust store.
        creds = grpc.ssl_channel_credentials(root_certificates=tls_root_certs)
        return grpc.secure_channel(target, creds, options=opts)

    # ------------------------------------------------------------------ #
    # Lifecycle / context manager                                         #
    # ------------------------------------------------------------------ #
    def close(self) -> None:
        """Close the underlying gRPC channel and release its resources."""
        self._channel.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @property
    def is_authenticated(self) -> bool:
        """True if the client currently holds a token (does not validate it)."""
        return self._token_holder.token is not None

    def set_token(self, token: str) -> None:
        """Set/replace the bearer token used for subsequent RPCs.

        Useful when you obtained a token out of band (e.g. an API key). The token
        is stored only in the in-memory holder and only ever sent as request
        metadata — never logged or persisted by the SDK.
        """
        self._token_holder.token = token

    # ------------------------------------------------------------------ #
    # Auth precondition: fail fast, locally, before any wire call         #
    # ------------------------------------------------------------------ #
    def _require_auth(self) -> None:
        """Guard every authenticated method: no token → fail fast, locally.

        WHY (interview point): without this, calling e.g. ``list_models()`` on a
        client that never logged in would still make a round-trip and come back
        with an opaque server ``UNAUTHENTICATED``. That wastes a network call and
        hides the real cause (you forgot to authenticate). Raising
        :class:`AuthenticationRequiredError` here — BEFORE the RPC — turns that
        into an immediate, self-explanatory client-side error.

        Only :meth:`login` is exempt: it is the one method that *mints* the token,
        so it must run without one. Everything else (including ``create_user``,
        which needs an admin token) calls this first.
        """
        if not self._token_holder.token:
            raise AuthenticationRequiredError(
                "no token set; call login(email, password) or construct "
                "Client(token=...) first"
            )

    # ------------------------------------------------------------------ #
    # Internal RPC wrapper: timeout + error translation in ONE place      #
    # ------------------------------------------------------------------ #
    def _timeout(self, override: Optional[float]) -> Optional[float]:
        return override if override is not None else self._default_timeout

    def _call(self, stub_method, request, timeout: Optional[float]):
        """Invoke a unary RPC, translating grpc.RpcError → typed SDK error.

        This is the SINGLE boundary where raw gRPC errors are caught and mapped,
        so every public method gets typed exceptions for free and no method has to
        repeat the try/except.
        """
        try:
            return stub_method(request, timeout=self._timeout(timeout))
        except grpc.RpcError as e:
            raise from_rpc_error(e) from None

    # ================================================================== #
    # AUTH                                                                #
    # ================================================================== #
    def login(self, email: str, password: str, *, timeout: Optional[float] = None) -> str:
        """Authenticate with email + password; store and return the access token.

        On success the returned JWT is stored in the shared token holder, so EVERY
        subsequent RPC on this client is automatically authenticated — you do not
        pass the token around. Returns the raw token string for callers that want
        to cache it (e.g. write to a keyring) — the SDK itself never persists it.

        SECURITY: ``password`` is sent only inside the LoginRequest body over the
        (TLS, by default) channel and is never logged. The resulting token is held
        in memory only.

        Raises:
            UnauthenticatedError: wrong email/password.
        """
        req = auth_pb2.LoginRequest(email=email, password=password)
        resp = self._call(self._auth.Login, req, timeout)
        # Store the token so the interceptor authenticates later calls.
        self._token_holder.token = resp.access_token
        return resp.access_token

    def create_user(
        self,
        *,
        email: str,
        name: str,
        password: str,
        team: str,
        timeout: Optional[float] = None,
    ) -> auth_pb2.User:
        """Provision a new user account (requires admin). Returns the new ``User``.

        The server bcrypt-hashes the password; the returned User never contains
        it. Requires the client to already be authenticated as an admin.
        """
        self._require_auth()
        req = auth_pb2.CreateUserRequest(email=email, name=name, password=password, team=team)
        resp = self._call(self._auth.CreateUser, req, timeout)
        return resp.user

    # ================================================================== #
    # MODEL REGISTRY                                                       #
    # ================================================================== #
    def register_model(
        self,
        *,
        name: str,
        description: str = "",
        framework: str = "",
        task_type: str = "",
        tags: Optional[Mapping[str, str]] = None,
        idempotency_key: str = "",
        timeout: Optional[float] = None,
    ) -> registry_pb2.Model:
        """Register a new model (the identity; versions are added separately).

        ``owner_id``/``team`` are derived SERVER-side from your token — the SDK
        cannot and does not set them. Pass an ``idempotency_key`` (a UUID) to make
        a retried call return the same model instead of erroring on the name.

        Raises:
            AlreadyExistsError: the name is already taken in your team (and no
                idempotency key matched).
        """
        self._require_auth()
        req = registry_pb2.RegisterModelRequest(
            name=name,
            description=description,
            framework=framework,
            task_type=task_type,
            tags=dict(tags or {}),
            idempotency_key=idempotency_key,
        )
        resp = self._call(self._registry.RegisterModel, req, timeout)
        return resp.model

    def get_model(
        self,
        *,
        id: str = "",
        name: str = "",
        timeout: Optional[float] = None,
    ) -> registry_pb2.Model:
        """Fetch one model by ``id`` (preferred) or ``name``.

        CONSISTENCY: served from the eventually-consistent Redis read projection —
        a just-registered model may briefly raise :class:`NotFoundError` until the
        projection catches up.
        """
        self._require_auth()
        if not id and not name:
            raise ConfigurationError("get_model requires either id= or name=")
        req = registry_pb2.GetModelRequest(id=id, name=name)
        resp = self._call(self._registry.GetModel, req, timeout)
        return resp.model

    def list_models(
        self,
        *,
        task_type_filter: str = "",
        framework_filter: str = "",
        include_archived: bool = False,
        page_size: int = 0,
        page_token: str = "",
        timeout: Optional[float] = None,
    ) -> registry_pb2.ListModelsResponse:
        """List models in your team (newest first), with optional filters.

        Returns the full response so you can read both ``.models`` and
        ``.pagination.next_page_token``. ``page_size`` is capped at 100 server-side.
        Team scoping is applied from your token — you only ever see your team.
        """
        self._require_auth()
        req = registry_pb2.ListModelsRequest(
            task_type_filter=task_type_filter,
            framework_filter=framework_filter,
            include_archived=include_archived,
            pagination=common_pb2.PaginationRequest(page_size=page_size, page_token=page_token),
        )
        return self._call(self._registry.ListModels, req, timeout)

    def list_versions(
        self,
        *,
        model_id: str,
        stage_filter: "registry_pb2.ModelStage.ValueType" = registry_pb2.MODEL_STAGE_UNSPECIFIED,
        page_size: int = 0,
        page_token: str = "",
        timeout: Optional[float] = None,
    ) -> registry_pb2.ListVersionsResponse:
        """List a model's versions (newest first), optionally filtered by stage.

        ``stage_filter`` is a ``registry_pb2.ModelStage`` enum value; the default
        (``MODEL_STAGE_UNSPECIFIED``) means "any stage". Use e.g.
        ``forgepoint.ModelStage.MODEL_STAGE_PRODUCTION`` to list only the prod
        version.
        """
        self._require_auth()
        req = registry_pb2.ListVersionsRequest(
            model_id=model_id,
            stage_filter=stage_filter,
            pagination=common_pb2.PaginationRequest(page_size=page_size, page_token=page_token),
        )
        return self._call(self._registry.ListVersions, req, timeout)

    # ================================================================== #
    # PIPELINE ORCHESTRATOR                                                #
    # ================================================================== #
    def start_execution(
        self,
        *,
        pipeline_id: str,
        input: Optional[Mapping[str, object]] = None,
        idempotency_key: str = "",
        timeout: Optional[float] = None,
    ) -> pipeline_pb2.Execution:
        """Trigger a new run of a pipeline. Returns the started ``Execution``.

        ``input`` is an arbitrary JSON-like mapping (e.g. ``{"model_id": "..."}``)
        converted to a protobuf Struct. Pass an ``idempotency_key`` so a client
        retry returns the SAME execution instead of starting a duplicate run —
        strongly recommended for any automated trigger.

        (Named ``start_execution``; ``start`` is also exposed as an alias.)
        """
        self._require_auth()
        req = pipeline_pb2.TriggerExecutionRequest(
            pipeline_id=pipeline_id,
            idempotency_key=idempotency_key,
        )
        if input:
            # Struct.update converts a Python dict into a protobuf Struct in place.
            req.input.update(dict(input))
        resp = self._call(self._pipeline.TriggerExecution, req, timeout)
        return resp.execution

    # Alias matching the task's "pipeline (start, ...)" wording.
    start = start_execution

    def get_execution(
        self, *, execution_id: str, timeout: Optional[float] = None
    ) -> pipeline_pb2.Execution:
        """Fetch the current state of a pipeline run (point-in-time poll).

        Includes the per-step timeline (``.step_executions``). For live updates,
        use :meth:`watch_execution` instead of polling this in a loop.
        """
        self._require_auth()
        req = pipeline_pb2.GetExecutionRequest(execution_id=execution_id)
        resp = self._call(self._pipeline.GetExecution, req, timeout)
        return resp.execution

    def watch_execution(
        self,
        *,
        execution_id: str,
        include_current_state: bool = True,
        timeout: Optional[float] = None,
    ) -> Iterator[pipeline_pb2.WatchExecutionResponse]:
        """Stream live updates for one execution as a Python generator.

        This wraps the server-streaming ``WatchExecution`` RPC. Iterate it::

            for update in fp.watch_execution(execution_id=eid):
                print(update.execution.status, update.sequence)
                if update.execution.status in TERMINAL_STATES:
                    break

        WHY A GENERATOR: server-streaming RPCs return an iterator of responses;
        exposing it as a generator is the Pythonic shape — the caller just loops.
        The auth interceptor authenticates the stream like any other call.

        With ``include_current_state=True`` (default) the server first replays the
        current state, then streams subsequent transitions — avoiding a
        get-then-watch race. The stream closes when the execution reaches a
        terminal state (COMPLETED/FAILED/CANCELLED).

        NOTE on timeout: a long-running watch should usually pass ``timeout=None``
        (no deadline) — a 30s default would cut the stream off. We therefore
        default the stream deadline to ``None`` rather than the client default.

        ERRORS: gRPC raises lazily on streams (on first iteration). We translate
        the error when it surfaces, so callers still catch typed SDK exceptions.

        RESOURCE SAFETY: if the caller abandons the generator early (``break`` or
        an exception before the terminal state), the underlying gRPC server-stream
        would otherwise stay open until GC — leaking a connection and leaving the
        server streaming into the void. We therefore cancel the stream in a
        ``finally``, which the generator runs whenever it is closed (loop exit,
        ``break``, ``.close()``, or GC). ``cancel()`` is idempotent, so cancelling
        an already-finished stream is harmless.
        """
        self._require_auth()
        req = pipeline_pb2.WatchExecutionRequest(
            execution_id=execution_id,
            include_current_state=include_current_state,
        )
        # Streams default to NO client deadline unless explicitly given one.
        stream = self._pipeline.WatchExecution(req, timeout=timeout)
        try:
            for update in stream:
                yield update
        except grpc.RpcError as e:
            raise from_rpc_error(e) from None
        finally:
            # Deterministic teardown: always cancel, even on early break/exception.
            stream.cancel()

    # ================================================================== #
    # MODEL MONITOR                                                        #
    # ================================================================== #
    def list_drift_reports(
        self,
        *,
        model_name: str,
        min_severity: "monitor_pb2.DriftSeverity.ValueType" = monitor_pb2.DRIFT_SEVERITY_UNSPECIFIED,
        page_size: int = 0,
        page_token: str = "",
        timeout: Optional[float] = None,
    ) -> monitor_pb2.ListDriftReportsResponse:
        """List a model's drift reports (newest first), optionally by severity.

        ``model_name`` is required (drift history is always read per-model).
        ``min_severity`` is a ``monitor_pb2.DriftSeverity`` enum (default = all).
        Use ``forgepoint.DriftSeverity.DRIFT_SEVERITY_CRITICAL`` to see only
        critical reports. ``page_size`` is capped at 100 server-side.
        """
        self._require_auth()
        req = monitor_pb2.ListDriftReportsRequest(
            model_name=model_name,
            min_severity=min_severity,
            pagination=common_pb2.PaginationRequest(page_size=page_size, page_token=page_token),
        )
        return self._call(self._monitor.ListDriftReports, req, timeout)

    def __repr__(self) -> str:  # pragma: no cover - trivial
        # Never leak the token. Show only connection-level state.
        return (
            f"forgepoint.Client(target={self._target!r}, "
            f"authenticated={self.is_authenticated})"
        )
