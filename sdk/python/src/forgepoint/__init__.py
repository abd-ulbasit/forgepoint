"""Forgepoint Python SDK — a typed, secure client for the Forgepoint ML platform.

Quickstart::

    import os
    import forgepoint

    with forgepoint.Client(target=os.environ["FORGEPOINT_TARGET"]) as fp:
        fp.login(email="me@corp.com", password=os.environ["FP_PASSWORD"])
        model = fp.register_model(name="fraud-detector", framework="onnx")
        for m in fp.list_models().models:
            print(m.name, m.production_version)

The public surface is intentionally small and stable:

  * :class:`Client` — the one object you use.
  * The typed error hierarchy (:class:`ForgepointError` and subclasses) for
    ``except`` handling.
  * Re-exported proto enums you commonly need as method arguments
    (:data:`ModelStage`, :data:`DriftSeverity`, :data:`ExecutionStatus`).

Everything under :mod:`forgepoint._proto` is GENERATED code (regenerate with
``buf generate --template sdk/python/buf.gen.yaml`` from ``proto/``); you can use
the generated message classes directly, but most users only need :class:`Client`.
"""

from __future__ import annotations

# FIRST: import the generated-stubs subpackage so it extends this package's
# __path__ (see forgepoint/_proto/__init__.py). This MUST precede any
# `from forgepoint.<svc>...` import below, or those absolute imports won't resolve.
from . import _proto  # noqa: F401  (import for its __path__ side effect)

# Importing client pulls in the per-service stubs (now resolvable) + the Client.
from .client import (
    Client,
    ENV_INSECURE,
    ENV_TARGET,
    ENV_TOKEN,
)
from .errors import (
    AlreadyExistsError,
    AuthenticationRequiredError,
    ConfigurationError,
    DeadlineExceededError,
    ForgepointError,
    InvalidArgumentError,
    NotFoundError,
    PermissionDeniedError,
    UnauthenticatedError,
    UnavailableError,
)

# Re-export the proto modules and the few enums callers pass to methods, so users
# can write `forgepoint.ModelStage.MODEL_STAGE_PRODUCTION` without reaching into
# the generated package path. (These resolve via the client import above.)
from forgepoint.auth.v1 import auth_pb2
from forgepoint.registry.v1 import registry_pb2
from forgepoint.pipeline.v1 import pipeline_pb2
from forgepoint.monitor.v1 import monitor_pb2
from forgepoint.common.v1 import common_pb2

# Convenience enum aliases (the enum "container" message types carry the values).
ModelStage = registry_pb2.ModelStage
VersionStatus = registry_pb2.VersionStatus
ExecutionStatus = pipeline_pb2.ExecutionStatus
PipelineType = pipeline_pb2.PipelineType
DriftSeverity = monitor_pb2.DriftSeverity

__version__ = "0.1.0"

__all__ = [
    "Client",
    "ForgepointError",
    "ConfigurationError",
    "AuthenticationRequiredError",
    "UnauthenticatedError",
    "PermissionDeniedError",
    "NotFoundError",
    "AlreadyExistsError",
    "InvalidArgumentError",
    "DeadlineExceededError",
    "UnavailableError",
    "ModelStage",
    "VersionStatus",
    "ExecutionStatus",
    "PipelineType",
    "DriftSeverity",
    "auth_pb2",
    "registry_pb2",
    "pipeline_pb2",
    "monitor_pb2",
    "common_pb2",
    "ENV_TARGET",
    "ENV_TOKEN",
    "ENV_INSECURE",
    "__version__",
]
