"""Typed exception hierarchy for the Forgepoint SDK.

WHY a typed hierarchy instead of leaking ``grpc.RpcError``
---------------------------------------------------------
gRPC surfaces every failure as a single ``grpc.RpcError`` carrying a
``grpc.StatusCode`` enum. That's accurate but not *ergonomic*: callers would have
to ``import grpc`` and ``except grpc.RpcError`` then branch on ``e.code()`` to
tell "wrong password" from "model not found". An SDK's job is to hide that. We
map the handful of status codes the platform actually returns to named Python
exceptions so user code reads naturally::

    try:
        client.get_model(name="fraud-detector")
    except forgepoint.NotFoundError:
        ...
    except forgepoint.UnauthenticatedError:
        client.login(email, password)   # token expired — re-auth and retry

DESIGN: every SDK error derives from :class:`ForgepointError`, so a caller that
just wants "did the SDK fail?" can ``except forgepoint.ForgepointError``. Each
error keeps the original ``grpc.StatusCode`` and server ``details`` string for
debugging, but the *type* carries the meaning.

The code↔exception map mirrors how the Forgepoint Go services set status codes
(see ``pkg/grpcutil`` and each service's handler): Login on bad creds →
UNAUTHENTICATED, RBAC denial → PERMISSION_DENIED, missing row → NOT_FOUND,
duplicate name/idempotency collision → ALREADY_EXISTS, validation → INVALID_ARGUMENT.
"""

from __future__ import annotations

from typing import Optional

import grpc


class ForgepointError(Exception):
    """Base class for every error raised by the Forgepoint SDK.

    Catch this to handle *any* SDK-originated failure. The original gRPC status
    code (when the error came from an RPC) is preserved on ``.code`` and the
    server-provided message on ``.details`` for logging/debugging.
    """

    def __init__(
        self,
        message: str,
        *,
        code: Optional[grpc.StatusCode] = None,
        details: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        # `details` is the raw server message; `message` is what we show.
        self.details = details if details is not None else message


class ConfigurationError(ForgepointError):
    """Raised for client-side misconfiguration before any RPC is attempted.

    Examples: an empty ``target``, or asking for an insecure channel without the
    explicit ``insecure=True`` opt-out (the secure-by-default guard).
    """


class AuthenticationRequiredError(ForgepointError):
    """Raised locally when an RPC needs a token but the client has none.

    This is distinct from :class:`UnauthenticatedError` (which the *server*
    returns for a bad/expired token). This one fires before the wire call, to
    fail fast with a clear "call login() or pass token=...".
    """


# --- Errors mapped from gRPC status codes -----------------------------------


class UnauthenticatedError(ForgepointError):
    """Server returned UNAUTHENTICATED: missing/invalid/expired credentials.

    Typical causes: wrong email/password on :meth:`Client.login`, an expired
    JWT, or a revoked API key. Re-authenticate and retry.
    """


class PermissionDeniedError(ForgepointError):
    """Server returned PERMISSION_DENIED: authenticated, but RBAC forbids it.

    The caller's role/scopes lack the resource+action this RPC requires. This is
    *not* fixable by re-login with the same identity — the identity itself lacks
    permission.
    """


class NotFoundError(ForgepointError):
    """Server returned NOT_FOUND: the addressed resource does not exist.

    Note Forgepoint's CQRS read path is eventually consistent — a just-created
    model can briefly 404 here until the Redis projection catches up.
    """


class AlreadyExistsError(ForgepointError):
    """Server returned ALREADY_EXISTS: a uniqueness constraint was violated.

    E.g. registering a model whose name is already taken in your team. With an
    ``idempotency_key`` the server returns the original resource instead of this.
    """


class InvalidArgumentError(ForgepointError):
    """Server returned INVALID_ARGUMENT: the request failed validation."""


class DeadlineExceededError(ForgepointError):
    """Server/transport returned DEADLINE_EXCEEDED: the call timed out."""


class UnavailableError(ForgepointError):
    """Server returned UNAVAILABLE: transient — the service is unreachable.

    Usually a connectivity issue or a service still starting. Safe to retry with
    backoff for idempotent calls.
    """


# StatusCode → exception class. Anything not here falls back to ForgepointError.
_CODE_MAP: dict[grpc.StatusCode, type[ForgepointError]] = {
    grpc.StatusCode.UNAUTHENTICATED: UnauthenticatedError,
    grpc.StatusCode.PERMISSION_DENIED: PermissionDeniedError,
    grpc.StatusCode.NOT_FOUND: NotFoundError,
    grpc.StatusCode.ALREADY_EXISTS: AlreadyExistsError,
    grpc.StatusCode.INVALID_ARGUMENT: InvalidArgumentError,
    grpc.StatusCode.DEADLINE_EXCEEDED: DeadlineExceededError,
    grpc.StatusCode.UNAVAILABLE: UnavailableError,
}


def from_rpc_error(err: grpc.RpcError) -> ForgepointError:
    """Translate a raw ``grpc.RpcError`` into the matching typed SDK error.

    Called from the single error-handling boundary in :mod:`forgepoint.client`
    so users never see ``grpc.RpcError`` directly. We pull ``code()`` and
    ``details()`` off the gRPC call object (both present on the ``grpc.Call``
    that ``RpcError`` also is) and look up the typed class.

    SECURITY: ``details`` is a server-authored message; it never contains the
    caller's token (we never put the token anywhere but request metadata). So it
    is safe to surface in the exception text.
    """
    # grpc.RpcError instances are also grpc.Call, exposing code()/details().
    code = err.code() if hasattr(err, "code") else None
    details = err.details() if hasattr(err, "details") else str(err)
    exc_cls = _CODE_MAP.get(code, ForgepointError) if code is not None else ForgepointError
    message = details or (code.name if code else "RPC failed")
    return exc_cls(message, code=code, details=details)
