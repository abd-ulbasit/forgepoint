"""gRPC client interceptors for auth and error translation.

This module holds the two cross-cutting concerns that every Forgepoint RPC needs,
implemented ONCE as channel interceptors rather than repeated in every method:

  1. :class:`_AuthInterceptor` — injects ``authorization: Bearer <jwt>`` metadata
     on every outgoing call, reading the token from a shared, mutable holder so a
     later ``login()`` transparently authenticates *already-created* stubs.

WHY AN INTERCEPTOR INSTEAD OF per-call ``metadata=[...]``
-----------------------------------------------------------------------------
gRPC-Python lets you pass ``metadata`` to each stub call, but doing that in every
SDK method is repetitive and easy to forget on one method (an auth hole). A
``grpc.UnaryUnaryClientInterceptor`` (and its streaming siblings) wraps the whole
channel: the token is attached structurally, so a NEW RPC added later is
authenticated automatically. This mirrors the SERVER side of Forgepoint, where a
single auth *interceptor* in ``pkg/grpcutil`` guards every handler rather than
each handler checking the token itself ("secure by construction").

WHY A MUTABLE TOKEN HOLDER (not the token value itself)
-------------------------------------------------------
``Client.login()`` obtains the JWT *after* the channel and stubs are built. If we
captured the token by value at interceptor-construction time, post-login calls
would still send the old (empty) token. Instead the interceptor holds a reference
to a tiny :class:`_TokenHolder` object and reads ``.token`` at call time, so the
moment ``login()`` writes the new token, every subsequent RPC picks it up.

  call-creds alternative: gRPC also offers per-RPC ``CallCredentials`` via
  ``grpc.metadata_call_credentials``. That is the more "official" mechanism, but
  it is ONLY honored on SECURE channels — on a local insecure channel gRPC raises
  if you attach call creds. Since the SDK must support an explicit insecure
  localhost opt-out for dev, a metadata interceptor (which works on both secure
  and insecure channels) is the portable choice. The tradeoff is documented here
  because "why not call credentials?" is the obvious question.

SECURITY: the token lives ONLY in the holder and is written ONLY into request
metadata. It is never logged, never put in a URL, never stored on disk by the
SDK. ``_TokenHolder.__repr__`` deliberately redacts it so an accidental
``print(holder)`` or a logged stack frame cannot leak the secret.
"""

from __future__ import annotations

from typing import Callable, Optional

import grpc

# The metadata key MUST be lowercase: gRPC requires lowercase header keys and the
# Forgepoint Go auth interceptor reads "authorization" from incoming metadata.
_AUTH_METADATA_KEY = "authorization"


class _TokenHolder:
    """Mutable, redaction-safe holder for the current bearer token.

    Shared by reference between the :class:`forgepoint.Client` and its auth
    interceptor so ``login()`` can update the token in place and have every
    later RPC use it. ``None`` means "no token yet" (anonymous calls).
    """

    __slots__ = ("_token",)

    def __init__(self, token: Optional[str] = None) -> None:
        self._token = token

    @property
    def token(self) -> Optional[str]:
        return self._token

    @token.setter
    def token(self, value: Optional[str]) -> None:
        self._token = value

    def __repr__(self) -> str:  # pragma: no cover - trivial
        # NEVER reveal the token. Redact in any debug/log output.
        state = "set" if self._token else "unset"
        return f"_TokenHolder(token=<{state}>)"


def _with_auth_metadata(
    holder: _TokenHolder,
    client_call_details: grpc.ClientCallDetails,
) -> grpc.ClientCallDetails:
    """Return new call details with the Authorization header appended.

    We copy existing metadata and append our header rather than replacing, so any
    caller-supplied metadata (e.g. a correlation id) survives. If the holder has
    no token, we return the details unchanged (anonymous call — e.g. Login).
    """
    token = holder.token
    if not token:
        return client_call_details

    metadata = list(client_call_details.metadata or [])
    metadata.append((_AUTH_METADATA_KEY, f"Bearer {token}"))

    # grpc.ClientCallDetails is read-only; build a small concrete replacement.
    return _ClientCallDetails(
        method=client_call_details.method,
        timeout=client_call_details.timeout,
        metadata=metadata,
        credentials=client_call_details.credentials,
        wait_for_ready=client_call_details.wait_for_ready,
        compression=getattr(client_call_details, "compression", None),
    )


class _ClientCallDetails(grpc.ClientCallDetails):
    """Concrete, mutable-at-construction ClientCallDetails.

    ``grpc.ClientCallDetails`` is an abstract, immutable interface; to change the
    metadata we must supply our own object with the same attributes. This is the
    documented pattern from grpc-python's own interceptor examples.
    """

    __slots__ = (
        "method",
        "timeout",
        "metadata",
        "credentials",
        "wait_for_ready",
        "compression",
    )

    def __init__(self, method, timeout, metadata, credentials, wait_for_ready, compression):
        self.method = method
        self.timeout = timeout
        self.metadata = metadata
        self.credentials = credentials
        self.wait_for_ready = wait_for_ready
        self.compression = compression


class _AuthInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """Attaches ``authorization: Bearer <token>`` to every outgoing RPC.

    Implements all four interceptor flavors so the SAME instance authenticates
    unary RPCs (login, register_model, get_model, ...) AND server-streaming RPCs
    (watch_execution, stream_drift_events). gRPC dispatches to the method matching
    the RPC's cardinality; we share one ``_intercept`` body since the only thing
    we do — rewrite call details — is identical for all four.
    """

    def __init__(self, holder: _TokenHolder) -> None:
        self._holder = holder

    def _intercept(
        self,
        continuation: Callable,
        client_call_details: grpc.ClientCallDetails,
        request_or_iterator,
    ):
        new_details = _with_auth_metadata(self._holder, client_call_details)
        return continuation(new_details, request_or_iterator)

    # The four required overrides all delegate to _intercept. gRPC calls exactly
    # the one matching the RPC's request/response cardinality.
    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return self._intercept(continuation, client_call_details, request_iterator)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return self._intercept(continuation, client_call_details, request_iterator)
