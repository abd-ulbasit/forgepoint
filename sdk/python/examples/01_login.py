#!/usr/bin/env python3
"""Example 01 — authenticate and identify yourself.

Run::

    export FORGEPOINT_TARGET=localhost:50051
    export FORGEPOINT_INSECURE=1          # localhost dev only
    export FP_EMAIL=me@corp.com
    export FP_PASSWORD=...                 # read from env; never hardcode
    python examples/01_login.py

Demonstrates:
  * Reading target/creds from the environment (no secrets in source).
  * Secure-by-default channel (TLS) with an explicit localhost insecure opt-out.
  * login() storing the token transparently — note we NEVER print the token.
"""

from __future__ import annotations

import os
import sys

import forgepoint


def main() -> int:
    target = os.environ.get("FORGEPOINT_TARGET")
    email = os.environ.get("FP_EMAIL")
    password = os.environ.get("FP_PASSWORD")
    if not (target and email and password):
        print(
            "Set FORGEPOINT_TARGET, FP_EMAIL, FP_PASSWORD (and FORGEPOINT_INSECURE=1 "
            "for a localhost target).",
            file=sys.stderr,
        )
        return 2

    # `insecure` defaults to the FORGEPOINT_INSECURE env var; only honored for
    # localhost. The channel is TLS otherwise.
    with forgepoint.Client(target=target) as fp:
        try:
            # login() returns the token, but we DO NOT print it — secrets stay secret.
            fp.login(email=email, password=password)
        except forgepoint.UnauthenticatedError:
            print("Login failed: bad email or password.", file=sys.stderr)
            return 1
        except forgepoint.ForgepointError as e:
            print(f"Login error ({e.code}): {e}", file=sys.stderr)
            return 1

        print(f"Authenticated to {target} (token held in memory, not shown).")
        print(f"Client: {fp!r}")  # repr redacts the token by design.

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
