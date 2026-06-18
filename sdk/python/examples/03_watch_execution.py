#!/usr/bin/env python3
"""Example 03 — trigger a pipeline execution and stream its progress.

Run::

    export FORGEPOINT_TARGET=localhost:50051
    export FORGEPOINT_INSECURE=1          # localhost dev only
    export FP_EMAIL=me@corp.com
    export FP_PASSWORD=...
    export FP_PIPELINE_ID=<a pipeline definition id>
    python examples/03_watch_execution.py

Demonstrates:
  * start_execution() with an idempotency_key and a JSON-like input,
  * watch_execution() as a Python GENERATOR over a server-streaming RPC,
  * stopping cleanly once the execution reaches a terminal state.
"""

from __future__ import annotations

import os
import sys
import uuid

import forgepoint

# Terminal execution states — stop watching once we hit one of these.
TERMINAL = {
    forgepoint.ExecutionStatus.EXECUTION_STATUS_COMPLETED,
    forgepoint.ExecutionStatus.EXECUTION_STATUS_FAILED,
    forgepoint.ExecutionStatus.EXECUTION_STATUS_CANCELLED,
}


def main() -> int:
    target = os.environ.get("FORGEPOINT_TARGET")
    email = os.environ.get("FP_EMAIL")
    password = os.environ.get("FP_PASSWORD")
    pipeline_id = os.environ.get("FP_PIPELINE_ID")
    if not (target and email and password and pipeline_id):
        print(
            "Set FORGEPOINT_TARGET, FP_EMAIL, FP_PASSWORD, FP_PIPELINE_ID.",
            file=sys.stderr,
        )
        return 2

    with forgepoint.Client(target=target) as fp:
        fp.login(email=email, password=password)

        # Trigger a run. The idempotency_key makes a retry return the SAME run.
        execution = fp.start_execution(
            pipeline_id=pipeline_id,
            input={"model_name": os.environ.get("FP_MODEL_NAME", "fraud-detector")},
            idempotency_key=str(uuid.uuid4()),
        )
        print(f"Triggered execution {execution.id} (status="
              f"{forgepoint.ExecutionStatus.Name(execution.status)})")

        # Stream live updates. include_current_state=True replays current state
        # first, so we never miss a transition that happened before we connected.
        # timeout=None: do NOT cut a long-running watch off with a client deadline.
        print("\nWatching for updates (Ctrl-C to stop):")
        try:
            for update in fp.watch_execution(execution_id=execution.id, timeout=None):
                ex = update.execution
                status = forgepoint.ExecutionStatus.Name(ex.status)
                step = update.changed_step.step_id if update.changed_step.step_id else "-"
                print(f"  seq={update.sequence:<4} status={status:<28} "
                      f"current_step={ex.current_step or '-':<16} changed={step}")
                if ex.status in TERMINAL:
                    print(f"\nExecution reached terminal state: {status}")
                    if ex.error:
                        print(f"  error: {ex.error}")
                    break
        except KeyboardInterrupt:
            print("\nStopped watching (the execution keeps running server-side).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
