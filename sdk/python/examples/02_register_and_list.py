#!/usr/bin/env python3
"""Example 02 — login, register a model, list models, list its versions.

Run::

    export FORGEPOINT_TARGET=localhost:50051
    export FORGEPOINT_INSECURE=1          # localhost dev only
    export FP_EMAIL=me@corp.com
    export FP_PASSWORD=...
    python examples/02_register_and_list.py

Demonstrates the core registry flow plus:
  * an idempotency_key so re-runs return the same model instead of erroring,
  * typed-error handling (AlreadyExistsError),
  * reading the paginated list response.
"""

from __future__ import annotations

import os
import sys
import uuid

import forgepoint


def main() -> int:
    target = os.environ.get("FORGEPOINT_TARGET")
    email = os.environ.get("FP_EMAIL")
    password = os.environ.get("FP_PASSWORD")
    if not (target and email and password):
        print("Set FORGEPOINT_TARGET, FP_EMAIL, FP_PASSWORD.", file=sys.stderr)
        return 2

    model_name = os.environ.get("FP_MODEL_NAME", "fraud-detector")

    with forgepoint.Client(target=target) as fp:
        fp.login(email=email, password=password)

        # Register a model. A stable idempotency_key makes a retried run return the
        # SAME model rather than raising AlreadyExistsError.
        try:
            model = fp.register_model(
                name=model_name,
                description="Fraud detection model (SDK example).",
                framework="onnx",
                task_type="classification",
                tags={"domain": "fraud", "pii": "false"},
                idempotency_key=str(uuid.uuid5(uuid.NAMESPACE_DNS, model_name)),
            )
            print(f"Registered model: {model.name} (id={model.id}, team={model.team})")
        except forgepoint.AlreadyExistsError:
            # No idempotency match (e.g. created out of band) — fetch it instead.
            model = fp.get_model(name=model_name)
            print(f"Model already exists: {model.name} (id={model.id})")

        # List models in your team (newest first). The response carries pagination.
        resp = fp.list_models(page_size=20)
        print(f"\nModels in your team ({len(resp.models)} on this page):")
        for m in resp.models:
            prod = m.production_version or "-"
            print(f"  - {m.name:<24} framework={m.framework:<10} prod_version={prod}")
        if resp.pagination.next_page_token:
            print(f"  …more (next_page_token={resp.pagination.next_page_token[:12]}…)")

        # List that model's versions (production only, as an example filter).
        versions = fp.list_versions(
            model_id=model.id,
            stage_filter=forgepoint.ModelStage.MODEL_STAGE_PRODUCTION,
        )
        print(f"\nProduction versions of {model.name}: {len(versions.versions)}")
        for v in versions.versions:
            print(f"  - {v.version} (status={forgepoint.VersionStatus.Name(v.status)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
