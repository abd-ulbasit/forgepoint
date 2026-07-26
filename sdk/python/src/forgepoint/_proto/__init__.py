"""Generated protobuf/gRPC stubs for Forgepoint — vendored, do not edit by hand.

REGENERATE with (from the repo root)::

    cd proto && buf generate --template ../sdk/python/buf.gen.yaml

WHY THIS FILE EXISTS (the grpc-python absolute-import quirk)
-----------------------------------------------------------
The protobuf/grpc Python plugins emit ABSOLUTE imports rooted at the proto
package path, e.g. ``auth_pb2_grpc.py`` contains::

    from forgepoint.auth.v1 import auth_pb2

That assumes ``forgepoint.auth.v1`` is importable as a top-level package path.
But in this SDK the GENERATED tree lives at ``forgepoint/_proto/forgepoint/...``
(nested under ``_proto`` so it doesn't collide with the HAND-WRITTEN
``forgepoint`` package that holds ``client.py`` / ``errors.py``). So out of the
box ``import forgepoint.auth.v1`` fails — the real ``forgepoint`` package has no
``auth`` submodule.

THE FIX — extend the parent package's ``__path__`` (the idiomatic way):
A Python package can be backed by MULTIPLE directories by extending its
``__path__`` list. We append the generated ``_proto/forgepoint`` directory to the
top-level ``forgepoint`` package's ``__path__``. After that, the import machinery
searches BOTH:

    forgepoint/                  (hand-written: client.py, errors.py, ...)
    forgepoint/_proto/forgepoint (generated: auth/, registry/, ...)

so ``forgepoint.auth.v1.auth_pb2`` resolves to the generated tree while
``forgepoint.client`` stays hand-written — no name collision, no edits to any
generated file, and it survives every regeneration.

NO ``__init__.py`` IN THE GENERATED TREE IS REQUIRED: the generated
``forgepoint/auth/v1/...`` directories work as PEP 420 implicit namespace
packages (Python 3.3+). So a fresh ``buf generate`` produces a directly-usable
tree with NO post-processing step — this single hand-written file does all the
wiring. (Verified: importing the stubs works with the generated dirs containing
no ``__init__.py`` markers.)

ORDER MATTERS: this must run BEFORE the package's ``__init__`` does any
``from forgepoint.<svc>...`` import. The hand-written ``forgepoint/__init__.py``
therefore does ``from . import _proto`` on its FIRST line, which executes this
module and patches ``__path__`` before the stub imports below it run.

ALTERNATIVES CONSIDERED (and why this one):
  * ``sys.path.insert(0, _proto_dir)`` — makes ``import forgepoint`` ambiguous
    (two ``forgepoint`` dirs on sys.path), which is exactly what broke first.
    Rejected.
  * ``protoc -M`` mappings / a post-gen ``sed`` to rewrite the generated imports
    to relative ones — brittle, re-runs every regen, fights the plugin. Rejected.
  * ``__path__`` extension — three lines, no generated edits, no ambiguity.
    Chosen. (This is ``pkgutil``-style namespace path extension, the same
    mechanism setuptools' ``pkg_resources`` namespace packages use.)

NOTE — how generated gRPC code is vendored
without editing it: protoc emits import paths mirroring the
proto ``package`` declaration, not the filesystem, so you make that package path
importable. Extending ``__path__`` does that cleanly.
"""

from __future__ import annotations

import os

# Absolute path to the GENERATED ``forgepoint`` directory (the one that directly
# contains auth/, registry/, pipeline/, monitor/, common/, ...).
_GENERATED_FORGEPOINT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forgepoint")

# Extend the PARENT package's (``forgepoint``) search path so the generated
# subpackages become importable as ``forgepoint.<svc>.v1.<...>``.
# ``__package__`` here is "forgepoint._proto"; the parent is "forgepoint".
import importlib  # noqa: E402

_parent = importlib.import_module(__package__.rsplit(".", 1)[0])  # the top-level forgepoint pkg
if _GENERATED_FORGEPOINT_DIR not in _parent.__path__:
    _parent.__path__.append(_GENERATED_FORGEPOINT_DIR)
