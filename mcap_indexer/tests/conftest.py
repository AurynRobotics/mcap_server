"""Shared pytest setup for mcap_indexer tests.

Inserts the repo root onto ``sys.path`` so ``from mcap_indexer import ...`` works
regardless of the invoking cwd. The ``tmp_db`` fixture is added in Task 10.
"""

import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parents[2])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pytest  # noqa: E402

from mcap_indexer.db import load_caches, open_db  # noqa: E402


@pytest.fixture
def tmp_db(tmp_path):
    """A fresh file-backed catalog DB + loaded caches; closed on teardown."""
    conn = open_db(str(tmp_path / "catalog.db"))
    caches = load_caches(conn)
    try:
        yield conn, caches
    finally:
        conn.close()
