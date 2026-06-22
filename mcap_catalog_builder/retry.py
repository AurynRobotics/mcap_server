"""Transient-failure retry with exponential backoff (parity with the Go server's
``storage.retryWith``): ~50–800 ms backoff, a PERMANENT error short-circuits
(re-raised immediately, never retried), and the total attempts are bounded so one
flaky object can never wedge the reconcile.

Used to wrap the S3 / GCS network call bodies (HEAD, range-GET, LIST). The local
filesystem source does not need it.
"""

import time

BASE_DELAY = 0.05  # 50 ms
MAX_DELAY = 0.8    # 800 ms
# 6 attempts => sleeps [50,100,200,400,800] ms, matching the Go reference
# (storage.retryWith defaultBackoffs = 50,100,200,400,800 — "at most six tries").
ATTEMPTS = 6


def retry_with(
    fn,
    *,
    attempts: int = ATTEMPTS,
    base_delay: float = BASE_DELAY,
    max_delay: float = MAX_DELAY,
    is_permanent=None,
    should_stop=None,
    sleep=time.sleep,
):
    """Call ``fn()``, retrying transient exceptions with exponential backoff.

    - ``is_permanent(exc) -> bool``: a permanent error (e.g. a 404 / NoSuchKey, an
      auth failure) is re-raised IMMEDIATELY without retry. The caller's own
      try/except then handles it (e.g. ``S3Source.stat`` maps a missing object to
      ``None``).
    - ``should_stop() -> bool``: cancel hook checked before each backoff sleep; if
      it returns True the last exception is re-raised at once (cancel-aware, like
      the Go version's ctx).
    - The final attempt's exception propagates. ``sleep`` is injectable for tests.
    """
    delay = base_delay
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 - classify below
            last_exc = e
            if is_permanent is not None and is_permanent(e):
                raise
            if i == attempts - 1:
                raise
            if should_stop is not None and should_stop():
                raise
            sleep(min(delay, max_delay))
            delay = min(delay * 2, max_delay)
    # Unreachable (the loop always returns or raises), but keep mypy/readers happy.
    assert last_exc is not None
    raise last_exc
