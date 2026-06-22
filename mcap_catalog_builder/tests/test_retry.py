"""Tests for retry_with (transient backoff with permanent short-circuit)."""

import pytest

from mcap_catalog_builder.retry import retry_with


def test_succeeds_first_try():
    assert retry_with(lambda: 42, sleep=lambda _s: None) == 42


def test_retries_transient_then_succeeds():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    slept: list[float] = []
    assert retry_with(flaky, sleep=slept.append) == "ok"
    assert calls["n"] == 3
    assert slept == [0.05, 0.1]  # exponential backoff, base 50ms


def test_permanent_error_short_circuits():
    calls = {"n": 0}

    class Missing(Exception):
        pass

    def fn():
        calls["n"] += 1
        raise Missing()

    with pytest.raises(Missing):
        retry_with(fn, is_permanent=lambda e: isinstance(e, Missing), sleep=lambda _s: None)
    assert calls["n"] == 1  # no retry on a permanent error


def test_default_schedule_matches_go():
    # Default ATTEMPTS=6 exhausted => exactly [50,100,200,400,800] ms (Go parity).
    slept: list[float] = []

    def fn():
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        retry_with(fn, sleep=slept.append)
    assert slept == [0.05, 0.1, 0.2, 0.4, 0.8]


def test_exhausts_attempts_and_raises_last():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise ValueError(f"attempt {calls['n']}")

    with pytest.raises(ValueError):
        retry_with(fn, attempts=4, sleep=lambda _s: None)
    assert calls["n"] == 4


def test_backoff_capped_at_max_delay():
    slept: list[float] = []
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        retry_with(fn, attempts=6, base_delay=0.3, max_delay=0.8, sleep=slept.append)
    # 0.3, 0.6, then capped at 0.8, 0.8 (5 sleeps for 6 attempts).
    assert slept == [0.3, 0.6, 0.8, 0.8, 0.8]


def test_should_stop_aborts():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise RuntimeError("transient")

    with pytest.raises(RuntimeError):
        retry_with(fn, should_stop=lambda: True, sleep=lambda _s: None)
    assert calls["n"] == 1  # cancelled before the first backoff sleep
