from types import SimpleNamespace

import httpx
import polars as pl
import pytest

from upstream_dlk.flows import is_transient


def failed_with(error: Exception) -> SimpleNamespace:
    return SimpleNamespace(result=lambda raise_on_failure: error)


def http_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://api")
    return httpx.HTTPStatusError(
        "", request=request, response=httpx.Response(status, request=request)
    )


@pytest.mark.parametrize(
    ("error", "retried"),
    [
        (httpx.ConnectError("refused"), True),
        (httpx.ReadTimeout("slow"), True),
        (http_error(503), True),
        (http_error(400), False),
        (OSError("disk full"), True),
        (FileNotFoundError("gone"), False),
        (pl.exceptions.ComputeError("bad json"), False),
        (ValueError("never landed"), False),
    ],
)
def test_retries_only_transient_errors(error, retried):
    assert is_transient(None, None, failed_with(error)) is retried
