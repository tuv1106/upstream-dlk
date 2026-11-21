import json
from collections.abc import Callable
from datetime import UTC, datetime

import pytest

from upstream_dlk.layers.bronze import BronzeLayer
from upstream_dlk.layers.gold import GoldLayer
from upstream_dlk.layers.silver import SilverLayer
from upstream_dlk.storage import LocalStorage
from upstream_dlk.tables import Partition, RunLog

T0 = 1795180000000  # 2026-11-20 13:06:40 UTC, in ms


def message(**overrides) -> dict:
    """Returns an API message with sensible defaults."""
    return {
        "vin": "VIN1",
        "manufacturer": "Jeep",
        "year": 2016,
        "model": "Compass",
        "latitude": 41.52,
        "longitude": -88.33,
        "timestamp": T0,
        "velocity": 120,
        "frontLeftDoorState": "UNLOCKED",
        "wipersState": False,
        "gearPosition": "3",
        "driverSeatbeltState": "LOCKED",
    } | overrides


class FakeClient:
    """API stand-in returning queued batches and counting calls."""

    def __init__(self, *batches: list[dict]):
        self.batches = list(batches)
        self.calls = 0

    def fetch(self, amount: int) -> bytes:
        self.calls += 1
        return json.dumps(self.batches.pop(0)).encode()


@pytest.fixture
def fake_client() -> type[FakeClient]:
    return FakeClient


def epoch_ms(*args: int) -> int:
    """Returns the epoch ms of a UTC datetime, e.g. epoch_ms(2026, 11, 20, 23)."""
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


def run_id(n: int) -> str:
    """Returns the id of the n-th test run; ids sort in run order."""
    return f"20261121T{n:02d}0000000000Z"


class Lake:
    """All layers on a temporary local DataLake, fed by a fake API."""

    def __init__(self, root: str, *batches: list[dict]):
        self.storage = LocalStorage(root)
        self.client = FakeClient(*batches)
        self.run_log = RunLog(self.storage)
        self.bronze = BronzeLayer(self.storage, self.client, self.run_log)
        self.silver = SilverLayer(self.storage, max_velocity_kmh=300)
        self.gold = GoldLayer(self.storage)

    def ingest(self, n: int) -> list[Partition]:
        """Runs land -> stage -> publish for the n-th run."""
        self.bronze.land(run_id(n), amount=10)
        return self.bronze.publish(run_id(n), self.bronze.stage(run_id(n))).partitions

    def run(self, n: int) -> list[Partition]:
        """Runs bronze, silver and gold for the n-th run, like the pipeline flow."""
        partitions = self.ingest(n)
        self.silver.build(run_id(n), partitions)
        self.gold.build(partitions)
        return partitions

    def files(self, table: str) -> list[str]:
        """Returns the parquet files of a table, relative to the lake root."""
        return [
            f.removeprefix(self.storage.root + "/")
            for f in self.storage.glob(f"{self.storage.path(table)}/**/*.parquet")
        ]


@pytest.fixture
def make_lake(tmp_path) -> Callable[..., Lake]:
    """Returns a factory building a Lake whose fake API serves the given batches in order."""
    return lambda *batches: Lake(str(tmp_path), *batches)


SPEC_REGEX = (
    r"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}"
    r"|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"
)
DROP_REGEX = r"\bDROP\b"
UNION_REGEX = r"\bUNION( +ALL)?\b"
REGEXES = [SPEC_REGEX, DROP_REGEX, UNION_REGEX]

REGEX_CASES = {
    "plain value": [],
    "DROP": sorted([SPEC_REGEX, DROP_REGEX]),
    "drop": [],
    "SELECTOR": [],
    "x UNION ALL y": sorted([SPEC_REGEX, UNION_REGEX]),
    "it's": [],
    "'quoted'": [SPEC_REGEX],
    "a;b": [SPEC_REGEX],
}
"""Vin value -> regexes it must match: keywords, case, word boundaries, quotes, semicolons."""


def regex_case_messages() -> list[dict]:
    """Returns one message per regex case, each in its own millisecond."""
    return [message(vin=vin, timestamp=T0 + i) for i, vin in enumerate(REGEX_CASES)]
