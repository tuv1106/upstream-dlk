import polars as pl

from upstream_dlk.schema import HOUR_PARTITIONS
from upstream_dlk.storage import LocalStorage
from upstream_dlk.tables import PartitionedTable


def test_relative_local_root_lists_partitions(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    storage = LocalStorage("data")
    table = PartitionedTable(storage, "bronze", HOUR_PARTITIONS)
    storage.write_parquet(
        pl.DataFrame({"a": [1]}), storage.path("bronze", "date=2026-11-20", "hour=7", "x.parquet")
    )

    assert table.partitions() == [{"date": "2026-11-20", "hour": "7"}]
