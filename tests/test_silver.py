import os

import polars as pl
import pytest
from conftest import T0, epoch_ms, message, run_id

from upstream_dlk.tables import Run

HOUR_14 = epoch_ms(2026, 11, 20, 14)


def silver_rows(lake) -> pl.DataFrame:
    return lake.silver.table.scan().collect()


def test_dedupes_exact_messages_within_and_across_runs(make_lake):
    original = message()
    same_vin_and_time = message(velocity=50, latitude=1.0)
    lake = make_lake([original, original], [original, same_vin_and_time])

    touched = lake.ingest(1) + lake.ingest(2)
    lake.silver.build(run_id(2), touched)

    rows = silver_rows(lake).sort("velocity")
    assert rows["velocity"].to_list() == [50, 120]
    first_ingestion = rows.filter(pl.col("velocity") == 120)["ingested_at"].item()
    assert first_ingestion == Run.ingested_at(run_id(1))


def test_late_event_rebuilds_only_its_own_hour(make_lake):
    lake = make_lake([message(), message(timestamp=HOUR_14)], [message(timestamp=T0 + 1)])
    lake.silver.build(run_id(1), lake.ingest(1))
    hour_14_file = lake.storage.path("silver", "date=2026-11-20", "hour=14", "part-0.parquet")
    written_at = os.stat(hour_14_file).st_mtime_ns

    lake.silver.build(run_id(2), lake.ingest(2))

    assert silver_rows(lake).group_by("hour").len().sort("hour").rows() == [(13, 2), (14, 1)]
    assert os.stat(hour_14_file).st_mtime_ns == written_at


def test_maps_gears_and_reports_unknown_values(make_lake):
    gears = ["1", "6", "0", "NEUTRAL", "-1", "REVERSE", None, "PARK"]
    lake = make_lake([message(gearPosition=g, timestamp=T0 + i) for i, g in enumerate(gears)])
    partitions = lake.ingest(1)

    report = lake.silver.build(run_id(1), partitions)

    assert silver_rows(lake)["gear_position"].to_list() == [1, 6, 0, 0, -1, -1, None, None]
    assert (report["unknown_gear_rows"], report["unknown_gear_values"]) == (1, ["PARK"])


def test_flags_only_velocities_above_the_threshold(make_lake):
    velocities = [450, 300, None]
    lake = make_lake([message(velocity=v, timestamp=T0 + i) for i, v in enumerate(velocities)])
    partitions = lake.ingest(1)

    report = lake.silver.build(run_id(1), partitions)

    assert silver_rows(lake).select("velocity", "is_velocity_suspect").rows() == [
        (None, True),
        (300, False),
        (None, False),
    ]
    assert (report["suspect_velocity_rows"], report["suspect_velocity_vins"]) == (1, ["VIN1"])


def test_rows_are_sorted_by_vin_then_time_in_the_file(make_lake):
    lake = make_lake(
        [
            message(vin="B", timestamp=T0),
            message(vin="A", timestamp=T0 + 2),
            message(vin="A", timestamp=T0 + 1),
        ]
    )
    partitions = lake.ingest(1)

    lake.silver.build(run_id(1), partitions)

    file = lake.storage.path("silver", "date=2026-11-20", "hour=13", "part-0.parquet")
    assert pl.read_parquet(file).select("vin", pl.col("timestamp").dt.epoch("ms")).rows() == [
        ("A", T0 + 1),
        ("A", T0 + 2),
        ("B", T0),
    ]


def test_failed_build_keeps_old_data_and_succeeds_on_rerun(make_lake, monkeypatch):
    lake = make_lake([message(vin="A")], [message(vin="B")])
    lake.silver.build(run_id(1), lake.ingest(1))
    touched = lake.ingest(2)
    with monkeypatch.context() as patch:
        patch.setattr(
            lake.silver.table, "overwrite", lambda *_: (_ for _ in ()).throw(OSError("disk"))
        )
        with pytest.raises(OSError):
            lake.silver.build(run_id(2), touched)
    assert silver_rows(lake)["vin"].to_list() == ["A"]

    lake.silver.build(run_id(2), touched)  # rerun of the failed flow, same parameters

    assert silver_rows(lake)["vin"].sort().to_list() == ["A", "B"]


def test_drops_null_vins_and_strips_text(make_lake):
    lake = make_lake([message(vin=None), message(manufacturer=" Buick ", model=" V8 ")])

    lake.silver.build(run_id(1), lake.ingest(1))

    assert silver_rows(lake).select("vin", "manufacturer", "model").rows() == [
        ("VIN1", "Buick", "V8")
    ]
