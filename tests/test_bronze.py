from datetime import timedelta

import polars as pl
import pytest
from conftest import epoch_ms, message, run_id

from upstream_dlk.schema import API_SCHEMA
from upstream_dlk.tables import Run


def test_partitions_by_utc_event_hour_across_midnight(make_lake):
    lake = make_lake(
        [
            message(timestamp=epoch_ms(2026, 11, 20, 23, 59, 59) + 999),
            message(timestamp=epoch_ms(2026, 11, 21, 0, 0, 0)),
        ]
    )

    partitions = lake.ingest(1)

    assert sorted(partitions, key=str) == [
        {"date": "2026-11-20", "hour": "23"},
        {"date": "2026-11-21", "hour": "0"},
    ]
    assert lake.files("bronze") == [
        f"bronze/date=2026-11-20/hour=23/{run_id(1)}.parquet",
        f"bronze/date=2026-11-21/hour=0/{run_id(1)}.parquet",
    ]


def test_keeps_raw_values_untouched(make_lake):
    dirty = [
        message(vin=None, manufacturer=" Buick ", gearPosition="NEUTRAL", velocity=450),
        message(frontLeftDoorState=None, wipersState=None, gearPosition=None, timestamp=1),
    ]
    lake = make_lake(dirty)

    lake.ingest(1)

    stored = lake.bronze.table.scan().select(list(API_SCHEMA)).sort("timestamp").collect()
    assert stored.to_dicts() == sorted(dirty, key=lambda m: m["timestamp"])


def test_retried_landing_calls_the_api_once(make_lake):
    lake = make_lake([message()], [message(vin="OTHER")])

    lake.bronze.land(run_id(1), amount=10)
    lake.bronze.land(run_id(1), amount=10)

    assert lake.client.calls == 1


def test_retry_after_a_crash_mid_publish_leaves_no_duplicates(make_lake):
    lake = make_lake([message(), message(timestamp=epoch_ms(2026, 11, 21, 5))])
    lake.bronze.land(run_id(1), amount=10)
    staged = lake.bronze.stage(run_id(1))
    lake.bronze.table.append(
        lake.bronze._staging_dir(run_id(1)), staged[:1]
    )  # crash after one move

    lake.bronze.publish(run_id(1), lake.bronze.stage(run_id(1)))

    assert len(lake.files("bronze")) == 2
    assert lake.bronze.table.scan().select(pl.len()).collect().item() == 2
    assert lake.client.calls == 1 and lake.run_log.get(run_id(1)) is not None


def test_empty_response_publishes_nothing_downstream(make_lake):
    lake = make_lake([])

    assert lake.ingest(1) == []
    assert lake.files("bronze") == []
    assert lake.silver.build(run_id(1), []) is None
    assert lake.gold.build([]) == 0


def test_message_without_timestamp_is_quarantined(make_lake):
    lake = make_lake([message(), message(vin="NO_TS", timestamp=None)])

    partitions = lake.ingest(1)

    assert lake.files("bronze") == [f"bronze/date=2026-11-20/hour=13/{run_id(1)}.parquet"]
    assert lake.files("quarantine") == [f"quarantine/bronze/{run_id(1)}.parquet"]
    assert lake.silver.build(run_id(1), partitions)["silver_rows"] == 1


def test_raw_response_is_archived_and_numbers_are_not_truncated(make_lake):
    lake = make_lake([message(velocity=12.7)])

    lake.ingest(1)

    assert lake.storage.exists(lake.storage.path("bronze", "_raw", f"{run_id(1)}.json"))
    assert lake.bronze.table.scan().collect()["velocity"].to_list() == [12.7]


def test_successful_pipeline_leaves_no_staging(make_lake):
    lake = make_lake([message()])

    partitions = lake.ingest(1)
    lake.silver.build(run_id(1), partitions)
    lake.gold.build(partitions)

    assert lake.storage.fs.find(lake.storage.path("_staging")) == []


def test_expires_raw_responses_older_than_retention(make_lake):
    lake = make_lake([message()], [message()])
    lake.ingest(1)
    lake.ingest(2)

    deleted = lake.bronze.expire_raw(
        retention_days=1, now=Run.ingested_at(run_id(2)) + timedelta(hours=23, minutes=30)
    )

    assert deleted == 1
    assert lake.storage.glob(lake.storage.path("bronze", "_raw", "*.json"))[0].endswith(
        f"{run_id(2)}.json"
    )


def test_resume_needs_a_landed_unpublished_run(make_lake):
    lake = make_lake([message()], [message()])
    lake.bronze.land(run_id(1), amount=10)
    lake.ingest(2)

    assert lake.bronze.check_resumable(run_id(1)) is None
    assert lake.bronze.check_resumable(run_id(2)) == lake.run_log.get(run_id(2))
    with pytest.raises(ValueError, match="never landed"):
        lake.bronze.check_resumable(run_id(3))


def test_keeps_column_types_when_a_batch_column_is_all_null(make_lake):
    lake = make_lake([message(vin=None, wipersState=None)])

    lake.ingest(1)

    schema = lake.bronze.table.scan().collect_schema()
    assert (schema["vin"], schema["wipersState"]) == (pl.String, pl.Boolean)


def test_crash_after_archiving_raw_can_be_retried_and_resumed(make_lake):
    lake = make_lake([message()])
    lake.bronze.land(run_id(1), amount=10)
    partitions = lake.bronze.stage(run_id(1))
    lake.bronze.table.append(lake.bronze._staging_dir(run_id(1)), partitions)
    lake.storage.move(lake.bronze._raw_file(run_id(1)), lake.bronze._archived_raw(run_id(1)))
    # crash here: raw archived, run not recorded

    assert lake.bronze.check_resumable(run_id(1)) is None
    lake.bronze.publish(run_id(1), partitions)

    assert lake.run_log.get(run_id(1)) is not None
    assert lake.client.calls == 1
