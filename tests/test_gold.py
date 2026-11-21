import polars as pl
import pytest
from conftest import T0, epoch_ms, message


def state(lake) -> dict:
    """Returns vin_last_state as {vin: (door, door_ts ms, wipers, last_reported ms)}."""
    df = lake.gold.vin_last_state.scan().collect()
    ms = pl.col("door_ts", "last_reported_timestamp").dt.epoch("ms")
    rows = df.with_columns(ms).select(
        "vin", "front_left_door_state", "door_ts", "wipers_state", "last_reported_timestamp"
    )
    return {vin: rest for vin, *rest in rows.rows()}


def fastest(lake) -> list[tuple]:
    df = lake.gold.fastest.scan().collect()
    return df.select("hour", "vin", "top_velocity").rows()


def test_each_state_column_keeps_its_own_last_non_null_value(make_lake):
    lake = make_lake(
        [message(frontLeftDoorState="LOCKED", wipersState=True)],
        [message(timestamp=T0 + 1, frontLeftDoorState=None, wipersState=False)],
    )

    lake.run(1)
    lake.run(2)

    assert state(lake)["VIN1"] == [["LOCKED"], T0, [False], T0 + 1]


def test_conflicting_values_at_the_same_time_are_all_kept(make_lake):
    lake = make_lake(
        [message(frontLeftDoorState="UNLOCKED")],
        [message(frontLeftDoorState="LOCKED", velocity=1)],
    )

    lake.run(1)
    lake.run(2)

    assert state(lake)["VIN1"][0] == ["LOCKED", "UNLOCKED"]


@pytest.mark.parametrize(
    ("late_ts", "expected_door", "expected_door_ts"),
    [(T0 + 3, ["UNLOCKED"], T0 + 3), (T0 + 1, ["LOCKED"], T0 + 2)],
    ids=["newer-than-door-ts-wins", "older-than-door-ts-ignored"],
)
def test_late_event_is_merged_by_event_time(make_lake, late_ts, expected_door, expected_door_ts):
    lake = make_lake(
        [
            message(timestamp=T0 + 2, frontLeftDoorState="LOCKED"),
            message(timestamp=T0 + 4, frontLeftDoorState=None),
        ],
        [message(timestamp=late_ts, frontLeftDoorState="UNLOCKED")],
    )

    lake.run(1)
    lake.run(2)

    assert state(lake)["VIN1"][:2] == [expected_door, expected_door_ts]
    assert state(lake)["VIN1"][3] == T0 + 4


def test_vin_that_never_reports_a_door_gets_null(make_lake):
    lake = make_lake([message(frontLeftDoorState=None)])

    lake.run(1)

    assert state(lake)["VIN1"][:2] == [None, None]


def test_rerunning_gold_with_the_same_partitions_changes_nothing(make_lake):
    lake = make_lake([message(), message(vin="VIN2", timestamp=epoch_ms(2026, 11, 20, 14))])
    partitions = lake.run(1)
    before = (state(lake), fastest(lake))

    lake.gold.build(partitions)

    assert (state(lake), fastest(lake)) == before


def test_failed_gold_build_succeeds_on_rerun(make_lake, monkeypatch):
    lake = make_lake([message()])
    partitions = lake.ingest(1)
    lake.silver.build("run", partitions)
    with monkeypatch.context() as patch:
        patch.setattr(
            lake.gold.fastest, "overwrite", lambda *_: (_ for _ in ()).throw(OSError("disk"))
        )
        with pytest.raises(OSError):
            lake.gold.build(partitions)  # vin_last_state written, fastest failed

    lake.gold.build(partitions)  # rerun of the failed flow, same parameters

    assert state(lake)["VIN1"][0] == ["UNLOCKED"]
    assert fastest(lake) == [(13, "VIN1", 120)]


def test_top_10_uses_max_speed_per_vin_keeps_ties_skips_suspect_speeds(make_lake):
    speeds = [190, 180, 170, 160, 150, 140, 130, 120, 110, 100, 100, 90]
    batch = [message(vin=f"V{i:02d}", velocity=v) for i, v in enumerate(speeds)]
    slower_again = message(vin="V00", velocity=10, timestamp=T0 + 1)
    lake = make_lake(batch + [slower_again, message(vin="STUCK", velocity=450)])

    lake.run(1)

    report = fastest(lake)
    assert [v for _, _, v in report] == speeds[:11]
    assert "STUCK" not in {vin for _, vin, _ in report}


def test_late_event_recomputes_its_day_without_losing_other_hours(make_lake):
    hour_14 = epoch_ms(2026, 11, 20, 14)
    lake = make_lake(
        [message(vin="A", velocity=100), message(vin="B", velocity=50, timestamp=hour_14)],
        [message(vin="C", velocity=150, timestamp=T0 + 1)],
    )

    lake.run(1)
    lake.run(2)

    assert fastest(lake) == [(13, "C", 150), (13, "A", 100), (14, "B", 50)]
