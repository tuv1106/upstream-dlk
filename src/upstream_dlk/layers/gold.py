"""Gold: reports built incrementally from the silver partitions touched since the last build."""

import polars as pl

from upstream_dlk.schema import DATE_PARTITIONS, HOUR_PARTITIONS
from upstream_dlk.storage import Storage
from upstream_dlk.tables import Partition, PartitionedTable, SingleFileTable

STATE_COLUMNS = {"front_left_door_state": "door_ts", "wipers_state": "wipers_ts"}
"""Each state column and the timestamp its last non-null value was reported at."""

VIN_LAST_STATE_SCHEMA: dict[str, pl.DataType] = {
    "vin": pl.String(),
    "last_reported_timestamp": pl.Datetime("ms", "UTC"),
    "front_left_door_state": pl.List(pl.String()),
    "door_ts": pl.Datetime("ms", "UTC"),
    "wipers_state": pl.List(pl.Boolean()),
    "wipers_ts": pl.Datetime("ms", "UTC"),
}


class VinLastStateReport:
    """Per vin: last reported time and the last non-null door and wipers states."""

    @staticmethod
    def merge(state: pl.LazyFrame | None, silver: pl.LazyFrame) -> pl.LazyFrame:
        """Merges new silver rows into the previous report.

        The previous report is turned back into rows and unioned with the new rows.
        Per vin and state column, the values at the newest timestamp win; values sharing
        that timestamp are all kept as a sorted list. Merging the same rows twice is a no-op.

        Args:
            state: Previous report, or None on the first build.
            silver: Silver rows of the touched partitions.

        Returns:
            The updated report, one row per vin.
        """
        if state is None:
            state = pl.LazyFrame(schema=VIN_LAST_STATE_SCHEMA)
        last_seen = (
            pl.concat(
                [
                    state.select("vin", ts="last_reported_timestamp"),
                    silver.select("vin", ts="timestamp"),
                ]
            )
            .group_by("vin")
            .agg(last_reported_timestamp=pl.col("ts").max())
        )
        report = last_seen
        for value_column, ts_column in STATE_COLUMNS.items():
            candidates = pl.concat(
                [
                    state.select("vin", value_column, ts=ts_column).explode(value_column),
                    silver.select("vin", value_column, ts="timestamp"),
                ]
            ).drop_nulls(value_column)
            latest = (
                candidates.filter(pl.col("ts") == pl.col("ts").max().over("vin"))
                .group_by("vin")
                .agg(pl.col("ts").first().alias(ts_column), pl.col(value_column).unique().sort())
            )
            report = report.join(latest, on="vin", how="left")
        return report.select(list(VIN_LAST_STATE_SCHEMA)).sort("vin")


class FastestVehiclesReport:
    """Top vehicles by highest velocity within each date and hour."""

    @staticmethod
    def compute(silver: pl.LazyFrame, top_n: int = 10) -> pl.LazyFrame:
        """Ranks vins by their max velocity per hour, keeping ties at the cut-off.

        Args:
            silver: Silver rows of whole days.
            top_n: Rank cut-off; vins tied at the last rank are all kept.

        Returns:
            `vin, date, hour, top_velocity`, fastest first within each hour.
        """
        return (
            silver.filter(pl.col("velocity").is_not_null())
            .group_by("date", "hour", "vin")
            .agg(top_velocity=pl.col("velocity").max())
            .filter(
                pl.col("top_velocity").rank("min", descending=True).over("date", "hour") <= top_n
            )
            .select("vin", "date", "hour", "top_velocity")
            .sort(["date", "hour", "top_velocity", "vin"], descending=[False, False, True, False])
        )


class GoldLayer:
    """Updates the gold reports from rebuilt silver partitions."""

    def __init__(self, storage: Storage):
        """
        Args:
            storage: Backend holding the DataLake.
        """
        self.storage = storage
        self.silver = PartitionedTable(storage, "silver", HOUR_PARTITIONS)
        self.vin_last_state = SingleFileTable(storage, "gold/vin_last_state")
        self.fastest = PartitionedTable(storage, "gold/fastest_vehicles_per_hour", DATE_PARTITIONS)

    def build(self, partitions: list[Partition]) -> int:
        """Updates both reports from the given silver partitions. Safe to rerun with the same input.

        Args:
            partitions: Silver partitions rebuilt since the last gold build.

        Returns:
            Number of silver partitions processed.
        """
        silver = self.silver.scan(partitions)
        if silver is None:
            return 0
        state = VinLastStateReport.merge(self.vin_last_state.scan(), silver).collect()
        self.vin_last_state.overwrite(state)
        self._build_fastest(sorted({p["date"] for p in partitions}))
        return len(partitions)

    def _build_fastest(self, dates: list[str]) -> None:
        silver_days = self.silver.scan([{"date": date} for date in dates])
        report = FastestVehiclesReport.compute(silver_days).collect()
        staging_dir = self.storage.path("_staging", "gold_fastest")
        self.storage.delete(staging_dir)
        self.fastest.overwrite(
            staging_dir, self.fastest.stage(report, staging_dir, "part-0.parquet")
        )
        self.storage.delete(staging_dir)
