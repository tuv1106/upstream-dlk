"""Silver: cleaned, deduplicated and standardized messages, partitioned like bronze."""

import json
from typing import Any

import polars as pl

from upstream_dlk.schema import API_SCHEMA, EVENT_TIME, HOUR_PARTITIONS, SILVER_NAMES
from upstream_dlk.storage import Storage
from upstream_dlk.tables import Partition, PartitionedTable

GEAR_ALIASES = {"NEUTRAL": "0", "REVERSE": "-1"}


class SilverLayer:
    """Rebuilds the silver partitions touched by a bronze run."""

    def __init__(self, storage: Storage, max_velocity_kmh: int):
        """
        Args:
            storage: Backend holding the DataLake.
            max_velocity_kmh: Velocities above this are flagged as suspect and nulled.
        """
        self.storage = storage
        self.max_velocity_kmh = max_velocity_kmh
        self.bronze = PartitionedTable(storage, "bronze", HOUR_PARTITIONS)
        self.table = PartitionedTable(storage, "silver", HOUR_PARTITIONS)

    def build(self, run_id: str, partitions: list[Partition]) -> dict[str, Any] | None:
        """Rebuilds partitions from all their bronze files. Safe to rerun with the same input.

        Args:
            run_id: Bronze run that touched the partitions; names the quality report.
            partitions: Bronze partitions to rebuild.

        Returns:
            The data quality report of the rebuilt partitions, or None if there was nothing to read.
        """
        bronze = self.bronze.scan(partitions)
        if bronze is None:
            return None
        silver, quality = pl.collect_all(
            [self.clean(bronze, self.max_velocity_kmh), self.quality(bronze, self.max_velocity_kmh)]
        )
        staging_dir = self.storage.path("_staging", "silver")
        self.storage.delete(staging_dir)
        self.table.overwrite(staging_dir, self.table.stage(silver, staging_dir, "part-0.parquet"))
        self.storage.delete(staging_dir)

        report = quality.to_dicts()[0] | {"silver_rows": silver.height}
        self.storage.write_bytes(
            self.storage.path("silver", "_quality", f"{run_id}.json"),
            json.dumps(report, indent=2).encode(),
        )
        return report

    @staticmethod
    def clean(bronze: pl.LazyFrame, max_velocity_kmh: int) -> pl.LazyFrame:
        """Applies the silver rules to bronze rows.

        Drops null vins, removes exact duplicate messages (keeping the first ingestion),
        strips text fields, converts gears to integers, flags and nulls implausible
        velocities, renames columns to snake_case and sorts by vin and time.

        Args:
            bronze: Bronze rows of the partitions to rebuild.
            max_velocity_kmh: Velocities above this are flagged as suspect and nulled.

        Returns:
            The silver rows.
        """
        suspect = pl.col("velocity") > max_velocity_kmh
        return (
            bronze.filter(pl.col("vin").is_not_null())
            .group_by([*API_SCHEMA, *HOUR_PARTITIONS])
            .agg(pl.col("ingested_at").min())
            .with_columns(
                pl.col("manufacturer", "model").str.strip_chars(),
                timestamp=EVENT_TIME,
                gearPosition=SilverLayer.gear_to_int(pl.col("gearPosition")),
                velocity=pl.when(suspect).then(None).otherwise(pl.col("velocity")),
                is_velocity_suspect=suspect.fill_null(False),
            )
            .rename(SILVER_NAMES)
            .sort("vin", "timestamp")
        )

    @staticmethod
    def quality(bronze: pl.LazyFrame, max_velocity_kmh: int) -> pl.LazyFrame:
        """Counts the rows each silver rule drops or changes.

        Args:
            bronze: Bronze rows of the partitions to rebuild.
            max_velocity_kmh: Velocity threshold used by `clean`.

        Returns:
            One row of counts plus the offending vins and gear values.
        """
        suspect = pl.col("velocity") > max_velocity_kmh
        raw_gear = pl.col("gearPosition")
        unknown_gear = raw_gear.is_not_null() & SilverLayer.gear_to_int(raw_gear).is_null()
        return bronze.select(
            bronze_rows=pl.len(),
            null_vin_rows=pl.col("vin").is_null().sum(),
            unknown_gear_rows=unknown_gear.sum(),
            unknown_gear_values=raw_gear.filter(unknown_gear).unique().sort().implode(),
            suspect_velocity_rows=suspect.sum(),
            suspect_velocity_vins=pl.col("vin")
            .filter(suspect)
            .drop_nulls()
            .unique()
            .sort()
            .implode(),
            suspect_velocity_reason=pl.lit(f"velocity > {max_velocity_kmh} km/h is implausible"),
        )

    @staticmethod
    def gear_to_int(gear: pl.Expr) -> pl.Expr:
        """Maps gear text to integers: NEUTRAL -> 0, REVERSE -> -1, digits as is, else null."""
        return (
            gear.str.strip_chars()
            .str.to_uppercase()
            .replace(GEAR_ALIASES)
            .cast(pl.Int8, strict=False)
        )
