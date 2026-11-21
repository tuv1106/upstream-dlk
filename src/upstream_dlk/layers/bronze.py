"""Bronze: raw API messages, partitioned by event date and hour (UTC)."""

import io
import json
import logging
from datetime import UTC, datetime, timedelta

import polars as pl

from upstream_dlk.api import VehicleMessagesClient
from upstream_dlk.schema import API_SCHEMA, EVENT_TIME, HOUR_PARTITIONS
from upstream_dlk.storage import Storage
from upstream_dlk.tables import Partition, PartitionedTable, Run, RunLog

logger = logging.getLogger(__name__)


class BronzeLayer:
    """Lands API responses and publishes them as an append-only partitioned table.

    A run goes through `land` -> `stage` -> `publish`. Each step is safe to retry:
    landing is skipped once the raw response exists, staging is rewritten, and
    publishing moves files named after the run id, so a retry overwrites its own files.
    The raw response is archived in `bronze/_raw`, so the parquet can always be rebuilt.
    Rows without a timestamp cannot be partitioned and go to `quarantine/bronze`.
    """

    def __init__(self, storage: Storage, client: VehicleMessagesClient, run_log: RunLog):
        """
        Args:
            storage: Backend holding the DataLake.
            client: Source of the raw messages.
            run_log: Record of published runs.
        """
        self.storage = storage
        self.client = client
        self.run_log = run_log
        self.table = PartitionedTable(storage, "bronze", HOUR_PARTITIONS)

    def check_resumable(self, run_id: str) -> Run | None:
        """Checks whether an existing run can be resumed.

        Args:
            run_id: Id of a run started earlier.

        Returns:
            The run if it is already published, or None if it was landed but not published.

        Raises:
            ValueError: If the run never landed, so there is nothing to resume.
        """
        published = self.run_log.get(run_id)
        landed = self.storage.exists(self._raw_file(run_id)) or self.storage.exists(
            self._archived_raw(run_id)
        )
        if published is None and not landed:
            raise ValueError(f"Run {run_id} never landed; start a new run instead")
        return published

    def land(self, run_id: str, amount: int) -> None:
        """Saves the raw API response of a run, unless it was already saved.

        Args:
            run_id: Id of the run.
            amount: Number of messages to request.
        """
        raw_file = self._raw_file(run_id)
        if not self.storage.exists(raw_file):
            self.storage.write_bytes(raw_file, self.client.fetch(amount))

    def stage(self, run_id: str) -> list[Partition]:
        """Converts the landed response to partitioned parquet in the run's staging area.

        Args:
            run_id: Id of a landed run.

        Returns:
            The partitions staged.
        """
        staging_dir = self._staging_dir(run_id)
        self.storage.delete(staging_dir)
        self.storage.delete(self._staged_quarantine(run_id))
        raw = self.storage.read_bytes(self._raw_file(run_id))
        self._warn_on_unknown_fields(raw)
        df = self.to_frame(raw, Run.ingested_at(run_id))
        invalid = df.filter(pl.col("timestamp").is_null())
        if invalid.height:
            logger.warning("Quarantined %d messages without a timestamp", invalid.height)
            self.storage.write_parquet(invalid, self._staged_quarantine(run_id))
        valid = df.filter(pl.col("timestamp").is_not_null())
        return self.table.stage(valid, staging_dir, f"{run_id}.parquet")

    def publish(self, run_id: str, partitions: list[Partition]) -> Run:
        """Moves staged files into bronze, archives the raw response and commits the run.

        Args:
            run_id: Id of a staged run.
            partitions: Partitions returned by `stage`.

        Returns:
            The published run.
        """
        self.table.append(self._staging_dir(run_id), partitions)
        if self.storage.exists(self._staged_quarantine(run_id)):
            self.storage.move(
                self._staged_quarantine(run_id),
                self.storage.path("quarantine", "bronze", f"{run_id}.parquet"),
            )
        if self.storage.exists(self._raw_file(run_id)):
            self.storage.move(self._raw_file(run_id), self._archived_raw(run_id))
        run = Run(run_id, partitions)
        self.run_log.record(run)
        self.storage.delete(self.storage.path("_staging", run_id))
        return run

    def expire_raw(self, retention_days: int, now: datetime | None = None) -> int:
        """Deletes archived raw responses of runs older than the retention period.

        Args:
            retention_days: Number of days raw responses are kept.
            now: Reference UTC time; defaults to the current time.

        Returns:
            Number of raw responses deleted.
        """
        cutoff = (now or datetime.now(UTC)) - timedelta(days=retention_days)
        expired = [
            file
            for file in self.storage.glob(self.storage.path("bronze", "_raw", "*.json"))
            if Run.ingested_at(file.rsplit("/", 1)[-1].removesuffix(".json")) < cutoff
        ]
        for file in expired:
            self.storage.delete(file)
        return len(expired)

    @staticmethod
    def to_frame(raw: bytes, ingested_at: datetime) -> pl.DataFrame:
        """Parses a raw API response, adding only partition and lineage columns.

        Args:
            raw: JSON array of messages as returned by the API.
            ingested_at: UTC time of the run.

        Returns:
            The untouched message fields plus `ingested_at`, `date` and `hour`.
        """
        return pl.read_json(io.BytesIO(raw), schema=API_SCHEMA).with_columns(
            ingested_at=pl.lit(ingested_at, dtype=pl.Datetime("us", "UTC")),
            date=EVENT_TIME.dt.date(),
            hour=EVENT_TIME.dt.hour(),
        )

    @staticmethod
    def _warn_on_unknown_fields(raw: bytes) -> None:
        unknown = set().union(*map(dict.keys, json.loads(raw))) - set(API_SCHEMA)
        if unknown:
            logger.warning("API sent fields missing from the schema: %s", sorted(unknown))

    def _archived_raw(self, run_id: str) -> str:
        return self.storage.path("bronze", "_raw", f"{run_id}.json")

    def _raw_file(self, run_id: str) -> str:
        return self.storage.path("_staging", run_id, "raw.json")

    def _staging_dir(self, run_id: str) -> str:
        return self.storage.path("_staging", run_id, "bronze")

    def _staged_quarantine(self, run_id: str) -> str:
        return self.storage.path("_staging", run_id, "quarantine.parquet")
