"""Table layouts on top of a storage backend, and the run bookkeeping between layers."""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import polars as pl

from upstream_dlk.storage import Storage

Partition = dict[str, str]
"""Partition column name to its value as written in the path, e.g. {"date": "2026-11-20"}."""


class PartitionedTable:
    """Hive-style parquet table: `<name>/<col>=<value>/.../<file>.parquet`."""

    def __init__(self, storage: Storage, name: str, partition_schema: dict[str, pl.DataType]):
        """
        Args:
            storage: Backend holding the table.
            name: Table directory under the storage root.
            partition_schema: Partition columns, outermost first, with their types.
        """
        self.storage = storage
        self.path = storage.path(name)
        self.partition_schema = partition_schema

    @property
    def partition_by(self) -> list[str]:
        """Partition column names, outermost first."""
        return list(self.partition_schema)

    def scan(self, partitions: list[Partition] | None = None) -> pl.LazyFrame | None:
        """Lazily scans the given partitions, or the whole table.

        Args:
            partitions: Partitions to read. A partition may name only the outer columns
                (e.g. just a date) to read everything below it. None reads all partitions.

        Returns:
            A LazyFrame including the partition columns, or None if no file matches.
        """
        if partitions is None:
            prefixes = [self.path]
        else:
            prefixes = list(dict.fromkeys(map(self._partition_dir, partitions)))
        files = [
            file for prefix in prefixes for file in self.storage.glob(f"{prefix}/**/*.parquet")
        ]
        return self.storage.scan_parquet(files, self.partition_schema) if files else None

    def partitions(self) -> list[Partition]:
        """Returns all partitions holding at least one file, sorted."""
        dirs = {file.rsplit("/", 1)[0] for file in self.storage.glob(f"{self.path}/**/*.parquet")}
        return [
            dict(segment.split("=", 1) for segment in d.removeprefix(f"{self.path}/").split("/"))
            for d in sorted(dirs)
        ]

    def stage(self, df: pl.DataFrame, staging_dir: str, file_name: str) -> list[Partition]:
        """Writes a DataFrame split by partition under a staging directory.

        Args:
            df: Rows to write, including the partition columns.
            staging_dir: Directory mirroring the table layout.
            file_name: Parquet file name used in every partition.

        Returns:
            The partitions written.
        """
        partitions = []
        for key, rows in df.partition_by(
            self.partition_by, as_dict=True, include_key=False
        ).items():
            partition = dict(zip(self.partition_by, map(str, key), strict=True))
            self.storage.write_parquet(
                rows, f"{self._relative_dir(partition, staging_dir)}/{file_name}"
            )
            partitions.append(partition)
        return partitions

    def append(self, staging_dir: str, partitions: list[Partition]) -> None:
        """Moves staged files into their partitions, next to the existing files."""
        for partition in partitions:
            target_dir = self._partition_dir(partition)
            for file in self.storage.glob(
                f"{self._relative_dir(partition, staging_dir)}/*.parquet"
            ):
                self.storage.move(file, f"{target_dir}/{file.rsplit('/', 1)[-1]}")

    def overwrite(self, staging_dir: str, partitions: list[Partition]) -> None:
        """Swaps staged partitions in, replacing the existing ones."""
        for partition in partitions:
            self.storage.replace_dir(
                self._relative_dir(partition, staging_dir), self._partition_dir(partition)
            )

    def _partition_dir(self, partition: Partition) -> str:
        return self._relative_dir(partition, self.path)

    def _relative_dir(self, partition: Partition, base: str) -> str:
        return "/".join(
            [base, *(f"{col}={partition[col]}" for col in self.partition_by if col in partition)]
        )


class SingleFileTable:
    """Small table stored as one parquet file, replaced as a whole."""

    def __init__(self, storage: Storage, name: str):
        """
        Args:
            storage: Backend holding the table.
            name: Table directory under the storage root.
        """
        self.storage = storage
        self.file = storage.path(name, "data.parquet")

    def scan(self) -> pl.LazyFrame | None:
        """Returns the table as a LazyFrame, or None if it was never written."""
        return self.storage.scan_parquet([self.file]) if self.storage.exists(self.file) else None

    def overwrite(self, df: pl.DataFrame) -> None:
        """Replaces the table content through a temporary file."""
        staged = f"{self.file}.tmp"
        self.storage.write_parquet(df, staged)
        self.storage.move(staged, self.file)


@dataclass(frozen=True)
class Run:
    """One published bronze run.

    Attributes:
        run_id: Id of the run, its UTC start time, e.g. "20261121T101500123456Z".
        partitions: Bronze partitions that received rows in this run.
    """

    run_id: str
    partitions: list[Partition]

    RUN_ID_FORMAT = "%Y%m%dT%H%M%S%fZ"

    @classmethod
    def new_id(cls) -> str:
        """Returns a run id for the current UTC time."""
        return datetime.now(UTC).strftime(cls.RUN_ID_FORMAT)

    @classmethod
    def ingested_at(cls, run_id: str) -> datetime:
        """Returns the UTC ingestion time encoded in a run id."""
        return datetime.strptime(run_id, cls.RUN_ID_FORMAT).replace(tzinfo=UTC)


class RunLog:
    """Record of published bronze runs: one `<run_id>.json` file per run, written last."""

    def __init__(self, storage: Storage, name: str = "bronze/_runs"):
        """
        Args:
            storage: Backend holding the log.
            name: Log directory under the storage root.
        """
        self.storage = storage
        self.path = storage.path(name)

    def record(self, run: Run) -> None:
        """Marks a run as published."""
        self.storage.write_bytes(self._file(run.run_id), json.dumps(asdict(run)).encode())

    def get(self, run_id: str) -> Run | None:
        """Returns a published run, or None if it was never published."""
        if not self.storage.exists(self._file(run_id)):
            return None
        return Run(**json.loads(self.storage.read_bytes(self._file(run_id))))

    def _file(self, run_id: str) -> str:
        return f"{self.path}/{run_id}.json"
