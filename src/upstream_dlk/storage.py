"""Storage backends that hide where the DataLake files physically live."""

import os
from abc import ABC, abstractmethod

import fsspec
import polars as pl

from upstream_dlk.config import StorageConfig


class Storage(ABC):
    """File and parquet IO on top of an fsspec filesystem, rooted at a base path."""

    def __init__(self, root: str, options: dict[str, str] | None = None) -> None:
        """
        Args:
            root: Root path or URI of the DataLake.
            options: Backend specific options, also forwarded to Polars.
        """
        self.root = root.rstrip("/")
        self.options = options or {}
        self.fs = self._create_filesystem()

    @classmethod
    def from_config(cls, config: StorageConfig) -> "Storage":
        """Creates the storage backend named in the config.

        Args:
            config: Storage section of the settings.

        Returns:
            A ready to use storage backend.
        """
        backends: dict[str, type[Storage]] = {"local": LocalStorage, "s3": S3Storage}
        if config.type not in backends:
            raise ValueError(
                f"Unknown storage type {config.type!r}, expected one of {list(backends)}"
            )
        return backends[config.type](config.root, config.options)

    @abstractmethod
    def _create_filesystem(self) -> fsspec.AbstractFileSystem:
        """Returns the fsspec filesystem of this backend."""

    @abstractmethod
    def move(self, source: str, target: str) -> None:
        """Moves a file or directory to `target`, overwriting a file at `target`."""

    def path(self, *parts: str) -> str:
        """Joins path parts under the storage root."""
        return "/".join([self.root, *parts])

    def exists(self, path: str) -> bool:
        """Returns whether a file or directory exists."""
        return self.fs.exists(path)

    def read_bytes(self, path: str) -> bytes:
        """Returns the content of a file."""
        return self.fs.cat_file(path)

    def write_bytes(self, path: str, data: bytes) -> None:
        """Writes `data` to a file, creating parent directories as needed."""
        with self.fs.open(path, "wb") as file:
            file.write(data)

    def glob(self, pattern: str) -> list[str]:
        """Returns the sorted paths matching a glob pattern."""
        return sorted(self._qualify(path) for path in self.fs.glob(pattern))

    def delete(self, path: str) -> None:
        """Deletes a file or directory tree if it exists."""
        if self.exists(path):
            self.fs.rm(path, recursive=True)

    def replace_dir(self, source: str, target: str) -> None:
        """Swaps directory `source` into `target`, then removes the old `target`."""
        backup = f"{target}._old"
        self.delete(backup)
        if self.exists(target):
            self.move(target, backup)
        self.move(source, target)
        self.delete(backup)

    def write_parquet(self, df: pl.DataFrame, path: str) -> None:
        """Writes a DataFrame as a single parquet file."""
        with self.fs.open(path, "wb") as file:
            df.write_parquet(file)

    def scan_parquet(
        self, paths: list[str], hive_schema: dict[str, pl.DataType] | None = None
    ) -> pl.LazyFrame:
        """Lazily scans parquet files, reading hive partition columns from their paths.

        Args:
            paths: Parquet files to scan.
            hive_schema: Types of the partition columns encoded in the paths, if any.

        Returns:
            A LazyFrame over all files.
        """
        return pl.scan_parquet(
            paths,
            hive_partitioning=hive_schema is not None,
            hive_schema=hive_schema,
            storage_options=self.options or None,
        )

    def _qualify(self, path: str) -> str:
        """Returns a path in the form Polars and fsspec both accept."""
        return path


class LocalStorage(Storage):
    """DataLake on the local disk. Moves are atomic renames."""

    def __init__(self, root: str, options: dict[str, str] | None = None) -> None:
        super().__init__(os.path.abspath(root), options)

    def _create_filesystem(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem("file", auto_mkdir=True)

    def move(self, source: str, target: str) -> None:
        os.makedirs(os.path.dirname(target), exist_ok=True)
        os.replace(source, target)


class S3Storage(Storage):
    """DataLake on S3. Moves are copy + delete, so they are not atomic."""

    def _create_filesystem(self) -> fsspec.AbstractFileSystem:
        return fsspec.filesystem("s3", **self.options)

    def move(self, source: str, target: str) -> None:
        self.fs.mv(source, target, recursive=True)

    def _qualify(self, path: str) -> str:
        return path if path.startswith("s3://") else f"s3://{path}"
