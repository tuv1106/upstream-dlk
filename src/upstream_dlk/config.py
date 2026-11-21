"""Pipeline settings, loaded from one TOML file per environment."""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

CONFIG_DIR = Path("config")


@dataclass(frozen=True)
class StorageConfig:
    """Where the DataLake lives.

    Attributes:
        type: Storage backend name, e.g. "local" or "s3".
        root: Root path or URI of the DataLake.
        options: Backend specific options (credentials, region, ...).
    """

    type: str
    root: str
    options: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ApiConfig:
    """Vehicle messages API settings.

    Attributes:
        base_url: Base URL of the API server.
        amount: Number of messages requested per run.
        timeout_seconds: HTTP request timeout.
    """

    base_url: str
    amount: int
    timeout_seconds: float


@dataclass(frozen=True)
class BronzeConfig:
    """Bronze layer settings.

    Attributes:
        raw_retention_days: Archived raw API responses older than this are deleted.
    """

    raw_retention_days: int


@dataclass(frozen=True)
class QualityConfig:
    """Data quality thresholds.

    Attributes:
        max_velocity_kmh: Velocities above this are physically implausible and flagged.
    """

    max_velocity_kmh: int


@dataclass(frozen=True)
class Settings:
    """All pipeline settings for one environment."""

    storage: StorageConfig
    api: ApiConfig
    bronze: BronzeConfig
    quality: QualityConfig

    @classmethod
    def load(cls, env: str, config_dir: Path = CONFIG_DIR) -> "Settings":
        """Loads the settings of an environment.

        Args:
            env: Environment name, matching `<config_dir>/<env>.toml`.
            config_dir: Directory holding the environment files.

        Returns:
            The parsed settings.
        """
        with open(config_dir / f"{env}.toml", "rb") as file:
            raw = tomllib.load(file)
        return cls(
            storage=StorageConfig(**raw["storage"]),
            api=ApiConfig(**raw["api"]),
            bronze=BronzeConfig(**raw["bronze"]),
            quality=QualityConfig(**raw["quality"]),
        )
