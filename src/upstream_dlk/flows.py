"""Prefect flows: bronze -> silver -> gold, each layer a flow and each step a task."""

import argparse
from dataclasses import dataclass

import httpx
from prefect import Task, flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.client.schemas.objects import TaskRun
from prefect.states import State

from upstream_dlk.api import VehicleMessagesClient
from upstream_dlk.config import Settings
from upstream_dlk.layers.bronze import BronzeLayer
from upstream_dlk.layers.gold import GoldLayer
from upstream_dlk.layers.silver import SilverLayer
from upstream_dlk.layers.sql_injection import SqlInjectionReport
from upstream_dlk.storage import Storage
from upstream_dlk.tables import Partition, Run, RunLog


def is_transient(task: Task, task_run: TaskRun, state: State) -> bool:
    """Retries only errors that may pass on their own: network, HTTP 5xx and storage IO.

    Data errors (bad JSON, wrong types) and missing files fail the same way every time,
    so they fail fast.
    """
    error = state.result(raise_on_failure=False)
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.is_server_error
    if isinstance(error, FileNotFoundError):
        return False
    return isinstance(error, httpx.TransportError | OSError)


TASK_OPTIONS = {
    "retries": 3,
    "retry_delay_seconds": 10,
    "retry_condition_fn": is_transient,
    "cache_policy": NO_CACHE,
}


@dataclass(frozen=True)
class DataLake:
    """The layers of one environment, wired to a shared storage and run log."""

    settings: Settings
    bronze: BronzeLayer
    silver: SilverLayer
    gold: GoldLayer
    sql_injection: SqlInjectionReport

    @classmethod
    def from_env(cls, env: str) -> "DataLake":
        """Builds the layers of an environment.

        Args:
            env: Environment name, see `Settings.load`.

        Returns:
            The wired layers.
        """
        settings = Settings.load(env)
        storage = Storage.from_config(settings.storage)
        run_log = RunLog(storage)
        client = VehicleMessagesClient(settings.api.base_url, settings.api.timeout_seconds)
        return cls(
            settings=settings,
            bronze=BronzeLayer(storage, client, run_log),
            silver=SilverLayer(storage, settings.quality.max_velocity_kmh),
            gold=GoldLayer(storage),
            sql_injection=SqlInjectionReport(storage),
        )


@task(**TASK_OPTIONS)
def land_messages(bronze: BronzeLayer, run_id: str, amount: int) -> None:
    """Saves the raw API response of the run."""
    bronze.land(run_id, amount)


@task(**TASK_OPTIONS)
def stage_bronze(bronze: BronzeLayer, run_id: str) -> list[Partition]:
    """Converts the landed response to staged partitioned parquet."""
    return bronze.stage(run_id)


@task(**TASK_OPTIONS)
def publish_bronze(bronze: BronzeLayer, run_id: str, partitions: list[Partition]) -> Run:
    """Moves the staged files into bronze and records the run as published."""
    run = bronze.publish(run_id, partitions)
    get_run_logger().info("Published run %s to %d partitions", run_id, len(partitions))
    return run


@task(**TASK_OPTIONS)
def expire_raw_responses(bronze: BronzeLayer, retention_days: int) -> None:
    """Deletes archived raw responses older than the retention period."""
    get_run_logger().info("Deleted %d expired raw responses", bronze.expire_raw(retention_days))


@task(**TASK_OPTIONS)
def build_silver(silver: SilverLayer, run_id: str, partitions: list[Partition]) -> None:
    """Rebuilds the given silver partitions."""
    report = silver.build(run_id, partitions)
    get_run_logger().info("Silver quality report: %s", report or "no rows")


@task(**TASK_OPTIONS)
def build_gold(gold: GoldLayer, partitions: list[Partition]) -> None:
    """Updates the gold reports from the given silver partitions."""
    get_run_logger().info("Gold processed %d silver partitions", gold.build(partitions))


@task(**TASK_OPTIONS)
def build_sql_injection_report(
    report: SqlInjectionReport, columns: list[str], regexes: list[str]
) -> None:
    """Writes the bronze messages matching SQL injection patterns to gold."""
    violations = report.build(columns, regexes)
    get_run_logger().info("Found %d SQL injection violations", violations.height)


@flow(name="bronze")
def bronze_flow(env: str = "local", run_id: str | None = None) -> Run:
    """Fetches one batch of messages into bronze.

    Args:
        env: Environment name.
        run_id: Id of a failed run to resume; a new run is started when omitted.

    Returns:
        The published run, with the partitions downstream layers must rebuild.
    """
    lake = DataLake.from_env(env)
    if run_id and (published := lake.bronze.check_resumable(run_id)):
        get_run_logger().info("Run %s is already published, skipping bronze", run_id)
        return published
    run_id = run_id or Run.new_id()
    land_messages(lake.bronze, run_id, lake.settings.api.amount)
    return publish_bronze(lake.bronze, run_id, stage_bronze(lake.bronze, run_id))


@flow(name="silver")
def silver_flow(run_id: str, partitions: list[Partition], env: str = "local") -> None:
    """Rebuilds the silver partitions touched by a bronze run.

    Args:
        run_id: Bronze run that touched the partitions.
        partitions: Partitions to rebuild.
        env: Environment name.
    """
    build_silver(DataLake.from_env(env).silver, run_id, partitions)


@flow(name="gold")
def gold_flow(partitions: list[Partition], env: str = "local") -> None:
    """Updates the gold reports from rebuilt silver partitions.

    Args:
        partitions: Silver partitions to read.
        env: Environment name.
    """
    build_gold(DataLake.from_env(env).gold, partitions)


@flow(name="vehicle-messages-pipeline")
def pipeline_flow(env: str = "local", run_id: str | None = None) -> None:
    """Runs bronze, then silver and gold on the partitions bronze touched, then housekeeping.

    Args:
        env: Environment name.
        run_id: Id of a failed run to resume; a new run is started when omitted. Silver and
            gold rerun for a run that is already published, since both are idempotent.
    """
    run = bronze_flow(env, run_id)
    silver_flow(run.run_id, run.partitions, env)
    gold_flow(run.partitions, env)
    lake = DataLake.from_env(env)
    expire_raw_responses(lake.bronze, lake.settings.bronze.raw_retention_days)


@flow(name="sql-injection-report")
def sql_injection_flow(columns: list[str], regexes: list[str], env: str = "local") -> None:
    """Scans bronze for SQL injection patterns, on demand.

    Args:
        columns: Columns to check; non-text columns are skipped.
        regexes: Patterns that mark a value as a violation.
        env: Environment name.
    """
    build_sql_injection_report(DataLake.from_env(env).sql_injection, columns, regexes)


def main() -> None:
    """Command line entry point: `dlk [--env local] [--run-id ID]`."""
    parser = argparse.ArgumentParser(description=pipeline_flow.__doc__)
    parser.add_argument("--env", default="local", help="environment config in config/<env>.toml")
    parser.add_argument("--run-id", help="resume a failed run")
    args = parser.parse_args()
    pipeline_flow(args.env, args.run_id)


def sql_injection_main() -> None:
    """Command line entry point: `dlk-sqli --column vin --regex ';' [--print-trino-sql]`."""
    parser = argparse.ArgumentParser(description=sql_injection_flow.__doc__)
    parser.add_argument("--column", action="append", required=True, help="column to check")
    parser.add_argument("--regex", action="append", required=True, help="violation pattern")
    parser.add_argument("--env", default="local", help="environment config in config/<env>.toml")
    parser.add_argument(
        "--print-trino-sql", action="store_true", help="print the equivalent Trino query and exit"
    )
    args = parser.parse_args()
    if args.print_trino_sql:
        print(DataLake.from_env(args.env).sql_injection.trino_sql(args.column, args.regex))
    else:
        sql_injection_flow(args.column, args.regex, args.env)
