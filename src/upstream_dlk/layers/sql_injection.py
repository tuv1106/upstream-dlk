"""SQL injection detection over the raw bronze messages."""

import logging
from datetime import UTC, datetime

import polars as pl

from upstream_dlk.schema import HOUR_PARTITIONS
from upstream_dlk.storage import Storage
from upstream_dlk.tables import PartitionedTable

logger = logging.getLogger(__name__)


class SqlInjectionReport:
    """Finds bronze messages whose text values match SQL injection patterns.

    Works one partition at a time to keep memory bounded. Regexes run only on the
    distinct values of each column; the full messages are then fetched by value.
    """

    def __init__(self, storage: Storage):
        """
        Args:
            storage: Backend holding the DataLake.
        """
        self.storage = storage
        self.bronze = PartitionedTable(storage, "bronze", HOUR_PARTITIONS)
        self.path = storage.path("gold", "sql_injection_report")

    def build(self, columns: list[str], regexes: list[str]) -> pl.DataFrame:
        """Scans bronze and writes the violating messages to `gold/sql_injection_report`.

        Args:
            columns: Columns to check. Non-text columns are skipped with a warning.
            regexes: Patterns that mark a value as a violation.

        Returns:
            One row per violating message and column: the original message,
            `violating_column` and the `matched_regexes`.

        Raises:
            ValueError: If no regex is given or a column does not exist in bronze.
        """
        if not regexes:
            raise ValueError("At least one regex is required")
        text_columns = self._text_columns(columns)
        batches = [
            self.detect(self.bronze.scan([partition]), text_columns, regexes)
            for partition in (self.bronze.partitions() if text_columns else [])
        ]
        found = [batch for batch in batches if batch.height]
        report = pl.concat(found) if found else pl.DataFrame()
        self.storage.write_parquet(
            report, f"{self.path}/{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}.parquet"
        )
        return report

    @staticmethod
    def detect(messages: pl.LazyFrame, columns: list[str], regexes: list[str]) -> pl.DataFrame:
        """Returns the messages of one batch whose values in `columns` match a regex.

        Args:
            messages: Bronze messages of one batch.
            columns: Text columns to check, already validated against the schema.
            regexes: Patterns that mark a value as a violation.

        Returns:
            Violating messages with `violating_column` and `matched_regexes`.
        """
        distinct_values = pl.SQLContext(messages=messages).execute(
            " UNION ALL ".join(
                f"SELECT DISTINCT '{column}' AS violating_column, \"{column}\" AS value "
                f'FROM messages WHERE "{column}" IS NOT NULL'
                for column in columns
            )
        )
        violations = (
            pl.concat(
                distinct_values.filter(pl.col("value").str.contains(regex)).with_columns(
                    matched_regex=pl.lit(regex)
                )
                for regex in regexes
            )
            .group_by("violating_column", "value")
            .agg(matched_regexes=pl.col("matched_regex").sort())
            .collect()
        )
        found = []
        for column in violations["violating_column"].unique().sort():
            bad = violations.filter(pl.col("violating_column") == column).drop("violating_column")
            found.append(
                messages.filter(pl.col(column).is_in(bad["value"].implode()))
                .join(bad.lazy(), left_on=column, right_on="value")
                .with_columns(violating_column=pl.lit(column))
            )
        return pl.concat(found).collect(engine="streaming") if found else pl.DataFrame()

    def trino_sql(self, columns: list[str], regexes: list[str], table: str = "bronze") -> str:
        """Builds the same report as one Trino query, for full-table scans at scale.

        Trino broadcasts the small `violations` side of each join and pushes its values
        into the bronze scan as a dynamic filter, so only matching row groups are read.

        Args:
            columns: Columns to check; validated and filtered to text like `build`.
            regexes: Patterns that mark a value as a violation.
            table: Bronze table name in the Trino catalog.

        Returns:
            The SQL query text.
        """
        text_columns = self._text_columns(columns)
        patterns = ", ".join(f"({_sql_literal(regex)})" for regex in regexes)
        distinct = "\n    UNION ALL\n    ".join(
            f"SELECT DISTINCT '{c}' AS violating_column, \"{c}\" AS value "
            f'FROM {table} WHERE "{c}" IS NOT NULL'
            for c in text_columns
        )
        messages = "\nUNION ALL\n".join(
            f"SELECT b.*, v.violating_column, v.matched_regexes\nFROM {table} b\n"
            f"JOIN violations v ON v.violating_column = '{c}' AND b.\"{c}\" = v.value"
            for c in text_columns
        )
        return (
            f"WITH patterns(regex) AS (VALUES {patterns}),\n"
            f"distinct_values AS (\n    {distinct}\n),\n"
            "violations AS (\n"
            "    SELECT d.violating_column, d.value,\n"
            "        array_agg(p.regex ORDER BY p.regex) AS matched_regexes\n"
            "    FROM distinct_values d CROSS JOIN patterns p\n"
            "    WHERE regexp_like(d.value, p.regex)\n"
            "    GROUP BY d.violating_column, d.value\n"
            ")\n"
            f"{messages}"
        )

    def _text_columns(self, columns: list[str]) -> list[str]:
        bronze = self.bronze.scan()
        schema = bronze.collect_schema() if bronze is not None else {}
        unknown = [c for c in columns if c not in schema]
        if unknown:
            raise ValueError(f"Unknown columns {unknown}, expected some of {list(schema)}")
        skipped = [c for c in columns if schema[c] != pl.String]
        if skipped:
            logger.warning("Skipping non-text columns %s: they cannot hold SQL text", skipped)
        return [c for c in columns if schema[c] == pl.String]


def _sql_literal(text: str) -> str:
    """Quotes text as a SQL string literal."""
    return "'" + text.replace("'", "''") + "'"
