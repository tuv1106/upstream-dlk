"""Runs the generated Trino SQL on a real Trino; skipped unless one listens on localhost:8080.

Start one with `docker run -d -p 8080:8080 trinodb/trino`.
"""

import pytest
import trino
from conftest import REGEX_CASES, REGEXES, regex_case_messages

from upstream_dlk.layers.sql_injection import SqlInjectionReport

TABLE = "memory.default.bronze"


@pytest.fixture(scope="module")
def cursor():
    try:
        cursor = trino.dbapi.connect(host="localhost", port=8080, user="test").cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
    except Exception:
        pytest.skip("Trino is not running on localhost:8080")
    return cursor


def load_into_trino(cursor, rows: list[tuple[str, str]]) -> None:
    values = ", ".join(f"('{vin.replace(chr(39), chr(39) * 2)}', '{model}')" for vin, model in rows)
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cursor.fetchall()
    cursor.execute(f"CREATE TABLE {TABLE} AS SELECT * FROM (VALUES {values}) AS t(vin, model)")
    cursor.fetchall()


def test_trino_query_matches_polars_report(make_lake, cursor):
    lake = make_lake(regex_case_messages())
    lake.ingest(1)
    sqli = SqlInjectionReport(lake.storage)
    bronze = lake.bronze.table.scan().select("vin", "model").collect()
    load_into_trino(cursor, bronze.rows())

    cursor.execute(sqli.trino_sql(["vin", "model"], REGEXES, table=TABLE))
    trino_found = {row[0]: sorted(row[-1]) for row in cursor.fetchall()}
    polars_found = dict(
        sqli.build(["vin", "model"], REGEXES).select("vin", "matched_regexes").rows()
    )

    assert trino_found == polars_found == {v: r for v, r in REGEX_CASES.items() if r}
