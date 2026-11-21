import pytest
from conftest import T0, epoch_ms, message

from upstream_dlk.layers.sql_injection import SqlInjectionReport

SPEC_REGEX = (
    r"('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|INSERT( +INTO){0,1}"
    r"|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"
)


@pytest.fixture
def report(make_lake):
    lake = make_lake(
        [
            message(vin="CLEAN1"),
            message(vin="1C4NJ'; DROP TABLE vehicles;--", velocity=1),
            message(
                vin="1C4NJ'; DROP TABLE vehicles;--",
                velocity=2,
                timestamp=epoch_ms(2026, 11, 20, 14),
            ),
            message(vin="BOTH", model="SELECT 1", timestamp=T0 + 1),
            message(vin=None, model="Compass", timestamp=T0 + 2),
        ]
    )
    lake.ingest(1)
    return SqlInjectionReport(lake.storage), lake


def test_reports_each_violating_message_and_column_across_partitions(report):
    sqli, _ = report

    df = sqli.build(["vin", "model"], [SPEC_REGEX, r";"])

    rows = df.sort("violating_column", "velocity").select(
        "vin", "velocity", "violating_column", "matched_regexes"
    )
    assert rows.rows() == [
        ("BOTH", 120, "model", [SPEC_REGEX]),
        ("1C4NJ'; DROP TABLE vehicles;--", 1, "vin", [SPEC_REGEX, ";"]),
        ("1C4NJ'; DROP TABLE vehicles;--", 2, "vin", [SPEC_REGEX, ";"]),
    ]
    assert {"manufacturer", "timestamp", "gearPosition", "date", "hour"} <= set(df.columns)


def test_writes_the_report_under_gold(report):
    sqli, lake = report

    sqli.build(["vin"], [SPEC_REGEX])

    assert len(lake.files("gold/sql_injection_report")) == 1


def test_skips_non_text_columns_and_rejects_unknown_ones(report):
    sqli, _ = report

    assert sqli.build(["year"], [SPEC_REGEX]).is_empty()
    with pytest.raises(ValueError, match="Unknown columns"):
        sqli.build(["vinn"], [SPEC_REGEX])
    with pytest.raises(ValueError, match="regex"):
        sqli.build(["vin"], [])


def test_clean_data_gives_an_empty_report(report):
    sqli, _ = report

    assert sqli.build(["manufacturer"], [SPEC_REGEX]).is_empty()


def test_trino_sql_covers_text_columns_and_escapes_quotes(report):
    sqli, _ = report

    sql = sqli.trino_sql(["vin", "model", "year"], [SPEC_REGEX])

    assert "('(''(''''|[^''])*'')" in sql
    assert 'b."vin" = v.value' in sql and 'b."model" = v.value' in sql
    assert '"year"' not in sql
