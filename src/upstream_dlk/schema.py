"""Column names and types shared by the layers."""

import polars as pl

API_SCHEMA: dict[str, pl.DataType] = {
    "vin": pl.String(),
    "manufacturer": pl.String(),
    "year": pl.Int32(),
    "model": pl.String(),
    "latitude": pl.Float64(),
    "longitude": pl.Float64(),
    "timestamp": pl.Int64(),
    "velocity": pl.Float64(),
    "frontLeftDoorState": pl.String(),
    "wipersState": pl.Boolean(),
    "gearPosition": pl.String(),
    "driverSeatbeltState": pl.String(),
}
"""Message fields as the API sends them. Explicit, so all-null batches keep their types.
Numbers that may carry decimals are floats, so a value like 12.7 is never truncated."""

HOUR_PARTITIONS: dict[str, pl.DataType] = {"date": pl.Date(), "hour": pl.Int8()}
DATE_PARTITIONS: dict[str, pl.DataType] = {"date": pl.Date()}

SILVER_NAMES: dict[str, str] = {
    "frontLeftDoorState": "front_left_door_state",
    "wipersState": "wipers_state",
    "gearPosition": "gear_position",
    "driverSeatbeltState": "driver_seatbelt_state",
}
"""API camelCase names renamed to snake_case in silver."""

EVENT_TIME: pl.Expr = pl.col("timestamp").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC")
"""The API epoch-ms `timestamp` as a UTC datetime."""
