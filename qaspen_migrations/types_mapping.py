from __future__ import annotations
import typing

from qaspen import columns


if typing.TYPE_CHECKING:
    from qaspen.columns.base import Column


POSTGRES_TYPE_MAPPING: typing.Final[dict[str, type[Column[typing.Any]]]] = {
    "int2": columns.SmallIntColumn,
    "int4": columns.IntegerColumn,
    "int8": columns.BigIntColumn,
    "numeric": columns.DecimalColumn,
    "float4": columns.RealColumn,
    "float8": columns.DoublePrecisionColumn,
    "boolean": columns.BooleanColumn,
    "varchar": columns.VarCharColumn,
    "char": columns.CharColumn,
    "text": columns.TextColumn,
    "date": columns.DateColumn,
    "time": columns.TimeColumn,
    "timetz": columns.TimeTZColumn,
    "timestamp": columns.TimestampColumn,
    "timestamptz": columns.TimestampTZColumn,
    "interval": columns.IntervalColumn,
    "json": columns.JsonColumn,
    "jsonb": columns.JsonbColumn,
    "array": columns.ArrayColumn,
}
