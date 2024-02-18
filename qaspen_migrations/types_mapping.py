from __future__ import annotations
import typing

from qaspen import fields


if typing.TYPE_CHECKING:
    from qaspen.fields.base import Field


POSTGRES_TYPE_MAPPING: typing.Final[dict[str, type[Field[typing.Any]]]] = {
    "int2": fields.SmallIntField,
    "int4": fields.IntegerField,
    "int8": fields.BigIntField,
    "numeric": fields.DecimalField,
    "float4": fields.RealField,
    "float8": fields.DoublePrecisionField,
    "boolean": fields.BooleanField,
    "varchar": fields.VarCharField,
    "char": fields.CharField,
    "text": fields.TextField,
    "date": fields.DateField,
    "time": fields.TimeField,
    "timetz": fields.TimeTZField,
    "timestamp": fields.TimestampField,
    "timestamptz": fields.TimestampTZField,
    "interval": fields.IntervalField,
    "json": fields.JsonField,
    "jsonb": fields.JsonbField,
    "array": fields.ArrayField,
}
