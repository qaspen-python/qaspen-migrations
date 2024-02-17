import typing

from qaspen.sql_type import complex_types, primitive_types


POSTGRES_TYPE_MAPPING: typing.Final = {
    "int2": primitive_types.SmallInt,
    "int4": primitive_types.Integer,
    "int8": primitive_types.BigInt,
    "numeric": primitive_types.Decimal,
    "float4": primitive_types.Real,
    "float8": primitive_types.DoublePrecision,
    "boolean": primitive_types.Boolean,
    "varchar": primitive_types.VarChar,
    "char": primitive_types.Char,
    "text": primitive_types.Text,
    "date": primitive_types.Date,
    "time": primitive_types.Time,
    "timetz": primitive_types.TimeTZ,
    "timestamp": primitive_types.Timestamp,
    "timestamptz": primitive_types.TimestampTZ,
    "interval": primitive_types.Interval,
    "json": complex_types.Json,
    "jsonb": complex_types.Jsonb,
    "array": complex_types.Array,
}
