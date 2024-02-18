from __future__ import annotations
import typing

from qaspen import fields
from qaspen_psycopg.engine import PsycopgEngine

from qaspen_migrations.exceptions import FieldParsingError
from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.types_mapping import POSTGRES_TYPE_MAPPING
from qaspen_migrations.utils import get_int_attribute


if typing.TYPE_CHECKING:
    from qaspen import BaseTable
    from qaspen.fields.base import Field


def _parse_numeric_attributes(
    attribute_name: str,
    table_field: type[Field[typing.Any]],
    values: dict[typing.Any, typing.Any],
) -> int | None:
    attribute_value: typing.Final = values.get(attribute_name)
    if table_field not in [fields.DecimalField, fields.NumericField]:
        return None
    try:
        return int(attribute_value)  # type: ignore[arg-type]
    except ValueError:
        return None


class PostgresInspector(
    BaseInspector[PsycopgEngine],
):
    # TODO: implement scale and precision fetching for decimal/numeric array types  # noqa: TD002, E501
    inspect_info_query = """
        SELECT
            ic.column_name,
            ic.udt_name AS sql_type,
            ic.is_nullable AS is_null,
            ic.column_default AS database_default,
            ic.numeric_precision AS precision,
            ic.numeric_scale AS scale,
            CASE
                WHEN att.atttypid = ANY (ARRAY[1002, 1015])
                    AND att.atttypmod > 0 THEN att.atttypmod - 4
                ELSE ic.character_maximum_length
            END AS max_length
        FROM
            information_schema.columns ic
        JOIN
            pg_catalog.pg_namespace nsp ON nsp.nspname = ic.table_schema
        JOIN
            pg_catalog.pg_class cls ON cls.relname = ic.table_name
                AND cls.relnamespace = nsp.oid
        JOIN
            pg_catalog.pg_attribute att ON att.attrelid = cls.oid
                AND att.attname = ic.column_name
        WHERE
            ic.table_catalog = '{}'
            AND ic.table_name = '{}'
            AND ic.table_schema = '{}'
            AND att.attnum > 0
            AND NOT att.attisdropped;
    """

    def build_inspect_info_query(self, table: type[BaseTable]) -> str:
        return self.inspect_info_query.format(
            self.engine.database,
            table.original_table_name(),
            table._table_meta.table_schema,
        )

    def database_info_to_field(
        self,
        incoming_data: dict[typing.Any, typing.Any],
    ) -> Field[typing.Any]:
        column_name: typing.Final = incoming_data.get("column_name")
        if column_name is None:
            raise FieldParsingError("Field name is empty.")

        incoming_type = incoming_data.get("sql_type", "")
        is_array: typing.Final = incoming_type.startswith("_")
        try:
            table_field_type: typing.Final = POSTGRES_TYPE_MAPPING[
                incoming_type.strip("_")
            ]
        except LookupError as exception:
            raise FieldParsingError(
                f"Unknown sql type '{incoming_type}'.",
            ) from exception

        is_null: typing.Final = bool(incoming_data.get("is_null") == "YES")
        max_length: typing.Final = get_int_attribute(
            incoming_data,
            "max_length",
        )
        database_default = incoming_data.get("database_default")
        scale: typing.Final = _parse_numeric_attributes(
            "scale",
            table_field_type,
            incoming_data,
        )
        precision: typing.Final = _parse_numeric_attributes(
            "precision",
            table_field_type,
            incoming_data,
        )

        return self.__map_field_type(
            table_field_type,
            column_name,
            database_default,
            is_null,
            precision,
            scale,
            max_length,
            is_array,
        )
