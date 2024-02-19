from __future__ import annotations
import typing

from qaspen import columns
from qaspen.columns.base import Column
from qaspen_psycopg.engine import PsycopgEngine

from qaspen_migrations.exceptions import ColumnParsingError
from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.schema import ColumnInfo
from qaspen_migrations.types_mapping import POSTGRES_TYPE_MAPPING
from qaspen_migrations.utils.parsing import parse_int_attribute


if typing.TYPE_CHECKING:
    from qaspen import BaseTable


def _parse_numeric_attributes(
    attribute_name: str,
    table_column: type[Column[typing.Any]],
    values: dict[typing.Any, typing.Any],
) -> int | None:
    attribute_value: typing.Final = values.get(attribute_name)
    if table_column not in [columns.DecimalColumn, columns.NumericColumn]:
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
            ic.column_name as db_column_name,
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

    def database_column_to_column_info(
        self,
        incoming_data: dict[typing.Any, typing.Any],
    ) -> ColumnInfo:
        db_column_name: typing.Final = incoming_data.get("db_column_name")
        if db_column_name is None:
            raise ColumnParsingError("Column name is empty.")

        incoming_type = incoming_data.get("sql_type", "")
        try:
            column_type: typing.Final = POSTGRES_TYPE_MAPPING[
                incoming_type.strip("_")
            ]
        except LookupError as exception:
            raise ColumnParsingError(
                f"Unknown sql type '{incoming_type}'.",
            ) from exception

        is_null: typing.Final = bool(incoming_data.get("is_null") == "YES")
        max_length: typing.Final = parse_int_attribute(
            incoming_data,
            "max_length",
        )
        database_default = incoming_data.get("database_default")
        scale: typing.Final = _parse_numeric_attributes(
            "scale",
            column_type,
            incoming_data,
        )
        precision: typing.Final = _parse_numeric_attributes(
            "precision",
            column_type,
            incoming_data,
        )
        is_array: typing.Final = incoming_data.get("sql_type", "").startswith(
            "_",
        )
        return ColumnInfo(
            main_column_type=typing.cast(
                type[Column[typing.Any]],
                columns.ArrayColumn if is_array else column_type,
            ),
            inner_column_type=column_type if is_array else None,
            db_column_name=db_column_name,
            is_null=is_null,
            max_length=max_length,
            database_default=database_default,
            scale=scale,
            precision=precision,
        )
