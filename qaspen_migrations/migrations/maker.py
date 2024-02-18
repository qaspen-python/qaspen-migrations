from __future__ import annotations
import dataclasses
import typing

from qaspen_migrations.ddl.mapping import map_ddl_generator
from qaspen_migrations.exceptions import (
    MigrationGenerationError,
)
from qaspen_migrations.inspector.mapping import map_inspector
from qaspen_migrations.migrations.writer import MigrationsWriter
from qaspen_migrations.schema import (
    MigrationChangesSchema,
    TableDumpSchema,
)


if typing.TYPE_CHECKING:
    from qaspen.abc.db_engine import BaseEngine
    from qaspen.fields.base import Field
    from qaspen.table.base_table import BaseTable


@dataclasses.dataclass
class MigrationsMaker:
    engine: BaseEngine[
        typing.Any,
        typing.Any,
        typing.Any,
    ]
    migrations_path: str
    tables: list[type[BaseTable]]

    async def make_migrations(self) -> None:
        inspector: typing.Final = map_inspector(self.engine, self.tables)
        migration_changes: typing.Final = generate_migration_changes(
            inspector.inspect_local_state(),
            await inspector.inspect_database(),
        )
        ddl_generator: typing.Final = map_ddl_generator(
            self.engine,
            migration_changes,
        )

        to_apply, to_rollback = ddl_generator.generate_ddl_elements()

        migrations_writer = MigrationsWriter(
            self.migrations_path,
            self.engine.engine_type,
            to_apply,
            to_rollback,
        )

        await migrations_writer.save_migration()


def generate_migration_changes(
    dump_from_local_state: list[TableDumpSchema],
    dump_from_database: list[TableDumpSchema],
) -> list[MigrationChangesSchema]:
    migration_changes: typing.Final = []

    for table_dump_from_local_state, table_dump_from_database in zip(
        dump_from_local_state,
        dump_from_database,
    ):
        if table_dump_from_local_state.table != table_dump_from_database.table:
            raise MigrationGenerationError(
                "Local table name doesn't match "
                "table name from database: "
                f"{table_dump_from_local_state.table} "
                f"!= {table_dump_from_database.table}.",
            )

        field_names_from_local_state = (
            table_dump_from_local_state.all_field_names()
        )
        field_names_from_database = table_dump_from_database.all_field_names()
        # Get to add fields according to
        # name field name presence in set from database
        to_add_fields = {
            field_info
            for field_info in table_dump_from_local_state.table_fields_set
            if field_info.field_name not in field_names_from_database
        }
        # Get to drop fields according to
        # name field name presence in set from local state
        to_drop_fields = {
            field_info
            for field_info in table_dump_from_database.table_fields_set
            if field_info.field_name not in field_names_from_local_state
        }

        # Fields are suspected for update
        # if they are neither created nor deleted
        suspected_for_update_fields_from_local_state = (
            table_dump_from_local_state.table_fields_set - to_add_fields
        )
        suspected_for_update_fields_from_database = (
            table_dump_from_database.table_fields_set - to_drop_fields
        )

        to_alter_fields: set[
            tuple[Field[typing.Any], Field[typing.Any]]
        ] = set()
        for (
            suspected_field_from_local_state,
            suspected_field_from_database,
        ) in zip(
            sorted(
                suspected_for_update_fields_from_local_state,
                key=lambda suspect_field: suspect_field.field_name,
            ),
            sorted(
                suspected_for_update_fields_from_database,
                key=lambda suspect_field: suspect_field.field_name,
            ),
        ):
            if (
                suspected_field_from_local_state.field_name
                != suspected_field_from_database.field_name
            ):
                raise MigrationGenerationError(
                    "field names are not equal: "
                    f"{suspected_field_from_local_state.field_name} "
                    f"!= {suspected_field_from_database.field_name}.",
                )
            # If suspected field schemas are not equal -
            # field should be updated
            #
            # Here we are not looking for a name difference,
            # because all such fields are already either in
            # to_add_fields or to_drop_fields
            if (
                suspected_field_from_local_state
                != suspected_field_from_database
            ):
                # Generating a tuple like (from_field, to_field)
                to_alter_fields.add(
                    (
                        suspected_field_from_database,
                        suspected_field_from_local_state,
                    ),
                )

        migration_changes.append(
            MigrationChangesSchema(
                table=table_dump_from_local_state.table,
                to_add_fields=to_add_fields,
                to_alter_fields=to_alter_fields,
                to_drop_fields=to_drop_fields,
            ),
        )

    return migration_changes
