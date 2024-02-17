from __future__ import annotations

import pydantic
from qaspen import BaseTable  # noqa: TCH002

from qaspen_migrations.schema.column_info import (
    ColumnInfoSchema,  # noqa: TCH001
)


class MigrationChangesSchema(pydantic.BaseModel):
    table: type[BaseTable]
    to_create_columns: set[ColumnInfoSchema] = pydantic.Field(
        default_factory=set,
    )
    to_update_columns: set[
        tuple[ColumnInfoSchema, ColumnInfoSchema]
    ] = pydantic.Field(
        default_factory=set,
    )
    to_delete_columns: set[ColumnInfoSchema] = pydantic.Field(
        default_factory=set,
    )

    @property
    def should_create_table(self) -> bool:
        return (
            bool(self.to_create_columns)
            and not bool(self.to_update_columns)
            and not bool(self.to_delete_columns)
        )

    @property
    def should_skip_table(self) -> bool:
        return (
            not bool(self.to_create_columns)
            and not bool(self.to_update_columns)
            and not bool(self.to_delete_columns)
        )

    @property
    def should_drop_table(self) -> bool:
        return (
            not bool(self.to_create_columns)
            and not bool(self.to_update_columns)
            and bool(self.to_delete_columns)
        )
