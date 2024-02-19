from __future__ import annotations
import dataclasses
import typing

from qaspen import columns
from qaspen.table.base_table import BaseTable  # noqa: TCH002

from qaspen_migrations.utils.column_repr import to_column_repr


if typing.TYPE_CHECKING:
    from qaspen.columns.base import Column


@dataclasses.dataclass(slots=True, frozen=True)
class ColumnInfo:
    main_column_type: type[Column[typing.Any]]
    inner_column_type: type[Column[typing.Any]] | None
    db_column_name: str
    is_null: bool
    database_default: str | None
    max_length: int | None
    precision: int | None
    scale: int | None

    def to_dict(
        self,
        include: set[str] | None = None,
    ) -> dict[str, typing.Any]:
        if include is None:
            include = set()
        return {
            field.name: getattr(self, field.name)
            for field in dataclasses.fields(self)
            if field.name in include and getattr(self, field.name) is not None
        }

    def __hash__(self) -> int:
        return hash(tuple(self.to_dict()))

    def to_table_column_repr(self) -> str:
        if self.is_array:
            assert self.inner_column_type is not None
            return to_column_repr(
                self.main_column_type,
                db_column_name=self.db_column_name,
                is_null=self.is_null,
                database_default=self.database_default,
                inner_column=to_column_repr(
                    self.inner_column_type,
                    precision=self.precision,
                    scale=self.scale,
                    max_length=self.max_length,
                ),
            )

        return to_column_repr(
            self.main_column_type,
            db_column_name=self.db_column_name,
            is_null=self.is_null,
            database_default=self.database_default,
            precision=self.precision,
            scale=self.scale,
            max_length=self.max_length,
        )

    @property
    def is_array(self) -> bool:
        return issubclass(self.main_column_type, columns.ArrayColumn)


@dataclasses.dataclass(slots=True, frozen=True)
class TableDump:
    table: type[BaseTable]
    table_columns: set[ColumnInfo] = dataclasses.field(default_factory=set)

    def add_column_info(
        self,
        column_info: ColumnInfo,
    ) -> None:
        self.table_columns.add(column_info)

    def all_column_names(self) -> set[str]:
        return {
            table_column.db_column_name for table_column in self.table_columns
        }


@dataclasses.dataclass(slots=True, frozen=True)
class TableDiff:
    table: type[BaseTable]
    to_add_columns: set[ColumnInfo] = dataclasses.field(
        default_factory=set,
    )
    to_alter_columns: set[tuple[ColumnInfo, ColumnInfo]] = dataclasses.field(
        default_factory=set,
    )
    to_drop_columns: set[ColumnInfo] = dataclasses.field(
        default_factory=set,
    )

    @property
    def should_create_table(self) -> bool:
        return (
            bool(self.to_add_columns)
            and not bool(self.to_alter_columns)
            and not bool(self.to_drop_columns)
        )

    @property
    def should_skip_table(self) -> bool:
        return (
            not bool(self.to_add_columns)
            and not bool(self.to_alter_columns)
            and not bool(self.to_drop_columns)
        )

    @property
    def should_drop_table(self) -> bool:
        return (
            not bool(self.to_add_columns)
            and not bool(self.to_alter_columns)
            and bool(self.to_drop_columns)
        )
