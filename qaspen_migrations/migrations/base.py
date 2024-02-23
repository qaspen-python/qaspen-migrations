from __future__ import annotations
import abc
import typing

from qaspen_migrations.operations.mapping import map_operations_implementer


if typing.TYPE_CHECKING:
    from qaspen_migrations.ddl.base import BaseDDLElement
    from qaspen_migrations.operations.base import BaseOperationsImplementer


class BaseMigration(abc.ABC):
    version: str
    previous_version: str | None
    created_datetime: str

    def __init__(self, engine_type: str) -> None:
        self.operations: BaseOperationsImplementer[
            typing.Any,
            typing.Any,
            typing.Any,
            typing.Any,
            typing.Any,
        ] = map_operations_implementer(engine_type)

    @abc.abstractmethod
    def migrate(self) -> list[BaseDDLElement]:
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self) -> list[BaseDDLElement]:
        raise NotImplementedError
