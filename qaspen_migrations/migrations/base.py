from __future__ import annotations
import abc
import typing


if typing.TYPE_CHECKING:
    from qaspen_migrations.ddl.base import BaseDDLElement
    from qaspen_migrations.operations.base import BaseOperationsImplementer


class BaseMigration(abc.ABC):
    current_version: str
    previous_version: str | None
    created_datetime: str

    def __init__(self, engine_type: str) -> None:
        self.engine_type = engine_type
        self.operations: BaseOperationsImplementer = None  # type: ignore[type-arg, assignment]

    @abc.abstractmethod
    def migrate(self) -> list[BaseDDLElement]:
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self) -> list[BaseDDLElement]:
        raise NotImplementedError
