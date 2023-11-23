import dataclasses
import typing

from qaspen import BaseTable
from qaspen.abc.db_engine import BaseEngine

from qaspen_migrations.exceptions import ConfigurationError
from qaspen_migrations.inspector.base import BaseInspector
from qaspen_migrations.inspector.mapping import map_inspector
from qaspen_migrations.models import QaspenMigration
from qaspen_migrations.utils import import_module_by_path


@dataclasses.dataclass()
class ModelsManager:
    model_paths: typing.List[str]
    __models: typing.List[typing.Type[BaseTable]] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    def __post_init__(self) -> None:
        self.__load_models()

    def __load_models_from_file(
        self,
        models_file_path: str,
    ) -> None:
        models_module: typing.Final = import_module_by_path(models_file_path)
        for module_member in dir(models_module):
            models_module_attribute = getattr(
                models_module,
                module_member,
            )
            try:
                if (
                    issubclass(
                        models_module_attribute,
                        BaseTable,
                    )
                    and not models_module_attribute._table_meta.abstract
                ):
                    self.__models.append(models_module_attribute)
            except TypeError:
                continue

    def __load_models(self) -> None:
        for model_path in self.model_paths:
            self.__load_models_from_file(model_path)
        self.__models.append(QaspenMigration)

    @property
    def models(self) -> typing.List[typing.Type[BaseTable]]:
        return self.__models


@dataclasses.dataclass
class MigrationsManager:
    engine_path: str
    migrations_path: str
    models_manager: ModelsManager
    __inspector: BaseInspector = dataclasses.field(init=False)
    __engine: BaseEngine = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.__engine = self.__load_engine()
        self.__inspector = map_inspector(self.__engine)

    def __load_engine(self) -> BaseEngine:
        engine_path, engine_object = self.engine_path.split(":")
        engine_module: typing.Final = import_module_by_path(engine_path)

        try:
            engine: typing.Final = getattr(engine_module, engine_object)
        except AttributeError as exc:
            raise ConfigurationError("No engine object found.") from exc
        if not issubclass(type(engine), BaseEngine):
            raise ConfigurationError("No engine object found.")

        return engine

    async def make_migrations(self) -> None:
        await self.__inspector.inspect_database(self.models_manager.models)
