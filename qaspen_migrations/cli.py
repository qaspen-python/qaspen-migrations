from __future__ import annotations
import collections
import typing
from pathlib import Path

import click
import toml
from click import Context

from qaspen_migrations.migrations.applyer import MigrationsApplyer
from qaspen_migrations.migrations.maker import MigrationMaker
from qaspen_migrations.migrations.versioner import MigrationsVersioner
from qaspen_migrations.settings import (
    QASPEN_MIGRATIONS_TOML_KEY,
    QaspenMigrationsSettings,
)
from qaspen_migrations.utils.common import (
    as_coroutine,
    convert_abs_path_to_relative,
)
from qaspen_migrations.utils.loaders import (
    MigrationLoader,
    TableLoader,
    load_config,
    load_engine,
)


@click.group()
@click.option(
    "-c",
    "--config",
    default="pyproject.toml",
    show_default=True,
    help="Config file.",
)
@click.pass_context
def cli(ctx: Context, config: str) -> None:
    ctx.ensure_object(dict)
    config_path = Path(config)
    if ctx.invoked_subcommand == "init":
        ctx.obj["config_path"] = config_path
    else:
        ctx.obj["config"] = load_config(config_path)


@cli.command(help="Initialize config file and generate migrations directory.")
@click.option(
    "-m",
    "--migrations-path",
    default="./migrations",
    show_default=True,
    help="Migrations location.",
)
@click.option(
    "-e",
    "--engine-path",
    show_default=False,
    help="Path to your qaspen engine.",
    required=False,
)
@click.pass_context
def init(
    ctx: Context,
    migrations_path: str,
    engine_path: str | None,
) -> None:
    config_path = ctx.obj["config_path"]
    rel_migrations_path: typing.Final = convert_abs_path_to_relative(
        migrations_path,
    )

    if engine_path is not None:
        engine_path, engine_object = engine_path.split(":")
        rel_engine_path: typing.Final = convert_abs_path_to_relative(
            engine_path,
        )
        config_engine_path = f"{rel_engine_path}:{engine_object}"
    else:
        config_engine_path = "path.to.your.engine:object"

    if config_path.exists():
        content: typing.Final = config_path.read_text()
        settings = toml.loads(content)
    else:
        settings = toml.loads(f"[tool.{QASPEN_MIGRATIONS_TOML_KEY}]")

    settings = collections.defaultdict(dict, **settings)

    settings["tool"][QASPEN_MIGRATIONS_TOML_KEY] = QaspenMigrationsSettings(
        migrations_path=rel_migrations_path,
        engine_path=config_engine_path,
    ).to_dict()
    config_path.write_text(toml.dumps(settings))
    Path(rel_migrations_path).mkdir(parents=True, exist_ok=True)

    click.secho(
        f"Successfully created migrations location {migrations_path}",
        fg="green",
    )
    click.secho(f"Successful wrote a config to {config_path}", fg="green")


@cli.command(help="Make migrations for provided tables.")
@click.pass_context
@as_coroutine
async def makemigrations(ctx: Context) -> None:
    migrations_config = ctx.obj["config"]
    assert isinstance(migrations_config, QaspenMigrationsSettings)

    await MigrationMaker(
        engine=load_engine(migrations_config.engine_path),
        migrations_path=migrations_config.migrations_path,
        tables=TableLoader(migrations_config.tables).load_tables(),
    ).make_migrations()


@cli.command(help="Apply migrations.")
@click.pass_context
@as_coroutine
async def migrate(ctx: Context) -> None:
    migrations_config = ctx.obj["config"]
    assert isinstance(migrations_config, QaspenMigrationsSettings)

    await MigrationsApplyer(
        engine=load_engine(migrations_config.engine_path),
        migrations_versioner=MigrationsVersioner(
            MigrationLoader(
                load_engine(migrations_config.engine_path).engine_type,
                migrations_config.migrations_path,
            ),
        ),
    ).apply_changes()


@cli.command(help="Rollback migrations to certain version.")
@click.argument(
    "to-version",
    nargs=1,
)
@click.pass_context
def rollback(ctx: Context) -> None:
    migrations_config = ctx.obj["config"]
    assert isinstance(migrations_config, QaspenMigrationsSettings)

    print("Rollback!")  # noqa: T201
