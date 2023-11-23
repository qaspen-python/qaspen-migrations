import typing
from pathlib import Path

import click
import toml
from click import Context

from qaspen_migrations.migrations import MigrationsManager, ModelsManager
from qaspen_migrations.settings import QaspenMigrationsSettings
from qaspen_migrations.utils import (as_coroutine,
                                     convert_abs_path_to_relative, load_config)


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
    required=True,
)
@click.pass_context
def init(ctx: Context, migrations_path: str, engine_path: str) -> None:
    config_path = ctx.obj["config_path"]

    rel_migrations_path: typing.Final = convert_abs_path_to_relative(
        migrations_path,
    )
    engine_path, engine_object = engine_path.split(":")
    rel_engine_path: typing.Final = convert_abs_path_to_relative(engine_path)

    if config_path.exists():
        content: typing.Final = config_path.read_text()
        settings = toml.loads(content)
    else:
        settings = toml.loads("[tool.qaspen-migrations]")

    settings["tool"]["qaspen"]["migrations"] = QaspenMigrationsSettings(
        migrations_path=rel_migrations_path,
        engine_path=f"{rel_engine_path}:{engine_object}",
    ).model_dump()
    config_path.write_text(toml.dumps(settings))
    Path(rel_migrations_path).mkdir(parents=True, exist_ok=True)

    click.secho(
        f"Success create migrate location {migrations_path}",
        fg="green",
    )
    click.secho(f"Success write config to {config_path}", fg="green")


@cli.command(help="Make migrations for provided models.")
@click.pass_context
@as_coroutine
async def makemigrations(ctx: Context) -> None:
    migrations_config = ctx.obj["config"]
    assert isinstance(migrations_config, QaspenMigrationsSettings)

    migrations_manager: typing.Final = MigrationsManager(
        engine_path=migrations_config.engine_path,
        migrations_path=migrations_config.migrations_path,
        models_manager=ModelsManager(migrations_config.models),
    )
    await migrations_manager.make_migrations()


@cli.command(help="Apply migrations.")
@click.pass_context
def migrate(ctx: Context) -> None:
    migrations_config = ctx.obj["config"]
    assert isinstance(migrations_config, QaspenMigrationsSettings)

    print("Migrate!")


@cli.command(help="Rollback migrations to certain version.")
@click.argument(
    "to-version",
    nargs=1,
)
@click.pass_context
def rollback(ctx: Context) -> None:
    migrations_config = ctx.obj["config"]
    assert isinstance(migrations_config, QaspenMigrationsSettings)

    print("Rollback!")
