import asyncio
import typer

from typer import Typer
from typing_extensions import Annotated
from datetime import datetime

from cd2datamanager.logger import Logger, LogLevel
from cd2datamanager import constants
from cd2datamanager.tsv_generator import TsvGenerator
from cd2datamanager.dap_client import DapClient
from cd2datamanager.settings import Settings
from cd2datamanager.workspace import Workspace
from cd2datamanager.schema_writer import SchemaWriter


app = Typer()


def display_title(logger):
    logger.force(f"   __________ ___      ____        __           __  ___                                 ")
    logger.force(f"  / ____/ __ \\__ \\    / __ \\____ _/ /_____ _   /  |/  /___ _____  ____ _____ ____  _____")
    logger.force(f" / /   / / / /_/ /   / / / / __ `/ __/ __ `/  / /|_/ / __ `/ __ \\/ __ `/ __ `/ _ \\/ ___/")
    logger.force(f"/ /___/ /_/ / __/   / /_/ / /_/ / /_/ /_/ /  / /  / / /_/ / / / / /_/ / /_/ /  __/ /    ")
    logger.force(f"\\____/_____/____/  /_____/\\__,_/\\__/\\__,_/  /_/  /_/\\__,_/_/ /_/\\__,_/\\__, /\\___/_/     ")
    logger.force(f"                                                                     /____/             ")

    logger.force(f" ")
    logger.force(f"(c){datetime.utcnow().year:04d} - Tiber Health Public Benefits Company / Tiber Health Innovations ")
    logger.force(f" ")

@app.command()
def extract(
        concurrent_limit: Annotated[int, typer.Option(help="Max concurrent download threads")] = constants.default_concurrent_limit,
        max_download_attempts: Annotated[int, typer.Option(help="Number of time to attempt to download a table if an error occurs before aborting or skipping table")] = constants.default_max_download_attempts,
        semaphore_timeout_seconds: Annotated[int, typer.Option(help="Number of seconds to wait for a semaphore lock before aborting or trying again")] = constants.default_semaphore_timeout_seconds,
        max_lock_attempts: Annotated[int, typer.Option(help="Number of attempts allowed for grabbing a semaphore lock before throwing error")] = constants.default_max_lock_attempts,
        sleep_between_attempts_seconds: Annotated[int, typer.Option(help="Number of seconds to pause when a thread receives and error before attempting again")] = constants.default_sleep_between_attempts_seconds,
        thread_pause: Annotated[int, typer.Option(help="Number of seconds to pause between download thread starts.")] = constants.default_thread_pause,
        log_level: Annotated[str, typer.Option(help=f"Logging detail level. Values: {', '.join([log_level.name for log_level in LogLevel])}")] = constants.default_log_level.name,
        schema_only: Annotated[bool, typer.Option(help="Flag to only include the SQL schema scripts. No data download")] = False,
        no_schema: Annotated[bool, typer.Option(help="Flag to not generate the SQL Schema scripts")] = False,
        include_sql_load: Annotated[bool, typer.Option(help="Flag as to include the SQL Load statements in the SQL scripts")] = True,
        import_warnings: Annotated[bool, typer.Option(help="Flag indicating if warnings and errors should display after imports")] = False,
        workspace_root: Annotated[str, typer.Option(help="Root location for generated files")] = constants.default_root_workspace,
        tsv_workspace: Annotated[str, typer.Option(help="Location for the table TSV files. Overrides the default location under --workspace_root")] = None,
        sql_workspace: Annotated[str, typer.Option(help="Location for the table SQL and load script. Overrides the default location under --workspace_root")] = None,
        raw_workspace: Annotated[str, typer.Option(help="Location for the RAW data files. Overrides the default location under --workspace_root")] = None,
        env_locale: Annotated[str, typer.Option(help="he locale setting for output strings and numbers")] = "en_US.UTF-8",
        tables: Annotated[str, typer.Option(help="Tables to download from CD2. Semi-colon or comma seperated")] = None,
        excluded_tables: Annotated[str, typer.Option(help="List of tables to exclude from the CD2 download. Semi-colon or comma separated")] = None,
        dap_yaml: Annotated[str, typer.Option(help="Location of the YAML with the Canvas instance credentials")] = constants.default_dap_yaml,
        settings_yaml: Annotated[str, typer.Option(help="Location of settings YAML File. YAML file override all switch options and defaults")] = constants.default_settings_yaml
    ):
    settings = Settings(
        concurrent_limit=concurrent_limit,
        max_download_attempts=max_download_attempts,
        semaphore_timeout_seconds=semaphore_timeout_seconds,
        max_lock_attempts=max_lock_attempts,
        sleep_between_attempts_seconds=sleep_between_attempts_seconds,
        thread_pause=thread_pause,
        log_level=log_level,
        schema_only=schema_only,
        no_schema=no_schema,
        include_sql_load=include_sql_load,
        import_warnings=import_warnings,
        workspace_root=workspace_root,
        tsv_workspace=tsv_workspace,
        sql_workspace=sql_workspace,
        raw_workspace=raw_workspace,
        env_locale=env_locale,
        tables=tables,
        excluded_tables=excluded_tables,
        dap_yaml_file=dap_yaml,
        settings_yaml_file = settings_yaml
    )

    asyncio.run(process(settings))


async def process(settings):
    logger = Logger(settings.log_level)

    workspace = Workspace(logger, settings)
    workspace.initialize()

    client = DapClient(logger, workspace, settings)
    meta = await client.get_tables()

    tsv_headers = None
    if not settings.schema_only:
        if ('files' in meta) and (len(meta['files']) > 0):
            tsv_details = TsvGenerator(logger, workspace, settings).build(meta['files'])

    if not settings.no_schema:
        if ('schema' in meta) and (len(meta['schema']) > 0):
            (SchemaWriter(logger, workspace, settings).write(meta['schema'], tsv_details))


def entry_point():
    display_title(Logger(LogLevel.WARNING))
    app()


if __name__ == '__main__':
    entry_point()

