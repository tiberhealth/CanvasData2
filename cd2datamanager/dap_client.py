import asyncio
import yaml

import cd2datamanager.constants as constants

from tqdm import tqdm
from dap.api import DAPClient, DAPSession
from dap.dap_types import Credentials, Format, SnapshotQuery, Mode

from cd2datamanager.semaphore_control import SemaphoreControl
from cd2datamanager.schema_generator import SchemaGenerator


class DapClient:

    def __init__(self, logger, workspace, settings):
        self.url = constants.default_api_url
        self.namespace = constants.default_namespace

        self._logger = logger
        self._settings = settings
        self._workspace = workspace

        self.client_id = None
        self.client_secret = None

        self._load_yaml(settings.dap_yaml_file)

    @property
    def credentials(self) -> Credentials:
        return Credentials.create(client_id=self.client_id, client_secret=self.client_secret)

    async def table_list(self, session) -> list[str]:
        self._logger.debug("Starting: Retrieving table list")
        table_list =  await session.get_tables(self.namespace) or []

        if self._settings.tables is not None:
            self._logger.detail(f"Table filter is set - only pulling the following tables:")
            table_filter = self._settings.tables.replace(',', ';').split(';')
            table_list = [table for table in table_list if table in table_filter]
            [self._logger.detail(f"{' '.ljust(6, ' ')}{table_name}") for table_name in table_list]

        if self._settings.excluded_tables is not None:
            self._logger.detail(f"Excluded table filter set - skipping the following tables:")
            table_filter = self._settings.excluded_tables.replace(',', ';').split(';')
            exclude_table_list = [table for table in table_list if table in table_filter]
            [self._logger.detail(f"{' '.ljust(6, ' ')}{table_name}") for table_name in exclude_table_list]
            table_list = [table for table in table_list if table not in exclude_table_list]

        self._logger.detail(f"Retrieving {self._settings.readable_number(len(table_list))} tables from Canvas Data 2")

        return table_list

    def _load_yaml(self, yaml_path):
        with open(yaml_path, 'r') as yaml_file:
            config = yaml.safe_load(yaml_file)

            if config is None:
                return

            self.client_id = config["client_id"] # Want error if this is missing
            self.client_secret = config["client_secret"] # Want error if this is missing
            self.url = config.get("root_url", self.url)
            self.namespace = config.get("namespace", self.namespace)

    def connect(self) -> DAPSession:
        return DAPClient(self.url, self.credentials)

    async def get_tables(self) -> dict:
        self._logger.detail("Start tables downloaded")
        job_table = dict()
        table_schema = dict()

        semaphore = SemaphoreControl(self._settings, self._logger)

        async with self.connect() as session:
            tables = await self.table_list(session)
            pbar = tqdm(total=len(tables)) if not self._logger.is_debug else None

            async with asyncio.TaskGroup() as tg:
                [await self.build_task(tg, session, semaphore, table, pbar, job_table, table_schema) for table in tables]

            if pbar is not None:
                pbar.close()

            self._logger.detail("Completed tables downloaded")

        return {
            'schema': table_schema,
            'files': job_table
        }

    async def build_task(self, tg, session, semaphore, table_name, pbar, job_table, schema):
        attempt = 0
        while True:
            try:
                await asyncio.wait_for(semaphore.acquire(), timeout=self._settings.semaphore_timeout_seconds)
                self._logger.debug(f"Semaphore lock obtained for table {table_name} - attempt {attempt + 1}")

                if not self._settings.no_schema:
                    schema[table_name] = await SchemaGenerator(self._logger, self.namespace, self._settings, table_name).initialize(session)

                if not self._settings.schema_only:
                    tg.create_task(self.download_table(session, semaphore, table_name, pbar, job_table))
                    await asyncio.sleep(self._settings.thread_pause)
                else:
                    await semaphore.release() # Release semaphore since we are not downloading the data
                    if pbar is not None:
                        pbar.update(1)

                break

            except asyncio.TimeoutError:
                attempt += 1
                if attempt >= self._settings.max_lock_attempts:
                    self._logger.error(f"Timed out waiting for semaphore lock for table {table_name} - semaphore count {semaphore.active_semaphores}")
                    if pbar is not None:
                        pbar.update(1)
                    break

                else:
                    self._logger.debug(f"Unable to obtain semaphore lock for {table_name} - attempt {attempt}")
                    await asyncio.sleep(self._settings.sleep_between_attempts_seconds)

    async def download_table(self, session, semaphore, table_name, pbar, job_table):
        self._logger.debug(f"Start downloading table {table_name}")
        attempt = 0

        while True:
            try:
                asset = await session.download_table_data(
                    self.namespace,
                    table_name,
                    SnapshotQuery(format=Format.TSV, filter=None, mode=Mode.condensed),
                    self._workspace.raw)

                self._logger.debug(f"Table {table_name} downloaded - attempt {attempt + 1}")
                async with asyncio.Lock():
                    try:
                        job_table[table_name] = asset
                    except Exception as e:
                        self._logger.error(f"Failed to load table {table_name} meta data - {e}")

                break

            except Exception as e:
                attempt += 1
                if attempt >= self._settings.max_download_attempts:
                    self._logger.error(f"Error: Unable to download table {table_name} [Attempts {attempt}]\nError:\n{e}")
                    break

                else:
                    self._logger.debug(f"Failed to download table {table_name} attempt {attempt} -> {e}")
                    await asyncio.sleep(self._settings.sleep_between_attempts_seconds)

        await semaphore.release()
        if pbar is not None:
            pbar.update(1)

        self._logger.debug(f"Completed downloading table {table_name}")
