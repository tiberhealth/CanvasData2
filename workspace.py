import os
import shutil


class Workspace:

    def __init__(self, logger, settings, root_path="./workspace", raw_path=None, csv_path=None, sql_path=None):

        self._root_path = settings.workspace_root or root_path
        self._raw_path = settings.raw_workspace or raw_path
        self._csv_path = settings.csv_workspace or csv_path
        self._sql_path = settings.sql_workspace or sql_path

        self._logger = logger

    @property
    def raw(self) -> str:
        return self._raw_path or f"{self._root_path}/raw"

    @property
    def csv(self) -> str:
        return self._csv_path or f"{self._root_path}/csv"

    @property
    def sql(self) -> str:
        return self._sql_path or f"{self._root_path}/sql"

    def initialize(self):
        self.clear_workspace(self.raw)
        self.clear_workspace(self.csv)
        self.clear_workspace(self.sql)

    def clear_workspace(self, workspace_directory, create_new = True) -> str:
        if os.path.exists(workspace_directory):
            self._logger.debug(f"Clearing workspace directory {workspace_directory}")
            shutil.rmtree(workspace_directory)

        if create_new:
            self._logger.debug(f"Creating workspace directory {workspace_directory}")
            os.makedirs(workspace_directory, exist_ok=True)

        return workspace_directory
