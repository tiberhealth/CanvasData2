import os
import shutil


class Workspace:

    def __init__(self, logger, settings, root_path="./workspace", raw_path=None, tsv_path=None, sql_path=None):

        self._root_path = settings.workspace_root or root_path
        self._raw_path = settings.raw_workspace or raw_path
        self._tsv_path = settings.tsv_workspace or tsv_path
        self._sql_path = settings.sql_workspace or sql_path

        self._logger = logger

    @property
    def raw(self) -> str:
        return self._raw_path or f"{self._root_path}/raw"

    @property
    def tsv(self) -> str:
        return self._tsv_path or f"{self._root_path}/tsv"

    @property
    def sql(self) -> str:
        return self._sql_path or f"{self._root_path}/sql_loaders"

    def initialize(self):
        self.clear_workspace(self.raw)
        self.clear_workspace(self.tsv)
        self.clear_workspace(self.sql)

    def clear_workspace(self, workspace_directory, create_new = True) -> str:
        if os.path.exists(workspace_directory):
            self._logger.debug(f"Clearing workspace directory {workspace_directory}")
            shutil.rmtree(workspace_directory)

        if create_new:
            self._logger.debug(f"Creating workspace directory {workspace_directory}")
            os.makedirs(workspace_directory, exist_ok=True)

        return workspace_directory
