import locale
import os.path
import yaml

import cd2datamanager.constants as constants
from cd2datamanager.logger import LogLevel


class Settings:

    def __init__(self,
                 concurrent_limit=constants.default_concurrent_limit,
                 max_download_attempts=constants.default_max_download_attempts,
                 semaphore_timeout_seconds=constants.default_semaphore_timeout_seconds,
                 max_lock_attempts= constants.default_max_lock_attempts,
                 sleep_between_attempts_seconds = constants.default_sleep_between_attempts_seconds,
                 thread_pause = constants.default_thread_pause,
                 log_level=constants.default_log_level.name,
                 schema_only = False,
                 no_schema = False,
                 include_sql_load = True,
                 import_warnings = False,
                 workspace_root = constants.default_root_workspace,
                 tsv_workspace = None,
                 sql_workspace = None,
                 raw_workspace = None,
                 env_locale = "en_US.UTF-8",
                 tables = None,
                 excluded_tables = None,
                 dap_yaml_file=constants.default_dap_yaml,
                 settings_yaml_file=constants.default_settings_yaml):

        self.concurrent_limit = concurrent_limit
        self.max_download_attempts = max_download_attempts
        self.semaphore_timeout_seconds = semaphore_timeout_seconds
        self.max_lock_attempts = max_lock_attempts
        self.sleep_between_attempts_seconds = sleep_between_attempts_seconds
        self.thread_pause = thread_pause
        self.schema_only = schema_only
        self.include_sql_load = include_sql_load
        self.import_warnings = import_warnings
        self.no_schema = no_schema
        self.env_locale = env_locale
        self.tables = tables
        self.excluded_tables = excluded_tables
        self.dap_yaml_file = dap_yaml_file

        self.workspace_root = workspace_root
        self.tsv_workspace = tsv_workspace
        self.sql_workspace = sql_workspace
        self.raw_workspace = raw_workspace

        self.set_log_level(log_level)

        self._load_yaml(settings_yaml_file)

        locale.setlocale(locale.LC_ALL, self.env_locale)

    def _load_yaml(self, yaml_path):
        if not os.path.exists(yaml_path):
            return

        with open(yaml_path, 'r') as yaml_file:
            config = yaml.safe_load(yaml_file)

            if config is None:
                return

            self.concurrent_limit = config.get("concurrent_limit", self.concurrent_limit)
            self.max_download_attempts = config.get("max_download_attempts", self.max_download_attempts)
            self.semaphore_timeout_seconds = config.get("semaphore_timeout_seconds", self.semaphore_timeout_seconds)
            self.max_lock_attempts = config.get("max_lock_attempts", self.max_lock_attempts)
            self.sleep_between_attempts_seconds = config.get("sleep_between_attempts_seconds", self.sleep_between_attempts_seconds)
            self.thread_pause = config.get("thread_pause", self.thread_pause)
            self.schema_only = config.get("schema_only", self.schema_only)
            self.schema_only = config.get("no_schema", self.schema_only)
            self.include_sql_load = config.get("include_sql_load", self.include_sql_load)
            self.import_warnings = config.get("import_warnings", self.import_warnings)
            self.env_locale = config.get("env_locale", self.env_locale)
            self.tables = config.get("tables", self.tables)
            self.excluded_tables = config.get("excluded_tables", self.excluded_tables)

            self.dap_yaml_file = config.get("dap_yaml_file", self.dap_yaml_file)

            self.workspace_root = config.get("workspace_root", self.workspace_root)
            self.tsv_workspace = config.get("csv_workspace", self.tsv_workspace)
            self.sql_workspace = config.get("sql_workspace", self.sql_workspace)
            self.raw_workspace = config.get("raw_workspace", self.raw_workspace)

            if "log_level" in config:
                log_level_str = config.get("log_level")
                self.set_log_level(log_level_str)

    def set_log_level(self, log_level_str):
        if log_level_str:
            try:
                self.log_level = LogLevel[log_level_str.upper()]
            except KeyError:
                print(f"Invalid log level '{log_level_str}' in YAML, using default.")

    @staticmethod
    def readable_number(number) -> str:
        return locale.format_string("%d", number, grouping=True)
