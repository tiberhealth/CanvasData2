from datetime import datetime
from enum import Enum


class LogLevel (Enum):
    DEBUG = 1
    DETAIL = 2
    WARNING = 3
    ERROR = 4
    LOG_SYSTEM = 99


class Logger:

    @staticmethod
    def builder(logger_or_level):
        if isinstance(logger_or_level, Logger):
            return logger_or_level
        else:
            return Logger(logger_or_level)

    def __init__(self, level):
        self._level = level

    @property
    def level(self) -> LogLevel:
        return self._level

    @property
    def is_debug(self) -> bool:
        return self.level.value <= LogLevel.DEBUG.value

    def log(self, message,  log_level = LogLevel.DETAIL):
        if log_level.value < self._level.value:
            return

        display_level = log_level.name
        if log_level == LogLevel.LOG_SYSTEM:
            display_level = " "

        current_time_string = datetime.utcnow().strftime("%m/%d/%Y %I:%M:%S %p Zulu")
        if len(message.strip()) <= 0 and log_level == LogLevel.LOG_SYSTEM:
            current_time_string = " "

        print(f"{current_time_string:<22}\t{display_level:<10}\t{message}")

    def force(self, message):
        self.log(message, LogLevel.LOG_SYSTEM)

    def detail(self, message):
        self.log(message, LogLevel.DETAIL)

    def debug(self, message):
        self.log(message, LogLevel.DEBUG)

    def warning(self, message):
        self.log(message, LogLevel.WARNING)

    def error(self, message):
        self.log(message, LogLevel.ERROR)
