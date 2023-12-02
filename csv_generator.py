import csv
import gzip
import os.path
import constants

from tqdm import tqdm
from settings import Settings


class CsvGenerator:

    def __init__(self, logger, workspace, settings):
        self._logger = logger
        self._workspace = workspace

        csv.field_size_limit(settings.csv_field_size_limit)

    def build(self, raw_meta) -> dict:
        self._logger.detail(f"Start decompressing raw table files - {len(raw_meta)} files to decompress")

        pbar = tqdm(total=len(raw_meta)) if not self._logger.is_debug else None
        details = {}

        for table, meta in raw_meta.items():
            details[table] = self._decompress(table, meta, pbar)

        if pbar is not None:
            pbar.close()

        self._logger.detail(f"Completed decompressing RAW table files")
        return details

    def _decompress(self, table, meta, pbar) -> dict:
        workspace_file = os.path.abspath(f"{self._workspace.csv}/{table}.csv")
        self._logger.debug(f"Starting decompressing {table} into {workspace_file}")

        csv_details = {
            constants.csv_detail_headers: None,
            constants.csv_detail_file: workspace_file,
            constants.csv_detail_row_count: None
        }

        file_number = 0
        row_counter = 0
        for datafile in meta.downloaded_files:
            file_number += 1
            with gzip.open(datafile, 'rt', newline='', encoding='UTF-8') as compressed_file:
                csv_reader = csv.reader(compressed_file)
                if file_number >= 2:  # if not the first file - skip the header line as we are combining the files
                    self._logger.debug(f"Decompressing {table} file #{file_number}")
                    next(csv_reader)

                with open(workspace_file, 'a', encoding="UTF-8") as csv_file:
                    csv_writer = csv.writer(csv_file)
                    for row in csv_reader:
                        row_counter += 1
                        if row_counter == 1:
                            csv_details[constants.csv_detail_headers] = row or []

                        csv_writer.writerow(row)

                    csv_file.flush()
                    csv_file.close()

                compressed_file.close()

        if pbar is not None:
            pbar.update(1)

        row_counter -= 1 if csv_details[constants.csv_detail_headers] is not None else 0
        csv_details[constants.csv_detail_row_count] = row_counter
        self._logger.debug(f"Table {table} contains {Settings.readable_number(row_counter)} rows - including header row")
        self._logger.debug(f"Completed decompressing {table}")

        return csv_details
