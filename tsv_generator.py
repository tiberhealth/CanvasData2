import csv
import gzip
import io
import os.path
import constants

from tqdm import tqdm
from settings import Settings


class TsvGenerator:

    def __init__(self, logger, workspace, settings):
        self._logger = logger
        self._workspace = workspace

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
        workspace_file = os.path.abspath(f"{self._workspace.tsv}/{table}.tsv")
        self._logger.debug(f"Starting decompressing {table} into {workspace_file}")

        tsv_details = {
            constants.tsv_detail_headers: None,
            constants.tsv_detail_file: workspace_file,
            constants.tsv_detail_row_count: None
        }

        file_number = 0
        row_counter = 0
        with open(workspace_file, 'w', encoding="UTF-8") as tsv_file:
            for datafile in meta.downloaded_files:
                file_number += 1
                with gzip.open(datafile, "rt", encoding="UTF-8") as compressed_file:
                    if file_number >= 2:
                        self._logger.debug(f"Decompressing {table} file #{file_number}")
                        compressed_file.readline()

                    for line in compressed_file:
                        row_counter += 1
                        tsv_file.write(line)
                        if row_counter == 1:
                            tsv_details[constants.tsv_detail_headers] = self.process_headers(line)

                    compressed_file.close()

            tsv_file.flush()
            tsv_file.close()

        if pbar is not None:
            pbar.update(1)

        row_counter -= 1 if tsv_details[constants.tsv_detail_headers] is not None else 0
        tsv_details[constants.tsv_detail_row_count] = row_counter
        self._logger.debug(f"Table {table} contains {Settings.readable_number(row_counter)} rows - including header row")
        self._logger.debug(f"Completed decompressing {table}")

        return tsv_details

    @staticmethod
    def process_headers(line) -> list:
        csv_reader = csv.reader(io.StringIO(line.replace("\t", ",")))
        results = [row for row in csv_reader]

        return results[0] if results is not None and len(results) > 0 else []
