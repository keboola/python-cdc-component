import glob
import logging
import os
from csv import DictReader
from typing import Protocol, Callable

from db_components.db_common.table_schema import TableSchema
from workspace_client import SnowflakeClient


def _get_csv_header(file_path: str) -> list[str]:
    with open(file_path) as inp:
        reader = DictReader(inp, lineterminator='\n', delimiter=',', quotechar='"')
        return list(reader.fieldnames)


class Staging(Protocol):
    convert_column_types: Callable
    normalize_columns: Callable

    def process_table(self, table_path: str, result_table_name: str, schema: TableSchema, dedupe_required: bool):
        """
        Processes the table and uploads it to the staging area
        Args:
            table_path: path to the table
            result_table_name: name of the table in the staging area
            schema: schema of the table
            dedupe_required: if dedupe is required
        """


class SnowflakeStaging(Staging):
    def __init__(self, workspace_credentials: dict, column_type_convertor: Callable):
        snfwlk_credentials = {
            "account": workspace_credentials['host'].replace('.snowflakecomputing.com', ''),
            "user": workspace_credentials['user'],
            "password": workspace_credentials['password'],
            "database": workspace_credentials['database'],
            "schema": workspace_credentials['schema'],
            "warehouse": workspace_credentials['warehouse']
        }
        self.convert_column_types = column_type_convertor
        self._snowflake_client = SnowflakeClient(**snfwlk_credentials)

    def process_table(self, table_path, result_table_name, schema: TableSchema, dedupe_required: bool):
        logging.info(f"Creating table {result_table_name} in stage")
        column_types = self._convert_to_snowflake_column_definitions(schema.fields)
        self._snowflake_client.create_table(result_table_name, column_types)

        logging.info(f"Uploading data into table {result_table_name} in stage")
        # chunks if multiple schema changes during execution
        tables = glob.glob(os.path.join(table_path, '*.csv'))
        for table in tables:
            csv_columns = _get_csv_header(table)
            csv_columns = self.normalize_columns(csv_columns)
            self._snowflake_client.copy_csv_into_table_from_file(result_table_name, csv_columns, table)

        # dedupe only if running sync from binlog and not in append_incremental mode
        if dedupe_required():
            self._dedupe_stage_table(table_name=result_table_name, id_columns=schema.primary_keys)


class DuckDBStaging(Protocol):
    def __init__(self):
        pass

    def process_table(self, table_path, result_table_name, schema, dedupe_required):
        logging.info(f"Creating table {result_table_name} in stage")
        self._snowflake_client.create_table(result_table_name, schema.column_types)

        logging.info(f"Uploading data into table {result_table_name} in stage")
        # chunks if multiple schema changes during execution
        tables = glob.glob(os.path.join(table_path, '*.csv'))
        for table in tables:
            csv_columns = get_csv_header(table)
            csv_columns = normalize_columns(csv_columns)
            snowflake_client.copy_csv_into_table_from_file(result_table_name, csv_columns, table)

        # dedupe only if running sync from binlog and not in append_incremental mode
        if dedupe_required():
            self._dedupe_stage_table(table_name=self._result_table_name, id_columns=self._schema.primary_keys)
