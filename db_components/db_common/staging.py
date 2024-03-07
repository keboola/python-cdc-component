import glob
import logging
import os
from csv import DictReader
from typing import Protocol, Callable

from db_components.db_common.table_schema import TableSchema
from db_components.db_common.workspace_client import SnowflakeClient


def _get_csv_header(file_path: str) -> list[str]:
    with open(file_path) as inp:
        reader = DictReader(inp, lineterminator='\n', delimiter=',', quotechar='"')
        return list(reader.fieldnames)


class Staging(Protocol):
    convert_column_types: Callable
    normalize_columns: Callable
    multi_threading_support: bool

    def process_table(self, table_path: str, result_table_name: str, schema: TableSchema, dedupe_required: bool):
        """
        Processes the table and uploads it to the staging area
        Args:
            table_path: path to the table (folder) with csv files
            result_table_name: name of the table in the staging area
            schema: schema of the table
            dedupe_required: if dedupe is required
        """


class SnowflakeStaging(Staging):
    def __init__(self, workspace_credentials: dict, column_type_convertor: Callable, convert_column_names: Callable):
        snfwlk_credentials = {
            "account": workspace_credentials['host'].replace('.snowflakecomputing.com', ''),
            "user": workspace_credentials['user'],
            "password": workspace_credentials['password'],
            "database": workspace_credentials['database'],
            "schema": workspace_credentials['schema'],
            "warehouse": workspace_credentials['warehouse']
        }
        self.convert_column_types = column_type_convertor
        self.normalize_columns = convert_column_names
        self._snowflake_client = SnowflakeClient(**snfwlk_credentials)
        self.multi_threading_support = True

    def connect(self):
        return self._snowflake_client.connect()

    def process_table(self, table_path: str, result_table_name: str, schema: TableSchema, dedupe_required: bool):
        """
        Processes the table and uploads it to the staging area

        Args:
            table_path: path to the table (folder) with csv files
            result_table_name: name of the table in the staging area
            schema: schema of the table
            dedupe_required: if dedupe is required

        """
        logging.info(f"Creating table {result_table_name} in stage")
        column_types = self.convert_column_types(schema.fields)
        self._snowflake_client.create_table(result_table_name, column_types)

        logging.info(f"Uploading data into table {result_table_name} in stage")
        # chunks if multiple schema changes during execution
        tables = glob.glob(os.path.join(table_path, '*.csv'))
        for table in tables:
            csv_columns = _get_csv_header(table)
            csv_columns = self.normalize_columns(csv_columns)
            self._snowflake_client.copy_csv_into_table_from_file(result_table_name, csv_columns, table)

        # dedupe only if running sync from binlog and not in append_incremental mode
        if dedupe_required:
            self._dedupe_stage_table(table_name=result_table_name, id_columns=schema.primary_keys)

    def _dedupe_stage_table(self, table_name: str, id_columns: list[str],
                            order_by_column: str = 'kbc__batch_event_order'):
        """
        Dedupe staging table and keep only latest records.
        Based on the internal column kbc__batch_event_order produced by CDC engine
        Args:
            table_name:
            id_columns:
            order_by_column: Column used to order and keep the latest record

        Returns:

        """
        id_cols = self._snowflake_client.wrap_columns_in_quotes(id_columns)
        id_cols_str = ','.join([f'"{table_name}".{col}' for col in id_cols])
        unique_id_concat = (f"CONCAT_WS('|',{id_cols_str},"
                            f"\"{self.SYSTEM_COLUMN_NAME_MAPPING[order_by_column]}\")")

        query = f"""DELETE FROM
                                        "{table_name}" USING (
                                        SELECT
                                            {unique_id_concat} AS "__CONCAT_ID"
                                        FROM
                                            "{table_name}"
                                            QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_cols_str} ORDER BY
                              "{self.SYSTEM_COLUMN_NAME_MAPPING[order_by_column]}"::INT DESC) != 1) TO_DELETE
                                    WHERE
                                        TO_DELETE.__CONCAT_ID = {unique_id_concat}
                        """

        logging.debug(f'Dedupping table {table_name}: {query}')
        self._snowflake_client.execute_query(query)


class DuckDBStaging(Protocol):
    def __init__(self):
        self.multi_threading_support = False

    def process_table(self, table_path, result_table_name, schema, dedupe_required):
        """
        Processes the table and uploads it to the staging area

        Args:
            table_path: path to the table (folder) with csv files
            result_table_name: name of the table in the staging area
            schema: schema of the table
            dedupe_required: if dedupe is required

        """
        pass
