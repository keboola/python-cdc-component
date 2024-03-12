import fileinput
import gc
import glob
import logging
import os
import subprocess
import sys
from csv import DictReader
from pathlib import Path
from typing import Protocol, Callable

import duckdb

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

    def process_table(self, table_path: str, result_table_name: str, schema: TableSchema, dedupe_required: bool,
                      order_by_column: str = 'kbc__batch_event_order'):
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

    def process_table(self, table_path: str, result_table_name: str, schema: TableSchema, dedupe_required: bool,
                      order_by_column: str = 'kbc__batch_event_order'):
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
            id_columns = self.normalize_columns(schema.primary_keys)
            order_by_column = self.normalize_columns([order_by_column])[0]
            self._dedupe_stage_table(table_name=result_table_name, id_columns=id_columns,
                                     order_by_column=order_by_column)

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
                            f"\"{order_by_column}\")")

        query = f"""DELETE FROM
                                        "{table_name}" USING (
                                        SELECT
                                            {unique_id_concat} AS "__CONCAT_ID"
                                        FROM
                                            "{table_name}"
                                            QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_cols_str} ORDER BY
                              "{order_by_column}"::INT DESC) != 1) TO_DELETE
                                    WHERE
                                        TO_DELETE.__CONCAT_ID = {unique_id_concat}
                        """

        logging.debug(f'Dedupping table {table_name}: {query}')
        self._snowflake_client.execute_query(query)


class StagingException(Exception):
    pass


class DuckDBStaging(Protocol):
    TMP_DB_PATH = "/tmp/my-db.duckdb"

    def __init__(self, column_type_convertor: Callable, convert_column_names: Callable,
                 max_threads: int = 1, memory_limit: str = '2GB', max_memory: str = '2GB'):
        self.multi_threading_support = False
        self.max_threads = max_threads
        self.memory_limit = memory_limit
        self.max_memory = max_memory
        self.convert_column_types = column_type_convertor
        self.normalize_columns = convert_column_names
        self.connect()

    def connect(self):
        duckdb.connect(database=self.TMP_DB_PATH, read_only=False)
        duckdb.execute("SET temp_directory	='/tmp/dbtmp'")
        duckdb.execute(f"SET threads TO {self.max_threads}")
        duckdb.execute(f"SET memory_limit='{self.memory_limit}'")
        duckdb.execute(f"SET max_memory='{self.max_memory}'")

    def process_table(self, table_path: str, result_table_name: str, schema: TableSchema, dedupe_required: bool,
                      order_by_column: str = 'kbc__batch_event_order',
                      null_string: str = 'KBC__NULL'):
        """
        Processes the table and uploads it to the staging area.

        This method is responsible for creating a table in the staging area with the provided schema,
        uploading data from the CSV files located at the provided table path into the created table,
        and performing deduplication if required.

        Args:
            table_path (str): Path to the directory containing the CSV files to be uploaded.
            result_table_name (str): Name of the table to be created in the staging area.
            schema (TableSchema): Schema of the table to be created.
            dedupe_required (bool): Flag indicating whether deduplication is required.
            order_by_column: Column used to order and keep the latest record
            null_string: String that represents NULL values in the CSV files.

        """
        tables = glob.glob(os.path.join(table_path, '*.csv'))

        self._slice_input(tables[0], table_path)
        # delete the original file
        os.remove(tables[0])

        # create pkey table
        duckdb.execute("CREATE OR REPLACE TABLE PKEY_CACHE (slice_id INT, pkey TEXT)")

        column_types = self.convert_column_types(schema.fields)
        datatypes = {col_type["name"]: col_type["type"] for col_type in column_types}
        id_cols = self.wrap_columns_in_quotes(schema.primary_keys)
        id_cols_str = ','.join([f'{col}' for col in id_cols])
        unique_id_concat = (f"CONCAT_WS('|',{id_cols_str},"
                            f"\"{order_by_column}\")")

        # MAP
        new_tables = glob.glob(os.path.join(table_path, '*'))
        for index, table in enumerate(new_tables):
            logging.debug(f"Loading slice {index}")
            select_statement = self.generate_select_column_statement(datatypes)
            sql_create = f"""
                           CREATE OR REPLACE TABLE SLICE_{index} AS SELECT {select_statement}, 
                                                                           {unique_id_concat} as __PK_TMP
                                                       FROM
                                                          read_csv('{table}', delim=',', header=false, 
                                                          columns={datatypes}, auto_detect=false,
                                                          nullstr='{null_string}')
                                                          QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_cols_str}
                                                           ORDER BY "{order_by_column}"::INT DESC) = 1"""
            duckdb.execute(sql_create)

            # colect pkeys:
            duckdb.execute(
                f"INSERT INTO PKEY_CACHE SELECT {index} as slice_id, {unique_id_concat} as pkey FROM SLICE_{index}")

        # REDUCE
        tables.sort(reverse=True)
        slice_nr = len(new_tables)

        for index, table in enumerate(new_tables):
            logging.debug(f"Exporting slice {index}")
            offload_query = (
                f"COPY (SELECT {select_statement} FROM SLICE_{slice_nr - 1 - index} t "
                f"LEFT JOIN PKEY_CACHE pc ON t.__PK_TMP=pc.pkey "
                f"and pc.slice_id >= {slice_nr - index} "
                f"WHERE pc.pkey IS NULL) TO \'{table_path}/slice_{index}\' (HEADER false, DELIMITER \',\');")
            logging.debug(offload_query)
            # without this the Duckdb fails on OOM
            gc.collect()

            duckdb.execute(offload_query)

        # delete old tables
        for table in new_tables:
            os.remove(table)

        # rename source folder to the result_table_name
        os.rename(table_path, f'{Path(table_path).parent.as_posix()}/{result_table_name}')

    def generate_select_column_statement(self, column_types: dict) -> str:
        column_statements = []
        for name, data_type in column_types.items():
            if data_type in ['DATE', 'TIMESTAMP', 'TIME']:
                column_statements.append(self.wrap_in_quote(name))
                column_types[name] = 'BIGINT'
            else:
                column_statements.append(self.wrap_in_quote(name))
        return ','.join(column_statements)

    def wrap_columns_in_quotes(self, columns):
        return [self.wrap_in_quote(col) for col in columns]

    def wrap_in_quote(self, s):
        return s if s.startswith('"') else '"' + s + '"'

    def _slice_input(self, table_path: str, result_path: str, slice_size_mb: int = 500):
        """
        Slices the input file into smaller files to ensure memory efficiency.

        Args:
            table_path (str): Path to the directory containing the CSV files to be uploaded.
            result_path (str): Path to the directory where the sliced files will be stored.

        """

        # overrides from ENV for tests
        slice_size_mb = int(os.environ.get('SLICER_SLICE_SIZE_MB', slice_size_mb))
        table_name = Path(table_path).name
        # in case the input table is small, just copy it
        size_threshold_mb = os.environ.get('SLICER_INPUT_SIZE_THRESHOLD', 50)
        if os.path.getsize(table_path) < size_threshold_mb * 1024 * 1024:
            logging.debug(f"Table {table_name} is small enough, copying it directly")
            self._remove_header_in_file(table_path)

            result_path = os.path.join(result_path, f'copied_{table_name}')

        args = ['kbc_slicer', f'--table-name={table_name}',
                f'--table-input-path={table_path}',
                f'--table-output-path={result_path}',
                f'--table-output-manifest-path=/tmp/{table_name}_slicer.manifest',
                f'--bytes-per-slice={slice_size_mb}MB',
                f'--input-size-low-exit-code=0',
                f'--gzip=false']

        process = subprocess.Popen(args,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

        logging.debug(f'Slicing table: {table_name}')
        stdout, stderr = process.communicate()
        process.poll()
        err_string = stderr.decode('utf-8')
        if process.poll() != 0:
            raise StagingException(
                f'Failed to slice the table {table_name}: {stderr.decode("utf-8")}')
        elif stderr:
            logging.warning(err_string)

        logging.debug(stdout.decode('utf-8'))

    def _remove_header_in_file(self, file_path: str):
        with fileinput.input(files=(file_path,), inplace=True) as f:
            for i, line in enumerate(f):
                if i > 0:  # skip the first line
                    sys.stdout.write(line)
