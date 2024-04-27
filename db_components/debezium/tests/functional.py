import glob
import json
import os
import shutil
import tempfile
from pathlib import Path

import duckdb
from datadirtest import TestDataDir
from keboola.component import CommonInterface

from component import Component
from db_components.db_common.db_connection import JDBCConnection
from db_components.debezium.tests.db_test_traits.traits import DbTestTable


class TestDatabaseEnvironment:

    def __init__(self, connection: JDBCConnection):
        self.connection = connection

    def create_signal_table(self):
        self.prepare_initial_table('signal_table.sql')

    def perform_query(self, query: str):
        return list(self.connection.perform_query(query))

    def prepare_initial_table(self, script_name: str):
        self.connection.connect()
        table = DbTestTable(script_name)
        queries = table.init_queries
        for q in queries:
            if q.strip():
                self.perform_query(q)


class DebeziumCDCDatadirTest(TestDataDir):

    def setUp(self):

        try:
            if Path.cwd().name != 'src':
                os.chdir('./src')
            comp = Component(data_path_override=self.source_data_dir)
            with comp._init_client() as config:
                connection = comp._client.connection
                db_client = TestDatabaseEnvironment(connection)

        except Exception as e:
            raise e

        self.context_parameters['db_client'] = db_client
        super().setUp()

    def _create_temporary_copy(self):
        temp_dir = tempfile.mkdtemp(prefix=Path(self.orig_dir).name, dir='/tmp')
        dst_path = os.path.join(temp_dir, 'test_data')
        if os.path.exists(dst_path):
            shutil.rmtree(dst_path)
        if not os.path.exists(self.orig_dir):
            raise ValueError(f"{self.orig_dir} does not exist. ")
        shutil.copytree(self.orig_dir, dst_path)
        return dst_path

    @staticmethod
    def _remove_column_slice(manifest_path: str, table_path: str, drop_columns: list[str], order_by_column: str):
        tmp_path = f'{table_path}__tmp.csv'
        columns = json.load(open(manifest_path))['columns']

        # Create table
        table_name = Path(table_path).stem
        duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
        # Read the CSV file into a DataFrame using DuckDB
        duckdb.read_csv(table_path, header=False, names=columns, all_varchar=True).create(table_name)

        # Remove the specified columns
        select_columns = ','.join([f'"{column}"' for column in columns if column not in drop_columns])

        # if empty do nothing
        if not duckdb.execute(f"SELECT * FROM {table_name}").fetchall():
            return

        # Write the DataFrame back to a CSV file
        duckdb.execute(f"COPY (SELECT {select_columns} FROM {table_name} ORDER BY {order_by_column}::INT ASC) "
                       f"TO '{tmp_path}' (FORMAT CSV, HEADER false, DELIMITER \',\')")
        duckdb.execute(f"DROP TABLE {table_name}")

        os.remove(table_path)
        shutil.move(tmp_path, table_path)

    def _cleanup_result_data(self):
        """
        We cannot compare binlog read_at timestamp, so exclude these columns from the comparison.


        Returns:

        """
        # help with CI package
        ci = CommonInterface(self.source_data_dir)
        in_tables: list = glob.glob(f'{ci.tables_out_path}/*.csv')

        for in_table in in_tables:
            # we now we need to remove last 2columns
            columns_to_remove = ['KBC__EVENT_TIMESTAMP_MS']
            order_by_column = 'KBC__BATCH_EVENT_ORDER'
            if 'debezium_signals' in in_table:
                # in case of debezium signal we need to remove id column
                columns_to_remove.append('id')

            if 'io_debezium_connector' in in_table and 'schema_changes' in in_table:
                columns_to_remove = ['source', 'ts_ms']

            if not os.path.isdir(in_table):
                self._remove_column_slice(f'{in_table}.manifest',
                                          in_table, columns_to_remove, order_by_column)
            else:
                slices = glob.glob(os.path.join(in_table, '*.csv'))
                for slice in slices:
                    self._remove_column_slice(f'{Path(slice).parent.as_posix()}.manifest',
                                              slice, columns_to_remove, order_by_column)

    def run_component(self):
        super().run_component()
        self._cleanup_result_data()
