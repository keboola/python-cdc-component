import csv
import glob
import json
import os
import shutil
import tempfile
from pathlib import Path

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
    def _remove_column_slice(table_path: str, column_names: list[str]):
        tmp_path = f'{table_path}__tmp.csv'
        columns = json.load(open(f'{table_path}.manifest'))['columns']
        with open(table_path, 'r') as inp, open(tmp_path, 'w+') as outp:

            reader = csv.DictReader(inp, fieldnames=columns)
            new_columns = reader.fieldnames.copy()
            for col in column_names:
                new_columns.remove(col)

            writer = csv.DictWriter(outp, fieldnames=new_columns, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for row in reader:
                for col in column_names:
                    row.pop(col)
                writer.writerow(row)

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
            if 'debezium_signals' in in_table:
                # in case of debezium signal we need to remove id column
                columns_to_remove.append('id')
            if not os.path.isdir(in_table):
                self._remove_column_slice(in_table, columns_to_remove)
            else:
                slices = glob.glob(os.path.join(in_table, '*.csv'))
                for slice in slices:
                    self._remove_column_slice(slice, columns_to_remove)

    def run_component(self):
        super().run_component()
        self._cleanup_result_data()
