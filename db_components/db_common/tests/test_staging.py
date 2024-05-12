import glob
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from db_components.db_common.staging import DuckDBStagingExporter


class TestDuckDBStagingExporter(unittest.TestCase):
    @patch.object(DuckDBStagingExporter, 'get_extracted_tables')
    def test_get_table_chunks(self, mock_get_extracted_tables):
        # Arrange
        mock_get_extracted_tables.return_value = ['table1_chunk_1', 'table1_chunk_2', 'table2',
                                                  'table1_123_chunk_1', 'table1_chunk_chunk_1']
        exporter = DuckDBStagingExporter(db_path='test_db_path')
        expected_result = ['table1_chunk_1', 'table1_chunk_2']

        # Act
        result = exporter.get_table_chunks('table1')

        # Assert
        self.assertEqual(result, expected_result)

        # Act
        result = exporter.get_table_chunks('table2')
        # Assert
        self.assertEqual(result, ['table2'])

    @staticmethod
    def _prep_duckdb_slices_n_exporter() -> tuple[DuckDBStagingExporter, list[tuple]]:
        # Arrange
        db_path = Path(tempfile.gettempdir()).joinpath("testdb.duckdb")
        # delete the db file if it exists
        if os.path.exists(db_path):
            if os.path.isfile(db_path):
                os.remove(db_path)
            else:
                os.rmdir(db_path)

        exporter = DuckDBStagingExporter(db_path.as_posix())
        with exporter.connect() as conn:
            # chunk 0
            conn.execute(
                "CREATE TABLE my_table_chunk_0 ( id INTEGER, col_a VARCHAR, kbc__batch_event_order INTEGER)")

            conn.execute("INSERT INTO my_table_chunk_0 VALUES (1, 'value1', 1)")
            conn.execute("INSERT INTO my_table_chunk_0 VALUES (2, 'value2', 2)")
            conn.execute("INSERT INTO my_table_chunk_0 VALUES (1, 'value3', 3)")
            conn.execute("INSERT INTO my_table_chunk_0 VALUES (3, 'value4', 4)")

            # chunk 1
            conn.execute(
                "CREATE TABLE my_table_chunk_1 ( id INTEGER, col_a VARCHAR, kbc__batch_event_order INTEGER)")

            conn.execute("INSERT INTO my_table_chunk_1 VALUES (4, 'value5', 5)")
            conn.execute("INSERT INTO my_table_chunk_1 VALUES (5, 'value6', 6)")
            conn.execute("INSERT INTO my_table_chunk_1 VALUES (4, 'value7', 7)")
            conn.execute("INSERT INTO my_table_chunk_1 VALUES (4, 'value8', 8)")
            conn.execute("INSERT INTO my_table_chunk_1 VALUES (1, 'value9', 9)")

            # chunk 2
            conn.execute(
                "CREATE TABLE my_table_chunk_2 ( id INTEGER, col_a VARCHAR, kbc__batch_event_order INTEGER)")

            conn.execute("INSERT INTO my_table_chunk_2 VALUES (7, 'value10', 10)")
            conn.execute("INSERT INTO my_table_chunk_2 VALUES (6, 'value11', 11)")
            conn.execute("INSERT INTO my_table_chunk_2 VALUES (6, 'value12', 12)")

            deduped_result = [(2, 'value2', 2),
                              (3, 'value4', 4),
                              (5, 'value6', 6),
                              (4, 'value8', 8),
                              (1, 'value9', 9),
                              (7, 'value10', 10),
                              (6, 'value12', 12)]

        return exporter, deduped_result

    def test_process_table_dedupe(self):
        # Arrange
        exporter, expected_results = self._prep_duckdb_slices_n_exporter()
        result_path = Path(tempfile.gettempdir()).joinpath("testdb").as_posix()
        result_columns = ['id', 'col_a', 'kbc__batch_event_order']
        exporter.process_table_dedupe('my_table', result_path, ['id'],
                                      result_columns, 'kbc__batch_event_order')

        # load all result csv files (chunks result_path/slice_0, result_path/slice_1) into duckdb table
        with exporter.connect() as conn:
            conn.execute('CREATE TABLE result_test ( id INTEGER, col_a VARCHAR, kbc__batch_event_order INTEGER)')
            for table_slice in glob.glob(os.path.join(result_path, '*.csv')):
                conn.execute(f"COPY result_test FROM '{table_slice}' WITH (FORMAT csv, HEADER false)")

            # compare that the result_table contains the expected results
            result = conn.execute('SELECT * FROM result_test ORDER BY "kbc__batch_event_order"').fetchall()
            self.assertEqual(result, expected_results)


if __name__ == '__main__':
    unittest.main()
