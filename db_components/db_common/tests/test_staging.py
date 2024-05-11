import unittest
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


if __name__ == '__main__':
    unittest.main()
