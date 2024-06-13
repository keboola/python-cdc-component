import os
import unittest

from datadirtest import DataDirTester

from db_components.ex_oracle_cdc.src.component import OracleComponent
from db_components.debezium.tests.db_test_traits import traits as db_test_traits
from db_components.debezium.tests.functional import DebeziumCDCDatadirTest


class TestComponent(unittest.TestCase):
    # @freeze_time("2024-02-03 14:50:42.833622")
    def test_functional(self):
        sql_traits_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'sql_test_traits')

        db_test_traits.set_sql_traits_folder(sql_traits_path)

        DebeziumCDCDatadirTest.set_component_class(OracleComponent)
        functional_tests = DataDirTester(test_data_dir_class=DebeziumCDCDatadirTest)
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
