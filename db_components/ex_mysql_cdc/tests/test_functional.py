import os
import unittest

from datadirtest import DataDirTester

from configuration import DbOptions, Adapter
from db_components.debezium.tests.db_test_traits import traits as db_test_traits
from db_components.debezium.tests.functional import DebeziumCDCDatadirTest, TestDatabaseEnvironment
from db_components.ex_mysql_cdc.src.component import MySqlCDCComponent
from extractor.mysql_extractor import MySQLDebeziumExtractor


class MySQLDatadirTest(DebeziumCDCDatadirTest):

    def setUp(self):

        # create a root client and ro_user
        config = DbOptions(adapter=Adapter.mysql, host=os.environ['MYSQL_HOST'], port=3306,
                           user='root', pswd_password='rootpassword')
        ex = MySQLDebeziumExtractor(config, is_maria_db=config.adapter == Adapter.mariadb)
        ex.connect()
        root_client = TestDatabaseEnvironment(ex.connection)
        self.context_parameters['root_client'] = root_client
        root_client.connection.connect()

        user_exists = root_client.perform_query("SELECT COUNT(*) FROM mysql.user WHERE user = 'ro_user';")
        if user_exists[0][0] == 0:
            root_client.perform_query("CREATE USER 'ro_user'@'%' IDENTIFIED BY 'password';")
        root_client.perform_query(
            "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'ro_user'")

        super().setUp()
        self.component_class = MySqlCDCComponent


class TestComponent(unittest.TestCase):
    # @freeze_time("2024-02-03 14:50:42.833622")
    def test_functional(self):
        sql_traits_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'sql_test_traits')
        db_test_traits.set_sql_traits_folder(sql_traits_path)

        MySQLDatadirTest.set_component_class(MySqlCDCComponent)
        functional_tests = DataDirTester(test_data_dir_class=MySQLDatadirTest)
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
