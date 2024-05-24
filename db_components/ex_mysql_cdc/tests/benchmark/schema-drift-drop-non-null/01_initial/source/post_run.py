import os

from datadirtest import TestDataDir


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    # remove the io_debezium_connector_mysql_schema_changes file from comparison
    # as it may contain changes from previous tests
    if os.path.exists(
            os.path.join(context.source_data_dir, 'out', 'tables', 'io_debezium_connector_mysql_schema_changes.csv')):
        os.remove(
            os.path.join(context.source_data_dir, 'out', 'tables', 'io_debezium_connector_mysql_schema_changes.csv'))

    if os.path.exists(os.path.join(context.source_data_dir, 'out', 'tables',
                                   'io_debezium_connector_mysql_schema_changes.csv.manifest')):
        os.remove(os.path.join(context.source_data_dir, 'out', 'tables',
                               'io_debezium_connector_mysql_schema_changes.csv.manifest'))
