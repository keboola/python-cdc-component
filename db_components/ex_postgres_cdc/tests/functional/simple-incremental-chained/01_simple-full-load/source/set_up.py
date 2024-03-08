import os

from datadirtest import TestDataDir

from tests.test_functional import TestDatabaseEnvironment


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    schema = 'inventory'
    sql_client.prepare_initial_table('SalesTable', schema)
    sql_client.create_signal_table()
    print("Running before script")
