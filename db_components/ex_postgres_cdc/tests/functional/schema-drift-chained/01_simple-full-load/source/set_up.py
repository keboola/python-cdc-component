import os

from datadirtest import TestDataDir

from db_components.debezium.tests.functional import TestDatabaseEnvironment



def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    sql_client.prepare_initial_table('sales_table.sql')
    sql_client.connection.connect()
    # TODO: figure our how to cleanup WAL, otherqise this test fails on consecutive runs and volume needs to be removed
    sql_client.perform_query("DROP PUBLICATION IF EXISTS publication_kbc_none_prod")
    sql_client.create_signal_table()
    print("Running before script")
