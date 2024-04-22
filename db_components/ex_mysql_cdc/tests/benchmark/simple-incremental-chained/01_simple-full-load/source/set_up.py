import os

from datadirtest import TestDataDir

from db_components.debezium.tests.functional import TestDatabaseEnvironment



def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    sql_client.prepare_initial_table('sales_table.sql')
    sql_client.prepare_initial_table('products_table.sql')
    sql_client.create_signal_table()
    print("Running before script")
