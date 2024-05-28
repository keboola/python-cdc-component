import os
from pathlib import Path

from datadirtest import TestDataDir

from db_components.debezium.tests.functional import TestDatabaseEnvironment



def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    sql_client.prepare_initial_table('sales_table.sql')
    sql_client.create_signal_table()
    os.environ['KBC_COMPONENTID'] = 'test-01_simple-full-load'
    os.environ['KBC_STACKID'] = 'connection.keboola.com'
    os.environ['KBC_CONFIGID'] = '123'
    os.environ['KBC_CONFIGROWID'] = '456'
    os.environ['KBC_BRANCHID'] = Path(__file__).parent.parent.parent.name
    os.environ['KBC_PROJECTID'] = '10'
    print("Running before script")
