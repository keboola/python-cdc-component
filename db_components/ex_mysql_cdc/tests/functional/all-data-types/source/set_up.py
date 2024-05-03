import os

from datadirtest import TestDataDir

from db_components.debezium.tests.functional import TestDatabaseEnvironment


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    sql_client.prepare_initial_table('all_data_types.sql')
    sql_client.create_signal_table()
    print("Running before script")
    os.environ['KBC_COMPONENTID'] = 'kds-team-ex-mysql-cdc-local'
    os.environ['KBC_STACKID'] = 'connection.keboola.com'
    os.environ['KBC_CONFIGID'] = '123'
    os.environ['KBC_CONFIGROWID'] = '456'
    os.environ['KBC_BRANCHID'] = '789'
    os.environ['KBC_PROJECTID'] = '10'
