from datadirtest import TestDataDir

from db_components.debezium.tests.functional import TestDatabaseEnvironment


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    # drop tables created in the same schema by other tests
    sql_client.prepare_initial_table('cleanup.sql')
    print("Running after script")
