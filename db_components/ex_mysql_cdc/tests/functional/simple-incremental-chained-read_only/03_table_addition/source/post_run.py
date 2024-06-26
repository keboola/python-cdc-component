from datadirtest import TestDataDir

from db_components.debezium.tests.functional import TestDatabaseEnvironment


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['root_client']

    sql_client.connection.connect()
    # drop tables created in the same schema by other tests
    sql_client.perform_query('DROP TABLE IF EXISTS inventory.debezium_signals')
    print("Running after script")
