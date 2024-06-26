import os

from datadirtest import TestDataDir

from db_components.debezium.tests.db_test_traits import traits
from db_components.debezium.tests.functional import TestDatabaseEnvironment


def get_transactions_queries():
    transactions_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'transactions.sql')
    return str(open(transactions_path, 'r').read()).split(';')


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    sql_client.connection.connect()
    sql_client.prepare_initial_table('products.sql')

    sql_client.connection.close()

    traits.set_order_by_columns('inventory_products', ['id', 'KBC__EVENT_TIMESTAMP_MS'])
    print("Running before script")
