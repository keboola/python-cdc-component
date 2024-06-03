import os

from datadirtest import TestDataDir

from db_components.debezium.tests.functional import TestDatabaseEnvironment


def get_transactions_queries():
    transactions_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'transactions.sql')
    return str(open(transactions_path, 'r').read()).split(';')


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    sql_client.connection.connect()

    queries = get_transactions_queries()
    for q in queries:
        if q.strip():
            sql_client.perform_query(q)
    sql_client.perform_query('commit')
    sql_client.connection.close()
    print("Running before script")
    os.environ['KBC_COMPONENTID'] = 'kds-team-ex-mysql-cdc-local'
    os.environ['KBC_STACKID'] = 'connection.keboola.com'
    os.environ['KBC_CONFIGID'] = '123'
    os.environ['KBC_CONFIGROWID'] = '456'
    os.environ['KBC_BRANCHID'] = '789'
    os.environ['KBC_PROJECTID'] = '10'
