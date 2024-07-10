import os
from pathlib import Path
from datadirtest import TestDataDir
from dotenv import load_dotenv

from db_components.ex_oracle_cdc.tests.scripts_executor import OracleSQLExecutor

traits_folder = '/code/db_components/ex_oracle_cdc/tests/sql_test_traits'


def run(context: TestDataDir):
    load_dotenv()
    user = os.getenv("TEST_ORACLE_USER")
    password = os.getenv("TEST_ORACLE_PASSWORD")
    oracle_host = os.getenv("ORACLE_HOST")
    oracle_port = 1525
    oracle_p_database = os.getenv("ORACLE_P_DATABASE")
    oracle_executor = OracleSQLExecutor(user, password, f'{oracle_host}:{oracle_port}/'f'{oracle_p_database}')

    oracle_executor.execute_sql("""
    INSERT INTO TESTUSER01.SALES (USERGENDER, USERCITY, USERSENTIMENT, ZIPCODE, SKU, CREATEDATE, CATEGORY, PRICE, COUNTRY, COUNTRYCODE, USERSTATE, CATEGORYGROUP)
    VALUES ('Male', 'New York', 1, '10001', 'SKU10', '2024-01-01', 'Electronics', 199.99, 'New York', 'NY', 'NY', 'Electronics')
    """)

    oracle_executor.execute_sql_from_file(os.path.join(traits_folder, 'users_table.sql'))

    print("Running before script")
    os.environ['KBC_COMPONENTID'] = 'kds-team-ex-oracle-cdc-local'
    os.environ['KBC_STACKID'] = 'connection.keboola.com'
    os.environ['KBC_CONFIGID'] = '123'
    os.environ['KBC_CONFIGROWID'] = '456'
    os.environ['KBC_BRANCHID'] = Path(__file__).parent.parent.parent.name
    os.environ['KBC_PROJECTID'] = '10'
