import logging
import logging.handlers
import tempfile
from typing import Optional
import os
from jaydebeapi import DatabaseError

from db_components.db_common.db_connection import JDBCConnection
from db_components.db_common.metadata import JDBCMetadataProvider
from db_components.db_common.table_schema import BaseTypeConverter
from db_components.ex_oracle_cdc.src.configuration import DbOptions

JDBC_PATH = '../jdbc/ojdbc8.jar'


class ExtractorUserException(Exception):
    pass


# noinspection PyCompatibility
class OracleBaseTypeConverter(BaseTypeConverter):
    MAPPING = {"tinyint": "INTEGER",
               "smallint": "INTEGER",
               "mediumint": "INTEGER",
               "int": "INTEGER",
               "bigint": "INTEGER",
               "decimal": "NUMERIC",
               "float": "NUMERIC",
               "double": "NUMERIC",
               "bit": "STRING",
               "date": "DATE",
               "datetime": "TIMESTAMP",
               "timestamp": "TIMESTAMP",
               "time": "TIMESTAMP",
               "year": "INTEGER",
               "char": "STRING",
               "varchar": "STRING",
               "binary": "STRING",
               "varbinary": "STRING",
               "tinyblob": "STRING",
               "blob": "STRING",
               "mediumblob": "STRING",
               "longblob": "STRING",
               "tinytext": "STRING",
               "text": "STRING",
               "mediumtext": "STRING",
               "longtext": "STRING",
               "enum": "STRING",
               "set": "STRING"}

    def __call__(self, source_type: str, length: Optional[str] = None) -> str:
        source_type_lower = source_type.lower()
        match source_type_lower:
            case 'bit' if str(length) == '1':
                return 'BOOLEAN'
            case 'boolean':
                return 'BOOLEAN'
            case _:
                return self.MAPPING.get(source_type_lower, 'STRING')


SUPPORTED_TYPES = ["smallint",
                   "integer",
                   "bigint",
                   "decimal",
                   "numeric",
                   "real",
                   "double",
                   "smallserial",
                   "serial",
                   "bigserial",
                   "timestamp",
                   "date",
                   "time",
                   "boolean",
                   "varchar",
                   "char",
                   "text"]


def build_debezium_property_file(user: str, password: str, hostname: str, port: str, database: str, p_database: str,
                                 offset_file_path: str,
                                 schema_whitelist: list[str],
                                 table_whitelist: list[str],
                                 schema_history_filepath: str,
                                 snapshot_mode: str = 'initial',
                                 signal_table: str = None,
                                 snapshot_fetch_size: int = 10240,
                                 snapshot_max_threads: int = 1,
                                 additional_properties: dict = None,
                                 repl_suffix: str = 'dbz') -> str:
    """
    Builds temporary file with Postgres related Debezium properties.
    For documentation see:
     https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-connector-properties

    Args:
        user:
        password:
        hostname:
        port:
        database:
        p_database:
        offset_file_path: Path to the file where the connector will store the offset.
        schema_whitelist: List of schemas to sync.
        table_whitelist: List of tables to sync.
        schema_history_filepath: Path to the file where the connector will store the schema history (dbhistory.dat).
        additional_properties:
        snapshot_max_threads:
        snapshot_fetch_size: Maximum number of records to fetch from the database when performing an incremental
                             snapshot.
        snapshot_mode: 'initial' or 'never'
        signal_table: Name of the table where the signals will be stored, fully qualified name, e.g. schema.table
        repl_suffix: Suffixed to the publication and slot name to avoid name conflicts.

    Returns:

    """
    if not additional_properties:
        additional_properties = dict()
    schema_include = ''
    if schema_whitelist:
        schema_include = ','.join(schema_whitelist)
    table_include = ''
    if table_whitelist:
        table_include = ','.join(table_whitelist)
    properties = {
        # Engine properties
        "offset.storage": "org.apache.kafka.connect.storage.FileOffsetBackingStore",
        "offset.storage.file.filename": offset_file_path,
        "offset.flush.interval.ms": 0,
        # connector properties
        "connector.class": "io.debezium.connector.oracle.OracleConnector",
        "database.hostname": hostname,
        "database.port": port,
        "database.user": user,
        "database.password": password,
        "database.dbname": database,
        "database.pdb.name": p_database,
        "snapshot.max.threads": snapshot_max_threads,
        "snapshot.fetch.size": snapshot_fetch_size,
        "snapshot.mode": snapshot_mode,
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "schema.include.list": schema_include,
        "table.include.list": table_include,
        "errors.max.retries": 3,
        "signal.enabled.channels": "source",
        "signal.data.collection": signal_table,
        "schema.history.internal": "io.debezium.storage.file.history.FileSchemaHistory",
        "schema.history.internal.file.filename": schema_history_filepath,
        "schema.history.internal.store.only.captured.tables.ddl": "true"
        # "heartbeat.interval.ms": 5000
    }

    properties |= additional_properties
    logging.info(f"Oracle properties: {properties}")

    temp_file = tempfile.NamedTemporaryFile(suffix='.properties', delete=False)
    with open(temp_file.name, 'w+', newline='\n') as outp:
        for key, value in properties.items():
            outp.write(f'{key}={value}\n')

    return temp_file.name


class OracleDebeziumExtractor:

    def __init__(self, db_credentials: DbOptions, jdbc_path=JDBC_PATH):
        self.__credentials = db_credentials
        driver_args = {'user': db_credentials.user, 'password': db_credentials.pswd_password}

        jdbc_path = os.path.abspath(jdbc_path)
        logging.info(f"JDBC path: {jdbc_path}")
        if not os.path.exists(jdbc_path):
            raise ExtractorUserException(f"Debezium jar not found at {jdbc_path}")

        # For pluggable database
        self.connection = JDBCConnection('oracle.jdbc.driver.OracleDriver',
                                         url=f'jdbc:oracle:thin:@//{db_credentials.host}:{db_credentials.port}/'
                                             f'{db_credentials.p_database}',
                                         driver_args=driver_args,
                                         jars=jdbc_path)
        # TODO: Add support for non-pluggable databases?
        self.user = db_credentials.user
        self.metadata_provider = JDBCMetadataProvider(self.connection, OracleBaseTypeConverter())

    def connect(self):
        logging.info("Connecting to database.")
        try:
            self.connection.connect()
        except DatabaseError as e:
            raise ExtractorUserException(f"Login to database failed, please check your credentials. Detail: {e}") from e

    def test_connection(self):
        self.connect()
        self.test_has_privileges()
        self.close_connection()

    def test_has_privileges(self):
        expected_results = {
            ('SYS', 'V_$STATNAME', 'SELECT'),
            ('SYS', 'V_$DATABASE', 'SELECT'),
            ('SYS', 'DBMS_LOGMNR', 'EXECUTE'),
            ('SYS', 'V_$LOGFILE', 'SELECT'),
            ('SYS', 'V_$LOGMNR_LOGS', 'SELECT'),
            ('SYS', 'V_$TRANSACTION', 'SELECT'),
            ('SYS', 'V_$MYSTAT', 'SELECT'),
            ('SYS', 'V_$ARCHIVED_LOG', 'SELECT'),
            ('SYS', 'V_$LOG_HISTORY', 'SELECT'),
            ('SYS', 'V_$LOGMNR_PARAMETERS', 'SELECT'),
            ('SYS', 'V_$ARCHIVE_DEST_STATUS', 'SELECT'),
            ('SYS', 'V_$LOGMNR_CONTENTS', 'SELECT'),
            ('SYS', 'C##DBZUSER', 'INHERIT PRIVILEGES'),
            ('SYS', 'V_$LOG', 'SELECT'),
            ('SYS', 'DBMS_LOGMNR_D', 'EXECUTE')
        }

        query = """
            SELECT owner, table_name, privilege
            FROM user_tab_privs
        """
        results = set(tuple(row) for row in self.connection.perform_query(query))
        missing_privileges = expected_results - results

        if missing_privileges:
            error_messages = ["Missing privileges: " + ", ".join(str(privilege) for privilege in missing_privileges)]
            raise ExtractorUserException("\n".join(error_messages))

    def get_tables(self):
        query = """
        SELECT
            cdt.owner, cdt.table_name
        FROM dba_tables cdt
            JOIN dba_users du ON (du.username = cdt.owner)
            JOIN dba_log_groups dlg ON (dlg.owner = cdt.owner AND dlg.table_name = cdt.table_name)
        WHERE du.oracle_maintained = 'N'
            AND du.username NOT IN (?)
        """
        results = set(tuple(row) for row in self.connection.perform_query(query, [self.user]))
        return results


    def close_connection(self):
        logging.debug("Closing the outer connection.")
        self.connection.connection.close()
