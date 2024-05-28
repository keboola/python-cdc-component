import logging
import logging.handlers
import tempfile
from typing import Optional
import os
from jaydebeapi import DatabaseError

from db_components.db_common.db_connection import JDBCConnection
from db_components.db_common.metadata import JDBCMetadataProvider
from db_components.db_common.table_schema import BaseTypeConverter

from db_components.ex_oracle_cdc.src.configuration import DbOptions, HeartBeatConfig, ColumnFilterType


JDBC_PATH = '../jdbc/ojdbc8.jar'


class ExtractorUserException(Exception):
    pass


# noinspection PyCompatibility
class OracleBaseTypeConverter(BaseTypeConverter):
    MAPPING = {
        "tinyint": "INTEGER",
        "smallint": "INTEGER",
        "mediumint": "INTEGER",
        "int": "INTEGER",
        "integer": "INTEGER",
        "bigint": "INTEGER",
        "decimal": "NUMBER",
        "dec": "NUMBER",
        "numeric": "NUMBER",
        "float": "FLOAT",
        "double": "DOUBLE",
        "real": "FLOAT",
        "binary_float": "FLOAT",
        "binary_double": "DOUBLE",
        "bit": "BOOLEAN",
        "date": "DATE",
        "datetime": "TIMESTAMP",
        "timestamp": "TIMESTAMP_NTZ",
        "time": "TIME",
        "year": "INTEGER",
        "char": "CHAR",
        "nchar": "CHAR",
        "varchar": "VARCHAR",
        "nvarchar": "VARCHAR",
        "varchar2": "VARCHAR",
        "nvarchar2": "VARCHAR",
        "clob": "STRING",
        "nclob": "STRING",
        "binary": "BINARY",
        "varbinary": "BINARY",
        "blob": "BINARY",
        "long": "STRING",
        "raw": "BINARY",
        "long raw": "BINARY",
        "bfile": "STRING",
        "rowid": "STRING",
        "urowid": "STRING",
        "json": "VARIANT",
        "xml": "STRING",
        "anydata": "VARIANT"
    }

    def __call__(self, source_type: str, length: Optional[str] = None) -> str:
        source_type_lower = source_type.lower()
        match source_type_lower:
            case 'bit' if str(length) == '1':
                return 'BOOLEAN'
            case 'boolean':
                return 'BOOLEAN'
            case _:
                return self.MAPPING.get(source_type_lower, 'STRING')


SUPPORTED_TYPES = [
    "tinyint",
    "smallint",
    "mediumint",
    "integer",
    "bigint",
    "decimal",
    "numeric",
    "float",
    "double",
    "real",
    "binary_float",
    "binary_double",
    "boolean",
    "bit",
    "timestamp",
    "timestamp_ntz",
    "timestamp_ltz",
    "timestamp_tz",
    "datetime",
    "date",
    "time",
    "year",
    "char",
    "nchar",
    "varchar",
    "nvarchar",
    "varchar2",
    "nvarchar2",
    "clob",
    "nclob",
    "text",
    "long",
    "binary",
    "varbinary",
    "blob",
    "long raw",
    "bfile",
    "rowid",
    "urowid",
    "json",
    "xml",
    "variant",
    "anydata"
]


def build_debezium_property_file(user: str, password: str, hostname: str, port: str, database: str, p_database: str,
                                 offset_file_path: str,
                                 schema_whitelist: list[str],
                                 table_whitelist: list[str],
                                 column_filter_type: ColumnFilterType,
                                 column_filter: list[str],
                                 schema_history_filepath: str,
                                 snapshot_mode: str = 'initial',
                                 signal_table: str = None,
                                 snapshot_fetch_size: int = 10240,
                                 snapshot_max_threads: int = 1,
                                 additional_properties: dict = None,
                                 heartbeat_config: HeartBeatConfig = None) -> str:
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
        column_filter_type: Type of column filter, 'none', 'exclude' or 'include'
        column_filter: List of columns to include or exclude.
        schema_history_filepath: Path to the file where the connector will store the schema history (dbhistory.dat).
        additional_properties:
        snapshot_max_threads:
        snapshot_fetch_size: Maximum number of records to fetch from the database when performing an incremental
                             snapshot.
        snapshot_mode: 'initial' or 'never'
        signal_table: Name of the table where the signals will be stored, fully qualified name, e.g. schema.table
        heartbeat_config:

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
        "schema.history.internal.store.only.captured.tables.ddl": "true",
        "log.mining.archive.destination.name": "LOG_ARCHIVE_DEST_1",
        "log.mining.strategy": "online_catalog"
    }

    if heartbeat_config:
        properties["heartbeat.interval.ms"] = heartbeat_config.interval_ms
        properties["heartbeat.action.query"] = heartbeat_config.action_query

    if column_filter_type != ColumnFilterType.none:
        filter_key = f"column.{column_filter_type.value}.list"
        filter_value = ','.join(column_filter)
        properties[filter_key] = filter_value

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
             , decode(dlg.log_group_type, 'ALL COLUMN LOGGING', 'OK', 'PRIMARY KEY LOGGING'
             , 'PARTIALLY (NOT ALL COLUMNS)', 'NOT SET') AS "suppLogSetting"
             , dlg.*
          FROM dba_tables cdt
            JOIN dba_users du ON (du.username = cdt.owner)
            LEFT OUTER JOIN dba_log_groups dlg ON (dlg.owner = cdt.owner AND dlg.table_name = cdt.table_name)
         WHERE du.oracle_maintained = 'N'
           AND du.username NOT IN (?)
        """
        results = set(tuple(row) for row in self.connection.perform_query(query, [self.user])) # noqa
        return results

    def close_connection(self):
        logging.debug("Closing the outer connection.")
        self.connection.connection.close()
