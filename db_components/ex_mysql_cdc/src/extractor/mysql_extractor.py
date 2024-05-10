import logging
import logging.handlers
import tempfile
from typing import Optional

from jaydebeapi import DatabaseError

from db_components.db_common.db_connection import JDBCConnection
from db_components.db_common.metadata import JDBCMetadataProvider
from db_components.db_common.table_schema import BaseTypeConverter
from db_components.debezium.common import SignallingConfig
from db_components.ex_mysql_cdc.src.configuration import DbOptions, ColumnFilterType

JDBC_PATH = '../jdbc/mysql-connector-j-8.3.0.jar'


class ExtractorUserException(Exception):
    pass


class MySQLBaseTypeConverter(BaseTypeConverter):
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


def build_debezium_property_file(user: str, password: str, hostname: str, port: str,
                                 offset_file_path: str,
                                 schema_history_file_path: str,
                                 schema_whitelist: list[str],
                                 table_whitelist: list[str],
                                 column_filter_type: ColumnFilterType,
                                 column_filter: list[str],
                                 server_id_unique: int,
                                 snapshot_mode: str = 'initial',
                                 signal_config: SignallingConfig = None,
                                 snapshot_fetch_size: int = 10240,
                                 snapshot_max_threads: int = 1,
                                 additional_properties: dict = None,
                                 binary_handling_mode: str = 'hex',
                                 read_only:bool = False) -> str:
    """
    Builds temporary file with Postgres related Debezium properties.
    For documentation see:
     https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-connector-properties

    Args:
        user:
        password:
        hostname:
        port:
        offset_file_path: Path to the file where the connector will store the offset.
        schema_whitelist: List of schemas to sync.
        table_whitelist: List of tables to sync.
        column_filter_type: Type of column filter, 'none', 'exclude' or 'include'
        column_filter: List of columns to include or exclude.
        server_id_unique: Unique server id for the connector.
        additional_properties:
        snapshot_max_threads:
        snapshot_fetch_size: Maximum number of records to fetch from the database when performing an incremental
                             snapshot.
        snapshot_mode: 'initial' or 'never'
        signal_config: SignallingConfig
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
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.server.id": server_id_unique,
        "database.hostname": hostname,
        "database.port": port,
        "database.user": user,
        "database.password": password,
        "snapshot.max.threads": snapshot_max_threads,
        "snapshot.fetch.size": snapshot_fetch_size,
        "snapshot.mode": snapshot_mode,
        "read.only": read_only,
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "binary.handling.mode": binary_handling_mode,
        "schema.history.internal": "io.debezium.storage.file.history.FileSchemaHistory",
        "schema.history.internal.file.filename": schema_history_file_path,
        "schema.history.internal.store.only.captured.tables.ddl": True,
        "database.include.list": schema_include,
        "table.include.list": table_include,
        "errors.max.retries": 3,
        "signal.enabled.channels": "source",
        "max.batch.size": 5000,
        "max.queue.size": 10000
    }

    if signal_config:
        properties |= signal_config.debezium_properties()

    if column_filter_type != ColumnFilterType.none:
        filter_key = f"column.{column_filter_type.value}.list"
        filter_value = ','.join(column_filter)
        properties[filter_key] = filter_value

    properties |= additional_properties

    temp_file = tempfile.NamedTemporaryFile(suffix='.properties', delete=False)
    with open(temp_file.name, 'w+', newline='\n') as outp:
        for key, value in properties.items():
            outp.write(f'{key}={value}\n')

    return temp_file.name


class MySQLDebeziumExtractor:

    def __init__(self, db_credentials: DbOptions, jdbc_path=JDBC_PATH):
        self.__credentials = db_credentials
        logging.debug(f'Driver {jdbc_path}')
        self.connection = JDBCConnection('com.mysql.cj.jdbc.Driver',
                                         url=f'jdbc:mysql://{db_credentials.host}:{db_credentials.port}',
                                         driver_args={'user': db_credentials.user,
                                                      'password': db_credentials.pswd_password},
                                         jars=jdbc_path)
        self.user = db_credentials.user
        self.metadata_provider = JDBCMetadataProvider(self.connection, MySQLBaseTypeConverter())

    def connect(self):
        logging.info("Connecting to database.")
        try:
            self.connection.connect()
        except DatabaseError as e:
            raise ExtractorUserException(f"Login to database failed, please check your credentials. Detail: {e}") from e

    def test_connection(self):
        self.connect()
        # test if user has appropriate privileges
        self.test_has_replication_privilege()
        self.close_connection()

    def test_has_replication_privilege(self):
        # query = f"SELECT  rolreplication, rolcanlogin FROM pg_catalog.pg_roles r WHERE r.rolname = '{self.user}'"
        # results = list(self.connection.perform_query(query))
        # errors = []
        # if not results[0][0]:
        #     errors.append(f"User '{self.user}' must have REPLICATION privileges.")
        # if not results[0][1]:
        #     errors.append(f"User '{self.user}' must have LOGIN privileges.")
        # if errors:
        #     raise ExtractorUserException('\n'.join(errors))
        pass

    def close_connection(self):
        logging.debug("Closing the outer connection.")
        self.connection.connection.close()
