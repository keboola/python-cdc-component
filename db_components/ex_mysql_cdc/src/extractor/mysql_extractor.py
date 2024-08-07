import logging
import logging.handlers
import tempfile
from pathlib import Path
from typing import Optional

from jaydebeapi import DatabaseError
from keboola.component import UserException

from db_components.db_common.db_connection import JDBCConnection
from db_components.db_common.metadata import JDBCMetadataProvider
from db_components.db_common.table_schema import BaseTypeConverter
from db_components.ex_mysql_cdc.src.configuration import DbOptions, ColumnFilterType, Adapter, SnapshotStatementOverride

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
                                 adapter: Adapter = Adapter.mysql,
                                 snapshot_mode: str = 'initial',
                                 signal_table: str = None,
                                 signal_file: str = None,
                                 snapshot_fetch_size: int = 10240,
                                 snapshot_max_threads: int = 1,
                                 snapshot_statement_overrides: list[SnapshotStatementOverride] = None,
                                 additional_properties: dict = None,
                                 binary_handling_mode: str = 'hex',
                                 max_batch_size: int = 2048,
                                 max_queue_size: int = 8192,
                                 read_only: bool = True
                                 ) -> str:
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
        adapter: Adapter type, MySQL or MariaDB
        additional_properties:
        snapshot_max_threads:
        snapshot_fetch_size: Maximum number of records to fetch from the database when performing an incremental
                             snapshot.
        snapshot_mode: 'initial' or 'never'
        snapshot_statement_overrides: List of statements to override the default snapshot statement.
        signal_table: Name of the table where the signals will be stored, fully qualified name, e.g. schema.table
        signal_file: Path to the file where the signals will be stored.
        repl_suffix: Suffixed to the publication and slot name to avoid name conflicts.
        read_only: enable read only incremental snapshot mode for the connector

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
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "binary.handling.mode": binary_handling_mode,
        "schema.history.internal": "io.debezium.storage.file.history.FileSchemaHistory",
        "schema.history.internal.file.filename": schema_history_file_path,
        "schema.history.internal.store.only.captured.tables.ddl": True,
        "database.include.list": schema_include,
        "table.include.list": table_include,
        "errors.max.retries": 3,
        "max.batch.size": max_batch_size,
        "max.queue.size": max_queue_size
    }

    if column_filter_type != ColumnFilterType.none:
        filter_key = f"column.{column_filter_type.value}.list"
        filter_value = ','.join(column_filter)
        properties[filter_key] = filter_value

    if snapshot_statement_overrides:
        properties["snapshot.select.statement.overrides"] = ','.join(
            [override.table for override in snapshot_statement_overrides])
        for override in snapshot_statement_overrides:
            properties[f"snapshot.select.statement.overrides.{override.table}"] = override.statement

    if adapter == Adapter.mariadb:
        properties["connector.adapter"] = "mariadb"
        properties["database.protocol"] = "jdbc:mariadb"
        properties["database.jdbc.driver"] = "org.mariadb.jdbc.Driver"
        properties["database.ssl.mode"] = "disabled"

    if read_only:
        # properties["read.only"] = True
        properties["signal.enabled.channels"] = "file"
        properties["signal.file"] = signal_file
    else:
        properties["signal.enabled.channels"] = "source"
        properties["signal.data.collection"] = signal_table

    properties |= additional_properties

    temp_file = tempfile.NamedTemporaryFile(suffix='.properties', delete=False)
    with open(temp_file.name, 'w+', newline='\n') as outp:
        for key, value in properties.items():
            outp.write(f'{key}={value}\n')

    return temp_file.name


class MySQLDebeziumExtractor:

    def __init__(self, db_credentials: DbOptions, is_maria_db: bool = False):
        self.__credentials = db_credentials
        # include both jars, so they are on classpath to enable functional tests and driver switching

        # get current file path
        jdbc_folder = Path(__file__).parent.parent.parent

        jars = [jdbc_folder.joinpath('jdbc/mysql-connector-j-8.3.0.jar').as_posix(),
                jdbc_folder.joinpath('jdbc/mariadb-java-client-3.4.0.jar').as_posix()]
        if not is_maria_db:
            driver_class = 'com.mysql.cj.jdbc.Driver'
            protocol = 'jdbc:mysql'
        else:
            driver_class = 'org.mariadb.jdbc.Driver'
            protocol = 'jdbc:mariadb'

        logging.debug(f'Driver {driver_class}')
        self.connection = JDBCConnection(driver_class,
                                         url=f'{protocol}://{db_credentials.host}:{db_credentials.port}',
                                         driver_args={'user': db_credentials.user,
                                                      'password': db_credentials.pswd_password},
                                         jars=jars)
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

    def get_target_position(self) -> tuple[str, int]:
        """
        Get the current position of the master.
        Returns: file name and position

        """
        query = "SHOW MASTER STATUS"
        results = list(self.connection.perform_query(query))
        if not results:
            raise UserException(
                "SHOW MASTER STATUS returned an empty set. Please check if the binary logging is enabled."
                "If you recently performed some changes to the server config, please restart the server.")
        logging.info(f"Master status: {results}")
        return results[0][0], int(str(results[0][1]))

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
