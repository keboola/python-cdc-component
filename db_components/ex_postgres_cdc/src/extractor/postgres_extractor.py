import logging
import logging.handlers
import tempfile

from jaydebeapi import DatabaseError

from db_components.db_common.db_connection import JDBCConnection
from db_components.db_common.metadata import JDBCMetadataProvider
from db_components.db_common.table_schema import BaseTypeConverter
from db_components.ex_postgres_cdc.src.configuration import DbOptions, HeartBeatConfig, ColumnFilterType, \
    SnapshotStatementOverride

JDBC_PATH = '../jdbc/postgresql-42.6.0.jar'


class ExtractorUserException(Exception):
    pass


class PostgresBaseTypeConverter(BaseTypeConverter):
    MAPPING = {"smallint": "INTEGER",
               "integer": "INTEGER",
               "int2": "INTEGER",
               "int4": "INTEGER",
               "int8": "INTEGER",
               "bigint": "INTEGER",
               "decimal": "NUMERIC",
               "numeric": "NUMERIC",
               "float4": "NUMERIC",
               "real": "NUMERIC",
               "double": "NUMERIC",
               "float8": "NUMERIC",
               "money": "NUMERIC",
               "smallserial": "INTEGER",
               "serial": "INTEGER",
               "serial4": "INTEGER",
               "bigserial": "INTEGER",
               "serial8": "INTEGER",
               "timestamp": "TIMESTAMP",
               "timestamptz": "TIMESTAMP",
               "date": "DATE",
               "time": "TIMESTAMP",
               "timetz": "TIMESTAMP",
               "boolean": "BOOLEAN",
               "bool": "BOOLEAN",
               "varchar": "STRING",
               "char": "STRING",
               "bpchar": "STRING",
               "text": "STRING"}

    def __call__(self, source_type: str, length: str = None) -> str:
        return self.MAPPING.get(source_type.lower(), 'STRING')


SUPPORTED_TYPES = list(PostgresBaseTypeConverter.MAPPING.keys())


def build_postgres_property_file(user: str, password: str, hostname: str, port: str, database: str,
                                 offset_file_path: str,
                                 schema_whitelist: list[str],
                                 table_whitelist: list[str],
                                 column_filter_type: ColumnFilterType,
                                 column_filter: list[str],
                                 snapshot_mode: str = 'initial',
                                 signal_table: str = None,
                                 snapshot_fetch_size: int = 10240,
                                 snapshot_max_threads: int = 1,
                                 snapshot_statement_overrides: list[SnapshotStatementOverride] = None,
                                 additional_properties: dict = None,
                                 repl_suffix: str = 'dbz',
                                 hearbeat_config: HeartBeatConfig = None) -> str:
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
        offset_file_path: Path to the file where the connector will store the offset.
        schema_whitelist: List of schemas to sync.
        table_whitelist: List of tables to sync.
        column_filter_type: Type of column filter, 'none', 'exclude' or 'include'
        column_filter: List of columns to include or exclude.
        additional_properties:
        snapshot_max_threads:
        snapshot_fetch_size: Maximum number of records to fetch from the database when performing an incremental
                             snapshot.
        snapshot_mode: 'initial' or 'never'
        snapshot_statement_overrides: List of statements to override the default snapshot statement.
        signal_table: Name of the table where the signals will be stored, fully qualified name, e.g. schema.table
        repl_suffix: Suffixed to the publication and slot name to avoid name conflicts.
        hearbeat_config: Configuration for the heartbeat signal.

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
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": hostname,
        "database.port": port,
        "database.user": user,
        "database.password": password,
        "database.dbname": database,
        "snapshot.max.threads": snapshot_max_threads,
        "snapshot.fetch.size": snapshot_fetch_size,
        "snapshot.mode": snapshot_mode,
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        "schema.include.list": schema_include,
        "table.include.list": table_include,
        "errors.max.retries": 3,
        "publication.autocreate.mode": "filtered",
        "publication.name": f'publication_{repl_suffix.lower()}',
        "slot.name": f'slot_{repl_suffix.lower()}',
        "plugin.name": "pgoutput",
        "signal.enabled.channels": "source",
        "signal.data.collection": signal_table}

    if column_filter_type != ColumnFilterType.none:
        filter_key = f"column.{column_filter_type.value}.list"
        filter_value = ','.join(column_filter)
        properties[filter_key] = filter_value

    if snapshot_statement_overrides:
        properties["snapshot.select.statement.overrides"] = ','.join(
            [override.table for override in snapshot_statement_overrides])
        for override in snapshot_statement_overrides:
            properties[f"snapshot.select.statement.overrides.{override.table}"] = override.statement

    if hearbeat_config:
        properties["heartbeat.interval.ms"] = hearbeat_config.interval_ms
        properties["heartbeat.action.query"] = hearbeat_config.action_query

    properties |= additional_properties

    temp_file = tempfile.NamedTemporaryFile(suffix='.properties', delete=False)
    with open(temp_file.name, 'w+', newline='\n') as outp:
        for key, value in properties.items():
            outp.write(f'{key}={value}\n')

    return temp_file.name


class PostgresDebeziumExtractor:

    def __init__(self, db_credentials: DbOptions, jdbc_path=JDBC_PATH):
        self.__credentials = db_credentials
        logging.debug(f'Driver {jdbc_path}')
        self.connection = JDBCConnection('org.postgresql.Driver',
                                         url=f'jdbc:postgresql://{db_credentials.host}:{db_credentials.port}'
                                             f'/{db_credentials.database}',
                                         driver_args={'user': db_credentials.user,
                                                      'password': db_credentials.pswd_password,
                                                      'database': db_credentials.database},
                                         jars=jdbc_path)
        self.user = db_credentials.user
        self.metadata_provider = JDBCMetadataProvider(self.connection, PostgresBaseTypeConverter())

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
        query = f"SELECT  rolreplication, rolcanlogin FROM pg_catalog.pg_roles r WHERE r.rolname = '{self.user}'"
        results = list(self.connection.perform_query(query))
        errors = []
        if not results[0][0]:
            errors.append(f"User '{self.user}' must have REPLICATION privileges.")
        if not results[0][1]:
            errors.append(f"User '{self.user}' must have LOGIN privileges.")
        if errors:
            raise ExtractorUserException('\n'.join(errors))

    def close_connection(self):
        logging.debug("Closing the outer connection.")
        self.connection.connection.close()
