"""
Template Component main class.

"""
import base64
import logging
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from functools import cached_property
from pathlib import Path

from keboola.component import CommonInterface
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
# configuration variables
from keboola.component.sync_actions import SelectElement, ValidationResult

from db_components.db_common import artefacts
from db_components.db_common.ssh.ssh_utils import create_ssh_tunnel, SomeSSHException, generate_ssh_key_pair
from db_components.db_common.staging import Staging, DuckDBStagingExporter
from db_components.db_common.table_schema import TableSchema, ColumnSchema, init_table_schema_from_dict
from db_components.debezium.common import get_schema_change_table_metadata
from db_components.debezium.executor import DebeziumExecutor, DebeziumException, DuckDBParameters, LoggerOptions, \
    MySQLStoppingCondition, StoppingCondition
from db_components.ex_mysql_cdc.src.configuration import Configuration, DbOptions, SnapshotMode, Adapter
from db_components.ex_mysql_cdc.src.extractor.mysql_extractor import MySQLDebeziumExtractor, \
    build_debezium_property_file, MySQLBaseTypeConverter
from db_components.ex_mysql_cdc.src.extractor.mysql_extractor import SUPPORTED_TYPES
from extractor.mysql_extractor import ExtractorUserException

COMPONENT_TIMEOUT = 85500

SCHEMA_CHANGE_TABLE_NAME = 'io_debezium_connector_mysql_SchemaChangeValue'

SCHEMA_HISTORY_FILENAME = 'schema_history.jsonl'

DUCK_DB_DIR = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'duckdb_stage')

KEY_DEBEZIUM_SCHEMA = 'last_debezium_schema'

KEY_STAGING_TYPE = 'staging_type'

DEBEZIUM_CORE_PATH = os.environ.get(
    'DEBEZIUM_CORE_PATH') or "../../../debezium_core/jars/kbcDebeziumEngine-jar-with-dependencies.jar"
KEY_LAST_SYNCED_TABLED = 'last_synced_tables'

KEY_LAST_SCHEMA = "last_schema"

KEY_LAST_OFFSET = 'last_offset'

DEFAULT_TOPIC_NAME = 'testcdc'

REQUIRED_IMAGE_PARS = []


class MySqlCDCComponent(ComponentBase):
    SYSTEM_COLUMNS = [
        ColumnSchema(name="KBC__OPERATION", source_type="STRING"),
        ColumnSchema(name="KBC__EVENT_TIMESTAMP_MS", source_type="TIMESTAMP"),
        ColumnSchema(name="KBC__FILE", source_type="STRING"),
        ColumnSchema(name="KBC__POS", source_type="INTEGER"),
        ColumnSchema(name="KBC__DELETED", source_type="BOOLEAN"),
        ColumnSchema(name="KBC__BATCH_EVENT_ORDER", source_type="INTEGER")
    ]

    SYSTEM_COLUMN_NAME_MAPPING = {"kbc__operation": "KBC__OPERATION",
                                  "kbc__event_timestamp": "KBC__EVENT_TIMESTAMP_MS",
                                  "kbc__file": "KBC__FILE",
                                  "kbc__pos": "KBC__POS",
                                  "__deleted": "KBC__DELETED",
                                  "kbc__batch_event_order": "KBC__BATCH_EVENT_ORDER"}

    def __init__(self, data_path_override=None):
        super().__init__(data_path_override=data_path_override)
        self._client: MySQLDebeziumExtractor
        self._configuration: Configuration

        self._temp_offset_file = tempfile.NamedTemporaryFile(suffix='.dat', delete=False)
        self._temp_schema_history_file = Path(tempfile.gettempdir()).joinpath(SCHEMA_HISTORY_FILENAME).as_posix()
        self._signal_file = f'{self.data_folder_path}/signal.jsonl'
        self._source_schema_metadata: dict[str, TableSchema]

        self._staging: Staging

        if not self.configuration.parameters.get("debug"):
            logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

    def run(self):
        self._init_configuration()
        self.cleanup_duckdb()
        with self._init_client() as db_config:
            self._init_workspace_client()

            self._reconstruct_offsset_from_state()
            self._reconstruct_schema_history_file()
            sync_options = self._configuration.sync_options
            source_settings = self._configuration.source_settings
            logging.info(f"Running sync mode: {sync_options.snapshot_mode}")

            snapshot_overrides = sync_options.snapshot_statements
            debezium_properties = build_debezium_property_file(db_config.user, db_config.pswd_password,
                                                               db_config.host,
                                                               str(db_config.port),
                                                               self._temp_offset_file.name,
                                                               self._temp_schema_history_file,
                                                               source_settings.schemas,
                                                               source_settings.tables,
                                                               adapter=db_config.adapter,
                                                               column_filter_type=source_settings.column_filter_type,
                                                               column_filter=source_settings.column_filter,
                                                               server_id_unique=self._build_unique_server_id(),
                                                               snapshot_mode=self.get_snapshot_mode(),
                                                               signal_table=sync_options.source_signal_table,
                                                               signal_file=self._signal_file,
                                                               snapshot_fetch_size=sync_options.snapshot_fetch_size,
                                                               snapshot_max_threads=sync_options.snapshot_threads,
                                                               snapshot_statement_overrides=snapshot_overrides,
                                                               binary_handling_mode=sync_options.handle_binary.name,
                                                               max_batch_size=sync_options.batch_size,
                                                               max_queue_size=sync_options.queue_size,
                                                               read_only=sync_options.ro_mode)

            self._collect_source_metadata()

            if not os.path.exists(DEBEZIUM_CORE_PATH):
                raise Exception(f"Debezium jar not found at {DEBEZIUM_CORE_PATH}")

            log_artefact_path = os.path.join(self.data_folder_path, "artifacts", "out", "current", 'debezium.log')
            logging_properties = LoggerOptions(result_log_path=log_artefact_path, trace_mode=self._configuration.debug)
            if self.logging_type == 'gelf':
                logging_properties.gelf_host = f"tcp:{os.getenv('KBC_LOGGER_ADDR', 'localhost')}"
                logging_properties.gelf_port = int(os.getenv('KBC_LOGGER_PORT', 12201))

            duckdb_config = DuckDBParameters(self.duck_db_path,
                                             self.duck_db_tmp_dir,
                                             max_appender_cache_size=sync_options.max_duckdb_appender_cache_size,
                                             max_threads=sync_options.duckdb_threads)

            debezium_executor = DebeziumExecutor(properties_path=debezium_properties,
                                                 duckdb_config=duckdb_config,
                                                 logger_options=logging_properties,
                                                 jar_path=DEBEZIUM_CORE_PATH,
                                                 source_connection=self._client.connection, )

            newly_added_tables = self.get_newly_added_tables()
            if newly_added_tables:
                logging.warning(f"New tables detected: {newly_added_tables}. Running initial blocking snapshot.")
                channel = 'file' if sync_options.ro_mode else 'source'
                debezium_executor.signal_snapshot(newly_added_tables, 'blocking', channel=channel)

            logging.info("Running Debezium Engine")
            result_schema = debezium_executor.execute(self.tables_out_path,
                                                      stopping_condition=self._build_stopping_condition(),
                                                      mode='DEDUPE' if self.dedupe_required() else 'APPEND',
                                                      previous_schema=self.last_debezium_schema)

            start = time.time()
            result_tables = self._load_tables_to_stage(result_schema)
            end = time.time()
            logging.info(f"Load to stage finished in {end - start}")
            self.write_manifests([res[0] for res in result_tables])

            self._write_result_state(self._get_offest_string(), [res[1] for res in result_tables], result_schema)
            self._store_schema_history_file()

            self.cleanup_duckdb()

    def _build_stopping_condition(self) -> StoppingCondition:
        """
        Builds stopping condition based on the configuration specific for MySQL
        Returns:

        """

        max_duration_s = self._configuration.sync_options.max_runtime_s or COMPONENT_TIMEOUT
        file_name, position = self._client.get_target_position()
        return MySQLStoppingCondition(max_duration_s, self._configuration.sync_options.max_wait_s,
                                      file_name, position)

    def cleanup_duckdb(self):
        # cleanup duckdb (useful for local dev,to clean resources)
        if os.path.exists(DUCK_DB_DIR):
            shutil.rmtree(DUCK_DB_DIR)

    @cached_property
    def duck_db_path(self):
        duckdb_dir = DUCK_DB_DIR
        os.makedirs(duckdb_dir, exist_ok=True)
        tmpdb = tempfile.NamedTemporaryFile(suffix='_duckdb_stage.duckdb', delete=False, dir=duckdb_dir)
        os.remove(tmpdb.name)
        return tmpdb.name

    @cached_property
    def duck_db_tmp_dir(self):
        path = os.path.join(DUCK_DB_DIR, 'dbtmp')
        os.makedirs(path, exist_ok=True)
        return path

    def get_newly_added_tables(self) -> list[str]:
        """
        Returns new added tables in the last run
        Returns: List of tables that were added since the last run

        """
        last_synced_tabled = self.get_state_file().get(KEY_LAST_SYNCED_TABLED, [])
        new_tables = []
        # check only if it's not initial run.
        if not self.is_initial_run:
            new_tables = set(self.currently_synced_tables).difference(last_synced_tabled)

        return list(new_tables)

    @cached_property
    def currently_synced_tables(self) -> list[str]:
        """
        Returns currently synced tables.
        Returns: List of tables that are currently synced

        """
        all_tables = self._configuration.source_settings.tables
        if not all_tables:
            # if empty download all tables
            for schema in self._configuration.source_settings.schemas:
                tables = [f"{t[0]}.{t[2]}" for t in self._client.metadata_provider.get_tables(database=schema)]
                all_tables.extend(tables)

        return all_tables

    @contextmanager
    def _init_client(self) -> DbOptions:
        """
        Initialises client and establishes SSH tunnel if enabled.

        Returns: Updated DBOptions

        """
        params = self.configuration.parameters

        # fix eternal KBC issue
        if isinstance(params.get('db_settings', {}).get('ssh_options'), list):
            params['db_settings']['ssh_options'] = {}

        config: DbOptions = DbOptions.load_from_dict(params['db_settings'])
        tunnel = None
        self._client: MySQLDebeziumExtractor = None
        try:
            if config.ssh_options.sshHost:
                tunnel = create_ssh_tunnel(config.ssh_options, config.host, config.port)
                tunnel.start()
                config.host = config.ssh_options.LOCAL_BIND_ADDRESS
                config.port = config.ssh_options.LOCAL_BIND_PORT

            self._client = MySQLDebeziumExtractor(config, is_maria_db=config.adapter == Adapter.mariadb)
            try:
                self._client.connect()
                self._client.test_has_replication_privilege()
            except Exception as e:
                raise UserException(f"Failed to connect to database. {e}") from e

            yield config

        except SomeSSHException as e:
            raise UserException(f"Failed to establish SSH connection. {e}") from e
        except Exception as e:
            raise e
        finally:
            try:
                if self._client:
                    self._client.close_connection()
                if tunnel:
                    tunnel.stop(True)
            except Exception:
                pass

    def _init_workspace_client(self):
        logging.info("Using DuckDB staging")
        self._staging = DuckDBStagingExporter(self.duck_db_path)

    def _init_configuration(self):
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        params = self.configuration.parameters
        # fix eternal KBC issue
        if isinstance(params.get('db_settings', {}).get('ssh_options'), list):
            params['db_settings']['ssh_options'] = {}
        self._configuration: Configuration = Configuration.load_from_dict(
            params)

    def _reconstruct_offsset_from_state(self):
        """
        Reconstructs the Debezium offset file from the state file
        Returns:

        """
        last_state = self.get_state_file()

        if last_state.get(KEY_LAST_OFFSET):
            last_state = base64.b64decode(last_state[KEY_LAST_OFFSET].encode('ascii'))
            with open(self._temp_offset_file.name, 'wb') as outp:
                outp.write(last_state)
        elif self._configuration.sync_options.snapshot_mode == SnapshotMode.initial:
            logging.warning("No State found, running full sync.")

    def _reconstruct_schema_history_file(self):
        """
        Reconstructs the Debezium schema history file from the artefacts
        Returns:

        """
        # TODO: support all stacks
        existing_file, tags, rid = artefacts.get_artefact(SCHEMA_HISTORY_FILENAME, self, ['debezium'])

        if existing_file and not self.is_initial_run:
            shutil.move(existing_file, self._temp_schema_history_file)
        elif self.is_initial_run:
            logging.warning("No schema history file found, running initial load.")
        else:
            raise UserException("No schema history file found. This can happen when the file expires, "
                                "the configuration wasn't run for more then 14 days. Initial run is required "
                                "(reset the component state).")

    def _get_offest_string(self) -> str:
        image_data_binary = open(self._temp_offset_file.name, 'rb').read()
        return (base64.b64encode(image_data_binary)).decode('ascii')

    def _collect_source_metadata(self):
        """
        Collects metadata such as table and columns schema for the monitored tables from the source database.
        Returns:

        """
        table_schemas = dict()
        tables_to_collect = self.currently_synced_tables
        # in cae the signalling table is not in the synced tables list
        if (self._configuration.sync_options.source_signal_table
                and self._configuration.sync_options.source_signal_table not in tables_to_collect):
            tables_to_collect.append(self._configuration.sync_options.source_signal_table)

        for table in tables_to_collect:
            schema, table = table.split('.')
            ts = self._client.metadata_provider.get_table_metadata(database=schema,
                                                                   table_name=table)
            # TODO: change the topic name (testcdc)
            table_schemas[f"{DEFAULT_TOPIC_NAME}_{schema}_{table}"] = ts

        # schema changes table
        table_schemas[SCHEMA_CHANGE_TABLE_NAME] = get_schema_change_table_metadata(
            database_name='io_debezium_connector_mysql')

        self._source_schema_metadata = table_schemas

    def _load_tables_to_stage(self, debezium_result_schema: dict) -> list[tuple[TableDefinition, TableSchema]]:
        with self._staging.connect():
            result_table_defs = []
            for table, nr_chunks in self.get_extracted_tables().items():
                if table not in self._source_schema_metadata:
                    logging.warning(f"Table {table} not found in source metadata. Skipping.")
                    continue
                result_table_defs.append(self._process_table_in_stage(table, nr_chunks, debezium_result_schema))

        return result_table_defs

    def get_extracted_tables(self) -> dict[str, int]:
        """
        Get all tables extracted from the staging and number of chunks (0 if not chunked)
        Returns: Dict of tables and chunked flag

        """
        tables = dict()
        for table in self._staging.get_extracted_tables():
            if '_chunk_' in table:
                tables[table.split('_chunk_')[0]] = tables.get(table.split('_chunk_')[0], 0) + 1
            else:
                tables[table] = 0
        return tables

    def _process_table_in_stage(self, table_key: str, nr_chunks: int,
                                debezium_result_schema: dict) -> tuple[TableDefinition, TableSchema]:
        """
        Processes table in stage. Creates table definition and schema based on the result schema.
        Args:
            table_key:
            nr_chunks: Number of chunks
            debezium_result_schema: Result schema from Debezium run

        Returns:

        """
        staging_key = table_key
        if nr_chunks > 0:
            # get latest schema
            staging_key = table_key + f'_chunk_{nr_chunks - 1}'
        result_schema = self._staging.get_table_schema(staging_key)

        schema = self._get_source_table_schema(table_key, debezium_result_schema)
        self.sort_columns_by_result(schema, result_schema)

        incremental_load = self._configuration.destination.is_incremental_load
        # remove primary key when using append mode
        if (self._configuration.destination.load_type in ('append_incremental', 'append_full')
                and table_key != SCHEMA_CHANGE_TABLE_NAME):
            schema.primary_keys = []
        elif not schema.primary_keys:
            logging.warning(f"No primary keys found for table {table_key}, building primary key using all attributes.")
            schema.primary_keys = [c.name for c in schema.fields if c not in self.SYSTEM_COLUMNS]

        self._convert_to_snowflake_column_definitions(schema.fields)
        table_definition = self.create_out_table_definition_from_schema(schema, incremental=incremental_load)

        logging.info(f"Creating table {table_key} in stage")

        self._staging.process_table(table_key, table_definition.full_path, self.dedupe_required(), schema.primary_keys,
                                    list(result_schema.keys()))

        if table_key == SCHEMA_CHANGE_TABLE_NAME:
            # always incrementally load schema changes
            incremental_load = True

        output_bucket = self.generate_output_bucket_name()
        result_table_name = table_definition.name.replace('.csv', '')
        return self.create_out_table_definition_from_schema(schema, incremental=incremental_load,
                                                            destination=f"{output_bucket}.{result_table_name}"), schema

    def _convert_to_snowflake_column_definitions(self, columns: list[ColumnSchema]) -> list[dict[str, str]]:
        column_types = []
        for c in columns:

            dtype = c.base_type
            # Only NUMERIC types can have length
            length_clause = ''
            if c.length and c.base_type.upper() in ['NUMERIC', 'STRING'] and c.source_type in SUPPORTED_TYPES:
                # Postgres return full integer value if no length is specified
                if c.base_type.upper() == 'STRING' and c.length > 16777216:
                    c.length = 16777216

                length_clause += str(c.length)
            if c.precision and c.base_type.upper() in ['NUMERIC'] and c.source_type in SUPPORTED_TYPES:
                length_clause += f', {c.precision}'

            if length_clause:
                dtype += f'({length_clause})'

            column_types.append({"name": c.name, "type": dtype})
        return column_types

    def _get_source_table_schema(self, table_key: str, debezium_result_schema: dict) -> TableSchema:
        """
        Returns complete table schema from the source database
        including metadata fields and fields already existing in Storage
        Args:
            table_key:
            debezium_result_schema: result total schema in debezium

        Returns:

        """
        schema = self._source_schema_metadata[table_key]
        # do not modify if schemachange table
        if table_key == SCHEMA_CHANGE_TABLE_NAME:
            return schema

        last_schema = self.previous_storage_schema.get(table_key)
        debezium_key = f'{DEFAULT_TOPIC_NAME}.{schema.database_name}.{schema.name}'

        current_result_fields = [c['field'] for c in debezium_result_schema[debezium_key]]

        if last_schema:
            current_columns = [c.name for c in schema.fields]
            # Expand current schema with columns existing in storage
            for c in last_schema.fields:
                if not c.name.startswith('KBC__') and c.name not in current_columns:
                    # set nullable to true because it's a missing column
                    c.nullable = True
                    schema.fields.append(c)

                if not c.name.startswith('KBC__') and c.name not in current_result_fields:
                    # in case the current filter excluded existing columns in Storage, add them
                    current_result_fields.append(c.name)

        # remove all columns that are not in the result schema
        schema.fields = [c for c in schema.fields if c.name in current_result_fields]

        # add system fields
        schema.fields.extend(self.SYSTEM_COLUMNS)
        return schema

    def _write_result_state(self, offset: str, table_schemas: list[TableSchema], debezium_schema: dict):
        """
        Writes state file with last offset and last schema and last synced tables
        Args:
            offset:
            table_schemas:

        Returns:

        """

        state = {KEY_LAST_OFFSET: offset,
                 KEY_LAST_SCHEMA: {},
                 KEY_DEBEZIUM_SCHEMA: debezium_schema,
                 KEY_LAST_SYNCED_TABLED: self.currently_synced_tables}

        for schema in table_schemas:
            schema_key = self.generate_table_key(schema)
            state[KEY_LAST_SCHEMA][schema_key] = schema.as_dict()
        self.write_state_file(state)

    def generate_table_key(self, schema):
        schema_key = f"{DEFAULT_TOPIC_NAME}_{schema.database_name}_{schema.name}"
        return schema_key

    def _store_schema_history_file(self):
        """
        Stores the schema history file as an artefact
        Returns:

        """
        artefacts.store_artefact(self._temp_schema_history_file, self, ['debezium'])

    def dedupe_required(self) -> bool:
        """
        dedupe only if running sync from binlog and not in append_incremental mode.
        Initial run will always skip syncing from binlog.
        Returns:

        """
        return self._configuration.destination.load_type not in (
            'append_incremental', 'append_full') and not self.is_initial_run

    def generate_output_bucket_name(self):
        """
        Creates output bucket name based on the configuration or environment variables
        Args:
            bucket_name:

        Returns:

        """
        bucket_name = self._configuration.destination.outputBucket
        if bucket_name and bucket_name.strip() != '':
            return f'in.c-{bucket_name.strip()}'

        else:
            _component_id = self.environment_variables.component_id
            _configuration_id = self.environment_variables.config_id
            logging.debug(f"Env {_component_id} - {_configuration_id}")

            if _component_id is not None and _component_id is not None:
                return f"in.c-{_component_id.replace('.', '-')}-{_configuration_id}"

            else:
                return None

    @cached_property
    def is_initial_run(self):
        return self.get_state_file().get(KEY_LAST_OFFSET) is None

    @cached_property
    def last_debezium_schema(self) -> dict:
        return self.get_state_file().get(KEY_DEBEZIUM_SCHEMA, {})

    @cached_property
    def previous_storage_schema(self) -> dict[str, TableSchema]:
        schemas_dict: dict = self.get_state_file().get(KEY_LAST_SCHEMA, dict())
        schema_map = dict()
        if schemas_dict:
            for key, value in schemas_dict.items():
                schema_map[key] = init_table_schema_from_dict(value, MySQLBaseTypeConverter())

        return schema_map

    def get_snapshot_mode(self) -> str:
        """
        Returns snapshot mode to handle specific behaviour of the connector
        Returns:

        """
        if self.is_initial_run and self._configuration.sync_options.snapshot_mode == SnapshotMode.never:
            # if initial run and never is set, schema needs to be downloaded first
            snapshot_mode = 'schema_only'
            logging.warning("Initial run with snapshot mode 'Changes Only'."
                            " Running schema only recovery to record table schema. "
                            "The actual sync will start next execution")
        elif self.is_initial_run:
            # run only initial snapshot at the start
            snapshot_mode = 'initial_only'
        else:
            snapshot_mode = self._configuration.sync_options.snapshot_mode.name
        return snapshot_mode

    # SYNC ACTIONS
    @sync_action('testConnection')
    def test_connection(self):
        with self._init_client():
            self._client.test_connection()

    @sync_action('get_schemas')
    def get_schemas(self):
        with self._init_client():
            schemas = self._client.metadata_provider.get_catalogs()
            return [
                SelectElement(schema) for schema in schemas
            ]

    @sync_action('get_tables')
    def get_tables(self):
        with self._init_client():
            self._init_configuration()
            if not self._configuration.source_settings.schemas:
                raise UserException("Schema must be selected first!")
            tables = []
            for s in self._configuration.source_settings.schemas:
                tables.extend(self._client.metadata_provider.get_tables(database=s))
            return [SelectElement(f"{table[0]}.{table[2]}") for table in tables]

    @sync_action("generate_ssh_key")
    def generate_ssh_key(self):
        private_key, public_key = generate_ssh_key_pair()
        md_message = f"**Private Key**  (*Copy this to the `Private Key` configuration field*):\n\n" \
                     f"```\n{private_key}\n```\n\n" \
                     f"**Public Key**  (*Add this to your servers `ssh_keys`*): \n\n```\n{public_key}\n```"

        return ValidationResult(message=md_message)

    def _normalize_columns(self, csv_columns: list[str]) -> list[str]:
        """
        Normalizes result fields based on configuration.
        Modifies cases of the system fields
        Args:
            csv_columns:

        Returns:

        """
        new_columns = []
        for c in csv_columns:
            new_col = self.SYSTEM_COLUMN_NAME_MAPPING.get(c, c)
            new_columns.append(new_col)

        # TODO: Add upper/lower case conversions
        return new_columns

    def _build_unique_server_id(self) -> int:
        """
        Returns unique server id based on the configuration and context in the project it runs to ensure uniqueness.
        Returns:

        """
        config_id = self.environment_variables.config_id or 1
        branch_id = self.environment_variables.branch_id or 2
        project_id = self.environment_variables.project_id or 3

        # make sure it's not exceeding the max Long value
        unique_string = f"{project_id}{config_id}{branch_id}"
        server_id = hash(unique_string) % (2 ** 30)
        logging.info(f"Unique server id: {server_id}")
        return server_id

    def sort_columns_by_result(self, table_schema: TableSchema, result_schema: dict):
        """
        Sorts columns based on the result schema
        Args:
            table_schema:
            result_schema:

        Returns:

        """
        # rename column names of result schema
        result_order = self._normalize_columns([c for c in result_schema])
        table_schema.fields = sorted(table_schema.fields, key=lambda x: result_order.index(x.name))

    @property
    def logging_type(self) -> str:
        return CommonInterface.LOGGING_TYPE_GELF if os.getenv('KBC_LOGGER_ADDR',
                                                              None) else CommonInterface.LOGGING_TYPE_STD


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if work_dir := os.environ.get('WORKING_DIR'):
        os.chdir(work_dir)
    try:
        comp = MySqlCDCComponent()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except (UserException, ExtractorUserException) as exc:
        logging.exception(exc)
        exit(1)
    except DebeziumException as exc:
        logging.exception(exc, extra=exc.extra)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
