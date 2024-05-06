"""
Template Component main class.

"""
import base64
from functools import cached_property
import glob
import logging
from pathlib import Path
import os
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

from configuration import Configuration, DbOptions, SnapshotMode
from extractor.oracle_extractor import OracleDebeziumExtractor
from extractor.oracle_extractor import SUPPORTED_TYPES
from extractor.oracle_extractor import build_debezium_property_file
from ssh.ssh_utils import create_ssh_tunnel, SomeSSHException, generate_ssh_key_pair

from db_components.db_common import artefacts
from db_components.db_common.table_schema import TableSchema, ColumnSchema, init_table_schema_from_dict
from db_components.db_common.staging import Staging, DuckDBStagingExporter
from db_components.debezium.executor import DebeziumExecutor, DebeziumException, DuckDBParameters, LoggerOptions

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement, ValidationResult

DEBEZIUM_CORE_PATH = os.environ.get(
    'DEBEZIUM_CORE_PATH') or "../../../debezium_core/jars/kbcDebeziumEngine-jar-with-dependencies.jar"

DUCK_DB_DIR = os.path.join('/tmp', 'duckdb_stage')
SCHEMA_HISTORY_FILENAME = 'schema_history.jsonl'

KEY_DEBEZIUM_SCHEMA = 'last_debezium_schema'
KEY_STAGING_TYPE = 'staging_type'
KEY_LAST_SYNCED_TABLES = 'last_synced_tables'
KEY_LAST_SCHEMA = "last_schema"
KEY_LAST_OFFSET = 'last_offset'
DEFAULT_TOPIC_NAME = 'testcdc'

REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    SYSTEM_COLUMNS = [
        ColumnSchema(name="KBC__OPERATION", source_type="STRING"),
        ColumnSchema(name="KBC__EVENT_TIMESTAMP_MS", source_type="TIMESTAMP"),
        ColumnSchema(name="KBC__DELETED", source_type="BOOLEAN"),
        ColumnSchema(name="KBC__BATCH_EVENT_ORDER", source_type="INTEGER")]

    SYSTEM_COLUMN_NAME_MAPPING = {"kbc__operation": "KBC__OPERATION",
                                  "kbc__event_timestamp": "KBC__EVENT_TIMESTAMP_MS",
                                  "__deleted": "KBC__DELETED",
                                  "kbc__batch_event_order": "KBC__BATCH_EVENT_ORDER"}

    def __init__(self, data_path_override=None):
        super().__init__(data_path_override=data_path_override)
        self._client: OracleDebeziumExtractor
        self._configuration: Configuration

        self._temp_offset_file = tempfile.NamedTemporaryFile(suffix='.dat', delete=False)
        self._temp_schema_history_file = Path(tempfile.gettempdir()).joinpath(SCHEMA_HISTORY_FILENAME).as_posix()
        self._signal_file = f'{self.data_folder_path}/signal.jsonl'
        self._source_schema_metadata: dict[str, TableSchema]

        self._staging: Staging

        self._last_schema: dict[str, TableSchema] = self._get_schemas_from_state()

        if not self.configuration.parameters.get("debug"):
            logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

    def run(self):
        self._init_configuration()
        with self._init_client() as db_config:
            self._init_workspace_client()

            self._reconstruct_offset_from_state()
            self._reconstruct_schema_history_file()
            sync_options = self._configuration.sync_options

            prev_schema_history_file = self.get_artifact_full_in_path("dbhistory.dat")
            schema_history_file = self.get_artifact_full_out_path("dbhistory.dat")
            if os.path.exists(prev_schema_history_file):
                os.rename(prev_schema_history_file, schema_history_file)

            logging.info(f"Running sync mode: {sync_options.snapshot_mode}")

            debezium_properties = build_debezium_property_file(
                user=db_config.user,
                password=db_config.pswd_password,
                hostname=db_config.host,
                port=str(db_config.port),
                database=db_config.database,
                p_database=db_config.p_database,
                offset_file_path=self._temp_offset_file.name,
                schema_whitelist=self._configuration.source_settings.schemas,
                table_whitelist=self._configuration.source_settings.tables,
                schema_history_filepath=self._temp_schema_history_file,
                snapshot_mode=self.get_snapshot_mode(),
                signal_table=sync_options.source_signal_table,
                snapshot_fetch_size=sync_options.snapshot_fetch_size,
                snapshot_max_threads=sync_options.snapshot_threads,
                repl_suffix=self._build_unique_replication_suffix()
            )

            self._collect_source_metadata()

            if not os.path.exists(DEBEZIUM_CORE_PATH):
                raise Exception(f"Debezium jar not found at {DEBEZIUM_CORE_PATH}")

            log_artefact_path = os.path.join(self.data_folder_path, "artifacts", "out", "current", 'debezium.log')
            logging_properties = LoggerOptions(result_log_path=log_artefact_path)

            debezium_executor = DebeziumExecutor(properties_path=debezium_properties,
                                                 duckdb_config=DuckDBParameters(self.duck_db_path),
                                                 logger_options=logging_properties,
                                                 jar_path=DEBEZIUM_CORE_PATH,
                                                 source_connection=self._client.connection)

            newly_added_tables = self.get_newly_added_tables()
            if newly_added_tables:
                logging.warning(f"New tables detected: {newly_added_tables}. Running initial blocking snapshot.")
                debezium_executor.signal_snapshot(newly_added_tables, 'blocking', channel='file')

            logging.info("Running Debezium Engine")
            result_schema = debezium_executor.execute(self.tables_out_path,
                                                      mode='DEDUPE' if self.dedupe_required() else 'APPEND',
                                                      max_duration_s=8000,
                                                      max_wait_s=self._configuration.sync_options.max_wait_s,
                                                      previous_schema=self.last_debezium_schema)

            start = time.time()
            result_tables = self._load_tables_to_stage()
            end = time.time()
            logging.info(f"Load to stage finished in {end - start}")
            self.write_manifests([res[0] for res in result_tables])

            self._write_result_state(self._get_offset_string(), [res[1] for res in result_tables], result_schema)
            self._store_schema_history_file()

            self.cleanup_duckdb()

    @staticmethod
    def cleanup_duckdb():
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
    def last_debezium_schema(self) -> dict:
        return self.get_state_file().get(KEY_DEBEZIUM_SCHEMA, {})

    def get_newly_added_tables(self) -> list[str]:
        """
        Returns new added tables in the last run
        Returns: List of tables that were added since the last run

        """
        last_synced_tabled = self.get_state_file().get(KEY_LAST_SYNCED_TABLES, [])
        new_tables = []
        # check only if it's not initial run.
        if not self.is_initial_run:
            new_tables = set(self._configuration.source_settings.tables).difference(last_synced_tabled)

        return list(new_tables)

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
        self._client: OracleDebeziumExtractor = None
        try:
            if config.ssh_options.enabled:
                tunnel = create_ssh_tunnel(config.ssh_options, config.host, config.port)
                tunnel.start()
                config.host = config.ssh_options.LOCAL_BIND_ADDRESS
                config.port = config.ssh_options.LOCAL_BIND_PORT
            self._client = OracleDebeziumExtractor(config)
            try:
                self._client.connect()
                self._client.test_has_privileges()
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
        self._configuration: Configuration = Configuration.load_from_dict(params)

    def _reconstruct_offset_from_state(self):
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
        existing_file, tags = artefacts.get_artefact(SCHEMA_HISTORY_FILENAME, self, ['debezium'])

        if existing_file and not self.is_initial_run:
            shutil.move(existing_file, self._temp_schema_history_file)
        elif self.is_initial_run:
            logging.warning("No schema history file found, running initial load.")
        else:
            raise UserException("No schema history file found. This can happen when the file expires, "
                                "the configuration wasn't run for more then 14 days. Initial run is required "
                                "(reset the component state).")

    def _get_offset_string(self) -> str:
        image_data_binary = open(self._temp_offset_file.name, 'rb').read()
        return (base64.b64encode(image_data_binary)).decode('ascii')

    def _get_schemas_from_state(self) -> dict[str, TableSchema]:
        schemas_dict: dict = self.get_state_file().get(KEY_LAST_SCHEMA, dict())
        schema_map = dict()
        if schemas_dict:
            for key, value in schemas_dict.items():
                schema_map[key] = init_table_schema_from_dict(value)
        return schema_map

    def _collect_source_metadata(self):
        """
        Collects metadata such as table and columns schema for the monitored tables from the source database.
        Returns:

        """
        table_schemas = dict()
        tables_to_collect = self._configuration.source_settings.tables
        # in case the signalling table is not in the synced tables list

        if self._configuration.sync_options.source_signal_table not in tables_to_collect:
            tables_to_collect.append(self._configuration.sync_options.source_signal_table)

        logging.info(f"Collecting metadata for tables: {tables_to_collect}")
        for table in tables_to_collect:
            schema, table = table.split('.')
            ts = self._client.metadata_provider.get_table_metadata(schema=schema,
                                                                   table_name=table)

            table_schemas[f"{schema}.{table}"] = ts
        self._source_schema_metadata = table_schemas

    def _load_tables_to_stage(self) -> list[tuple[TableDefinition, TableSchema]]:
        result_tables = glob.glob(os.path.join(self.tables_out_path, '*.csv'))
        filtered_tables = [table for table in result_tables if not table.endswith('io.debezium.connector.oracle'
                                                                                  '.SchemaChangeValue.csv')]
        result_table_defs = []

        if self._staging.multi_threading_support:
            with self._staging.connect():
                with ThreadPoolExecutor(max_workers=self._configuration.max_workers) as executor:
                    futures = {
                        executor.submit(self._create_table_in_stage, table): table for
                        table in filtered_tables
                    }
                    for future in as_completed(futures):
                        if future.exception():
                            raise UserException(
                                f"Could not create table: {futures[future]}, reason: {future.exception()}")

                        result_table_defs.append(future.result())
        else:
            for table in filtered_tables:
                result_table_defs.append(self._create_table_in_stage(table))

        return result_table_defs

    def _create_table_in_stage(self, table_path: str) -> tuple[TableDefinition, TableSchema]:
        table_key = os.path.basename(table_path).split('.csv')[0].split('.', 1)[1]

        result_table_name = table_key.replace('.', '_')
        schema = self._get_table_schema(table_key)

        logging.info(f"Creating table {result_table_name} in stage")

        self._staging.process_table(table_path, result_table_name, schema, self.dedupe_required())

        incremental_load = self._configuration.destination.is_incremental_load
        # remove primary key when using append mode
        if self._configuration.destination.load_type in ('append_incremental', 'append_full'):
            schema.primary_keys = []

        return self.create_out_table_definition_from_schema(schema, incremental=incremental_load), schema

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

    def _get_table_schema(self, table_key: str) -> TableSchema:
        """
        Returns complete table schema including metadata fields and fields already existing in Storage
        Args:
            table_key:

        Returns:
        """
        _table_key = self.handle_pluggable_prefix(table_key)
        schema = self._source_schema_metadata[_table_key]

        last_schema = self._last_schema.get(table_key)

        if last_schema:
            current_columns = [c.name for c in schema.fields]
            # Expand current schema with columns existing in storage
            for c in last_schema.fields:
                if not c.name.startswith('KBC__') and c.name not in current_columns:
                    schema.fields.append(c)

        # add system fields
        schema.fields.extend(self.SYSTEM_COLUMNS)
        return schema

    @staticmethod
    def is_pluggable(name: str) -> bool:
        if name.upper().startswith("C__"):
            return True
        return False

    def handle_pluggable_prefix(self, name: str) -> str:
        logging.info(f"Checking pluggable prefix for table {name}")
        if self.is_pluggable(name):
            new_name = name.replace('C__', 'C##')
            logging.info(f"Pluggable prefix detected, replacing {name} with {new_name}")
            return new_name
        return name

    def _drop_helper_columns(self, table_name: str, schema: TableSchema):
        # drop helper column
        logging.debug(f'Dropping temp column {self.SYSTEM_COLUMN_NAME_MAPPING["kbc__batch_event_order"]} '
                      f'from table {table_name}')
        query = f'ALTER TABLE "{table_name}" DROP "{self.SYSTEM_COLUMN_NAME_MAPPING["kbc__batch_event_order"]}"'
        self._staging.execute_query(query)

        logging.debug(f'Dropping temp column {self.SYSTEM_COLUMN_NAME_MAPPING["kbc__operation"]} '
                      f'from table {table_name}')
        query = f'ALTER TABLE "{table_name}" DROP "{self.SYSTEM_COLUMN_NAME_MAPPING["kbc__operation"]}"'
        self._staging.execute_query(query)

        schema.remove_column(self.SYSTEM_COLUMN_NAME_MAPPING['kbc__batch_event_order'])
        schema.remove_column(self.SYSTEM_COLUMN_NAME_MAPPING['kbc__operation'])

    def _dedupe_stage_table(self, table_name: str, id_columns: list[str]):
        """
        Dedupe staging table and keep only latest records.
        Based on the internal column kbc__batch_event_order produced by CDC engine
        Args:
            table_name:
            id_columns:

        Returns:

        """
        id_cols = self._staging.wrap_columns_in_quotes(id_columns)
        id_cols_str = ','.join([f'"{table_name}".{col}' for col in id_cols])
        unique_id_concat = (f"CONCAT_WS('|',{id_cols_str},"
                            f"\"{self.SYSTEM_COLUMN_NAME_MAPPING['kbc__batch_event_order']}\")")

        query = f"""DELETE FROM
                                    "{table_name}" USING (
                                    SELECT
                                        {unique_id_concat} AS "__CONCAT_ID"
                                    FROM
                                        "{table_name}"
                                        QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_cols_str} ORDER BY
                          "{self.SYSTEM_COLUMN_NAME_MAPPING["kbc__batch_event_order"]}"::INT DESC) != 1) TO_DELETE
                                WHERE
                                    TO_DELETE.__CONCAT_ID = {unique_id_concat}
                    """

        logging.debug(f'Dedupping table {table_name}: {query}')
        self._staging.execute_query(query)

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
                 KEY_LAST_SYNCED_TABLES: self._configuration.source_settings.tables}

        for schema in table_schemas:
            schema_key = self.generate_table_key(schema)
            state[KEY_LAST_SCHEMA][schema_key] = schema.as_dict()
        self.write_state_file(state)

    @staticmethod
    def generate_table_key(schema):
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
        # TODO: Dedupe only when not init sync with no additional events.
        return self._configuration.destination.load_type not in (
            'append_incremental', 'append_full')

    @property
    def is_initial_run(self):
        return self.get_state_file().get(KEY_LAST_OFFSET) is None

    def get_snapshot_mode(self) -> str:
        """
        Returns snapshot mode based on configuration and initial run.
        Note that initial run is always in initial_only mode to avoid the necessity for deduping.
        Returns:

        """
        if self.is_initial_run and self._configuration.sync_options.snapshot_mode != SnapshotMode.never:
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
            schemas = self._client.metadata_provider.get_schemas()
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
                tables.extend(self._client.metadata_provider.get_tables(schema_pattern=s))
            return [SelectElement(f"{table[0]}.{table[1]}") for table in tables]

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

    def _build_unique_replication_suffix(self):
        """
        Returns unique publication name based on configuration and branch id
        Returns:

        """
        config_id = self.environment_variables.config_id
        branch_id = self.environment_variables.branch_id
        if branch_id:
            suffix = f"dev_{branch_id}"
        else:
            suffix = "prod"

        return f"kbc_{config_id}_{suffix}"

    def get_artifact_full_out_path(self, file_name: str) -> str:
        """
        Args:
            file_name: Name of the file to get from the artifact folder
        Returns: Full path to the artifact file
        """
        artifact_out_dir = os.path.join(self.data_folder_path, "artifacts", "out", "current")
        os.makedirs(artifact_out_dir, exist_ok=True)

        full_path = os.path.join(artifact_out_dir, file_name)
        return full_path

    def get_artifact_full_in_path(self, file_name: str) -> str:
        """
        Args:
            file_name: Name of the file to get from the artifact folder
        Returns: Full path to the artifact file
        """
        artifact_in_dir = os.path.join(self.data_folder_path, "artifacts", "in", "current")

        if os.path.exists(artifact_in_dir):
            full_path = os.path.join(artifact_in_dir, file_name)
            return full_path
        return ""


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if work_dir := os.environ.get('WORKING_DIR'):
        os.chdir(work_dir)
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except DebeziumException as exc:
        logging.exception(exc, extra=exc.extra)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
