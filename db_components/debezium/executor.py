import json
import logging
import os
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from typing import Tuple, Literal, Optional

from jproperties import Properties

from db_components.db_common.db_connection import JDBCConnection


class DebeziumException(Exception):
    def __init__(self, message, extra=None):
        super().__init__(message)
        self.extra = extra


class SnapshotSignal:
    def __init__(self, snapshot_id: str, table_names: list[str], snapshot_type: Literal['blocking', 'incremental']):
        self.id = snapshot_id
        self._table_names = table_names
        self._snapshot_type = snapshot_type

    def as_dict(self):
        return {"id": self.id, "type": "execute-snapshot",
                "data": {"type": self._snapshot_type.upper(), "data-collections": self._table_names}}

    @property
    def type(self) -> str:
        return 'execute-snapshot'


@dataclass
class DuckDBParameters:
    db_path: str
    tmp_dir_path: str
    max_threads: int = 3
    memory_limit: str = '800MB'
    memory_max: str = '800MB'
    dedupe_max_chunk_size: int = 5_000_000
    max_appender_cache_size: int = 500_000


@dataclass
class LoggerOptions:
    """
    Configuration for the GELF logger.

    Attributes:
        result_log_path: The path where the full log will be stored
        gelf_host: The host of the GELF server.
        gelf_port: The port of the GELF server.
    """
    result_log_path: str
    log4j_additional_properties: dict = None
    gelf_host: str = None
    gelf_port: int = None
    trace_mode: bool = False

    @property
    def gelf_enabled(self) -> bool:
        return bool(self.gelf_host)


class DebeziumExecutor:

    def __init__(self, properties_path: str, duckdb_config: DuckDBParameters, logger_options: LoggerOptions,
                 jar_path='cdc.jar', source_connection: Optional[JDBCConnection] = None):
        """
        Initialize the Debezium CDC engine with the given properties file and jar path.
        Args:
            properties_path:
            jar_path:
            source_connection: Optional JDBCConnection to the source database used for signaling
            duckdb_config: `DuckDBParameters` object with the configuration for the DuckDB instance
            logger_options: Optional GelfLoggerOptions object with the configuration for the GELF logger
        """
        self._jar_path = jar_path
        self._properties_path = properties_path
        self._keboola_properties_path = self.build_keboola_properties(duckdb_config)
        # print contents of the properties file
        with open(properties_path, 'r') as f:
            logging.info(f.read())

        self._source_connection = source_connection
        self.parsed_properties = self._parse_properties()
        self._log4j_properties = self.build_logger_properties(logger_options)
        self.logger_options = logger_options

    def _parse_properties(self) -> dict:
        with open(self._properties_path, 'rb') as config_file:
            configs = Properties()
            configs.load(config_file)
            db_configs_dict = {}
            # Iterate over the properties and add them to the dictionary
            for key, value in configs.items():
                db_configs_dict[key] = value.data
            return db_configs_dict

    def build_keboola_properties(self, duckdb_config: DuckDBParameters):
        """
        Append DuckDB configuration to the properties file.
        Args:
            duckdb_config:

        Returns:

        """
        temp_file = tempfile.NamedTemporaryFile(suffix='_keboola.properties', delete=False)
        with open(temp_file.name, 'w+') as config_file:
            config_file.write(f'keboola.duckdb.db.path={duckdb_config.db_path}\n')
            config_file.write(f'keboola.duckdb.temp.directory={duckdb_config.tmp_dir_path}\n')
            config_file.write(f'keboola.duckdb.max.threads={duckdb_config.max_threads}\n')
            config_file.write(f'keboola.duckdb.memory.limit={duckdb_config.memory_limit}\n')
            config_file.write(f'keboola.duckdb.memory.max={duckdb_config.memory_max}\n')
            config_file.write(f'keboola.converter.dedupe.max_chunk_size={duckdb_config.dedupe_max_chunk_size}\n')
            config_file.write(
                f'keboola.converter.dedupe.max_appender_cache_size={duckdb_config.max_appender_cache_size}\n')
        return temp_file.name

    def build_logger_properties(self, logger_options: LoggerOptions) -> str:
        """
        Append GELF logger configuration to the properties file.
        Args:
            logger_options: Configuration for the log4j logger

        Returns: Properties file path

        """
        temp_file = tempfile.NamedTemporaryFile(suffix='_log4j.properties', delete=False)

        log4j_config = Properties()
        with open(os.path.join(os.path.dirname(__file__), 'default_log4j.properties'), 'rb') as config_file:
            log4j_config.load(config_file)

        # default loggers
        log4j_config['logger.component.name'] = 'keboola.cdc.debezium'
        log4j_config['logger.component.level'] = 'info'
        log4j_config['logger.component.appenderRef.stdout.ref'] = 'consoleLogger'

        log4j_config['logger.debeziumchange.name'] = 'io.debezium.connector.mysql.MySqlStreamingChangeEventSource'
        log4j_config['logger.debeziumchange.level'] = 'warn'

        log4j_config['logger.debezium.name'] = 'io.debezium'
        log4j_config['logger.debezium.level'] = 'info'

        log4j_config['logger.debezium.additivity.io.debezium.connector'] = 'false'
        log4j_config['logger.debezium.additivity.io.debezium.relational.history'] = 'false'
        default_logger = 'consoleLogger'
        if logger_options.gelf_enabled:
            log4j_config['packages'] = 'biz.paluch.logging.gelf.log4j2'
            log4j_config['appender.gelf.type'] = 'Gelf'
            log4j_config['appender.gelf.name'] = 'gelf'
            log4j_config['appender.gelf.host'] = logger_options.gelf_host
            log4j_config['appender.gelf.port'] = str(logger_options.gelf_port)
            log4j_config['appender.gelf.version'] = '1.1'
            log4j_config['appender.gelf.extractStackTrace'] = 'true'
            log4j_config['appender.gelf.filterStackTrace'] = 'true'
            log4j_config['appender.gelf.mdcProfiling'] = 'true'
            log4j_config['appender.gelf.includeFullMdc'] = 'true'
            log4j_config['appender.gelf.maximumMessageSize'] = '32000'
            log4j_config['appender.gelf.originHost'] = '%host{fqdn}'
            default_logger = 'gelf'

        # File appender
        log4j_config['appender.file.type'] = 'File'
        log4j_config['appender.file.name'] = 'fileLogger'
        log4j_config['appender.file.fileName'] = logger_options.result_log_path
        log4j_config['appender.file.layout.type'] = 'PatternLayout'
        log4j_config['appender.file.layout.pattern'] = '[%p] %c{6} - %m%n'

        if logger_options.trace_mode:
            log4j_config['rootLogger.appenderRef.file.ref'] = 'fileLogger'
            log4j_config['logger.debezium.level'] = 'trace'
            log4j_config['logger.debezium.debeziumchange'] = 'debug'
            # log at least component outputs to the file
            log4j_config['logger.component.appenderRef.stdout.ref'] = default_logger
        else:
            log4j_config['rootLogger.appenderRef.file.ref'] = 'fileLogger'
            log4j_config['rootLogger.appenderRef.file.ref'] = default_logger

        if logger_options.log4j_additional_properties:
            for key, value in logger_options.log4j_additional_properties.items():
                log4j_config[key] = value

        with open(temp_file.name, 'wb') as config_file:
            log4j_config.store(config_file, encoding='utf-8')

        return temp_file.name

    def signal_snapshot(self, table_names: list[str], snapshot_type: Literal['blocking', 'incremental'] = 'blocking',
                        channel: Literal['file', 'source'] = 'file'):
        """
        Trigger snapshot via signal file. The signal file channel and location must be enabled in the properties file.
        Args:
            table_names: List of Table identifiers e.g. schema.table
            snapshot_type: blocking or incremental
            channel: Signal channel to use

        Returns:

        """
        if channel == 'file':
            self._signal_via_file(table_names, snapshot_type)
        elif channel == 'source':
            self._signal_via_source(table_names, snapshot_type)
        else:
            raise DebeziumException(f'Unsupported signal channel: {channel}')

    def _signal_via_source(self, table_names: list[str],
                           snapshot_type: Literal['blocking', 'incremental'] = 'blocking'):
        # validate configuration
        if not self._source_connection:
            raise DebeziumException('Source connection is not set')
        errors = []
        if not self.parsed_properties.get('signal.data.collection'):
            errors.append('signal.data.collection property is not set in the properties file')
        if not 'source' in self.parsed_properties.get('signal.enabled.channels', ''):
            errors.append('Source channel is not enabled in the properties file')
        if errors:
            raise DebeziumException(f'Cannot create signal file: {", ".join(errors)}')

        # create signal
        signal = SnapshotSignal(snapshot_id=uuid.uuid4().hex, table_names=table_names, snapshot_type=snapshot_type)
        # send signal
        result = self._source_connection.perform_query(
            f"INSERT INTO {self.parsed_properties['signal.data.collection']} "
            f"(id, type, data) VALUES ('{signal.id}', '{signal.type}', "
            f"'{json.dumps(signal.as_dict()['data'])}')")

        logging.debug(f'Signal sent: {list(result)}')

    def _signal_via_file(self, table_names: list[str], snapshot_type: Literal['blocking', 'incremental'] = 'blocking'):
        if not table_names:
            logging.warning('No tables to snapshot')
            return
        snapshot_id = uuid.uuid4().hex
        signal = SnapshotSignal(snapshot_id=snapshot_id, table_names=table_names, snapshot_type=snapshot_type)
        json.dump(signal.as_dict(), open(self.signal_file_path, 'w+'))

    @property
    def signal_file_path(self) -> str:
        errors = []
        if not self.parsed_properties.get('signal.file'):
            errors.append('singal.file property is not set in the properties file')
        if not 'file' in self.parsed_properties.get('signal.enabled.channels', ''):
            errors.append('File channel is not enabled in the properties file')
        if errors:
            raise DebeziumException(f'Cannot create signal file: {", ".join(errors)}')
        return self.parsed_properties['signal.file']

    @staticmethod
    def _build_args_from_dict(parameters: dict):
        args = [f"-{key}={value}" for key, value in parameters.items()]
        return args

    def execute(self, result_folder_path: str,
                mode: Literal['APPEND', 'DEDUPE'] = 'APPEND',
                max_duration_s: int = 3600,
                max_wait_s: int = 5, previous_schema: dict = None) -> dict:

        """
        Execute the Debezium CDC engine with the given properties file and additional arguments.
        Args:
            result_folder_path:
            mode: Mode of result processing, APPEND or DEDUPE. Dedupe keeps only latest event per record.
            max_duration_s:
            max_wait_s:
            previous_schema: Optional schema of the previous run to keep the expanding schema of tables

        Returns: Schema of the result

        """
        if previous_schema:
            with open(f'{result_folder_path}/schema.json', 'w+') as schema_file:
                json.dump(previous_schema, schema_file)

        additional_args = DebeziumExecutor._build_args_from_dict({"pf": self._keboola_properties_path,
                                                                  "md": max_duration_s, "mw": max_wait_s, "m": mode})
        tempdir_override = f"-Djava.io.tmpdir={os.environ.get('TMPDIR', '/tmp')}"
        args = ['java', f'-Dlog4j.configurationFile={self._log4j_properties}', tempdir_override,
                '-jar', self._jar_path] + [
                   self._properties_path, result_folder_path] + additional_args
        process = subprocess.Popen(args, stderr=subprocess.PIPE)

        logging.info(f'Running CDC Debezium Engine: {args}')

        process.wait()
        logging.info('Debezium CDC run finished, processing stderr...')
        err_string = process.stderr.read().decode('utf-8')
        if process.returncode != 0:
            message, stack_trace = self.process_java_log_message(err_string)

            raise DebeziumException(
                f'Failed to execute the the Debezium CDC Jar script: {message}. More detailed log in event detail.',
                extra={'additional_detail': err_string})

        logging.info('Debezium CDC run finished', extra={'additional_detail': err_string})
        return self._get_result_schema(result_folder_path)

    def _get_result_schema(self, result_folder_path: str):
        schema_path = f'{result_folder_path}/schema.json'
        schema = json.load(open(schema_path))
        # cleanup
        os.remove(schema_path)
        return schema

    def process_java_log_message(self, log_message: str) -> Tuple[str, str]:
        stack_trace = ''
        if 'at keboola.cdc.' in log_message:
            split = log_message.split('at keboola.cdc.')
            stack_trace = split[1]
            message = split[0]
        else:
            message = log_message
        return message, stack_trace
