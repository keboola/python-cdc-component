import json
import logging
import os
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
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
    max_threads: int = 6
    memory_limit: str = '2GB'
    memory_max: str = '1GB'


class DebeziumExecutor:

    def __init__(self, properties_path: str, duckdb_config: DuckDBParameters, jar_path='cdc.jar',
                 source_connection: Optional[JDBCConnection] = None,
                 result_log_path: str = None):
        """
        Initialize the Debezium CDC engine with the given properties file and jar path.
        Args:
            properties_path:
            jar_path:
            source_connection: Optional JDBCConnection to the source database used for signaling
            result_log_path: Optional path to the log file
            duckdb_config: `DuckDBParameters` object with the configuration for the DuckDB instance
        """
        self._jar_path = jar_path
        self._properties_path = properties_path
        self._keboola_properties_path = self.build_keboola_properties(duckdb_config)

        self._source_connection = source_connection
        self.parsed_properties = self._parse_properties()
        self.result_log_path = result_log_path

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
            config_file.write(f'keboola.duckdb.max.threads={duckdb_config.max_threads}\n')
            config_file.write(f'keboola.duckdb.memory.limit={duckdb_config.memory_limit}\n')
            config_file.write(f'keboola.duckdb.memory.max={duckdb_config.memory_max}\n')
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
                max_wait_s: int = 10, previous_schema: dict = None) -> dict:

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
        args = ['java', '-jar', self._jar_path] + [self._properties_path, result_folder_path] + additional_args
        process = subprocess.Popen(args,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

        logging.info(f'Running CDC Debezium Engine: {args}')

        Path(self.result_log_path).parent.mkdir(parents=True, exist_ok=True)

        with open(self.result_log_path, 'w+') as log_out:
            # Stream stdout
            for line in iter(process.stdout.readline, b''):
                line_str = line.decode('utf-8').rstrip('\n')
                logging.info(line_str)
                if self.result_log_path:
                    log_out.write(line_str)

            process.stdout.close()
            process.wait()
            logging.info('Debezium CDC run finished, processing stderr...')
            err_string = process.stderr.read().decode('utf-8')
            if process.returncode != 0:
                message, stack_trace = self.process_java_log_message(err_string)
                log_out.write(err_string)
                log_out.close()
                raise DebeziumException(
                    f'Failed to execute the the Debezium CDC Jar script: {message}. More detailed log in event detail.',
                    extra={'additional_detail': err_string})

            logging.info('Debezium CDC run finished', extra={'additional_detail': err_string})
            log_out.close()
            logging.debug(err_string)
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
