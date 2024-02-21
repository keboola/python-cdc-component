import logging
import subprocess
from typing import Tuple


class DebeziumException(Exception):
    def __init__(self, message, extra=None):
        super().__init__(message)
        self.extra = extra


class DebeziumExecutor:

    def __init__(self, jar_path='cdc.jar'):
        self._jar_path = jar_path

    @staticmethod
    def _build_args_from_dict(parameters: dict):
        args = [f"-{key}={value}" for key, value in parameters.items()]
        return args

    def execute(self, debezium_properties_path: str, result_folder_path: str,
                max_duration_s: int = 3600,
                max_wait_s: int = 10):
        additional_args = DebeziumExecutor._build_args_from_dict({"md": max_duration_s, "mw": max_wait_s})
        args = ['java', '-jar', self._jar_path] + [debezium_properties_path, result_folder_path] + additional_args
        process = subprocess.Popen(args,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

        logging.info(f'Running CDC Debezium Engine: {args}')
        stdout, stderr = process.communicate()
        process.poll()
        err_string = stderr.decode('utf-8')
        if process.poll() != 0:
            message, stack_trace = self.process_java_log_message(err_string)
            raise DebeziumException(
                f'Failed to execute the the Debezium CDC Jar script: {message}. More detailed log in event detail.',
                extra={'additional_detail': stdout})
        elif stderr:
            logging.warning(err_string)

        logging.info('Debezium CDC run finished', extra={'additional_detail': stdout})

    def process_java_log_message(self, log_message: str) -> Tuple[str, str]:
        stack_trace = ''
        if 'at keboola.cdc.' in log_message:
            split = log_message.split('at keboola.cdc.')
            stack_trace = split[1]
            message = split[0]
        else:
            message = log_message
        return message, stack_trace
