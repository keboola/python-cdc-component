import logging
import subprocess


class DebeziumException(Exception):
    pass


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

            raise DebeziumException(
                f'Failed to execute the the Debezium CDC Jar script. Log in event detail. {err_string}')
        elif stderr:
            logging.warning(err_string)

        logging.info('Debezium CDC run finished', extra={'additional_detail': stdout})
