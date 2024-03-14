"""
Template Component main class.

"""
import logging

from configuration import Configuration, DbOptions, SnapshotMode
from extractor.oracle_extractor import OracleDebeziumExtractor
from extractor.oracle_extractor import SUPPORTED_TYPES
from extractor.oracle_extractor import build_postgres_property_file

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

DEBEZIUM_CORE_PATH = "../../../debezium_core/jars/kbcDebeziumEngine-jar-with-dependencies.jar"

KEY_LAST_SCHEMA = "last_schema"

KEY_LAST_OFFSET = 'last_offset'

REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self._client: OracleDebeziumExtractor
        self._configuration: Configuration

    def run(self):
        pass


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
