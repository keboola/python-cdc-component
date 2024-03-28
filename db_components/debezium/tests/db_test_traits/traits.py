import glob
import os
from abc import ABC

traits_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'sql_trait_examples')


def set_sql_traits_folder(folder_path: str):
    global traits_folder
    traits_folder = folder_path


class DbTestTable(ABC):

    def __init__(self, table_script_name: str):
        supported_tables = DbTestTable.get_available_scripts()
        if table_script_name not in supported_tables:
            raise ValueError(f'{table_script_name} script was not found '
                             f'in folder {traits_folder}. Available scripts are: {supported_tables}')
        self.sql_script_name: str = table_script_name

    @property
    def init_queries(self) -> list[str]:
        """
        Return list of rows that match the table definition
        Returns:

        """
        transactions_path = os.path.join(traits_folder, self.sql_script_name)
        with open(transactions_path, 'r') as f:
            return f.read().split(';')

    @staticmethod
    def get_available_scripts() -> list[str]:
        scripts = []
        for script in glob.glob(os.path.join(traits_folder, '*.sql')):
            scripts.append(os.path.basename(script))
        return scripts
