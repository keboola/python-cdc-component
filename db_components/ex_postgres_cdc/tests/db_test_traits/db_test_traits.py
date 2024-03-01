from abc import ABC, abstractmethod


class DbTestTable(ABC):

    @property
    @abstractmethod
    def create_table_query(self) -> str:
        """create table statement"""

    @property
    @abstractmethod
    def initial_rows(self) -> list[str]:
        """
        Return list of rows that match the table definition
        Returns:

        """

    @property
    @abstractmethod
    def columns(self) -> list[str]:
        """
        Return list of columns
        Returns:

        """

    @property
    @abstractmethod
    def table_name(self) -> str:
        """
        Return result table name
        Returns:

        """

    @staticmethod
    def build_table(table_name: str):
        # TODO: validate parameters based on type
        supported_tables = DbTestTable.get_supported_tables()
        if table_name not in list(supported_tables.keys()):
            raise ValueError('{} is not supported test tavke, '
                             'suported values are: [{}]'.format(table_name,
                                                                DbTestTable.__subclasses__()))

        return supported_tables[table_name]()

    @staticmethod
    def get_supported_tables():
        supported_actions = {}
        for c in DbTestTable.__subclasses__():
            supported_actions[c.__name__] = c
        return supported_actions


class SalesTable(DbTestTable, ABC):

    @property
    def create_table_query(self) -> str:
        sql_query = '''CREATE TABLE inventory.sales (
                        usergender text,
                        usercity text,
                        usersentiment integer,
                        zipcode text,
                        sku text,
                        createdate varchar(64) NOT NULL PRIMARY KEY,
                        category text,
                        price decimal(12,5),
                        county text,
                        countycode text,
                        userstate text,
                        categorygroup text
                    );
'''
        return sql_query

    @property
    def table_name(self) -> str:
        return 'sales'

    @property
    def columns(self) -> list[str]:
        return ['usergender', 'usercity', 'usersentiment', 'zipcode', 'sku', 'createdate', 'category', 'price',
                'county',
                'countycode', 'userstate', 'categorygroup']

    @property
    def initial_rows(self) -> list:
        data = [
            ['Female', 'Mize', -1, '39153', 'SKU1', '2013-09-23 22:38:29', 'Cameras', 708, 'Smith', '28129',
             'Mississippi', 'Electronics'],
            ['Male', 'The Lakes', 1, '89124', 'SKU2', '2013-09-23 22:38:30', 'Televisions', 1546, 'Clark',
             '32003', 'Nevada', 'Electronics'],
            ['Male', 'Baldwin', 1, '21020', 'ZD111483', '2013-09-23 22:38:31', 'Loose Stones', 1262, 'Baltimore',
             '24005', 'Maryland', 'Jewelry'],
            ['Female', 'Archbald', 1, '18501', 'ZD111395', '2013-09-23 22:38:32', 'Stereo', 104, 'Lackawanna',
             '42069', 'Pennsylvania', 'Electronics'],
            ['Male', 'Berea', 0, '44127', 'ZD111451', '2013-09-23 22:38:33', 'Earings', 1007, 'Cuyahoga', '39035',
             'Ohio', 'Jewelry']
        ]

        return data
