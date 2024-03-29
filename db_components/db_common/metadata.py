from abc import ABC
from typing import Iterable

from db_components.db_common.db_connection import JDBCConnection
from db_components.db_common.table_schema import TableSchema, ColumnSchema, BaseTypeConverter


class JDBCMetadataProvider(ABC):
    def __init__(self, connection: JDBCConnection, base_type_converter: BaseTypeConverter):
        self.__connection = connection
        self._base_type_converter = base_type_converter

    def get_table_metadata(self, schema: str, table_name: str) -> TableSchema:

        columns = self.__connection.get_columns(schema_pattern=schema, table_name_pattern=table_name)
        primary_key_res = self.__connection.get_primary_keys(schema=schema, table=table_name)
        primary_keys = [r['COLUMN_NAME'] for r in primary_key_res]
        table_schema = TableSchema(name=table_name, schema_name=schema, primary_keys=primary_keys)

        for col in columns:
            # None if the column is autoincrement
            default_value = col['COLUMN_DEF'] if not str(col['COLUMN_DEF']).startswith('nextval') else None
            column_schema = ColumnSchema(name=col['COLUMN_NAME'],
                                         source_type=col['TYPE_NAME'],
                                         base_type_converter=self._base_type_converter,
                                         description=col['REMARKS'] or '',
                                         default=default_value,
                                         length=col['COLUMN_SIZE'],
                                         precision=col['COLUMN_DEF'],
                                         additional_properties=col
                                         )
            self._build_source_type_signature(column_schema)
            table_schema.add_column(column_schema)

        return table_schema

    def _build_source_type_signature(self, column: ColumnSchema):
        signature = ''
        if column.length:
            signature = f'({column.length}'
        if column.precision:
            signature += f',{column.precision}'
        signature += ')'
        column.source_type_signature = f'{column.source_type}{signature}'

    def get_tables(self, schema_pattern: str = None,
                   additional_types: list[str] = None) -> Iterable[tuple[str, str]]:
        """
        Get all available tables. Returns tuple (schema, table)
        Args:
            schema_pattern: optional schema/pattern e.g. %_some_suffix
            additional_types: By default include only TABLE types, additionally VIEW, SYSTEM TABLE
                              or SYSTEM VIEW can be included.

        Returns: tuple schema_name, table_name

        """
        table_types = ['TABLE']
        if additional_types:
            table_types.extend(additional_types)
        tables = self.__connection.get_tables(schema_pattern=schema_pattern,
                                              types=table_types)

        for table in tables:
            yield table["TABLE_SCHEM"], table["TABLE_NAME"]

    def get_schemas(self):
        yield from self.__connection.get_schemas()
