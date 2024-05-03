from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Callable
from typing import Optional


class BaseTypeConverter(ABC, Callable):
    """
    Override for specific implementations. conversion to
    STRING , INTEGER , NUMERIC , FLOAT , BOOLEAN , DATE , and TIMESTAMP
    """

    @abstractmethod
    def __call__(self, source_type: str, length: Optional[str] = None) -> str:
        return source_type


@dataclass
class ColumnSchema:
    """
    Defines the name and type specifications of a single field in a table
    """
    name: str
    source_type: Optional[str] = None
    source_type_signature: Optional[str] = None
    base_type_converter: BaseTypeConverter = field(default=lambda t, ln: t)
    description: Optional[str] = ""
    nullable: bool = False
    length: Optional[str] = None
    precision: Optional[str] = None
    default: Optional[str] = None
    additional_properties: dict = field(default_factory=dict)

    @property
    def base_type(self):
        return self.base_type_converter(self.source_type, self.length)

    def as_dict(self) -> dict:
        col_dict = asdict(self)
        col_dict['base_type_converter'] = None
        return col_dict


@dataclass
class TableSchema:
    """
    TableSchema class is used to define the schema and metadata of a table.
    """
    name: str
    schema_name: str
    database_name: str = None
    fields: List[ColumnSchema] = field(default_factory=list)
    primary_keys: Optional[List[str]] = None
    parent_tables: Optional[List[str]] = None
    description: Optional[str] = None
    additional_properties: dict = field(default_factory=dict)

    @property
    def field_names(self) -> List[str]:
        return [column.name for column in self.fields]

    @property
    def csv_name(self) -> str:
        name_prefixes = []
        if self.database_name:
            name_prefixes.append(self.database_name)
        if self.schema_name:
            name_prefixes.append(self.schema_name)
        name_prefix = '_'.join(name_prefixes)
        return f"{name_prefix}_{self.name}.csv"

    def add_column(self, column: ColumnSchema) -> None:
        """
        Adds extra field to the tableschema.
        Args:
            column:  ColumnSchema to add to the list of fields

        """
        self.fields.append(column)

    def remove_column(self, column_name: str) -> None:
        """
        Removes a field from the tableschema.
        Args:
            column_name:  Column name to remove from the list of fields

        """
        self.fields = [column for column in self.fields if column.name != column_name]

    def as_dict(self) -> dict:
        dict_schema = asdict(self)
        dict_schema['fields'] = list()

        for c in self.fields:
            dict_schema['fields'].append(c.as_dict())
        return dict_schema

    def get_column_by_name(self, c: str) -> ColumnSchema:
        """
        Get a column by name.
        Args:
            c:

        Returns:

        """
        for column in self.fields:
            if column.name == c:
                return column
        return None


def init_table_schema_from_dict(json_table_schema: Dict,
                                base_type_converter: Callable[[str | None], str] = field(
                                    default=lambda s: s)) -> TableSchema:
    """
    Function to initialize a Table Schema from a dictionary.
    Example of the json_table_schema structure:
    {
      "name": "product",
      "description": "this table holds data on products",
      "parent_tables": [],
      "primary_keys": [
        "id"
      ],
      "fields": [
        {
          "name": "id",
          "base_type": "string",
          "description": "ID of the product",
          "length": "100",
          "nullable": false
        },
        {
          "name": "name",
          "base_type": "string",
          "description": "Plain-text name of the product",
          "length": "1000",
          "default": "Default Name"
        }
      ]
    }
    """
    try:
        json_table_schema["fields"] = [ColumnSchema(**{**_field, **{"base_type_converter": base_type_converter}}) for
                                       _field in json_table_schema["fields"]]
    except TypeError as type_error:
        raise KeyError(
            f"When creating the table schema the definition of fields failed : {type_error}") from type_error
    try:
        ts = TableSchema(**json_table_schema)
    except TypeError as type_error:
        raise KeyError(
            f"When creating the table schema the definition of the table failed : {type_error}") from type_error
    return ts
