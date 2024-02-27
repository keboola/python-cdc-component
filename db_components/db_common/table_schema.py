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
    def __call__(self, source_type: str):
        return source_type


@dataclass
class ColumnSchema:
    """
    Defines the name and type specifications of a single field in a table
    """
    name: str
    source_type: Optional[str] = None
    source_type_signature: Optional[str] = None
    base_type_converter: BaseTypeConverter = field(default=lambda t: t)
    description: Optional[str] = ""
    nullable: bool = False
    length: Optional[str] = None
    precision: Optional[str] = None
    default: Optional[str] = None
    additional_properties: dict = field(default_factory=dict)

    @property
    def base_type(self):
        return self.base_type_converter(self.source_type)

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
    columns: List[ColumnSchema] = field(default_factory=list)
    primary_keys: Optional[List[str]] = None
    parent_tables: Optional[List[str]] = None
    description: Optional[str] = None
    additional_properties: dict = field(default_factory=dict)

    @property
    def field_names(self) -> List[str]:
        return [column.name for column in self.columns]

    @property
    def csv_name(self) -> str:
        return f"{self.schema_name}_{self.name}"

    def add_column(self, column: ColumnSchema) -> None:
        """
        Adds extra field to the tableschema.
        Args:
            column:  ColumnSchema to add to the list of columns

        """
        self.columns.append(column)

    def remove_column(self, column_name: str) -> None:
        """
        Removes a field from the tableschema.
        Args:
            column_name:  Column name to remove from the list of columns

        """
        self.columns = [column for column in self.columns if column.name != column_name]

    def as_dict(self) -> dict:
        dict_schema = asdict(self)
        dict_schema['columns'] = list()

        for c in self.columns:
            dict_schema['columns'].append(c.as_dict())
        return dict_schema


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
      "columns": [
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
        json_table_schema["columns"] = [ColumnSchema(**{**_field, **{"base_type_converter": base_type_converter}}) for
                                       _field in json_table_schema["columns"]]
    except TypeError as type_error:
        raise KeyError(
            f"When creating the table schema the definition of columns failed : {type_error}") from type_error
    try:
        ts = TableSchema(**json_table_schema)
    except TypeError as type_error:
        raise KeyError(
            f"When creating the table schema the definition of the table failed : {type_error}") from type_error
    return ts
