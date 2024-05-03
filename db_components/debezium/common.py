from db_components.db_common.table_schema import TableSchema, ColumnSchema


def get_schema_change_table_metadata(database_name: str = None, schema_name: str = None) -> TableSchema:
    """
    Get the metadata for the table that stores the schema changes.
    Args:
        database_name: The name of the database.
        schema_name: The name of the schema.
    Returns:
        The metadata for the table that stores the schema changes.
    """

    columns = [
        ColumnSchema(name="source", source_type="STRING"),
        ColumnSchema(name="ts_ms", source_type="INTEGER"),
        ColumnSchema(name="databaseName", source_type="STRING"),
        ColumnSchema(name="schemaName", source_type="STRING"),
        ColumnSchema(name="ddl", source_type="STRING"),
        ColumnSchema(name="tableChanges", source_type="STRING"),
        ColumnSchema(name="KBC__BATCH_EVENT_ORDER", source_type="INTEGER")
    ]
    return TableSchema(
        name='schema_changes',
        schema_name=schema_name,
        database_name=database_name,
        fields=columns,
        primary_keys=['ts_ms', "databaseName", "schemaName", "ddl"]
    )
