from dataclasses import dataclass


@dataclass
class Credentials:
    account: str
    user: str
    password: str
    warehouse: str
    database: str = None
    schema: str = None
    role: str = None

# class SnowflakeClient:
#     DEFAULT_FILE_FORMAT = dict(TYPE="CSV", FIELD_DELIMITER="','", SKIP_HEADER=1, FIELD_OPTIONALLY_ENCLOSED_BY="'\"'",
#                                ERROR_ON_COLUMN_COUNT_MISMATCH=False, COMPRESSION="GZIP", NULL_IF="('KBC__NULL')",
#                                ESCAPE_UNENCLOSED_FIELD="NONE")
#
#     def __init__(self, account, user, password, database, schema, warehouse):
#         self._connection: SnowflakeConnection
#         self._credentials = Credentials(account=account,
#                                         user=user,
#                                         password=password,
#                                         database=self.wrap_in_quote(database),
#                                         schema=self.wrap_in_quote(schema),
#                                         warehouse=self.wrap_in_quote(warehouse))
#
#     def execute_query(self, query):
#         """
#         executes the query
#         """
#         cur = self._connection.cursor(snowflake.connector.DictCursor)
#         results = cur.execute(query).fetchall()
#         return results
#
#     @contextmanager
#     def connect(self, session_parameters=None):
#         try:
#             if not session_parameters:
#                 session_parameters = {}
#             cfg = asdict(self._credentials)
#             cfg['session_parameters'] = session_parameters
#             self._connection = snowflake.connector.connect(**cfg)
#             yield self
#         except Exception as e:
#             raise UserException(f'Failed to connect to Snowflake - {e}') from e
#         finally:
#             self.close_connection()
#
#     def close_connection(self):
#         if self._connection:
#             self._connection.close()
#
#     def create_table(self, name, columns: [dict]):
#         query = f"""
#         CREATE OR REPLACE TABLE {self.wrap_in_quote(name)} (
#         """
#         col_defs = [f"\"{col['name']}\" {col['type']}" for col in columns]
#         query += ', '.join(col_defs)
#         query += ");"
#         self.execute_query(query)
#
#     def extend_table_columns(self, table_name, columns: [dict[str, str]]):
#         """
#         Expand add non-existent fields to the table.
#         :param table_name:
#         :param columns: dictionary {'name': ,'type'}
#         :return:
#         """
#         table_name = self.wrap_in_quote(table_name)
#         existing_columns = self.get_table_column_names(table_name)
#         for col in columns:
#             if col['name'] not in existing_columns:
#                 self.execute_query(f"ALTER TABLE {table_name} ADD COLUMN {col['name']} {col['type']};")
#
#     def get_table_column_names(self, table_name):
#         table_name = self.wrap_in_quote(table_name)
#         query = f"""select COLUMN_NAME
#                         from INFORMATION_SCHEMA.COLUMNS
#                         where TABLE_NAME = '{table_name}';
#                 """
#         return [col['column_name'] for col in self.execute_query(query)]
#
#     def copy_csv_into_table_from_s3(self, table_name, table_columns, path_to_object, aws_access_key_id,
#                                     aws_secret_access_key,
#                                     file_format: dict = None):
#         """
#         Import from S3 file by default CSV format with skip header setup and ERROR_ON_COLUMN_COUNT_MISMATCH=false.
#         :param path_to_object:
#         :param aws_access_key_id:
#         :param aws_secret_access_key:
#         :param file_format:
#         :return:
#         """
#         if not file_format:
#             file_format = self.DEFAULT_FILE_FORMAT
#
#         table_name = self.wrap_in_quote(table_name)
#         columns = self.wrap_columns_in_quotes(table_columns)
#         query = f"""
#         COPY INTO {table_name} ({', '.join(columns)}) FROM {path_to_object}
#              CREDENTIALS =(aws_key_id = '{aws_access_key_id}' aws_secret_key = '{aws_secret_access_key}')
#         """
#         query += "FILE_FORMAT = ("
#         for key in file_format:
#             query += f"{key}={file_format[key]} "
#         query += ");"
#         self.execute_query(query)
#
#     def create_temp_stage(self, name: str, file_format: dict = None):
#         if not file_format:
#             file_format = self.DEFAULT_FILE_FORMAT
#         query = f"""
#         CREATE OR REPLACE TEMP STAGE  {name}
#         """
#
#         query += " FILE_FORMAT = ("
#         for key in file_format:
#             query += f"{key}={file_format[key]} "
#         query += ");"
#
#         self.execute_query(query)
#
#     def copy_csv_into_table_from_file(self, table_name: str, table_columns: list[str], csv_file_path: str,
#                                       file_format: dict = None):
#         """
#         Import from file by default CSV format with skip header setup and ERROR_ON_COLUMN_COUNT_MISMATCH=false.
#
#         Args:
#             table_name:
#             table_columns:
#             csv_file_path:
#             file_format:
#         """
#         if not file_format:
#             file_format = self.DEFAULT_FILE_FORMAT
#
#         # create temp stage
#         self.create_temp_stage(table_name, file_format)
#         # copy to stage
#         stage_sql = f"PUT file://{csv_file_path} @{table_name} AUTO_COMPRESS=TRUE;"
#         self.execute_query(stage_sql)
#
#         # insert data
#         table_name = self.wrap_in_quote(table_name)
#         columns = self.wrap_columns_in_quotes(table_columns)
#         query = f"COPY INTO {table_name} ({', '.join(columns)}) FROM @{table_name.upper()}"
#         query += " FILE_FORMAT = ("
#         for key in file_format:
#             query += f"{key}={file_format[key]} "
#         query += ");"
#         self.execute_query(query)
#
#     def wrap_columns_in_quotes(self, columns):
#         return [self.wrap_in_quote(col) for col in columns]
#
#     def wrap_in_quote(self, s):
#         return s if s.startswith('"') else '"' + s + '"'
#
#     def close(self):
#         self._connection.close()
#         self._engine.dispose()
