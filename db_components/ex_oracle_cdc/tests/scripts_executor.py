import oracledb


class OracleSQLExecutor:
    def __init__(self, user, password, dsn, encoding="UTF-8"):
        self.config = {
            "user": user,
            "password": password,
            "dsn": dsn,
            "encoding": encoding
        }
        self.conn = self.connect_to_db()

    def connect_to_db(self):
        try:
            conn = oracledb.connect(
                user=self.config['user'],
                password=self.config['password'],
                dsn=self.config['dsn']
            )
            print("Successfully connected to the database.")
            return conn
        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Error connecting to the database: {error.message}")
            return None

    def execute_sql(self, sql):
        if not self.conn:
            print("No database connection.")
            return
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                self.conn.commit()
                print("SQL command executed successfully.")
        except Exception as e:
            print(f"Error executing SQL command: {e}")
            self.conn.rollback()

    def close_connection(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def execute_sql_from_file(self, sql_path: str):
        if not self.conn:
            print("No database connection.")
            return
        queries = self._parse_queries(sql_path)
        try:
            with self.conn.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)
                    self.conn.commit()
                print("SQL commands executed successfully.")
        except Exception as e:
            print(f"Error executing SQL commands: {e}")
            self.conn.rollback()

    @staticmethod
    def _parse_queries(sql_path: str) -> list[str]:
        """
        Return list of queries from sql file
        Returns:

        """
        with open(sql_path, 'r') as f:
            return f.read().split(';')


if __name__ == "__main__":
    executor = OracleSQLExecutor(user="testUser01", password="testUser01", dsn="35.232.223.228:1525/FREEPDB1")
    executor.execute_sql("SELECT * FROM EMPLOYEES")
    executor.close_connection()
