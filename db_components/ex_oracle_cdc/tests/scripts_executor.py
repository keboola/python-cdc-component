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
            return conn
        except oracledb.DatabaseError as e:
            error, = e.args
            raise Exception(f"Error connecting to the database: {error.message}")

    def execute_sql(self, sql):
        if not self.conn:
            raise Exception("No database connection.")
        try:
            with self.conn.cursor() as cursor:
                print(f"Executing query: {sql}")
                cursor.execute(sql)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error executing SQL command: {e}")

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
                    print(f"Executing query: {query}")
                    cursor.execute(query)
                    self.conn.commit()
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
    executor = OracleSQLExecutor(user="t", password="t", dsn="35.232.223.228:1525/FREE")
    executor.execute_sql("""
    INSERT INTO TESTUSER01.USERS (NAME, GENDER)
    VALUES ('Dominik', 'M'),
    ('David', 'M')
    """)
    executor.close_connection()
