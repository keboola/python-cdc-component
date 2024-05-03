import duckdb

# Create a connection to the database
conn = duckdb.connect(database='/Users/esner/Documents/Prace/KBC/CDC_DEBEZIUM/debezium_engine_wrapper/testing_config/test.duckdb', read_only=False)

# Execute queries using the connection object
conn.execute('SHOW TABLES').fetchall()
conn.execute('create table test2 (a int, b int)')
conn.execute('insert into test2 values (1, 2)')

# Close the connection
conn.close()