package keboola.cdc.debezium;

import lombok.Getter;
import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Getter
public class DuckDbWrapper {

	private static final String TMP_DB_PATH = "./debezium_core/tmp/my-db.duckdb";
	private static final int MAX_THREADS = 4; // replace with your value
	private static final String MEMORY_LIMIT = "4G"; // replace with your value
	private static final String MAX_MEMORY = "2G"; // replace with your value

	private final DuckDBConnection conn;

	public DuckDbWrapper() {
		try {
			// Load the DuckDB JDBC driver
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		// Establish a connection to the DuckDB database
		try {
			conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + TMP_DB_PATH);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		init();
	}

	private void init() {
		try {
			// Create a Statement object for sending SQL statements to the DB
			var stmt = createStatement();

			// Set the temporary directory for DuckDB
			stmt.execute("PRAGMA temp_directory='./tmp/dbtmp'");

			// Set the number of threads that DuckDB can use for parallel execution
			stmt.execute("PRAGMA threads=" + MAX_THREADS);

			// Set the maximum amount of memory that DuckDB can use
			stmt.execute("PRAGMA memory_limit='" + MEMORY_LIMIT + "'");

			// Set the maximum amount of memory that DuckDB can use for temporary data storage
			stmt.execute("PRAGMA max_memory='" + MAX_MEMORY + "'");

			// Close the statement and connection
			stmt.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private Statement createStatement() {
		try {
			return conn.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
