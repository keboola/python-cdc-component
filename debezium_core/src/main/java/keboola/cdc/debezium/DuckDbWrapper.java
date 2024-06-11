package keboola.cdc.debezium;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBConnection;

import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
@Getter
public class DuckDbWrapper {

	private final DuckDBConnection conn;

	private final Properties properties;

	public DuckDbWrapper() {
		this(Properties.defaults());
	}

	public DuckDbWrapper(java.util.Properties keboolaProperties) {
		this(Properties.parse(keboolaProperties));
	}

	private static java.util.Properties getConnectionProperties() {
		final java.util.Properties connectionProperties = new java.util.Properties();
		connectionProperties.setProperty("memory_limit", "2GB");
		connectionProperties.setProperty("max_memory", "2GB");
		return connectionProperties;
	}

	public DuckDbWrapper(Properties properties) {
		this.properties = properties;
		try {
			// Load the DuckDB JDBC driver
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		// Establish a connection to the DuckDB database
		try {
			this.conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + properties.dbPath(),
					getConnectionProperties());
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		init();
	}

	private void init() {
		try (var stmt = this.conn.createStatement()) {
			// Create a Statement object for sending SQL statements to the DB

			// Set the temporary directory for DuckDB
			stmt.execute("PRAGMA temp_directory='" + this.properties.tempDir() + "'");

			// Set the number of threads that DuckDB can use for parallel execution
			stmt.execute("PRAGMA threads=" + this.properties.maxThreads());

			// Set the maximum amount of memory that DuckDB can use
			stmt.execute("PRAGMA memory_limit='" + this.properties.memoryLimit() + "'");

			// Set the maximum amount of memory that DuckDB can use for temporary data storage
			stmt.execute("PRAGMA max_memory='" + this.properties.maxMemory() + "'");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		try {
			this.conn.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public record Properties(String dbPath, int maxThreads, String memoryLimit, String maxMemory,
							 String tempDir) {
		private static final String TMP_DB_PATH = "";
		private static final int MAX_THREADS = 4;
		private static final String MEMORY_LIMIT = "4G";
		private static final String MAX_MEMORY = "2G";
		private static final String TEMP_DIR = "/tmp/dbtmp";

		public static Properties defaults() {
			return new Properties(TMP_DB_PATH, MAX_THREADS, MEMORY_LIMIT, MAX_MEMORY, TEMP_DIR);
		}

		public static Properties parse(java.util.Properties keboolaProperties) {
			Properties properties = new Properties(
					keboolaProperties.getProperty("keboola.duckdb.db.path", TMP_DB_PATH),
					Integer.parseInt(keboolaProperties.getProperty("keboola.duckdb.max.threads", String.valueOf(MAX_THREADS))),
					keboolaProperties.getProperty("keboola.duckdb.memory.limit", MEMORY_LIMIT),
					keboolaProperties.getProperty("keboola.duckdb.memory.max", MAX_MEMORY),
					keboolaProperties.getProperty("keboola.duckdb.temp.directory", TEMP_DIR)
			);
			log.info("Duck db properties initialized: {}", properties);
			return properties;
		}
	}
}
