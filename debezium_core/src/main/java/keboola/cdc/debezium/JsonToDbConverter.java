package keboola.cdc.debezium;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.duckdb.DuckDBColumnType;

import java.lang.reflect.Type;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class JsonToDbConverter implements JsonConverter {
	private static final String KEY_EVENT_ORDER_COL = "kbc__batch_event_order";
	private static final SchemaElement KEY_EVENT_ORDER_FIELD = new SchemaElement(KEY_EVENT_ORDER_COL, "int", null, null, true, null);

	public static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
	}.getType();


	private final DuckDbWrapper conn;
	private final String tableName;
	private final ConcurrentMap<String, SchemaElement> schema;
	private final Gson gson;

	public JsonToDbConverter(DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) {
		this.gson = new Gson();
		this.conn = dbWrapper;
		this.tableName = tableName.replaceAll("\\.", "_");
		this.schema = new ConcurrentHashMap<>(initialSchema != null ? initialSchema.size() : 16);
		init(initialSchema);
	}

	private List<SchemaElement> deserialize(JsonArray fields) {
		return gson.fromJson(fields, SCHEMA_ELEMENT_LIST_TYPE);
	}

	private void init(@Nullable JsonArray initialSchema) {
		schema.put(KEY_EVENT_ORDER_COL, KEY_EVENT_ORDER_FIELD);
		if (initialSchema != null) {
			deserialize(initialSchema).forEach(e -> schema.put(e.getField(), e));
		}
		try {
			// Create a table
			Statement stmt = conn.createStatement();
			var columnDefinition = "(" + schema.values()
					.stream()
					.map(SchemaElement::columnDefinition)
					.collect(Collectors.joining(",")) + ")";
			String sql = "CREATE TABLE IF NOT EXISTS " + tableName + columnDefinition;
			log.info(sql);
			stmt.execute(sql);
			stmt.close();
		} catch (Exception e) {
			log.error("Error during JsonToDbConverter schema initialization!");
			throw new RuntimeException(e);
		}
	}

	private String getCurrentDatabase() {
		try {
			// Query to get the current database name
			String query = "SELECT current_database()";

			// Execute the query
			var stmt = conn.createStatement();
			var rs = stmt.executeQuery(query);

			// Get the database name
			String dbName = null;
			if (rs.next()) {
				dbName = rs.getString(1);
			}

			// Close the statement and connection
			rs.close();
			stmt.close();

			return dbName;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}


	@Override
	public void processJson(int lineNumber, JsonObject jsonValue, JsonObject jsonSchema) {
		// Extract keys from JSON object
		addColumnsIfNotExists(deserialize(jsonSchema.getAsJsonArray("fields")));

		// Write JSON values to CSV
		writeJsonToDb(lineNumber, jsonValue);
	}

	private void writeJsonToDb(final int lineNumber, final JsonObject jsonObject) {
		try {
			var stmt = conn.createStatement();
			var columnNames = new StringBuilder();
			var columnValues = new StringBuilder();
			for (var entry : jsonObject.entrySet()) {
				String key = entry.getKey();
				columnNames.append(key).append(", ");
				var value = convertDateValues(entry.getValue(), schema.get(key));
				columnValues.append(value).append(", ");
			}

			columnNames.append(KEY_EVENT_ORDER_COL);
			columnValues.append(lineNumber);

			var vals = columnValues.toString().replaceAll("\"", "'");

			String sqlQuery = "INSERT INTO " + tableName + " (" + columnNames.toString() + ") VALUES (" + vals + ")";
			log.info(sqlQuery);
			stmt.execute(sqlQuery);
			stmt.close();
		} catch (Exception e) {
			log.error("Error during JsonToDbConverter data writing!");
			throw new RuntimeException(e);
		}
	}

	public void checkActualData() {
		try {

			// Query to get columns of a specific table
			String query = "SELECT * FROM " + tableName;

			// Execute the query
			var stmt = conn.createStatement();
			var rs = stmt.executeQuery(query);

			// Print the data
			System.out.println("Data in table employees:");
			while (rs.next()) {
				System.out.println("ID: " + rs.getInt("id") + ", Name: " + rs.getString("name") + ", description: " + rs.getString("description") + ", weight: " + rs.getDouble("weight"));
			}

			// Close the statement and connection
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Converts values in Debezium.Date format from epoch days into epoch microseconds
	 *
	 * @param value
	 * @param field
	 */
	private Object convertDateValues(JsonElement value, SchemaElement field) {
		// TODO: make this configurable
		if (field.isDebeziumDate()) {
			return value.getAsInt() * 86400000000L;
		}
		return value;
	}

	private void addColumnsIfNotExists(List<SchemaElement> elements) {
		try {
			var stmt = conn.createStatement();
			for (var element : elements) {
				String field = element.getField();
				if (schema.putIfAbsent(field, element) == null) {
					// If the column does not exist, add it
					stmt.execute("ALTER TABLE " + tableName + " ADD COLUMN " + field + " " + element.dbType());
				}
			}
			// Close the statement and connection
			stmt.close();
		} catch (Exception e) {
			log.error("Error during table columns update", e);
		}
	}

	@Override
	public void close() {
		conn.close();
	}

	@Override
	public JsonElement getSchema() {
		return gson.toJsonTree(schema.values());
	}

	@Value
	@JsonInclude(JsonInclude.Include.NON_NULL)
	private static class SchemaElement {
		String field;
		String type;
		String name;
		Integer version;
		boolean optional;
		String defaultValue;

		public String columnDefinition() {
			return MessageFormat.format("{0} {1}{2}{3}",
					field, dbType(), optional ? "" : " NOT NULL",
					defaultValue != null ? "DEFAULT " + defaultValue : "");
		}

		boolean isDebeziumDate() {
			return name != null && (name.equals("io.debezium.time.Date") || name.equals("org.apache.kafka.connect.data.Date"));
		}

		public DuckDBColumnType dbType() {

			switch (type) {
				case "int":
				case "int32":
					if (!isDebeziumDate())
						return DuckDBColumnType.INTEGER;
					// if isDebeziumDate() then return DuckDBColumnType.BIGINT;
				case "int64":
					return DuckDBColumnType.BIGINT;
				case "timestamp":
					return DuckDBColumnType.TIMESTAMP;
				case "string":
					return DuckDBColumnType.VARCHAR;
				case "boolean":
					return DuckDBColumnType.BOOLEAN;
				case "float":
					return DuckDBColumnType.FLOAT;
				case "double":
					return DuckDBColumnType.DOUBLE;
				case "date":
					return DuckDBColumnType.DATE;
				case "time":
					return DuckDBColumnType.TIME;
				default:
					throw new IllegalArgumentException("Unknown type: " + type);
			}
		}
	}
}
