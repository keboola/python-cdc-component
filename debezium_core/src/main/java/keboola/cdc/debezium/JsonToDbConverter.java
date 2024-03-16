package keboola.cdc.debezium;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;

import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class JsonToDbConverter implements JsonConverter {
	private static final String KEY_EVENT_ORDER_COL = "kbc__batch_event_order";
	private static final SchemaElement KEY_EVENT_ORDER_FIELD = new SchemaElement(KEY_EVENT_ORDER_COL, "int", null, null, true, null);

	private static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
	}.getType();

	private final DuckDBConnection conn;
	private final String tableName;
	private final ConcurrentMap<String, SchemaElement> schema;
	private final Gson gson;
	private final ConcurrentMap<Thread, Pair> schemaStatementMap;

	public JsonToDbConverter(DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) {
		this.gson = new Gson();
		this.conn = dbWrapper.getConn();
		this.tableName = tableName.replaceAll("\\.", "_");
		this.schema = new ConcurrentHashMap<>(initialSchema != null ? initialSchema.size() : 16);
		this.schemaStatementMap = new ConcurrentHashMap<>();
		init(initialSchema);
	}

	private void init(@Nullable JsonArray initialSchema) {
		log.info("Initializing schema for json to DB converter {}.", tableName);
		schema.put(KEY_EVENT_ORDER_COL, KEY_EVENT_ORDER_FIELD);
		if (initialSchema != null) {
			log.info("Initializing schema with {} default fields: {}", initialSchema.size(), initialSchema);
			deserialize(initialSchema).forEach(e -> schema.put(e.getField(), e));
		}
		try {
			log.info("Creating table {} if does not exits.", tableName);
			final var stmt = conn.createStatement();
			final var columnDefinition = "(" + schema.values()
					.stream()
					.map(SchemaElement::columnDefinition)
					.collect(Collectors.joining(",")) + ")";
			stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName + columnDefinition);
			stmt.close();
			log.info("Table {} created", tableName);
		} catch (Exception e) {
			log.error("Error during JsonToDbConverter schema initialization!", e);
			throw new RuntimeException(e);
		}
		log.info("Json to db converter '{}' initialized", tableName);
	}

	private List<SchemaElement> deserialize(JsonArray fields) {
		return gson.fromJson(fields, SCHEMA_ELEMENT_LIST_TYPE);
	}

	@Override
	public void processJson(final int lineNumber, final JsonObject jsonValue, final JsonObject jsonSchema) {
		log.info("Processing {}. json value {} for table {} with scheme {}.", lineNumber, jsonValue, tableName, jsonSchema);
		var pair = schemaStatementMap.get(Thread.currentThread());
		if (pair == null || !Objects.equals(jsonSchema, pair.schema)) {
			log.info("Schema has changed for thread, updating schema.");
			updateSchema(jsonSchema);
		}
		appendToDb(lineNumber, jsonValue);
		log.info("Json added to table {} processed.", tableName);
	}

	private void updateSchema(JsonObject jsonSchema) {
		PreparedStatement statement;
		var thread = Thread.currentThread();
		// Extract keys from JSON object
		final var schemaElements = deserialize(jsonSchema.getAsJsonArray("fields"));
		addMissingColumns(schemaElements);
		final var columnNames = schemaElements.stream()
				.map(SchemaElement::getField)
				.collect(Collectors.joining(", "));
		try {
			close(schemaStatementMap.get(thread));
			log.info("Updating insert statement with new columns: {}", columnNames);
			final var sql = MessageFormat.format("INSERT INTO {0} ({1},{2}) VALUES (?{3});",
					tableName, columnNames, KEY_EVENT_ORDER_COL, ", ?".repeat(schemaElements.size()));
			statement = conn.prepareStatement(sql);
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter schema update!", e);
			throw new RuntimeException(e);
		}
		log.info("Putting new schema for thread {} to lastSchema on '{}' converter, schema: {}", thread, tableName, jsonSchema);
		schemaStatementMap.put(thread, new Pair(jsonSchema, statement));
	}

	private void addMissingColumns(List<SchemaElement> elements) {
		try {
			var stmt = conn.createStatement();
			for (var element : elements) {
				var field = element.getField();
				if (schema.putIfAbsent(field, element) == null) {
					// If the column does not exist, add it
					log.info("Adding missing column {} to table {}", field, tableName);
					stmt.execute(MessageFormat.format("ALTER TABLE {0} ADD COLUMN {1} {2}",
							tableName, field, element.dbType()));
				}
			}
			stmt.close();
			log.info("No more columns to add to table {}", tableName);
		} catch (Exception e) {
			log.error("Error during table columns update", e);
			throw new RuntimeException(e);
		}
	}

	private void appendToDb(final int lineNumber, final JsonObject jsonObject) {
		var statement = Objects.requireNonNull(schemaStatementMap.get(Thread.currentThread())).statement;
		try {
			var i = 1;
			log.info("Appending object to db: {}", jsonObject);
			for (var entry : jsonObject.entrySet()) {
				var value = convertValue(entry.getValue(), schema.get(entry.getKey()));
				statement.setObject(i++, value);
			}
			statement.setObject(i, lineNumber);
			statement.addBatch();
		} catch (Exception e) {
			log.error("Error during JsonToDbConverter data writing!", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Converts values in Debezium.Date format from epoch days into epoch microseconds
	 *
	 * @param value
	 * @param field
	 */
	private Object convertValue(JsonElement value, SchemaElement field) {
		if (field.isDebeziumDate()) {
			// TODO: make this configurable
			return value.getAsInt() * 86400000000L;
		}
		if (value.isJsonNull()) {
			return null;
		}
		if (value.isJsonPrimitive()) {
			return value.getAsString();
		}
		return value.toString();
	}

	@Override
	public void close() {
		schemaStatementMap.values()
				.forEach(JsonToDbConverter::close);
	}

	private static void close(Pair pair) {
		if (pair != null) {
			pair.close();
		}
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

	@Value
	private static class Pair {
		JsonObject schema;
		PreparedStatement statement;


		private void close() {
			try {
				statement.executeBatch();
				statement.close();
			} catch (SQLException e) {
				log.error("Error during JsonToDbConverter close!", e);
				throw new RuntimeException(e);
			}
		}
	}
}
