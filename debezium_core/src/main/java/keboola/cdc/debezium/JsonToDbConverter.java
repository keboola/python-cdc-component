package keboola.cdc.debezium;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class JsonToDbConverter implements JsonConverter {
	private static final String KBC_PRIMARY_KEY = "kbc__primary_key";
	private static final JsonElement PRIMARY_KEY_JSON_ELEMENT = createPrimaryKeyJsonElement();

	private static JsonObject createPrimaryKeyJsonElement() {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("field", KBC_PRIMARY_KEY);
		jsonObject.addProperty("type", "string");
		return jsonObject;
	}

	private static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
	}.getType();

	private final DuckDBConnection conn;
	private final String tableName;
	private final Gson gson;
	private final ConcurrentMap<String, SchemaElement> schema;
	private Tuple tuple;

	public JsonToDbConverter(DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) {
		this.gson = new Gson();
		this.conn = dbWrapper.getConn();
		this.tableName = tableName.replaceAll("\\.", "_");
		this.schema = new ConcurrentHashMap<>(initialSchema != null ? initialSchema.size() : 16);
		this.tuple = new Tuple(null, null, List.of());
		init(initialSchema);
	}

	private void init(@Nullable JsonArray initialSchema) {
		log.info("Initializing schema for json to DB converter {}.", tableName);
		List<SchemaElement> deserialized;
		if (initialSchema != null) {
			log.info("Initializing schema with {} default fields: {}", initialSchema.size(), initialSchema);
			if (!initialSchema.contains(PRIMARY_KEY_JSON_ELEMENT)) {
				initialSchema.add(PRIMARY_KEY_JSON_ELEMENT);
			}
			deserialized = deserialize(initialSchema);
		} else {
			log.info("No initial schema for table {} using schema with PK only.", tableName);
			var primaryKey = gson.fromJson(PRIMARY_KEY_JSON_ELEMENT, SchemaElement.class);
			deserialized = List.of(primaryKey);
		}

		//  initialize schema and prepare column definition
		var columnDefinition = deserialized
				.stream()
				.map(schemaElement -> {
					schema.put(schemaElement.field, schemaElement);
					return schemaElement.columnDefinition();
				})
				.collect(Collectors.joining(", "));
		try {
			log.info("Creating table {} if does not exits.", tableName);
			final var stmt = conn.createStatement();
			var createTable = "CREATE TABLE IF NOT EXISTS " + tableName + "(" + columnDefinition + ")";
			log.info("Create table: {}", createTable);
			stmt.execute(createTable);
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
	public void processJson(final int lineNumber, final Set<String> key,
	                        final JsonObject jsonValue, final JsonObject jsonSchema) {
		log.info("Processing {}. json value {} for table {} with scheme {}.", lineNumber, jsonValue, tableName, jsonSchema);
		adjustSchemaIfNecessary(jsonSchema.get("fields").getAsJsonArray());

		putToDb(key, jsonValue);
		log.info("Json added to table {} processed.", tableName);
	}

	private void putToDb(final Set<String> key, final JsonObject jsonValue) {
		try {
			for (int i = 0; i < tuple.columns.size(); i++) {
				var column = tuple.columns.get(i);
				var value = column.equals(KBC_PRIMARY_KEY)
						? key.stream().map(k -> jsonValue.get(k).getAsString()).collect(Collectors.joining("_"))
						: convertValue(jsonValue.get(column), schema.get(column));
				tuple.statement.setObject(i + 1, value);
			}
			tuple.statement.addBatch();
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter putToDb!", e);
			throw new RuntimeException(e);
		}
	}

	private void adjustSchemaIfNecessary(final JsonArray jsonSchema) {
		jsonSchema.add(PRIMARY_KEY_JSON_ELEMENT);
		if (!Objects.equals(tuple.schema, jsonSchema)) {
			tuple = adjustSchema(jsonSchema);
		}
	}

	private Tuple adjustSchema(JsonArray jsonSchema) {
		var deserialized = deserialize(jsonSchema);
		var columns = new ArrayList<String>(deserialized.size());
		try {
			var stmt = conn.createStatement();
			for (var element : deserialized) {
				columns.add(element.field);
				if (schema.putIfAbsent(element.field, element) == null) {
					stmt.execute(MessageFormat.format("ALTER TABLE {0} ADD COLUMN {1} {2};",
							tableName, element.field, element.dbType()));
				}
			}
			stmt.close();
			var columnNames = String.join(", ", columns);
			log.info("Updating insert statement with new columns: {}", columnNames);
			final var sql = MessageFormat.format("INSERT OR REPLACE INTO {0} ({1}) VALUES (?{2});",
					tableName, columnNames, ", ?".repeat(columns.size() - 1));

			return new Tuple(jsonSchema, conn.prepareStatement(sql), columns);
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter adjustSchema!", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Converts values in Debezium.Date format from epoch days into epoch microseconds
	 *
	 * @param value value to convert
	 * @param field schema element
	 */
	private Object convertValue(JsonElement value, SchemaElement field) {
		if (field.isDebeziumDate()) {
			return LocalDate.ofEpochDay(value.getAsInt())
					.format(DateTimeFormatter.ISO_LOCAL_DATE);
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
		tuple.close();
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
			if (field.equals(KBC_PRIMARY_KEY)) {
				return MessageFormat.format("{0} {1} PRIMARY KEY", field, dbType());
			}
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
					if (isDebeziumDate()) {
						return DuckDBColumnType.VARCHAR;
					}
					return DuckDBColumnType.INTEGER;
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
	private static class Tuple {
		JsonArray schema;
		PreparedStatement statement;
		List<String> columns;

		private void close() {
			try {
				if (statement != null) {
					statement.executeBatch();
					statement.close();
				}
			} catch (SQLException e) {
				log.error("Error during JsonToDbConverter close!", e);
				throw new RuntimeException(e);
			}
		}
	}
}
