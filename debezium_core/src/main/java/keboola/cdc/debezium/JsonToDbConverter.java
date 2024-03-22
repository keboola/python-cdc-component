package keboola.cdc.debezium;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
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
public class JsonToDbConverter {
	private static final String KBC_PRIMARY_KEY = "kbc__primary_key";
	private static final JsonElement PRIMARY_KEY_JSON_ELEMENT = createPrimaryKeyJsonElement();

	private static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
	}.getType();

	private final DuckDBConnection conn;
	private final String tableName;
	private final Gson gson;
	private final ConcurrentMap<String, SchemaElement> schema;
	private Memoized memoized;

	public JsonToDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) {
		this.gson = gson;
		this.conn = dbWrapper.getConn();
		this.tableName = tableName.replaceAll("\\.", "_");
		this.schema = new ConcurrentHashMap<>(initialSchema != null ? initialSchema.size() : 16);
		this.memoized = new Memoized(null, null, List.of());
		init(initialSchema);
	}

	private void init(@Nullable JsonArray initialSchema) {
		log.info("Initializing schema for json to DB converter {}.", this.tableName);
		List<SchemaElement> deserialized;
		if (initialSchema != null) {
			log.info("Initializing schema with {} default fields: {}", initialSchema.size(), initialSchema);
			if (!initialSchema.contains(PRIMARY_KEY_JSON_ELEMENT)) {
				initialSchema.add(PRIMARY_KEY_JSON_ELEMENT);
			}
			deserialized = deserialize(initialSchema);
		} else {
			log.info("No initial schema for table {} using schema with PK only.", this.tableName);
			var primaryKey = this.gson.fromJson(PRIMARY_KEY_JSON_ELEMENT, SchemaElement.class);
			deserialized = List.of(primaryKey);
		}

		var columnDefinition = deserialized.stream()
				.map(schemaElement -> {
					//  initialize schema and prepare column definition
					this.schema.put(schemaElement.field(), schemaElement);
					return schemaElement.columnDefinition();
				})
				.collect(Collectors.joining(", "));
		try {
			log.info("Creating table {} if does not exits.", this.tableName);
			final var stmt = this.conn.createStatement();
			var createTable = "CREATE TABLE IF NOT EXISTS " + this.tableName + "(" + columnDefinition + ")";
			log.info("Create table: {}", createTable);
			stmt.execute(createTable);
			stmt.close();
			log.info("Table {} created", this.tableName);
		} catch (Exception e) {
			log.error("Error during JsonToDbConverter schema initialization!", e);
			throw new RuntimeException(e);
		}
		log.info("Json to db converter '{}' initialized", this.tableName);
	}

	private List<SchemaElement> deserialize(JsonArray fields) {
		return this.gson.fromJson(fields, SCHEMA_ELEMENT_LIST_TYPE);
	}

	public void processJson(final Set<String> key, final JsonObject jsonValue, final JsonObject debeziumSchema) {
		log.debug("Processing json value {} for table {} with scheme {}.", jsonValue, this.tableName, debeziumSchema);
		adjustSchemaIfNecessary(debeziumSchema.get("fields").getAsJsonArray());

		putToDb(key, jsonValue);
		log.debug("Json added to table {} processed.", this.tableName);
	}

	/**
	 * Create new entry or update current one by ID in database
	 *
	 * @param key       column names of primary key
	 * @param jsonValue json value to be upserted
	 */
	private void putToDb(final Set<String> key, final JsonObject jsonValue) {
		try {
			for (int i = 0; i < this.memoized.columns().size(); i++) {
				var column = this.memoized.getColumn(i);
				var value = Objects.equals(column, KBC_PRIMARY_KEY)
						? key.stream().map(k -> jsonValue.get(k).getAsString()).collect(Collectors.joining("_"))
						: convertValue(jsonValue.get(column), this.schema.get(column));
				this.memoized.statement().setObject(i + 1, value);
			}
			this.memoized.statement().addBatch();
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter putToDb!", e);
			throw new RuntimeException(e);
		}
	}

	private void adjustSchemaIfNecessary(final JsonArray jsonSchema) {
		jsonSchema.add(PRIMARY_KEY_JSON_ELEMENT); // schema from debezium does not have primary key
		if (!Objects.equals(this.memoized.lastDebeziumSchema(), jsonSchema)) {
			memoized(jsonSchema);
		}
	}

	private void memoized(JsonArray jsonSchema) {
		var deserialized = deserialize(jsonSchema);
		var columns = new ArrayList<String>(deserialized.size());
		try {
			var stmt = this.conn.createStatement();
			for (var element : deserialized) {
				columns.add(element.field());
				if (this.schema.putIfAbsent(element.field(), element) == null) {
					stmt.execute(MessageFormat.format("ALTER TABLE {0} ADD COLUMN {1} {2};",
							this.tableName, element.field(), element.dbType()));
				}
			}
			stmt.close();
			var columnNames = String.join(", ", columns);
			log.info("Updating insert statement with new columns: {}", columnNames);
			final var sql = MessageFormat.format("INSERT OR REPLACE INTO {0} ({1}) VALUES (?{2});",
					this.tableName, columnNames, ", ?".repeat(columns.size() - 1));

			this.memoized = new Memoized(jsonSchema, this.conn.prepareStatement(sql), columns);
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

	public void close() {
		this.memoized.close();
	}

	public JsonElement getSchema() {
		return this.gson.toJsonTree(this.schema.values());
	}

	private static JsonObject createPrimaryKeyJsonElement() {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("field", KBC_PRIMARY_KEY);
		jsonObject.addProperty("type", "string");
		jsonObject.addProperty("optional", false);
		return jsonObject;
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	private record SchemaElement(String field, String type, String name, Integer version, boolean optional,
	                             String defaultValue) {
		public String columnDefinition() {
			if (this.field.equals(KBC_PRIMARY_KEY)) {
				return MessageFormat.format("{0} {1} PRIMARY KEY", this.field, dbType());
			}
			return MessageFormat.format("{0} {1}{2}{3}",
					this.field, dbType(), this.optional ? "" : " NOT NULL",
					this.defaultValue != null ? "DEFAULT " + this.defaultValue : "");
		}


		boolean isDebeziumDate() {
			return this.name != null
					&& (this.name.equals("io.debezium.time.Date")
					|| this.name.equals("org.apache.kafka.connect.data.Date"));
		}

		public DuckDBColumnType dbType() {
			return switch (this.type) {
				case "int", "int32" -> {
					if (isDebeziumDate()) {
						yield DuckDBColumnType.VARCHAR;
					}
					yield DuckDBColumnType.INTEGER;
				}
				case "int64" -> DuckDBColumnType.BIGINT;
				case "timestamp" -> DuckDBColumnType.TIMESTAMP;
				case "string" -> DuckDBColumnType.VARCHAR;
				case "boolean" -> DuckDBColumnType.BOOLEAN;
				case "float" -> DuckDBColumnType.FLOAT;
				case "double" -> DuckDBColumnType.DOUBLE;
				case "date" -> DuckDBColumnType.DATE;
				case "time" -> DuckDBColumnType.TIME;
				default -> throw new IllegalArgumentException("Unknown type: " + this.type);
			};
		}
	}

	private record Memoized(JsonArray lastDebeziumSchema, PreparedStatement statement, List<String> columns) {
		private void close() {
			try {
				if (this.statement != null) {
					this.statement.executeBatch();
					this.statement.close();
				}
			} catch (SQLException e) {
				log.error("Error during JsonToDbConverter close!", e);
				throw new RuntimeException(e);
			}
		}

		public String getColumn(int index) {
			return this.columns.get(index);
		}
	}
}
