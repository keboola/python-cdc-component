package keboola.cdc.debezium.converter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.Interval;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;

import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.Interval;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;

import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class DedupeDbConverter implements JsonConverter {
	public DedupeDbConverter(Gson gson, DuckDbWrapper duckDbWrapper, String tableName, JsonArray jsonElements) {

	}

	@Override
	public void processJson(String key, JsonObject jsonValue) {

	}

	@Override
	public void close() {

	}

	@Override
	public JsonElement getJsonSchema() {
		return null;
	}

	@Override
	public void adjustSchema(JsonArray debeziumFields) {

	}

	@Override
	public boolean isMissingAnyColumn(JsonObject jsonValue) {
		return false;
	}
//
//	public DedupeDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) {
//		super(gson, dbWrapper, tableName, initialSchema);
//	}
//
//	@Override
//	protected void init(@Nullable JsonArray initialSchema) {
//		log.info("Initializing schema for json to DB converter {}.", getTableName());
//		List<SchemaElement> deserialized;
//		if (initialSchema != null) {
//			log.info("Initializing schema with {} default fields: {}", initialSchema.size(), initialSchema);
//			if (!initialSchema.contains(PRIMARY_KEY_JSON_ELEMENT)) {
//				initialSchema.add(PRIMARY_KEY_JSON_ELEMENT);
//			}
//			deserialized = deserialize(initialSchema);
//		} else {
//			log.info("No initial schema for table {} using schema with PK only.", getTableName());
//			var primaryKey = getGson().fromJson(PRIMARY_KEY_JSON_ELEMENT, SchemaElement.class);
//			deserialized = List.of(primaryKey);
//		}
//		createTable(deserialized);
//	}
//
//	@Override
//	String upsertQuery(String tableName, List<String> columns) {
//		return MessageFormat.format("INSERT OR REPLACE INTO {0} ({1}) VALUES (?{2});",
//				tableName, String.join(", ", columns), ", ?".repeat(columns.size() - 1));
//	}
//
//	@Override
//	public void processJson(String keyJson, JsonObject jsonValue, JsonObject debeziumSchema) {
//		var keySet = extractPrimaryKey(JsonParser.parseString(keyJson).getAsJsonObject());
//		var fields = debeziumSchema.get("fields").getAsJsonArray();
//		if (!fields.contains(PRIMARY_KEY_JSON_ELEMENT)) {
//			fields.add(PRIMARY_KEY_JSON_ELEMENT);
//		}
//		processJson(keySet, jsonValue, debeziumSchema, fields);
//	}
//
//	private static Set<String> extractPrimaryKey(JsonObject key) {
//		var keySet = StreamSupport.stream(
//						key.getAsJsonObject("schema")
//								.get("fields")
//								.getAsJsonArray()
//								.spliterator(), false)
//				.map(jsonElement -> jsonElement.getAsJsonObject().get("field").getAsString())
//				.collect(Collectors.toUnmodifiableSet());
//		log.debug("Primary key columns: {}", keySet);
//		return keySet;
//	}
//
//
//	@Slf4j
//	@Getter
//	public abstract class AbstractDbConverter implements JsonConverter {
//		private static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
//		}.getType();
//		private static final int MAX_BATCH_SIZE = 100;
//
//		private final DuckDBConnection conn;
//		private final String tableName;
//		private final Gson gson;
//		private final LinkedHashMap<String, SchemaElement> schema;
//		private Memoized memoized;
//		private final AtomicInteger batchSize;
//
//		public AbstractDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) {
//			this.gson = gson;
//			this.conn = dbWrapper.getConn();
//			this.tableName = tableName.replaceAll("\\.", "_");
//			this.schema = new LinkedHashMap<>(initialSchema != null ? initialSchema.size() : 16);
//			this.memoized = new Memoized(null, null, List.of());
//			this.batchSize = new AtomicInteger(0);
//			init(initialSchema);
//		}
//
//		abstract void init(@Nullable JsonArray initialSchema);
//
//		abstract String upsertQuery(String tableName, List<String> columns);
//
//		public abstract void processJson(final String keyJson, final JsonObject jsonValue, final JsonObject debeziumSchema);
//
//		protected void processJson(final Set<String> key, final JsonObject jsonValue, final JsonObject debeziumSchema, JsonArray debeziumSchemaFields) {
//			log.debug("Processing json value {} for table {} with scheme {}.", jsonValue, this.tableName, debeziumSchema);
//			adjustSchemaIfNecessary(debeziumSchemaFields);
//
//			putToDb(key, jsonValue);
//			log.debug("Json added to table {} processed.", this.tableName);
//		}
//
//		protected void createTable(final List<SchemaElement> deserializedSchema) {
//			var columnDefinition = deserializedSchema.stream()
//					.map(schemaElement -> {
//						//  initialize schema and prepare column definition
//						this.schema.put(schemaElement.field(), schemaElement);
//						return schemaElement.columnDefinition();
//					})
//					.collect(Collectors.joining(", "));
//			try {
//				log.info("Creating table {} if does not exits.", this.tableName);
//				final var stmt = this.conn.createStatement();
//				var createTable = "CREATE TABLE IF NOT EXISTS " + this.tableName + "(" + columnDefinition + ")";
//				log.info("Create table: {}", createTable);
//				stmt.execute(createTable);
//				stmt.close();
//				log.info("Table {} created", this.tableName);
//			} catch (Exception e) {
//				log.error("Error during JsonToDbConverter schema initialization!", e);
//				throw new RuntimeException(e);
//			}
//			log.info("Json to db converter '{}' initialized", this.tableName);
//		}
//
//		protected List<SchemaElement> deserialize(JsonArray fields) {
//			return this.gson.fromJson(fields, SCHEMA_ELEMENT_LIST_TYPE);
//		}
//
//		/**
//		 * Create new entry or update current one by ID in database
//		 *
//		 * @param key       column names of primary key
//		 * @param jsonValue json value to be upserted
//		 */
//		private void putToDb(final Set<String> key, final JsonObject jsonValue) {
//			try {
//				for (int i = 0; i < this.memoized.columns().size(); i++) {
//					var column = this.memoized.getColumn(i);
//					var value = Objects.equals(column, JsonConverter.KBC_PRIMARY_KEY)
//							? key.stream().map(k -> jsonValue.get(k).getAsString()).collect(Collectors.joining("_"))
//							: convertValue(jsonValue.get(column), this.schema.get(column));
//					this.memoized.statement().setObject(i + 1, value);
//				}
//				this.memoized.statement().addBatch();
//
//				if (this.batchSize.incrementAndGet() > MAX_BATCH_SIZE) {
//					this.memoized.statement().executeBatch();
//					this.batchSize.set(0);
//				}
//			} catch (SQLException e) {
//				log.error("Error during JsonToDbConverter putToDb!", e);
//				throw new RuntimeException(e);
//			}
//		}
//
//		private void adjustSchemaIfNecessary(final JsonArray jsonSchema) {
//			if (!Objects.equals(getMemoized().lastDebeziumSchema(), jsonSchema)) {
//				memoized(jsonSchema);
//			}
//		}
//
//		protected void memoized(JsonArray jsonSchema) {
//			log.debug("New schema has been provided {}", jsonSchema);
//			var deserialized = deserialize(jsonSchema);
//			var columns = new ArrayList<String>(deserialized.size());
//			try {
//				this.memoized.closeStatement();
//
//				var stmt = this.conn.createStatement();
//				for (var element : deserialized) {
//					log.debug("Preparing column: {}", element.field());
//					columns.add(element.field());
//					if (this.schema.putIfAbsent(element.field(), element) == null) {
//						log.debug("Alter {} add column: {}", this.tableName, element);
//						stmt.execute(MessageFormat.format("ALTER TABLE {0} ADD COLUMN {1} {2};",
//								this.tableName, element.field(), element.dbType()));
//					}
//				}
//				stmt.close();
//				var columnNames = String.join(", ", columns);
//				log.info("Updating insert statement with new columns: {}", columnNames);
//
//				final var sql = upsertQuery(this.tableName, columns);
//				this.memoized = new Memoized(jsonSchema, this.conn.prepareStatement(sql), columns);
//			} catch (SQLException e) {
//				log.error("Error during JsonToDbConverter adjustSchema!", e);
//				throw new RuntimeException(e);
//			}
//		}
//
//		/**
//		 * Converts values in Debezium.Date format from epoch days into epoch microseconds
//		 *
//		 * @param value value to convert
//		 * @param field schema element
//		 */
//		private Object convertValue(JsonElement value, SchemaElement field) {
//			if (value.isJsonNull()) {
//				return null;
//			}
//			if (field.isDate()) {
//				return LocalDate.ofEpochDay(value.getAsInt())
//						.format(DateTimeFormatter.ISO_LOCAL_DATE);
//			}
//			if (field.isTimestamp()) {
//				return LocalDateTime.ofInstant(Instant.ofEpochMilli(value.getAsLong()), ZoneOffset.UTC);
//			}
//			if (value.isJsonPrimitive()) {
//				return value.getAsString();
//			}
//			return value.toString();
//		}
//
//		public void close() {
//			this.memoized.closeStatement();
//		}
//
//		public JsonElement getJsonSchema() {
//			return this.gson.toJsonTree(this.schema.values());
//		}
//
//		@JsonInclude(JsonInclude.Include.NON_NULL)
//		private record SchemaElement(String field, String type, String name, Integer version, boolean optional,
//									 String defaultValue) {
//			public String columnDefinition() {
//				if (this.field.equals(JsonConverter.KBC_PRIMARY_KEY)) {
//					return MessageFormat.format("{0} {1} PRIMARY KEY", this.field, dbType());
//				}
//				return MessageFormat.format("{0} {1}{2}{3}",
//						this.field, dbType(), this.optional ? "" : " NOT NULL",
//						this.defaultValue != null ? "DEFAULT " + this.defaultValue : "");
//			}
//
//			boolean isDate() {
//				return Objects.equals(this.name, Date.SCHEMA_NAME)
//						|| Objects.equals(this.name, org.apache.kafka.connect.data.Date.LOGICAL_NAME);
//			}
//
//			private boolean isTimestamp() {
//				return Objects.equals(this.name, Timestamp.SCHEMA_NAME)
//						|| Objects.equals(this.name, org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME);
//			}
//
//			public String dbType() {
//				return switch (this.type) {
//					case "int", "int16", "int32" -> {
//						if (isDate()) {
//							yield DuckDBColumnType.DATE.name();
//						}
//						yield DuckDBColumnType.INTEGER.name();
//					}
//					case "int64" -> {
//						if (isTimestamp()) {
//							yield DuckDBColumnType.TIMESTAMP.name();
//						}
//						yield DuckDBColumnType.BIGINT.name();
//					}
//					case "timestamp" -> DuckDBColumnType.TIMESTAMP.name();
//					case "string" -> {
//						if (Objects.equals(this.name, Uuid.LOGICAL_NAME)) {
//							yield DuckDBColumnType.UUID.name();
//						}
//						if (isZonedTimestamp()) {
//							yield "timestamptz";
//						}
//						if (isInterval()) {
//							yield DuckDBColumnType.INTERVAL.name();
//						}
//						yield DuckDBColumnType.VARCHAR.name();
//					}
//					case "bytes" -> DuckDBColumnType.VARCHAR.name();
//					case "array", "struct" -> DuckDBColumnType.VARCHAR.name();
//					case "boolean" -> DuckDBColumnType.BOOLEAN.name();
//					case "float" -> DuckDBColumnType.FLOAT.name();
//					case "double" -> DuckDBColumnType.DOUBLE.name();
//					case "date" -> DuckDBColumnType.DATE.name();
//					case "time" -> DuckDBColumnType.TIME.name();
//					default -> throw new IllegalArgumentException("Unknown type: " + this.type);
//				};
//			}
//
//			private boolean isInterval() {
//				return Objects.equals(this.name, Interval.SCHEMA_NAME);
//			}
//
//			private boolean isZonedTimestamp() {
//				return Objects.equals(this.name, ZonedTimestamp.SCHEMA_NAME);
//			}
//		}
//
//		protected record Memoized(JsonArray lastDebeziumSchema, PreparedStatement statement, List<String> columns) {
//			private void closeStatement() {
//				try {
//					if (this.statement != null) {
//						this.statement.executeBatch();
//						this.statement.close();
//					}
//				} catch (SQLException e) {
//					log.error("Error during JsonToDbConverter close!", e);
//					throw new RuntimeException(e);
//				}
//			}
//
//			public String getColumn(int index) {
//				return this.columns.get(index);
//			}
//		}
//	}

}
