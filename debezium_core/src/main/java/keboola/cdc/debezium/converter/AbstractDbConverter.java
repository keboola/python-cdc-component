package keboola.cdc.debezium.converter;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import io.debezium.data.Uuid;
import io.debezium.time.Date;
import io.debezium.time.Interval;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBAppender;
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
@Getter
abstract class AbstractDbConverter implements JsonConverter {
	private static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
	}.getType();
	private static final int MAX_BATCH_SIZE = 100;

	private final DuckDBConnection conn;
	private final String tableName;
	private final Gson gson;
	private final LinkedHashMap<String, SchemaElement> schema;
	private Memoized memoized;
	private final AtomicInteger batchSize;

	public AbstractDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, JsonArray initialSchema) {
		this.gson = gson;
		this.conn = dbWrapper.getConn();
		this.tableName = tableName.replaceAll("\\.", "_");
		this.schema = new LinkedHashMap<>(initialSchema.size());
		this.memoized = new Memoized(null, null, null, List.of());
		this.batchSize = new AtomicInteger(0);
		init(initialSchema);
	}

	abstract void init(JsonArray initialSchema);

	abstract String upsertQuery(String tableName, List<String> columns);

	public abstract void processJson(final String keyJson, final JsonObject jsonValue, final JsonObject debeziumSchema);

	protected void processJson(final Set<String> key, final JsonObject jsonValue, final JsonObject debeziumSchema, JsonArray debeziumSchemaFields) {
		log.debug("Processing json value {} for table {} with scheme {}.", jsonValue, this.tableName, debeziumSchema);
		adjustSchemaIfNecessary(debeziumSchemaFields);

		putToDb(key, jsonValue);
		log.debug("Json added to table {} processed.", this.tableName);
	}

	protected void createTable(final List<SchemaElement> deserializedSchema) {
		var columnDefinition = deserializedSchema.stream()
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

	protected List<SchemaElement> deserialize(JsonArray fields) {
		return this.gson.fromJson(fields, SCHEMA_ELEMENT_LIST_TYPE);
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
				var value = Objects.equals(column, JsonConverter.KBC_PRIMARY_KEY)
						? key.stream().map(k -> jsonValue.get(k).getAsString()).collect(Collectors.joining("_"))
						: convertValue(jsonValue.get(column), this.schema.get(column));
				this.memoized.statement().setObject(i + 1, value);
			}
			this.memoized.statement().addBatch();

			if (this.batchSize.incrementAndGet() > MAX_BATCH_SIZE) {
				this.memoized.statement().executeBatch();
				this.batchSize.set(0);
			}
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter putToDb!", e);
			throw new RuntimeException(e);
		}
	}

	protected void adjustSchemaIfNecessary(final JsonArray jsonSchema) {
		if (!Objects.equals(getMemoized().lastDebeziumSchema(), jsonSchema)) {
			memoized(jsonSchema);
		}
	}

	protected void memoized(JsonArray jsonSchema) {
		log.debug("New schema has been provided {}", jsonSchema);
		var deserialized = deserialize(jsonSchema);
		var columns = new ArrayList<String>(deserialized.size());
		try {
			this.memoized.closeStatement();

			var stmt = this.conn.createStatement();
			for (var element : deserialized) {
				log.debug("Preparing column: {}", element.field());
				columns.add(element.field());
				if (this.schema.putIfAbsent(element.field(), element) == null) {
					log.debug("Alter {} add column: {}", this.tableName, element);
					stmt.execute(MessageFormat.format("ALTER TABLE {0} ADD COLUMN {1} {2};",
							this.tableName, element.field(), element.dbType()));
				}
			}
			stmt.close();
			var columnNames = String.join(", ", columns);
			log.info("Updating insert statement with new columns: {}", columnNames);

			final var sql = upsertQuery(this.tableName, columns);
			this.memoized = new Memoized(jsonSchema, this.conn.prepareStatement(sql),
					this.conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, this.tableName),
					columns);
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
		if (value.isJsonNull()) {
			return null;
		}
		if (field.isDate()) {
			return LocalDate.ofEpochDay(value.getAsInt())
					.format(DateTimeFormatter.ISO_LOCAL_DATE);
		}
		if (field.isTimestamp()) {
			return LocalDateTime.ofInstant(Instant.ofEpochMilli(value.getAsLong()), ZoneOffset.UTC);
		}
		if (value.isJsonPrimitive()) {
			return value.getAsString();
		}
		return value.toString();
	}

	public void close() {
		this.memoized.closeStatement();
	}

	public JsonElement getJsonSchema() {
		return this.gson.toJsonTree(this.schema.values());
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	protected record SchemaElement(String type, boolean optional, @SerializedName("default") String defaultValue,
								   String name, Integer version, String field) {
		public String columnDefinition() {
			if (this.field.equals(JsonConverter.KBC_PRIMARY_KEY)) {
				return MessageFormat.format("{0} {1} PRIMARY KEY", this.field, dbType());
			}
			return MessageFormat.format("{0} {1}{2}{3}",
					this.field, dbType(), this.optional ? "" : " NOT NULL",
					this.defaultValue != null ? " DEFAULT " + this.defaultValue : "");
		}

		boolean isDate() {
			return Objects.equals(this.name, Date.SCHEMA_NAME)
					|| Objects.equals(this.name, org.apache.kafka.connect.data.Date.LOGICAL_NAME);
		}

		boolean isTimestamp() {
			return Objects.equals(this.name, Timestamp.SCHEMA_NAME)
					|| Objects.equals(this.name, org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME);
		}

		public String dbType() {
			return switch (this.type) {
				case "int", "int16", "int32" -> {
					if (isDate()) {
						yield DuckDBColumnType.DATE.name();
					}
					yield DuckDBColumnType.INTEGER.name();
				}
				case "int64" -> {
					if (isTimestamp()) {
						yield DuckDBColumnType.TIMESTAMP.name();
					}
					yield DuckDBColumnType.BIGINT.name();
				}
				case "timestamp" -> DuckDBColumnType.TIMESTAMP.name();
				case "string" -> {
					if (Objects.equals(this.name, Uuid.LOGICAL_NAME)) {
						yield DuckDBColumnType.UUID.name();
					}
					if (isZonedTimestamp()) {
						yield "timestamptz";
					}
					if (isInterval()) {
						yield DuckDBColumnType.INTERVAL.name();
					}
					yield DuckDBColumnType.VARCHAR.name();
				}
				case "bytes" -> DuckDBColumnType.VARCHAR.name();
				case "array", "struct" -> DuckDBColumnType.VARCHAR.name();
				case "boolean" -> DuckDBColumnType.BOOLEAN.name();
				case "float" -> DuckDBColumnType.FLOAT.name();
				case "double" -> DuckDBColumnType.DOUBLE.name();
				case "date" -> DuckDBColumnType.DATE.name();
				case "time" -> DuckDBColumnType.TIME.name();
				default -> throw new IllegalArgumentException("Unknown type: " + this.type);
			};
		}

		private boolean isInterval() {
			return Objects.equals(this.name, Interval.SCHEMA_NAME);
		}

		private boolean isZonedTimestamp() {
			return Objects.equals(this.name, ZonedTimestamp.SCHEMA_NAME);
		}

	}

	protected record Memoized(JsonArray lastDebeziumSchema, PreparedStatement statement,
							  org.duckdb.DuckDBAppender appender, List<String> columns) {
		private void closeStatement() {
			try {
				if (this.statement != null) {
					this.statement.executeBatch();
					this.statement.close();
				}
				if (this.appender != null) {
					this.appender.close();
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
