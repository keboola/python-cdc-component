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
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class AppendDbConverter  implements JsonConverter {
	private static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
	}.getType();

	private final DuckDBConnection conn;
	private final String tableName;
	private final Gson gson;
	private final LinkedHashMap<String, SchemaElement> schema;
	private DuckDBAppender appender;

	public AppendDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, JsonArray initialSchema) {
		this.gson = gson;
		this.conn = dbWrapper.getConn();
		this.tableName = tableName.replaceAll("\\.", "_");
		this.schema = new LinkedHashMap<>(initialSchema.size());
		init(initialSchema);
	}

	protected void init(final JsonArray initialSchema) {
		log.debug("Initializing schema with default fields: {}", initialSchema);
		final var deserialized = deserialize(initialSchema);
		log.debug("Deserialized schema: {}", deserialized);
		createTable(deserialized);
	}

	@Override
	public synchronized void processJson(String key, JsonObject jsonValue) {
		putToDb(jsonValue);
	}

	protected void createTable(final List<SchemaElement> deserializedSchema) {
		var columnDefinition = deserializedSchema.stream()
				.map(schemaElement -> {
					//  initialize schema and prepare column definition
					this.schema.put(schemaElement.field(), schemaElement);
					return schemaElement.columnDefinition();
				})
				.collect(Collectors.joining(", "));
		try (final var stmt = this.conn.createStatement()) {
			log.info("Creating table {} if does not exits.", this.tableName);
			stmt.execute("CREATE TABLE IF NOT EXISTS \"" + this.tableName + "\" (" + columnDefinition + ")");
			this.appender = this.conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, this.tableName);
		} catch (SQLException e) {
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
	 * @param jsonValue json value to be inserted
	 */
	private void putToDb(final JsonObject jsonValue) {
		try {
			var appender = this.appender;
			appender.beginRow();
			for (var entry : this.schema.entrySet()) {
				String column = entry.getKey();
				log.debug("Appending column: {}", column);
				appendValue(jsonValue.get(column), entry.getValue(), appender);
			}
			appender.endRow();
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter putToDb!", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Converts values in Debezium.Date format from epoch days into epoch microseconds
	 *
	 * @param field
	 * @param value value to convert
	 */
	private void appendValue(JsonElement value, SchemaElement field, DuckDBAppender appender) throws SQLException {
		if (value == null || value.isJsonNull()) {
			appender.append(null);
		} else if (field.isDate()) {
			var val = LocalDate.ofEpochDay(value.getAsInt())
					.format(DateTimeFormatter.ISO_LOCAL_DATE);
			appender.append(val);
		} else if (field.isTimestamp()) {
			appender.appendLocalDateTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(value.getAsLong()), ZoneOffset.UTC));
		} else if (value.isJsonPrimitive()) {
			appender.append(value.getAsString());
		} else {
			appender.append(value.toString());
		}
	}

	protected void memoized(JsonArray jsonSchema) {
		log.debug("New schema has been provided {}", jsonSchema);
		var deserialized = deserialize(jsonSchema);
		close();

		try (final var stmt = this.conn.createStatement()) {
			for (var element : deserialized) {
				log.debug("Preparing column: {}", element.field());
				if (this.schema.putIfAbsent(element.field(), element) == null) {
					log.debug("Alter {} add column: {}", this.tableName, element);
					stmt.execute(MessageFormat.format("ALTER TABLE {0} ADD COLUMN {1} {2};",
							this.tableName, element.field(), element.dbType()));
				}
			}
			this.appender = this.conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, this.tableName);
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter adjustSchema!", e);
			throw new RuntimeException(e);
		}
	}

	public void close() {
		try {
			if (this.appender != null) {
				this.appender.close();
			}
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter close!", e);
			throw new RuntimeException(e);
		}
	}

	public JsonElement getJsonSchema() {
		return this.gson.toJsonTree(this.schema.values());
	}

	@Override
	public void adjustSchema(JsonArray debeziumFields) {
		memoized(debeziumFields);
	}

	@Override
	public boolean isMissingAnyColumn(JsonObject jsonValue) {
		for (var column : jsonValue.keySet()) {
			if (!this.schema.containsKey(column)) {
				log.info("Missing column: {}, schema has to be updated", column);
				return true;
			}
		}
		return false;
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	protected record SchemaElement(String type, boolean optional, @SerializedName("default") String defaultValue,
								   String name, Integer version, String field) {
		public String columnDefinition() {
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

		public boolean isVarchar() {
			return Objects.equals(this.type, "string");
		}
	}
}
