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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
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
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Getter
@Setter(AccessLevel.PROTECTED)
abstract class AbstractDbConverter implements JsonConverter {
	protected static final SchemaElement ORDER_EVENT = new SchemaElement("int", false, null, null, 1, "event_order", true);
	private static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
	}.getType();

	private final DuckDBConnection conn;
	private final Gson gson;
	private final LinkedHashMap<String, SchemaElement> schema;
	private final AtomicInteger order;
	private DuckDBAppender appender;

	public AbstractDbConverter(Gson gson, DuckDbWrapper dbWrapper, JsonArray initialSchema) {
		this.gson = gson;
		this.conn = dbWrapper.getConn();
		this.schema = new LinkedHashMap<>(initialSchema.size());
		this.order = new AtomicInteger(0);
	}

	protected void createTable(String columnDefinition, String tableName) {
		try (final var stmt = this.conn.createStatement()) {
			log.debug("Creating table {} if does not exits.", tableName);
			stmt.execute("CREATE TABLE IF NOT EXISTS \"" + tableName + "\" (" + columnDefinition + ")");
			this.appender = this.conn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName);
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter schema initialization!", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create new entry or update current one by ID in database
	 *
	 * @param jsonValue json value to be inserted
	 */
	protected void store(final JsonObject jsonValue) {
		try {
			this.appender.beginRow();
			for (var entry : this.schema.entrySet()) {
				String column = entry.getKey();
				log.debug("Appending column: {}", column);
				appendValue(jsonValue.get(column), entry.getValue(), this.appender);
			}
			this.appender.endRow();
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter putToDb!", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Converts values in Debezium format and append them
	 *
	 * @param value    value to convert
	 * @param field    schema field
	 * @param appender appender to append value
	 */
	private void appendValue(JsonElement value, SchemaElement field, DuckDBAppender appender) throws SQLException {
		if (field.orderEvent()) {
			appender.append(this.order.getAndIncrement());
		} else if (value == null || value.isJsonNull()) {
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
	public boolean isMissingAnyColumn(JsonObject jsonValue) {
		for (var column : jsonValue.keySet()) {
			if (!this.schema.containsKey(column)) {
				log.info("Missing column: {}, schema has to be updated", column);
				return true;
			}
		}
		return false;
	}

	protected List<SchemaElement> deserialize(JsonArray fields) {
		return this.gson.fromJson(fields, SCHEMA_ELEMENT_LIST_TYPE);
	}

	@JsonInclude(JsonInclude.Include.NON_NULL)
	protected record SchemaElement(String type, boolean optional, @SerializedName("default") String defaultValue,
								   String name, Integer version, String field, boolean orderEvent) {

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
	}
}
