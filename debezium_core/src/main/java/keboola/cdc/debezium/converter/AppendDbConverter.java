package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBAppender;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
public class AppendDbConverter extends AbstractDbConverter implements JsonConverter {

	public AppendDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, JsonArray initialSchema) {
		super(gson, dbWrapper, tableName, initialSchema);
	}

	@Override
	protected void init(JsonArray initialSchema) {
		log.info("Initializing schema for json to DB converter {}.", getTableName());
		List<SchemaElement> deserialized;
		if (initialSchema != null) {
			log.info("Initializing schema with {} default fields: {}", initialSchema.size(), initialSchema);
			if (initialSchema.contains(PRIMARY_KEY_JSON_ELEMENT)) {
				initialSchema.remove(PRIMARY_KEY_JSON_ELEMENT);
			}
			deserialized = deserialize(initialSchema);
			log.debug("Deserialized schema: {}", deserialized);
			super.createTable(deserialized);
		}
	}

	@Override
	public void processJson(String keyJson, JsonObject jsonValue, JsonObject debeziumSchema) {
		log.debug("Processing json value {} for table {} with scheme {}.", jsonValue, getTableName(), debeziumSchema);
		adjustSchemaIfNecessary(debeziumSchema.get("fields").getAsJsonArray());

		putToDbUseAppender(jsonValue);
		log.debug("Json added to table {} processed.", getTableName());
	}

	private void putToDbUseAppender(final JsonObject jsonValue) {
		try {

			var appender = getMemoized().appender();
			appender.beginRow();
			for (int i = 0; i < getMemoized().columns().size(); i++) {
				var column = getMemoized().getColumn(i);
				appendValue(jsonValue.get(column), getSchema().get(column), appender);
			}
			appender.endRow();
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter putToDb!", e);
			throw new RuntimeException(e);
		}
	}

	private void appendValue(JsonElement value, SchemaElement field, DuckDBAppender appender) throws SQLException {
		if (value.isJsonNull()) {
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

	@Override
	protected String upsertQuery(String tableName, List<String> columns) {
		return MessageFormat.format("INSERT INTO {0} ({1}) VALUES (?{2});",
				tableName, String.join(", ", columns), ", ?".repeat(columns.size() - 1));
	}
}
