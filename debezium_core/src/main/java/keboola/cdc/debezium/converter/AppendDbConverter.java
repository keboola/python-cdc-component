package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;

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
			super.createTable(deserialized);
		}
	}

	@Override
	protected void adjustSchemaIfNecessary(final JsonArray jsonSchema) {
		if (!Objects.equals(getMemoized().lastDebeziumSchema(), jsonSchema)) {
			memoized(jsonSchema);
		}
	}

	@Override
	protected String upsertQuery(String tableName, List<String> columns) {
		return MessageFormat.format("INSERT INTO {0} ({1}) VALUES (?{2});",
				tableName, String.join(", ", columns), ", ?".repeat(columns.size() - 1));
	}
}
