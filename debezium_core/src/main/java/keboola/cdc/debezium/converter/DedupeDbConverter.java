package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class DedupeDbConverter extends AbstractDbConverter implements JsonConverter {

	public DedupeDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, JsonArray initialSchema) {
		super(gson, dbWrapper, tableName, initialSchema);
	}

	@Override
	protected void init(JsonArray initialSchema) {
		log.info("Initializing schema for json to DB converter {}.", getTableName());
		log.info("Initializing schema with {} default fields: {}", initialSchema.size(), initialSchema);
		if (!initialSchema.contains(PRIMARY_KEY_JSON_ELEMENT)) {
			initialSchema.add(PRIMARY_KEY_JSON_ELEMENT);
		}
		createTable(deserialize(initialSchema));
	}

	@Override
	String upsertQuery(String tableName, List<String> columns) {
		return MessageFormat.format("INSERT OR REPLACE INTO {0} ({1}) VALUES (?{2});",
				tableName, String.join(", ", columns), ", ?".repeat(columns.size() - 1));
	}

	@Override
	public void processJson(String keyJson, JsonObject jsonValue, JsonObject debeziumSchema) {
		var keySet = extractPrimaryKey(JsonParser.parseString(keyJson).getAsJsonObject());
		var fields = debeziumSchema.get("fields").getAsJsonArray();
		if (!fields.contains(PRIMARY_KEY_JSON_ELEMENT)) {
			fields.add(PRIMARY_KEY_JSON_ELEMENT);
		}
		processJson(keySet, jsonValue, debeziumSchema, fields);
	}

	private static Set<String> extractPrimaryKey(JsonObject key) {
		log.debug("Extracting primary key columns from key: {}", key);
		var keySet = StreamSupport.stream(
						key.getAsJsonObject("schema")
								.get("fields")
								.getAsJsonArray()
								.spliterator(), false)
				.map(jsonElement -> jsonElement.getAsJsonObject().get("field").getAsString())
				.collect(Collectors.toUnmodifiableSet());
		log.debug("Primary key columns: {}", keySet);
		return keySet;
	}
}
