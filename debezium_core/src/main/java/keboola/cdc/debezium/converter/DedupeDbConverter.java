package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.MessageFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class DedupeDbConverter extends AbstractDbConverter implements JsonConverter {

	public DedupeDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) {
		super(gson, dbWrapper, tableName, initialSchema);
	}

	@Override
	protected void init(@Nullable JsonArray initialSchema) {
		log.info("Initializing schema for json to DB converter {}.", getTableName());
		List<SchemaElement> deserialized;
		if (initialSchema != null) {
			log.info("Initializing schema with {} default fields: {}", initialSchema.size(), initialSchema);
			if (!initialSchema.contains(PRIMARY_KEY_JSON_ELEMENT)) {
				initialSchema.add(PRIMARY_KEY_JSON_ELEMENT);
			}
			deserialized = deserialize(initialSchema);
		} else {
			log.info("No initial schema for table {} using schema with PK only.", getTableName());
			var primaryKey = getGson().fromJson(PRIMARY_KEY_JSON_ELEMENT, SchemaElement.class);
			deserialized = List.of(primaryKey);
		}
		createTable(deserialized);
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
