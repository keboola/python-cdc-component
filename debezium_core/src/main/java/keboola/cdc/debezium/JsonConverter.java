package keboola.cdc.debezium;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.duckdb.DuckDBColumnType;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Set;

public interface JsonConverter {
	public static final String KBC_PRIMARY_KEY = "kbc__primary_key";
	public static final JsonElement PRIMARY_KEY_JSON_ELEMENT = createPrimaryKeyJsonElement();

	void processJson(Set<String> key, JsonObject jsonValue, JsonObject debeziumSchema);

	void close();

	JsonElement getSchema();

	interface ConverterProvider {
		JsonConverter getConverter(Gson gson, DuckDbWrapper dbWrapper,
								   String tableName, @Nullable JsonArray initialSchema);
	}

	private static JsonObject createPrimaryKeyJsonElement() {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("field", KBC_PRIMARY_KEY);
		jsonObject.addProperty("type", "string");
		jsonObject.addProperty("optional", false);
		return jsonObject;
	}
}
