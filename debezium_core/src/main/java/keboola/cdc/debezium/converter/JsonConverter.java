package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import keboola.cdc.debezium.DuckDbWrapper;

import java.util.Set;

public interface JsonConverter {
	String KBC_PRIMARY_KEY = "kbc__primary_key";
	JsonElement PRIMARY_KEY_JSON_ELEMENT = createPrimaryKeyJsonElement();

	void processJson(Set<String> key, JsonObject jsonValue, JsonObject debeziumSchema);

	void close();

	JsonElement getSchema();

	interface ConverterProvider {
		JsonConverter getConverter(Gson gson, DuckDbWrapper dbWrapper,
								   String tableName, JsonArray initialSchema);
	}

	private static JsonObject createPrimaryKeyJsonElement() {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("field", KBC_PRIMARY_KEY);
		jsonObject.addProperty("type", "string");
		jsonObject.addProperty("optional", false);
		return jsonObject;
	}
}
