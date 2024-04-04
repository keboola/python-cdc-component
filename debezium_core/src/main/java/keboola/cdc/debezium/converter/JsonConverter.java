package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import keboola.cdc.debezium.DuckDbWrapper;

public interface JsonConverter {

	void processJson(String key, JsonObject jsonValue, JsonObject debeziumSchema);

	void close();

	JsonElement getJsonSchema();

	interface ConverterProvider {
		JsonConverter getConverter(Gson gson, DuckDbWrapper dbWrapper,
								   String tableName, JsonArray initialSchema);
	}
}
