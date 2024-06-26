package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import keboola.cdc.debezium.DuckDbWrapper;

import java.sql.SQLException;

public interface JsonConverter {

	void processJson(JsonObject jsonValue);

	void close();

	JsonElement getJsonSchema();

	void adjustSchema(JsonArray debeziumFields);

	boolean isMissingAnyColumn(JsonObject jsonValue);

	void flush() throws SQLException;

	interface ConverterProvider {
		JsonConverter getConverter(Gson gson, DuckDbWrapper dbWrapper,
								   String tableName, JsonArray initialSchema);
	}
}
