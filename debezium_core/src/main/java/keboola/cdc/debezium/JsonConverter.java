package keboola.cdc.debezium;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.util.Set;

public interface JsonConverter {

	static JsonConverter dbConverter(DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) throws IOException {
		return new JsonToDbConverter(dbWrapper, tableName, initialSchema);
	}

	void processJson(int lineNumber, Set<String> key, JsonObject jsonValue, JsonObject jsonSchema) throws IOException;

	void close() ;

	JsonElement getSchema();
}
