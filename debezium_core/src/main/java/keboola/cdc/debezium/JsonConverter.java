package keboola.cdc.debezium;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

public interface JsonConverter {

	public static JsonConverter csvConverter(Path csvFolderPath, @Nullable JsonArray initialSchema) throws IOException {
		return new JsonToCsvConverter(csvFolderPath, initialSchema);
	}
	public static JsonConverter dbConverter(DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) throws IOException {
		return new JsonToDbConverter(dbWrapper, tableName, initialSchema);
	}

	void processJson(int lineNumber, JsonObject jsonValue, JsonObject jsonSchema) throws IOException;

	void close() ;

	JsonElement getSchema();
}
