package keboola.cdc.debezium;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBConnection;

import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
public class AppendDbConverter extends AbstractDbConverter implements JsonConverter {

	public AppendDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, @Nullable JsonArray initialSchema) {
		super(gson, dbWrapper, tableName, initialSchema);
	}

	@Override
	protected void init(@Nullable JsonArray initialSchema) {
		log.info("Initializing schema for json to DB converter {}.", getTableName());
		List<SchemaElement> deserialized;
		if (initialSchema != null) {
			log.info("Initializing schema with {} default fields: {}", initialSchema.size(), initialSchema);
			if (initialSchema.contains(PRIMARY_KEY_JSON_ELEMENT)) {
				initialSchema.remove(PRIMARY_KEY_JSON_ELEMENT);
			}
			deserialized = deserialize(initialSchema);
		} else {
			log.info("No initial schema for table {} using schema with PK only.", getTableName());
			var primaryKey = getGson().fromJson(PRIMARY_KEY_JSON_ELEMENT, SchemaElement.class);
			deserialized = List.of(primaryKey);
		}
		super.createTables(deserialized);
	}

	@Override
	String upsertQuery(String tableName, List<String> columns) {
		return MessageFormat.format("INSERT INTO {0} ({1}) VALUES (?{2});",
				tableName, String.join(", ", columns), ", ?".repeat(columns.size() - 1));
	}
}
