package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class DedupeDbConverter extends AbstractDbConverter implements JsonConverter {

	public static final String SUFFIX = "_all";
	private String lastKey;

	public DedupeDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, JsonArray initialSchema) {
		super(gson, dbWrapper, tableName + SUFFIX, initialSchema);
	}

	@Override
	public synchronized void processJson(String key, JsonObject jsonValue) {
		super.processJson(key, jsonValue);
		this.lastKey = key;
	}

	private void createView() {
		var viewName = getTableName().substring(0, getTableName().length() - 4);

		String viewSql;
		if (Objects.isNull(this.lastKey)) {
			viewSql = MessageFormat.format(
					"CREATE VIEW {0} AS " +
							"SELECT t.* " +
							"FROM {1} t;",
					viewName, getTableName());
		} else {
			var compositePk = extractKeyNames(this.lastKey);
			viewSql = MessageFormat.format(
					"CREATE VIEW {0} AS " +
							"SELECT t.* " +
							"FROM {1} t " +
							"JOIN ( " +
							"  SELECT {2}, MAX(kbc__event_timestamp) AS max_timestamp " +
							"  FROM {1} " +
							"  GROUP BY {2} " +
							") r " +
							"ON t.{2} = r.{2} AND t.kbc__event_timestamp = r.max_timestamp;",
					viewName, getTableName(), compositePk
			);
		}
		try (Statement statement = getConn().createStatement()) {
			statement.execute(viewSql);
		} catch (SQLException e) {
			throw new RuntimeException("Failed to create view " + viewName, e);
		}
	}

	@Override
	public void close() {
		createView();
		super.close();
	}

	private static String extractKeyNames(String keyRaw) {
		var key = JsonParser.parseString(keyRaw).getAsJsonObject();
		log.debug("Extracting primary key columns from key: {}", key);
		var keySet = StreamSupport.stream(
						key.getAsJsonObject("schema")
								.get("fields")
								.getAsJsonArray()
								.spliterator(), false)
				.map(jsonElement -> jsonElement.getAsJsonObject().get("field").getAsString())
				.collect(Collectors.joining(", "));
		log.debug("Primary key columns: {}", keySet);
		return keySet;
	}
}
