package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBConnection;

import java.lang.reflect.Type;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class AppendDbConverter extends AbstractDbConverter implements JsonConverter {
	private static final Type SCHEMA_ELEMENT_LIST_TYPE = new TypeToken<List<SchemaElement>>() {
	}.getType();

	private final String tableName;

	public AppendDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, JsonArray initialSchema) {
		super(gson, dbWrapper, initialSchema);
		this.tableName = tableName.replaceAll("\\.", "_");
		init(initialSchema);
	}

	protected void init(final JsonArray initialSchema) {
		log.debug("Initializing schema with default fields: {}", initialSchema);
		final var deserialized = deserialize(initialSchema);
		log.debug("Deserialized schema: {}", deserialized);
		var columnDefinition = deserialized.stream()
				.map(schemaElement -> {
					//  initialize schema and prepare column definition
					getSchema().put(schemaElement.field(), schemaElement);
					return schemaElement.columnDefinition();
				})
				.collect(Collectors.joining(", "));
		createTable(columnDefinition, this.tableName);
	}

	@Override
	public synchronized void processJson(JsonObject jsonValue) {
		store(jsonValue);
	}

	public void adjustSchema(JsonArray jsonSchema) {
		log.debug("New schema has been provided {}", jsonSchema);
		var deserialized = deserialize(jsonSchema);
		close();
		try (final var stmt = getConn().createStatement()) {
			for (var element : deserialized) {
				log.debug("Preparing column: {}", element.field());
				if (getSchema().putIfAbsent(element.field(), element) == null) {
					log.debug("Alter {} add column: {}", this.tableName, element);
					stmt.execute(MessageFormat.format("ALTER TABLE {0} ADD COLUMN {1} {2};",
							this.tableName, element.field(), element.dbType()));
				}
			}
			setAppender(getConn().createAppender(DuckDBConnection.DEFAULT_SCHEMA, this.tableName));
		} catch (SQLException e) {
			log.error("Error during JsonToDbConverter adjustSchema!", e);
			throw new RuntimeException(e);
		}
	}
}
