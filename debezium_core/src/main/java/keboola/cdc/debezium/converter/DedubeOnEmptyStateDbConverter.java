package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class DedubeOnEmptyStateDbConverter extends AbstractDbConverter implements JsonConverter {

	private final String baseTableName;
	private final AtomicInteger chunk;
	private final AtomicInteger actualChunkSize;

	public DedubeOnEmptyStateDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, JsonArray initialSchema) {
		super(gson, dbWrapper, initialSchema);
		this.chunk = new AtomicInteger(0);
		this.actualChunkSize = new AtomicInteger(0);
		this.baseTableName = tableName.replaceAll("\\.", "_");
		init(initialSchema);
	}

	protected void init(final JsonArray initialSchema) {
		log.debug("Initializing schema with default fields: {}", initialSchema);
		final var deserialized = deserialize(initialSchema);
		if (!deserialized.contains(ORDER_EVENT)) {
			deserialized.add(ORDER_EVENT);
		}
		log.debug("Deserialized schema: {}", deserialized);
		createNewChunkTable(deserialized);
	}

	@Override
	public synchronized void processJson(JsonObject jsonValue) {
		store(jsonValue);
	}

	private void createNewChunkTable(final List<SchemaElement> deserializedSchema) {
		deserializedSchema.forEach(schemaElement ->
				getSchema().putIfAbsent(schemaElement.field(), schemaElement));
		String columnDefinition = getSchema().values()
				.stream()
				.map(SchemaElement::columnDefinition)
				.collect(Collectors.joining(", "));
		createNewChunkTable(columnDefinition);
	}

	private void createNewChunkTable(String columnDefinition) {
		var tableName = this.baseTableName + "_chunk_" + this.chunk.getAndIncrement();
		createTable(columnDefinition, tableName);
		this.actualChunkSize.set(0);
	}

	public void adjustSchema(JsonArray jsonSchema) {
		log.debug("New schema has been provided {}", jsonSchema);
		var deserialized = deserialize(jsonSchema);
		close();
		createNewChunkTable(deserialized);
	}
}
