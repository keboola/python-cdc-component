package keboola.cdc.debezium;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class DbChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
	private static final Gson GSON = new Gson();

	private final AtomicInteger count;
	private final ConcurrentMap<String, JsonToDbConverter> converters;
	private final String jsonSchemaFilePath;
	private final DuckDbWrapper dbWrapper;
	@SuppressWarnings("unused")
	private final SyncStats syncStats;


	public DbChangeConsumer(AtomicInteger count, String resultFolder, SyncStats syncStats) {
		this.count = count;
		this.jsonSchemaFilePath = Path.of(resultFolder, "schema.json").toString();
		this.converters = new ConcurrentHashMap<>();
		this.dbWrapper = new DuckDbWrapper();
		this.syncStats = syncStats;
		init();
	}

	private void init() {
		var schemaFile = new File(this.jsonSchemaFilePath);
		if (schemaFile.exists()) {
			try {
				var schemaJson = new JsonParser().parse(new FileReader(schemaFile)).getAsJsonObject();
				for (var entry : schemaJson.entrySet()) {
					var tableIdentifier = entry.getKey();
					var initSchema = entry.getValue().getAsJsonArray();
					var converter = new JsonToDbConverter(GSON, this.dbWrapper, tableIdentifier, initSchema);
					this.converters.put(tableIdentifier, converter);
				}
			} catch (IOException e) {
				log.error("{}", e.getMessage(), e);
			}
		}
	}

	@Override
	public void handleBatch(List<ChangeEvent<String, String>> records,
	                        DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer)
			throws InterruptedException {
		this.syncStats.setLastRecord(ZonedDateTime.now());
		for (final var r : records) {
			this.count.incrementAndGet();
			try {
				writeToDb(r.key(), r.value());
			} catch (IOException e) {
				log.error("{}", e.getMessage(), e);
				throw new InterruptedException(e.toString());
			}
			committer.markProcessed(r);
		}
		committer.markBatchFinished();
		log.info("Processed {} records", this.count.get());
		this.syncStats.setRecordCount(this.count.intValue());
	}

	private void writeToDb(String key, String value) throws IOException {
		var jsonParser = new JsonParser();

		var valueJson = jsonParser.parse(value).getAsJsonObject();
		var payload = valueJson.getAsJsonObject("payload");
		var schema = valueJson.getAsJsonObject("schema");

		var keySet = extractPrimaryKey(jsonParser.parse(key).getAsJsonObject());
		var tableIdentifier = schema.get("name").getAsString().replace(".Value", "");

		this.converters.computeIfAbsent(tableIdentifier,
						tableName -> new JsonToDbConverter(GSON, this.dbWrapper, tableName, null))
				.processJson(keySet, payload, schema);
	}

	private static Set<String> extractPrimaryKey(JsonObject key) {
		var keySet = StreamSupport.stream(
						key.getAsJsonObject("schema")
								.get("fields")
								.getAsJsonArray()
								.spliterator(), false)
				.map(jsonElement -> jsonElement.getAsJsonObject().get("field").getAsString())
				.collect(Collectors.toUnmodifiableSet());
		log.debug("Primary key columns: {}", keySet);
		return keySet;
	}

	public AtomicInteger getRecordsCount() {
		return this.count;
	}

	public void closeWriterStreams() {
		this.converters.values()
				.forEach(JsonToDbConverter::close);
		this.dbWrapper.close();
	}

	public void storeSchemaMap() throws IOException {
		JsonObject obj = new JsonObject();
		for (var e : this.converters.entrySet()) {
			obj.add(e.getKey(), e.getValue().getSchema());
		}
		// Convert the list to JSON and write it to a file
		try (FileWriter writer = new FileWriter(this.jsonSchemaFilePath)) {
			GSON.toJson(obj, writer);
		}

	}
}
