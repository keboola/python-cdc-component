package keboola.cdc.debezium;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import keboola.cdc.debezium.converter.JsonConverter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DbChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
	private static final Gson GSON = new Gson();

	private final AtomicInteger count;
	private final ConcurrentMap<String, JsonConverter> converters;
	private final String jsonSchemaFilePath;
	private final DuckDbWrapper dbWrapper;
	@SuppressWarnings("unused")
	private final SyncStats syncStats;
	private final JsonConverter.ConverterProvider converterProvider;


	public DbChangeConsumer(AtomicInteger count, String resultFolder,
							SyncStats syncStats, DuckDbWrapper dbWrapper,
							JsonConverter.ConverterProvider converterProvider) {
		this.count = count;
		this.jsonSchemaFilePath = Path.of(resultFolder, "schema.json").toString();
		this.converters = new ConcurrentHashMap<>();
		this.dbWrapper = dbWrapper;
		this.syncStats = syncStats;
		this.converterProvider = converterProvider;
		init();
	}

	private void init() {
		var schemaFile = new File(this.jsonSchemaFilePath);
		if (schemaFile.exists()) {
			try {
				var jsonElement = JsonParser.parseReader(new FileReader(schemaFile));
				if (!jsonElement.isJsonNull()) {
					var schemaJson = jsonElement.getAsJsonObject();
					for (var entry : schemaJson.entrySet()) {
						var tableIdentifier = entry.getKey();
						var initSchema = entry.getValue().getAsJsonArray();
						var converter = this.converterProvider.getConverter(GSON, this.dbWrapper, tableIdentifier, initSchema);
						this.converters.put(tableIdentifier, converter);
					}
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
			writeToDb(r.key(), r.value());
			committer.markProcessed(r);
		}
		committer.markBatchFinished();
		log.info("Processed {} records", this.count.get());
		this.syncStats.setRecordCount(this.count.intValue());
		this.syncStats.addRecords(this.count.intValue());
		this.syncStats.setEndTime(ZonedDateTime.now());
	}

	private void writeToDb(String key, String value) {

		var valueJson = JsonParser.parseString(value).getAsJsonObject();
		var payload = valueJson.getAsJsonObject("payload");
		var schema = valueJson.getAsJsonObject("schema");

		var tableIdentifier = schema.get("name").getAsString().replace(".Value", "");

		this.converters.computeIfAbsent(tableIdentifier,
						tableName -> {
							log.info("Creating new converter for table {}", tableName);
							var fields = schema.get("fields").getAsJsonArray();
							return this.converterProvider.getConverter(GSON, this.dbWrapper, tableName, fields);
						})
				.processJson(key, payload, schema);
	}

	public AtomicInteger getRecordsCount() {
		return this.count;
	}

	public void closeWriterStreams() {
		this.converters.values()
				.forEach(JsonConverter::close);
		this.dbWrapper.close();
	}

	public void storeSchemaMap() throws IOException {
		JsonObject obj = new JsonObject();
		for (var e : this.converters.entrySet()) {
			obj.add(e.getKey(), e.getValue().getJsonSchema());
		}
		// Convert the list to JSON and write it to a file
		try (FileWriter writer = new FileWriter(this.jsonSchemaFilePath)) {
			GSON.toJson(obj, writer);
		}

	}
}
