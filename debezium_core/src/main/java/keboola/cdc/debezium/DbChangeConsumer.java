package keboola.cdc.debezium;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class DbChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
	private final AtomicInteger count;
	private final String resultFolder;
	private final ConcurrentMap<String, JsonConverter> converters;
	private final String jsonSchemaFilePath;
	private final DuckDbWrapper dbWrapper;

	public DbChangeConsumer(Logger logger, AtomicInteger count, String resultFolder) {
		this.count = count;
		this.resultFolder = resultFolder;
		this.jsonSchemaFilePath = Path.of(this.resultFolder, "schema.json").toString();
		this.converters = new ConcurrentHashMap<>();
		this.dbWrapper = new DuckDbWrapper();
		init();
	}

	private void init() {
		var schemaFile = new File(jsonSchemaFilePath);
		if (schemaFile.exists()) {
			try {
				var schemaJson = new JsonParser().parse(new FileReader(schemaFile)).getAsJsonObject();
				for (var entry : schemaJson.entrySet()) {
					var tableIdentifier = entry.getKey();
					var schema = entry.getValue().getAsJsonArray();
					var converter = JsonConverter.dbConverter(dbWrapper, tableIdentifier, schema);
					converters.put(tableIdentifier, converter);
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
		for (ChangeEvent<String, String> r : records) {
			this.count.incrementAndGet();
			try {
				this.writeToDb(r.key(), r.value());
			} catch (IOException e) {
				log.error("{}", e.getMessage(), e);
				throw new InterruptedException(e.toString());
			}
			committer.markProcessed(r);
		}
		committer.markBatchFinished();
		log.info("Processed {} records", this.count.get());
	}

	private void writeToDb(String key, String value) throws IOException {
		var valueJson = new JsonParser().parse(value).getAsJsonObject();
		var payload = valueJson.getAsJsonObject("payload");
		var schema = valueJson.getAsJsonObject("schema");

		var tableIdentifier = schema.get("name").getAsString().replace(".Value", "");

		var resultPath = Path.of(this.resultFolder, tableIdentifier);
		JsonConverter converter;
		if (converters.containsKey(tableIdentifier)) {
			converter = converters.get(tableIdentifier);
		} else {
			converter = JsonConverter.dbConverter(dbWrapper, tableIdentifier, null);
			converters.put(tableIdentifier, converter);
		}
		converter.processJson(this.count.get(), payload, schema);
	}


	public AtomicInteger getRecordsCount() {
		return this.count;
	}

	public void closeWriterStreams() throws IOException {
		converters.values()
				.forEach(JsonConverter::close);
	}

	public void storeSchemaMap() throws IOException {
		JsonObject obj = new JsonObject();
		for (var e : this.converters.entrySet()) {
			obj.add(e.getKey(), e.getValue().getSchema());
		}
		// Convert the list to JSON and write it to a file
		Gson gson = new Gson();
		try (FileWriter writer = new FileWriter(jsonSchemaFilePath)) {
			gson.toJson(obj, writer);
		}

	}


	@Override
	public boolean supportsTombstoneEvents() {
		return DebeziumEngine.ChangeConsumer.super.supportsTombstoneEvents();
	}
}
