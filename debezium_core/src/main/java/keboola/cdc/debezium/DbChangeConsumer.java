package keboola.cdc.debezium;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import keboola.cdc.debezium.converter.JsonConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static keboola.cdc.debezium.AbstractDebeziumTask.KBC_EVENT_TIMESTAMP_FIELD;
import static keboola.cdc.debezium.AbstractDebeziumTask.MAX_APPENDER_CACHE_SIZE;

@Slf4j
public class DbChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
	private static final Gson GSON = new Gson();

	private final AtomicInteger count;
	private final ConcurrentMap<String, JsonConverter> converters;
	private final Path jsonSchemaFilePath;
	private final DuckDbWrapper dbWrapper;
	private final JsonConverter.ConverterProvider converterProvider;

	public DbChangeConsumer(String resultFolder, DuckDbWrapper dbWrapper,
							JsonConverter.ConverterProvider converterProvider) {
		this.count = new AtomicInteger(0);
		this.jsonSchemaFilePath = Path.of(resultFolder, "schema.json");
		this.converters = new ConcurrentHashMap<>();
		this.dbWrapper = dbWrapper;
		this.converterProvider = converterProvider;
		init();
	}

	private void init() {
		var schemaFile = this.jsonSchemaFilePath.toFile();
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
		SyncStats.setProcessing(true);
		if (this.count.get() == 0) {
			SyncStats.started(System.currentTimeMillis());
			log.info("Starting to process records, set started: {} [millis].", SyncStats.started());
		}
		log.debug("Starting to process new batch of records: {}.", records.size());
		try {
			for (final var r : records) {
				handle(r.key(), r.value());
				committer.markProcessed(r);
				if (this.count.incrementAndGet() % MAX_APPENDER_CACHE_SIZE == 0) {
					log.info("Flushing all tables");
					this.converters.forEach((k, v) -> {
						try {
							v.flush();
						} catch (SQLException e) {
							log.error("Error during flushing", e);
						}
					});
				}
			}
			committer.markBatchFinished();
		} catch (StopEngineException e) {
			committer.markBatchFinished();
			throw e;
		} finally {
			SyncStats.recordCount(this.count.intValue());
			SyncStats.setProcessing(false);
			SyncStats.setLastRecord(ZonedDateTime.now());
			log.debug("Batch processing done, total records: {}.", this.count.get());
		}
	}

	private void handle(String key, String value) {
		log.trace("Key: {}", key);
		log.trace("Value: {}", value);
		var pair = extractTableNameAndPayload(value);
		final var tableIdentifier = pair.getKey();
		log.trace("Table identifier: {}", tableIdentifier);
		final var payload = pair.getValue();
		log.trace("Payload: {}", payload);
		final JsonConverter converter;
		if (shouldStop(payload)) {
			throw new StopEngineException("Stop processing, records out of scope.");
		}
		if (this.converters.containsKey(tableIdentifier)) {
			log.trace("Converter for {} already exists.", tableIdentifier);
			converter = this.converters.get(tableIdentifier);
			if (converter.isMissingAnyColumn(payload)) {
				log.trace("Unfortunately some columns are missing in payload. Adjusting schema.");
				var fields = extractSchemaFields(value);
				converter.adjustSchema(fields);
			}
		} else {
			log.trace("Converter missing for {}, create new.", tableIdentifier);
			var fields = extractSchemaFields(value);
			converter = this.converterProvider.getConverter(GSON, this.dbWrapper, tableIdentifier, fields);
			this.converters.put(tableIdentifier, converter);
		}
		converter.processJson(payload);
	}

	private boolean shouldStop(final JsonObject payload) {
		if (SyncStats.isSnapshotInProgress() || SyncStats.isInitialSnapshotOnly()) {
			log.trace("Snapshot phase, do not stop processing.");
			return false;
		}
		if (!payload.has(KBC_EVENT_TIMESTAMP_FIELD)) {
			log.trace("No timestamp field in payload, do not stop processing.");
			return false;
		}
		long timestampField = payload.getAsJsonPrimitive(KBC_EVENT_TIMESTAMP_FIELD)
				.getAsLong();
		if (timestampField > SyncStats.started() + 500) { // delta 500ms
			log.info("Stop processing out of scope records, {}: {} and started: {}",
					KBC_EVENT_TIMESTAMP_FIELD, timestampField, SyncStats.started());
			return true;
		}
		return false;
	}

	private static Pair<String, JsonObject> extractTableNameAndPayload(String value) {
		JsonObject payload = null;
		String tableName = null;
		try {
			var reader = new JsonReader(new StringReader(value));
			reader.beginObject();
			while (reader.hasNext()) {
				var nextName = reader.nextName();
				if ("payload".equals(nextName)) {
					payload = JsonParser.parseReader(reader).getAsJsonObject();
				} else if ("schema".equals(nextName)) {
					reader.beginObject();
					while (reader.hasNext()) {
						nextName = reader.nextName();
						if ("name".equals(nextName)) {
							tableName = reader.nextString().replace(".Value", "");
						} else {
							reader.skipValue(); // ignore other fields
						}
					}
					reader.endObject();
				} else {
					reader.skipValue(); // ignore other fields
				}
			}
			reader.endObject();
			reader.close();
		} catch (Exception e) {
			log.error("Here is error", e);
			throw new RuntimeException(e);
		}
		if (tableName == null || payload == null) {
			throw new IllegalArgumentException("Table name or payload not found in schema");
		}
		return new ImmutablePair<>(tableName, payload);
	}

	private static JsonArray extractSchemaFields(String value) {
		try {
			JsonReader reader = new JsonReader(new StringReader(value));
			reader.beginObject();
			while (reader.hasNext()) {
				if ("schema".equals(reader.nextName())) {
					reader.beginObject();
					while (reader.hasNext()) {
						if ("fields".equals(reader.nextName())) {
							JsonArray fields = JsonParser.parseReader(reader).getAsJsonArray();
							reader.close();
							return fields;
						} else {
							reader.skipValue(); // ignore other fields
						}
					}
					reader.endObject();
				} else {
					reader.skipValue(); // ignore other fields
				}
			}
			reader.endObject();
			reader.close();
		} catch (Exception e) {
			log.error("Here is error", e);
			throw new RuntimeException(e);
		}
		throw new IllegalArgumentException("Schema fields not found in schema");
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
		var parent = this.jsonSchemaFilePath.getParent().toFile();
		if (!parent.exists() && !parent.mkdirs()) {
			return;
		}
		// Convert the list to JSON and write it to a file
		try (FileWriter writer = new FileWriter(this.jsonSchemaFilePath.toFile())) {
			GSON.toJson(obj, writer);
		}
	}
}
