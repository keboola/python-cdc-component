package keboola.cdc.debezium;

import com.google.gson.*;
import com.opencsv.CSVWriter;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
	public static final String TABLE_NAME_COL = "kbc__table";
	public static final String KEY_EVENT_ORDER_COL = "kbc__event_order";
	public static final String KEY_NULL_VALUE_PLACEHOLDER = "KBC__NULL";
	private final AbstractDebeziumTask abstractDebeziumTask;
	private final Logger logger;

	private final AtomicInteger count;
	private final Map<String, CSVWriter> csvWriterMap;
	private final Map<String, JsonArray> lastSchema;

	private final String resultFolder;

	@SuppressWarnings("unused")
	private ZonedDateTime lastRecord;


	public ChangeConsumer(AbstractDebeziumTask abstractDebeziumTask, Logger logger, AtomicInteger count, ZonedDateTime lastRecord, String resultFolder) {
		this.abstractDebeziumTask = abstractDebeziumTask;
		this.logger = logger;
		this.count = count;
		this.lastRecord = lastRecord;
		this.csvWriterMap = new HashMap<>();
		this.lastSchema = new HashMap<>();
		this.resultFolder = resultFolder;
	}

	@Override
	public void handleBatch(List<ChangeEvent<String, String>> records, DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
		lastRecord = ZonedDateTime.now();

		for (ChangeEvent<String, String> r : records) {

			this.count.incrementAndGet();
			try {
				this.writeToCSV(r.key(), r.value());
			} catch (IOException e) {
				e.printStackTrace();
				throw new InterruptedException(e.toString());
			}


			committer.markProcessed(r);
		}

		committer.markBatchFinished();
	}

	/**
	 * Creates or gets writer from Cache. The writers are cached per table and expanding schema.
	 * => there may be multiple writers per table with different schemas.
	 *
	 * @param tablePath
	 * @param tableKey
	 * @param schemaHash
	 * @return
	 * @throws IOException
	 */
	private CSVWriter getWriter(Path tablePath, String tableKey, int schemaHash) throws IOException {
		// TODO: Handle chaging schema / index by columns too and write to separate files
		String hashKey = tableKey + schemaHash;
		List<String> schemaColumnList = this.getSchemaColumnList(this.lastSchema.get(tableKey));
		// append system column
		schemaColumnList.add(KEY_EVENT_ORDER_COL);
		String[] columns = schemaColumnList.toArray(String[]::new);

		if (!this.csvWriterMap.containsKey(hashKey)) {
			Path dirPath = Paths.get(tablePath.toString());

			File directory = new File(dirPath.toString());
			if (!directory.exists()) {
				directory.mkdir();
			}
			String resultPath = dirPath.resolve(schemaHash + ".csv").toString();
			CSVWriter writer = new CSVWriter(new FileWriter(resultPath));
			// Write the keys as the header row
			writer.writeNext(columns);
			this.csvWriterMap.put(hashKey, writer);
		}
		return this.csvWriterMap.get(hashKey);
	}

	private String convertEntryValueToString(JsonElement entry) {
		String result;
		if (entry.isJsonNull()) {
			result = KEY_NULL_VALUE_PLACEHOLDER;
		} else if (entry.isJsonPrimitive()) {
			result = entry.getAsString();
		} else {
			result = entry.toString();
		}

		return result;
	}

	private void writeToCSV(String key, String value) throws IOException {
		JsonObject valueJson = new JsonParser().parse(value).getAsJsonObject();
		JsonObject payload = valueJson.getAsJsonObject("payload");
		JsonObject schema = valueJson.getAsJsonObject("schema");
//        String tableName = payload.get(TABLE_NAME_COL).getAsString();

		String tableIdentifier = schema.get("name").getAsString().replace(".Value", "");


		Path resultPath = Path.of(this.resultFolder, tableIdentifier + ".csv");


		int schemaHash = this.updateLastSchema(tableIdentifier, schema.getAsJsonArray("fields"));

		CSVWriter writer = this.getWriter(resultPath, tableIdentifier, schemaHash);

		List<String> valuesList = payload.entrySet().stream()
				.map(e -> this.convertEntryValueToString(e.getValue())).collect(Collectors.toList());
		// add helper order index for later dedupe
		valuesList.add(String.valueOf(this.count));

		writer.writeNext(valuesList.toArray(String[]::new));
	}

	/**
	 * @param tableKey Schema key
	 * @param columns
	 * @return hash key of the current schema to be used with writer
	 */
	private int updateLastSchema(String tableKey, JsonArray columns) {

		JsonArray existingSchema = this.lastSchema.get(tableKey);


		final List<String> newFieldNames = new ArrayList<>();


		if (existingSchema != null) {
			Iterable<JsonElement> existIter = columns::iterator;
			List<JsonElement> newSchemaList = StreamSupport
					.stream(existIter.spliterator(), false)
					.collect(Collectors.toList());

			List<String> newColumns = this.getSchemaColumnList(columns);
			newFieldNames.addAll(newColumns);
			List<String> existingFieldNames = this.getSchemaColumnList(existingSchema);
			newFieldNames.removeAll(existingFieldNames);

			List<JsonElement> newFields = newSchemaList.stream().filter(e -> newFieldNames.contains(e.getAsJsonObject()
					.get("field").getAsString())).collect(Collectors.toList());
			// update if changed
			// place at the end before system
			int insertAfter = existingFieldNames.indexOf("kbc__event_timestamp");
			List<JsonElement> schemaList = this.convertJsonArrayToList(existingSchema);
			JsonArray newArray = new JsonArray();
			for (JsonElement field : newFields) {
				schemaList.add(insertAfter, field);
			}

			schemaList.forEach(newArray::add);
			this.lastSchema.put(tableKey, newArray);

		} else {
			this.lastSchema.put(tableKey, columns);
		}
		return buildSchemaHashKey(tableKey);

	}

	private int buildSchemaHashKey(String key) {
		List<String> columnNames = this.getSchemaColumnList(this.lastSchema.get(key));
		Collections.sort(columnNames);
		return Arrays.hashCode(columnNames.toArray());
	}

	private List<String> getSchemaColumnList(JsonArray schema) {
		List<JsonElement> oldSchemaList = this.convertJsonArrayToList(schema);
		List<String> existingFieldNames = oldSchemaList.stream().map(e -> e.getAsJsonObject().get("field").getAsString()).collect(Collectors.toList());
		return existingFieldNames;
	}

	private List<JsonElement> convertJsonArrayToList(JsonArray arr) {
		Iterable<JsonElement> existIter = arr::iterator;
		List<JsonElement> newList = StreamSupport
				.stream(existIter.spliterator(), false)
				.collect(Collectors.toList());
		return newList;
	}

	private String[] getColumnsFromPayload(JsonObject payload) {
		// remove table column
		Set<String> keySet = payload.keySet();
		keySet.remove(TABLE_NAME_COL);

		List<String> resultList = keySet.stream().map(element -> element.equals("__deleted") ? "kbc__deleted" : element)
				.collect(Collectors.toList());
		resultList.add(KEY_EVENT_ORDER_COL);
		return resultList.toArray(new String[0]);


	}

	public AtomicInteger getRecordsCount() {
		return this.count;
	}

	public void closeWriterStreams() throws IOException {
		for (CSVWriter wr : this.csvWriterMap.values()) {
			wr.close();
		}

	}

	public void storeSchemaMap() throws IOException {
		JsonObject obj = new JsonObject();
		for (Map.Entry<String, JsonArray> e : this.lastSchema.entrySet()) {
			obj.add(e.getKey(), e.getValue());
		}
		// Convert the list to JSON and write it to a file
		Gson gson = new Gson();
		try (FileWriter writer = new FileWriter(Path.of(this.resultFolder, "schema.json").toString())) {
			gson.toJson(obj, writer);
		}

	}


	@Override
	public boolean supportsTombstoneEvents() {
		return DebeziumEngine.ChangeConsumer.super.supportsTombstoneEvents();
	}
}