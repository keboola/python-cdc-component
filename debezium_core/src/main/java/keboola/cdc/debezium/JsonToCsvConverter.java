package keboola.cdc.debezium;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.opencsv.CSVWriter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import static keboola.cdc.debezium.ChangeConsumer.KEY_NULL_VALUE_PLACEHOLDER;

@Slf4j
public class JsonToCsvConverter implements JsonConverter {

	private static final String KEY_EVENT_ORDER_COL = "kbc__batch_event_order";
	private static final JsonElement KEY_EVENT_ORDER_FIELD = new JsonParser().parse("{\"field\":\"kbc__batch_event_order\", type:\"int\", \"optional\":true}");
	private final ConcurrentMap<String, Integer> columns;
	private final ConcurrentNavigableMap<Integer, String> csvSchema;
	@Getter
	private final JsonArray schema;
	private final AtomicInteger count;
	private CSVWriter csvWriter;
	private final Path csvFolderPath;

	public JsonToCsvConverter(Path csvFolderPath, @Nullable JsonArray initialSchema) throws IOException {
		this.count = new AtomicInteger(0);
		this.columns = new ConcurrentHashMap<>();
		this.csvSchema = new ConcurrentSkipListMap<>(Integer::compareTo);
		this.schema = new JsonArray();
		this.csvFolderPath = csvFolderPath;
		this.init(initialSchema);
	}

	private void init(@Nullable JsonArray initialSchema) throws IOException {
		File directory = new File(csvFolderPath.toString());
		if (!directory.exists()) {
			directory.mkdir();
		}
		if (initialSchema == null) {
			columns.put(KEY_EVENT_ORDER_COL, count.get());
			csvSchema.put(count.getAndIncrement(), KEY_EVENT_ORDER_COL);
			schema.add(KEY_EVENT_ORDER_FIELD);
		} else {
			updateColumns(initialSchema);
			log.info("Csv schema initialized: {}", Arrays.toString(csvSchema.values().toArray(String[]::new)));
		}
	}

	@Override
	public void processJson(int lineNumber, JsonObject jsonValue, JsonObject jsonSchema) throws IOException {
		// Extract keys from JSON object
		addColumnsToMap(jsonSchema.getAsJsonArray("fields"));

		// Write JSON values to CSV
		writeJsonToCsv(lineNumber, jsonValue);
	}

	private void addColumnsToMap(JsonArray fields) throws IOException {
		log.info("Current csv schema: {}", Arrays.toString(csvSchema.values().toArray(String[]::new)));
		if (updateColumns(fields) || csvWriter == null) {
			var schemaHash = columns.hashCode();
			if (csvWriter != null) {
				csvWriter.close();
			}
			var fileName = Path.of(csvFolderPath.toString(), schemaHash + ".csv").toString();
			this.csvWriter = new CSVWriter(new FileWriter(fileName));
			log.info("Schema changed, creating new CSV file {}.csv", schemaHash);
			var headerLine = csvSchema.values().toArray(String[]::new);
			csvWriter.writeNext(headerLine);
			log.info("Header line: {}", Arrays.toString(headerLine));
		}
	}

	private boolean updateColumns(final JsonArray fields) {
		var schemaChanged = false;
		for (var element : fields) {
			var column = element.getAsJsonObject().get("field").getAsString();
			if (!columns.containsKey(column)) {
				columns.put(column, count.get());
				csvSchema.put(count.getAndIncrement(), column);
				schema.add(element);
				schemaChanged = true;
			}
		}
		return schemaChanged;
	}

	private void writeJsonToCsv(final int lineNumber, final JsonObject jsonObject) throws IOException {
		var csvRecord = new String[count.get()];
		csvRecord[0] = lineNumber + "";
		for (var i = 1; i < count.get(); i++) {
			var value = jsonObject.get(csvSchema.get(i));
			csvRecord[i] = convertDateValues(convertEntryValueToString(value), schema.get(i));
		}
		log.info("Writing record line: {}", Arrays.toString(csvRecord));
		csvWriter.writeNext(csvRecord);
	}

	private String convertEntryValueToString(JsonElement entry) {
		if (entry == null || entry.isJsonNull()) {
			return KEY_NULL_VALUE_PLACEHOLDER;
		}
		if (entry.isJsonPrimitive()) {
			return entry.getAsString();
		}
		return entry.toString();
	}

	/**
	 * Converts values in Debezium.Date format from epoch days into epoch microseconds
	 *
	 * @param value
	 * @param field
	 */
	private String convertDateValues(String value, JsonElement field) {
		// TODO: make this configurable
		var nameElement = field.getAsJsonObject().get("name");
		if (nameElement != null
				&& (nameElement.getAsString().equals("io.debezium.time.Date")
				|| nameElement.getAsString().equals("org.apache.kafka.connect.data.Date"))) {
			return String.valueOf(Long.parseLong(value) * 86400000000L);
		}
		return value;
	}

	@Override
	public void close() {
		if (csvWriter != null) {
			try {
				csvWriter.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
