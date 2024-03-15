package keboola.cdc.debezium;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonToCsvConverterTest {

	private JsonToCsvConverter converter;

	@TempDir
	Path tempDir;

	@BeforeEach
	void setUp() throws IOException {
		converter = new JsonToCsvConverter(tempDir, null);
	}

	@Test
	void shouldInitializeWithEventOrderColumnWhenSchemaIsNull() {
		assertEquals(1, converter.getSchema().size());
		assertEquals("kbc__batch_event_order", converter.getSchema().get(0).getAsJsonObject().get("field").getAsString());
	}

	@Test
	void shouldAddNewColumnsWhenProcessingJsonWithNewFields() throws IOException {
		JsonObject jsonSchema = new JsonObject();
		JsonArray fields = new JsonArray();
		JsonObject field = new JsonObject();
		field.addProperty("field", "new_field");
		fields.add(field);
		jsonSchema.add("fields", fields);

		converter.processJson(1, new JsonObject(), jsonSchema);

		assertTrue(converter.getSchema().toString().contains("new_field"));
	}

	@Test
	void shouldNotAddExistingColumnsWhenProcessingJsonWithExistingFields() throws IOException {
		JsonObject jsonSchema = new JsonObject();
		JsonArray fields = new JsonArray();
		JsonObject field = new JsonObject();
		field.addProperty("field", "kbc__batch_event_order");
		fields.add(field);
		jsonSchema.add("fields", fields);

		converter.processJson(1, new JsonObject(), jsonSchema);

		assertEquals(1, converter.getSchema().size());
	}

	@Test
	void shouldWriteJsonToCsv() throws IOException, CsvException {
		JsonObject jsonValue = prepareJson();

		JsonObject jsonSchema = new JsonObject();
		JsonArray fields = new JsonArray();
		JsonObject field = new JsonObject();
		field.addProperty("field", "date_field");
		field.addProperty("name", "io.debezium.time.Date");
		fields.add(field);
		jsonSchema.add("fields", fields);

		converter.processJson(1, jsonValue, jsonSchema);
		converter.close();

		// Check the CSV file content
		// This is a simplified check, in a real test you would read the CSV file and check its content
		var csvFile = tempDir.resolve("1544010688.csv").toFile();
		assertTrue(csvFile.exists());

		// Check the CSV file content
		var reader = new CSVReader(new FileReader(csvFile));
		var lines = reader.readAll();

		Assertions.assertAll(
				() -> assertEquals(2, lines.size()),
				() -> assertEquals(List.of("kbc__batch_event_order", "date_field"), List.of(lines.get(0))),
				() -> assertEquals(List.of("1", "1710460800000000"), List.of(lines.get(1)))
		);
	}

	private static JsonObject prepareJson() {
		JsonObject jsonValue = new JsonObject();
		jsonValue.addProperty("date_field",
				io.debezium.time.Date.toEpochDay(19_797L, null));
		jsonValue.addProperty("string_field", "test");
		jsonValue.addProperty("integer_field", 123);
		jsonValue.addProperty("number_field", 123.456);
		return jsonValue;
	}
}
