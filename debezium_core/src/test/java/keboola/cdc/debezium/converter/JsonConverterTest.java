package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import keboola.cdc.debezium.DuckDbWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStreamReader;
import java.sql.SQLException;
import java.time.LocalDateTime;

class JsonConverterTest {

	@Test
	public void updateSchema() throws SQLException {
		var initSchema = readResource("initialSchema.json").getAsJsonArray();
		var payload = readResource("singleData_extended.json").getAsJsonObject();

		var dbWrapper = new DuckDbWrapper(new DuckDbWrapper.Properties("", 4,
				"4G", "2G"));

		var converter = new AppendDbConverter(new Gson(), dbWrapper, "testTable", initSchema);

		// check that there are missing fields
		Assertions.assertTrue(converter.isMissingAnyColumn(payload));

		// adjust schme
		var validationSchema = readResource("schema_extended.json")
				.getAsJsonObject()
				.getAsJsonArray("fields");
		converter.adjustSchema(validationSchema);

		// put new data
		converter.processJson(payload);
		converter.close();

		var stmt = dbWrapper.getConn().createStatement();
		var rs = stmt.executeQuery("SELECT * FROM testTable");
		Assertions.assertTrue(rs.next());
		Assertions.assertAll(
				() -> Assertions.assertEquals(122, rs.getInt("id")),
				() -> Assertions.assertEquals("ccc", rs.getString("name")),
				() -> Assertions.assertEquals("hafanana", rs.getString("description")),
				() -> Assertions.assertNull(rs.getString("weight")),
				() -> Assertions.assertEquals(LocalDateTime.parse("2023-01-01T12:34:56.789"),
						rs.getTimestamp("timestamp_col").toLocalDateTime()),
				() -> Assertions.assertEquals("u", rs.getString("kbc__operation")),
				() -> Assertions.assertEquals(1710349868992L, rs.getLong("kbc__event_timestamp")),
				() -> Assertions.assertEquals("false", rs.getString("__deleted"))
		);
		Assertions.assertFalse(rs.next());

		// Close the statement and connection
		rs.close();
		stmt.close();
		dbWrapper.close();

		var expected = readResource("schema_extended_expected.json").getAsJsonArray();
		Assertions.assertEquals(expected, converter.getJsonSchema());
	}

	private JsonElement readResource(String resource) {
		return JsonParser.parseReader(new InputStreamReader(getClass().getResourceAsStream(resource)));
	}
}
