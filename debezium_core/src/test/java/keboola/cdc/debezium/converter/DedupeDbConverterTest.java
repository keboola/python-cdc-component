package keboola.cdc.debezium.converter;

//import com.google.gson.Gson;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonParser;
//import keboola.cdc.debezium.DuckDbWrapper;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//
//import java.io.InputStreamReader;
//import java.sql.SQLException;
//
//import static keboola.cdc.debezium.converter.JsonConverter.KBC_PRIMARY_KEY;

class DedupeDbConverterTest {
//
//	@Test
//	public void saveSchema() throws SQLException {
//		var initSchema = readResource("initialSchema.json").getAsJsonArray();
//
//		var dbWrapper = new DuckDbWrapper(new DuckDbWrapper.Properties("", 4,
//				"4G", "2G"));
//
//		var dedupeDbConverter = new DedupeDbConverter(new Gson(), dbWrapper, "testTable", initSchema);
//
//		var dataArray = readResource("dataArray.json").getAsJsonArray();
//		dataArray.forEach(data ->
//				dedupeDbConverter.processJson(
//						key, data.getAsJsonObject(),
//						readResource("schema.json").getAsJsonObject())
//		);
//		dedupeDbConverter.close();
//
//		var stmt = dbWrapper.getConn().createStatement();
//		var rs = stmt.executeQuery("SELECT * FROM testTable");
//		Assertions.assertTrue(rs.next());
//		Assertions.assertAll(
//				() -> Assertions.assertEquals(122, rs.getInt(KBC_PRIMARY_KEY)),
//				() -> Assertions.assertEquals(122, rs.getInt("id")),
//				() -> Assertions.assertEquals("newName", rs.getString("name")),
//				() -> Assertions.assertEquals("newDescription", rs.getString("description")),
//				() -> Assertions.assertEquals(100.0, rs.getDouble("weight")),
//				() -> Assertions.assertEquals("u", rs.getString("kbc__operation")),
//				() -> Assertions.assertEquals(1710349898992L, rs.getLong("kbc__event_timestamp")),
//				() -> Assertions.assertEquals("false", rs.getString("__deleted"))
//		);
//		Assertions.assertFalse(rs.next());
//
//		// Close the statement and connection
//		rs.close();
//		stmt.close();
//		dbWrapper.close();
//	}
//
//	private JsonElement readResource(String resource) {
//		return JsonParser.parseReader(new InputStreamReader(getClass().getResourceAsStream(resource)));
//	}
}
