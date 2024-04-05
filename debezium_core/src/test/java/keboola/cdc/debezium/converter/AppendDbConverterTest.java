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

//import static keboola.cdc.debezium.converter.JsonConverter.KBC_PRIMARY_KEY;

class AppendDbConverterTest {
//
//	@Test
//	public void simpleTest() throws SQLException {
//		var initSchema = readResource("initialSchema.json").getAsJsonArray();
//		var dbWrapper = new DuckDbWrapper(new DuckDbWrapper.Properties("", 4,
//				"4G", "2G"));
//		var appendDbConverter = new AppendDbConverter(new Gson(), dbWrapper, "testTable", initSchema);
//
//		appendDbConverter.processJson(
//				key, readResource("singleData.json").getAsJsonObject(),
//				readResource("schema.json").getAsJsonObject());
//		appendDbConverter.close();
//
//		var stmt = dbWrapper.getConn().createStatement();
//		var rs = stmt.executeQuery("SELECT * FROM testTable");
//		Assertions.assertTrue(rs.next());
//		Assertions.assertAll(
//				() -> Assertions.assertThrows(SQLException.class, () -> rs.findColumn(KBC_PRIMARY_KEY)),
//				() -> Assertions.assertEquals(122, rs.getInt("id")),
//				() -> Assertions.assertEquals("ccc", rs.getString("name")),
//				() -> Assertions.assertEquals("hafanana", rs.getString("description")),
//				() -> Assertions.assertEquals(100.0, rs.getDouble("weight")),
//				() -> Assertions.assertEquals("u", rs.getString("kbc__operation")),
//				() -> Assertions.assertEquals(1710349868992L, rs.getLong("kbc__event_timestamp")),
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
//	@Test
//	public void updateData() throws SQLException {
//		final var initSchema = readResource("initialSchema.json").getAsJsonArray();
//		var dbWrapper = new DuckDbWrapper(new DuckDbWrapper.Properties("", 4,
//				"4G", "2G"));
//		final var appendDbConverter = new AppendDbConverter(new Gson(), dbWrapper, "testTable", initSchema);
//
//		final var dataArray = readResource("dataArray.json").getAsJsonArray();
//		dataArray.forEach(data ->
//				appendDbConverter.processJson(
//						key, data.getAsJsonObject(),
//						readResource("schema.json").getAsJsonObject())
//		);
//		appendDbConverter.close();
//
//		final var stmt = dbWrapper.getConn().createStatement();
//		final var rs = stmt.executeQuery("SELECT * FROM testTable");
//		Assertions.assertTrue(rs.next());
//
//		Assertions.assertAll(
//				() -> Assertions.assertEquals(122, rs.getInt("id")),
//				() -> Assertions.assertEquals("oldName", rs.getString("name")),
//				() -> Assertions.assertEquals("oldDescription", rs.getString("description")),
//				() -> Assertions.assertEquals(0.012, rs.getDouble("weight")),
//				() -> Assertions.assertEquals("i", rs.getString("kbc__operation")),
//				() -> Assertions.assertEquals(1710349868992L, rs.getLong("kbc__event_timestamp")),
//				() -> Assertions.assertEquals("false", rs.getString("__deleted"))
//		);
//
//		Assertions.assertTrue(rs.next());
//		Assertions.assertAll(
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
