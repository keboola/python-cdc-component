package keboola.cdc.debezium;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import keboola.cdc.debezium.converter.DedupeDbConverter;
import org.junit.jupiter.api.Test;

import java.text.MessageFormat;
import java.util.Set;

class DedupeDbConverterTest {
	private static final JsonArray TESTING_SCHEMA = JsonParser.parseString("[\n" +
			"    {\n" +
			"      \"type\": \"int32\",\n" +
			"      \"optional\": false,\n" +
			"      \"default\": 0,\n" +
			"      \"field\": \"id\"\n" +
			"    },\n" +
			"    {\n" +
			"      \"type\": \"double\",\n" +
			"      \"optional\": true,\n" +
			"      \"field\": \"weight\"\n" +
			"    },\n" +
			"    {\n" +
			"      \"type\": \"string\",\n" +
			"      \"optional\": false,\n" +
			"      \"field\": \"name\"\n" +
			"    },\n" +
			"    {\n" +
			"      \"type\": \"string\",\n" +
			"      \"optional\": true,\n" +
			"      \"field\": \"description\"\n" +
			"    },\n" +
			"    {\n" +
			"      \"type\": \"string\",\n" +
			"      \"optional\": true,\n" +
			"      \"field\": \"kbc__operation\"\n" +
			"    },\n" +
			"    {\n" +
			"      \"type\": \"int64\",\n" +
			"      \"optional\": true,\n" +
			"      \"field\": \"kbc__event_timestamp\"\n" +
			"    },\n" +
			"    {\n" +
			"      \"type\": \"string\",\n" +
			"      \"optional\": true,\n" +
			"      \"field\": \"__deleted\"\n" +
			"    }" +
			"  ]").getAsJsonArray();
	private static final JsonObject SCHEMA = JsonParser.parseString("{\n" +
			"    \"type\": \"struct\",\n" +
			"    \"fields\": [\n" +
			"      {\n" +
			"        \"type\": \"int32\",\n" +
			"        \"optional\": false,\n" +
			"        \"default\": 0,\n" +
			"        \"field\": \"id\"\n" +
			"      },\n" +
			"      {\n" +
			"        \"type\": \"string\",\n" +
			"        \"optional\": false,\n" +
			"        \"field\": \"name\"\n" +
			"      },\n" +
			"      {\n" +
			"        \"type\": \"string\",\n" +
			"        \"optional\": true,\n" +
			"        \"field\": \"description\"\n" +
			"      },\n" +
			"      {\n" +
			"        \"type\": \"double\",\n" +
			"        \"optional\": true,\n" +
			"        \"field\": \"weight\"\n" +
			"      },\n" +
			"      {\n" +
			"        \"type\": \"string\",\n" +
			"        \"optional\": true,\n" +
			"        \"field\": \"kbc__operation\"\n" +
			"      },\n" +
			"      {\n" +
			"        \"type\": \"int64\",\n" +
			"        \"optional\": true,\n" +
			"        \"field\": \"kbc__event_timestamp\"\n" +
			"      },\n" +
			"      {\n" +
			"        \"type\": \"string\",\n" +
			"        \"optional\": true,\n" +
			"        \"field\": \"__deleted\"\n" +
			"      }\n" +
			"    ],\n" +
			"    \"optional\": false,\n" +
			"    \"name\": \"testcdc.inventory.products.Value\"\n" +
			"  }").getAsJsonObject();

	private static final JsonObject TESTING_DATA = JsonParser.parseString("{\n" +
			"    \"id\": 122,\n" +
			"    \"name\": \"ccc\",\n" +
			"    \"description\": \"hafanana\",\n" +
			"    \"weight\": 100.0,\n" +
			"    \"kbc__operation\": \"u\",\n" +
			"    \"kbc__event_timestamp\": 1710349868992,\n" +
			"    \"__deleted\": \"false\"\n" +
			"  }").getAsJsonObject();


	@Test
	public void test() {
		var dbWrapper = new DuckDbWrapper(new DuckDbWrapper.Properties("", 1, "1GB", "1GB"));
		var converter = new DedupeDbConverter(new Gson(), dbWrapper, "products", TESTING_SCHEMA);
		converter.processJson(Set.of("id"), TESTING_DATA, SCHEMA);
//		converter.close();
		try {
			var stmt = dbWrapper.getConn().createStatement();
			var rs = stmt.executeQuery("SELECT * FROM products");
			while (rs.next()) {
				// "id","name","description","weight","kbc__operation","kbc__event_timestamp","__deleted","kbc__batch_event_order", "my_perfect_column2"
				var msg = MessageFormat.format("{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}", rs.getInt("kbc__primary_key"), rs.getInt("id"), rs.getString("name"),
						rs.getString("description"), rs.getDouble("weight"), rs.getString("kbc__operation"),
						rs.getString("kbc__event_timestamp"), rs.getBoolean("__deleted"));
				System.out.println(msg);
			}

			// Close the statement and connection
			rs.close();
			stmt.close();
			dbWrapper.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
