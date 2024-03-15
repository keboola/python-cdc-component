package keboola.cdc.debezium;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonToDbConverterTest {
	private static final JsonArray TESTING_SCHEMA = new JsonParser().parse("[\n" +
			"    {\n" +
			"      \"field\": \"kbc__batch_event_order\",\n" +
			"      \"type\": \"int\",\n" +
			"      \"optional\": true\n" +
			"    },\n" +
			"    {\n" +
			"      \"type\": \"int32\",\n" +
			"      \"optional\": false,\n" +
			"      \"default\": 0,\n" +
			"      \"field\": \"id\"\n" +
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
	private static final JsonObject SCHEMA = new JsonParser().parse("{\n" +
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

	private static final JsonObject TESTING_DATA = new JsonParser().parse("{\n" +
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
		JsonToDbConverter converter = new JsonToDbConverter(new DuckDbWrapper(), "products", TESTING_SCHEMA);
		converter.processJson(1, TESTING_DATA, SCHEMA);
		converter.checkActualData();
		converter.close();
	}

}
