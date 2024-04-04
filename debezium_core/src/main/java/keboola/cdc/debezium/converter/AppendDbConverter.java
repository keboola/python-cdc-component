package keboola.cdc.debezium.converter;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import keboola.cdc.debezium.DuckDbWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class AppendDbConverter extends AbstractDbConverter implements JsonConverter {

	public AppendDbConverter(Gson gson, DuckDbWrapper dbWrapper, String tableName, JsonArray initialSchema) {
		super(gson, dbWrapper, tableName, initialSchema);
	}
}
