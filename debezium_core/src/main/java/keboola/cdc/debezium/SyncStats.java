package keboola.cdc.debezium;

import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class SyncStats {
	private ZonedDateTime lastRecord;
	private int recordCount;

	public SyncStats() {
		this.lastRecord = ZonedDateTime.now();
		this.recordCount = 0;
	}

}
