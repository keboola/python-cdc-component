package keboola.cdc.debezium;

import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class SyncStats {
	private ZonedDateTime startTime;
	private ZonedDateTime endTime;
	private ZonedDateTime lastRecord;
	private int recordCount;
	private long totalRecords;

	public SyncStats() {
		this.lastRecord = ZonedDateTime.now();
		this.recordCount = 0;
		this.totalRecords = 0;
	}

	public void addRecords(int recordCount) {
		this.totalRecords += recordCount;
	}
}
