package keboola.cdc.debezium;

import java.time.ZonedDateTime;

public class SyncStats {
	private ZonedDateTime lastRecord;
	private int recordCount;

	public SyncStats() {
		this.lastRecord = ZonedDateTime.now();
		this.recordCount = 0;
	}

	public void setLastRecord(ZonedDateTime lastRecord) {
		this.lastRecord = lastRecord;
	}

	public ZonedDateTime getLastRecord() {
		return this.lastRecord;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}
}