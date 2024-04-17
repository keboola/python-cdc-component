package keboola.cdc.debezium;

import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class SyncStats {
	private ZonedDateTime startTime;
	private ZonedDateTime endTime;
	private ZonedDateTime lastRecord;
	private int recordCount;

	public SyncStats() {
		this.lastRecord = ZonedDateTime.now();
		this.recordCount = 0;
	}

	public double averageSpeed() {
		long millis = this.endTime.toInstant().toEpochMilli() - this.startTime.toInstant().toEpochMilli();
		return this.recordCount / (millis / 1000.0);
	}
}
