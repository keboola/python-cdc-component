package keboola.cdc.debezium;

import lombok.Data;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicReference;

@Data
public class SyncStats {
	private long started;
	private AtomicReference<ZonedDateTime> lastRecord;
	private boolean taskStarted;
	private int recordCount;

	public SyncStats() {
		this.lastRecord = new AtomicReference<>();
		this.recordCount = 0;
		this.taskStarted = false;
		this.started = 0;
	}

	public void setLastRecord(ZonedDateTime lastRecord) {
		this.lastRecord.set(lastRecord);
	}

	public ZonedDateTime getLastRecord() {
		return this.lastRecord.get();
	}
}
