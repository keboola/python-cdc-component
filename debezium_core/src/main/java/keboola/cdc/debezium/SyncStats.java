package keboola.cdc.debezium;

import lombok.Getter;
import lombok.Setter;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SyncStats {
	private final AtomicReference<ZonedDateTime> lastRecord;
	private final AtomicBoolean processing;

	@Getter
	@Setter
	private long started;

	@Getter
	@Setter
	private boolean taskStarted;

	@Getter
	@Setter
	private int recordCount;

	public SyncStats() {
		this.lastRecord = new AtomicReference<>(ZonedDateTime.now());
		this.recordCount = 0;
		this.taskStarted = false;
		this.started = 0;
		this.processing = new AtomicBoolean(false);
	}

	public void setLastRecord(ZonedDateTime lastRecord) {
		this.lastRecord.set(lastRecord);
	}

	public ZonedDateTime getLastRecord() {
		return this.lastRecord.get();
	}

	public boolean isProcessing() {
		return this.processing.get();
	}

	public void setProcessing(boolean processing) {
		this.processing.set(processing);
	}
}
