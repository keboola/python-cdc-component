package keboola.cdc.debezium;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SyncStats {
	private final static SyncStats singleton = new SyncStats();

	private final AtomicReference<ZonedDateTime> lastRecord;
	private final AtomicBoolean processing;
	private final AtomicLong started;
	private final AtomicBoolean snapshotInProgress;
	private final AtomicBoolean taskStarted;
	private final AtomicInteger recordCount;

	private SyncStats() {
		this.lastRecord = new AtomicReference<>();
		this.recordCount = new AtomicInteger(0);
		this.taskStarted = new AtomicBoolean(false);
		this.snapshotInProgress = new AtomicBoolean(false);
		this.started = new AtomicLong(0L);
		this.processing = new AtomicBoolean(false);
	}

	public static void setLastRecord(ZonedDateTime lastRecord) {
		singleton.lastRecord.set(lastRecord);
	}

	public static ZonedDateTime getLastRecord() {
		return singleton.lastRecord.get();
	}

	public static boolean isProcessing() {
		return singleton.processing.get();
	}

	public static void setProcessing(boolean processing) {
		singleton.processing.set(processing);
	}

	public static void setSnapshotInProgress(boolean newValue) {
		singleton.snapshotInProgress.set(newValue);
	}

	public static boolean isSnapshotInProgress() {
		return singleton.snapshotInProgress.get();
	}

	public static void taskStarted(boolean b) {
		singleton.taskStarted.set(b);
	}

	public static void started(long started) {
		singleton.started.set(started);
	}

	public static void recordCount(int recordCount) {
		singleton.recordCount.set(recordCount);
	}

	public static long started() {
		return singleton.started.get();
	}

	public static boolean taskStarted() {
		return singleton.taskStarted.get();
	}
}
