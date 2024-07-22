package keboola.cdc.debezium;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SyncStats {
	private final static SyncStats singleton = new SyncStats();
	private static final Logger log = LoggerFactory.getLogger(SyncStats.class);

	private final AtomicReference<ZonedDateTime> lastRecord;
	private final AtomicBoolean processing;
	private final AtomicLong started;
	private final AtomicBoolean snapshotInProgress;
	private final AtomicBoolean snapshotOnly;
	private final AtomicBoolean taskStarted;
	private final AtomicInteger recordCount;
	private final AtomicReference<String> targetFile;
	private final AtomicLong targetPosition;

	private SyncStats() {
		this.lastRecord = new AtomicReference<>();
		this.recordCount = new AtomicInteger(0);
		this.taskStarted = new AtomicBoolean(false);
		this.snapshotInProgress = new AtomicBoolean(false);
		this.snapshotOnly = new AtomicBoolean(false);
		this.started = new AtomicLong(0L);
		this.processing = new AtomicBoolean(false);
		this.targetFile = new AtomicReference<>();
		this.targetPosition = new AtomicLong(0);
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

	public static void setInitialSnapshotOnly(boolean initialOnly) {
		log.info("Setting initial snapshot only to: {}", initialOnly);
		singleton.snapshotOnly.set(initialOnly);
	}

	public static boolean isInitialSnapshotOnly() {
		return singleton.snapshotOnly.get();
	}

	public static void setTargetFile(String targetFile) {
		singleton.targetFile.set(targetFile);
	}

	public static String getTargetFile() {
		return singleton.targetFile.get();
	}

	public static void setTargetPosition(long targetPosition) {
		singleton.targetPosition.set(targetPosition);
	}

	public static long getTargetPosition() {
		return singleton.targetPosition.get();
	}

	public static boolean isTargetPositionDefined() {
		return !singleton.targetFile.get().isBlank() && singleton.targetPosition.get() > 0;
	}
}
