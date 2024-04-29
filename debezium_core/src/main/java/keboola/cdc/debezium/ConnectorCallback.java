package keboola.cdc.debezium;

import io.debezium.engine.DebeziumEngine;
import lombok.extern.slf4j.Slf4j;

import java.time.ZonedDateTime;

@Slf4j
public final class ConnectorCallback
		implements DebeziumEngine.ConnectorCallback {

	@Override
	public void taskStarted() {
		log.info("Task started");
		SyncStats.taskStarted(true);
		SyncStats.setLastRecord(ZonedDateTime.now());
	}

	@Override
	public void taskStopped() {
		log.info("Task stopped");
		SyncStats.taskStarted(false);
	}
}
