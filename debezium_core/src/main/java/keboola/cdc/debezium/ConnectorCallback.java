package keboola.cdc.debezium;

import io.debezium.engine.DebeziumEngine;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.time.ZonedDateTime;

@Slf4j
public record ConnectorCallback(SyncStats syncStats)
		implements DebeziumEngine.ConnectorCallback {

	@Override
	public void taskStarted() {
		log.info("Task started");
		this.syncStats.setTaskStarted(true);
		ZonedDateTime now = ZonedDateTime.now();
		this.syncStats.setLastRecord(now);
	}

	@Override
	public void taskStopped() {
		log.info("Task stopped");
		this.syncStats.setTaskStarted(false);
	}
}
