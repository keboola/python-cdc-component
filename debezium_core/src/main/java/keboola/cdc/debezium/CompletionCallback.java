package keboola.cdc.debezium;

import io.debezium.engine.DebeziumEngine;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;

@Slf4j
public class CompletionCallback implements DebeziumEngine.CompletionCallback {

	private final ExecutorService executorService;


	@Getter
	private Throwable error;
	private String errorMessage;
	@Getter
	private boolean success;

	public CompletionCallback(ExecutorService executorService) {
		this.executorService = executorService;
	}

	@Override
	public void handle(boolean success, String message, Throwable error) {
		this.success = success;
		if (success) {
			log.info("Debezium ended successfully with '{}'", message);
		} else {
			log.error("Debezium failed with '{}'", message);
			this.errorMessage = message;
		}

		this.error = error;
		this.executorService.shutdown();
	}

	public Exception getException() {
		return this.error != null ? new Exception(this.error) : new Exception(this.errorMessage);
	}

}
