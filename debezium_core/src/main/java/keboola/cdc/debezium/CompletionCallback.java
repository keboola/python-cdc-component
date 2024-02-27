package keboola.cdc.debezium;

import io.debezium.engine.DebeziumEngine;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;


public class CompletionCallback implements DebeziumEngine.CompletionCallback {
	private final Logger logger;

	private final ExecutorService executorService;


	private Throwable error;
	private String errorMessage;
	private boolean success;

	public CompletionCallback(Logger logger, ExecutorService executorService) {
		this.executorService = executorService;
		this.logger = logger;
	}

	public Throwable getError() {
		return error;
	}


	@Override
	public void handle(boolean success, String message, Throwable error) {
		this.success = success;
		if (success) {
			this.logger.info("Debezium ended successfully with '{}'", message);
		} else {
			this.logger.error("Debezium failed with '{}'", message);
			this.errorMessage = message;
		}

		this.error = error;
		this.executorService.shutdown();
	}

	public String getErrorMessage() {
		if (this.error != null) {
			return this.error.getMessage() + ": " + this.error.getCause().getMessage();
		} else {
			return errorMessage;
		}
	}

	public boolean isSuccess() {
		return success;
	}
}