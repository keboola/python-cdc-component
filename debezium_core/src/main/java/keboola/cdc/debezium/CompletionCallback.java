package keboola.cdc.debezium;

import io.debezium.engine.DebeziumEngine;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;


public class CompletionCallback implements DebeziumEngine.CompletionCallback {
    private final Logger logger;

    private final ExecutorService executorService;


    private Throwable error;

    public CompletionCallback(Logger logger, ExecutorService executorService) {
        this.executorService = executorService;
        this.logger = logger;
    }

    public Throwable getError() {
        return error;
    }


    @Override
    public void handle(boolean success, String message, Throwable error) {
        if (success) {
            this.logger.info("Debezium ended successfully with '{}'", message);
        } else {
            this.logger.warn("Debezium failed with '{}'", message);
        }

        this.error = error;
        this.executorService.shutdown();
    }
}