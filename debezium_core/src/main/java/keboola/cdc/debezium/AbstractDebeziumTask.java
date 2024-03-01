package keboola.cdc.debezium;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class AbstractDebeziumTask {

	private final Properties properties;

	private final Duration maxDuration;
	private final Path resultFolder;
	private final Logger logger;
	private Duration maxWait = Duration.ofSeconds(10);

	public AbstractDebeziumTask(Path propertiesFile, Logger logger,
								Duration maxDuration,
								Duration maxWait,
								Path resultFolder) {
		this.properties = this.loadProperties(propertiesFile);
		this.logger = logger;
		this.maxDuration = maxDuration;
		if (maxWait != null) {
			this.maxWait = maxWait;
		}
		this.resultFolder = resultFolder;

	}

	public void run() throws Exception {
		ExecutorService executorService = Executors.newSingleThreadExecutor();

		AtomicInteger count = new AtomicInteger();
		ZonedDateTime started = ZonedDateTime.now();
		ZonedDateTime lastRecord = ZonedDateTime.now();


		// callback
		CompletionCallback completionCallback = new CompletionCallback(this.logger, executorService);
		ChangeConsumer changeConsumer = new ChangeConsumer(this, this.logger, count, lastRecord,
				this.resultFolder.toString());

		// start
		try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
				.using(this.getClass().getClassLoader())
				.using(this.properties)
				.notifying(changeConsumer)
				.using(completionCallback)
				.build()) {
			executorService.execute(engine);

			Await.until(() -> this.ended(executorService, started, lastRecord), Duration.ofSeconds(1));

		} finally {
			changeConsumer.closeWriterStreams();
			changeConsumer.storeSchemaMap();
		}

		this.shutdown(this.logger, executorService);

		if (completionCallback.getError() != null || !completionCallback.isSuccess()) {
			throw new Exception(completionCallback.getErrorMessage());
		}


		this.logger.info(
				"Ended after receiving records: {}",
				changeConsumer.getRecordsCount()
		);

	}


	private Properties loadProperties(Path propertiesFile) {
		try {

			InputStream propsInputStream = new FileInputStream(propertiesFile.toString());
			Configuration config = Configuration.load(propsInputStream);
			Properties props = config.asProperties();

			props.setProperty("name", "kbc_cdc");
			props.setProperty("topic.prefix", "testcdc");
			props.setProperty("transforms", "unwrap");
			props.setProperty("transforms.unwrap.type", "keboola.cdc.debezium.transforms.ExtractNewRecordStateSchemaChanges");
			props.setProperty("transforms.unwrap.drop.tombstones", "true");
			props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite");
			props.setProperty("transforms.unwrap.add.fields", "op:operation,source.ts_ms:event_timestamp");
			props.setProperty("transforms.unwrap.add.fields.prefix", "kbc__");

			return props;

		} catch (Exception e) {
			throw new RuntimeException("Couldn't load properties file: " + propertiesFile);
		}

	}

	private void shutdown(Logger logger, ExecutorService executorService) {
		try {
			executorService.shutdown();
			while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
				logger.trace("Waiting another 5 seconds for the embedded engine to shut down");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@SuppressWarnings("RedundantIfStatement")
	private boolean ended(ExecutorService executorService, ZonedDateTime start, ZonedDateTime lastRecord) {
		if (executorService.isShutdown()) {
			return true;
		}
		if (this.maxDuration != null && ZonedDateTime.now().toEpochSecond() > start.plus(this.maxDuration).toEpochSecond()) {
			return true;
		}

//		this.logger.info("Time elapsed: {}, Last record before: {}", lastRecord.plus(this.maxWait).toEpochSecond(),
//				ZonedDateTime.now().toEpochSecond());
		if (this.maxWait != null && ZonedDateTime.now().toEpochSecond() > lastRecord.plus(this.maxWait).toEpochSecond()) {
			return true;
		}

		return false;
	}

}