package keboola.cdc.debezium;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import keboola.cdc.debezium.converter.JsonConverter;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AbstractDebeziumTask {
	public static int MAX_CHUNK_SIZE = 1000;

	private final Properties debeziumProperties;
	private final Properties keboolaProperties;
	private final Duration maxDuration;
	private final Path resultFolder;
	private final JsonConverter.ConverterProvider converterProvider;
	private final Duration maxWait;

	public AbstractDebeziumTask(Path debeziumPropertiesPath,
								Duration maxDuration,
								Duration maxWait,
								Path resultFolder,
								JsonConverter.ConverterProvider provider) {
		this(loadPropertiesWithDebeziumDefaults(debeziumPropertiesPath),
				new Properties(),
				maxDuration,
				resultFolder,
				provider,
				maxWait);
	}

	public AbstractDebeziumTask(Path debeziumPropertiesPath,
								Path keboolaPropertiesPath,
								Duration maxDuration,
								Duration maxWait,
								Path resultFolder,
								JsonConverter.ConverterProvider provider) {

		this(loadPropertiesWithDebeziumDefaults(debeziumPropertiesPath),
				loadProperties(keboolaPropertiesPath),
				maxDuration,
				resultFolder,
				provider,
				maxWait);
	}

	private AbstractDebeziumTask(Properties debeziumProperties,
								 Properties keboolaProperties,
								 Duration maxDuration,
								 Path resultFolder,
								 JsonConverter.ConverterProvider converterProvider,
								 Duration maxWait) {
		this.debeziumProperties = debeziumProperties;
		this.keboolaProperties = keboolaProperties;
		this.maxDuration = maxDuration;
		this.resultFolder = resultFolder;
		this.converterProvider = converterProvider;
		this.maxWait = maxWait == null ? Duration.ofSeconds(10) : maxWait;
		adjustMaxChunkSize(keboolaProperties);
	}

	private static void adjustMaxChunkSize(Properties keboolaProperties) {
		MAX_CHUNK_SIZE = Integer.parseInt(keboolaProperties.getProperty("keboola.converter.dedupe.max_chunk_size", "1000"));
	}

	public void run() throws Exception {
		ExecutorService executorService = Executors.newSingleThreadExecutor();

		SyncStats syncStats = new SyncStats();

		// callback
		var completionCallback = new CompletionCallback(executorService);
		var connectorCallback = new ConnectorCallback(syncStats);
		var changeConsumer = new DbChangeConsumer(this.resultFolder.toString(), syncStats,
				new DuckDbWrapper(this.keboolaProperties), this.converterProvider);

		// start
		try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
				.using(this.getClass().getClassLoader())
				.using(this.debeziumProperties)
				.notifying(changeConsumer)
				.using(connectorCallback)
				.using(completionCallback)
				.build()) {
			executorService.execute(engine);
			Await.until(() -> this.ended(executorService, syncStats), Duration.ofMillis(500));
		} finally {
			changeConsumer.closeWriterStreams();
			changeConsumer.storeSchemaMap();
		}

		this.shutdown(executorService);

		if (completionCallback.getError() != null || !completionCallback.isSuccess()) {
			throw new Exception(completionCallback.getErrorMessage());
		}

		log.info(
				"Ended after receiving records: {}",
				changeConsumer.getRecordsCount()
		);

	}

	private static Properties loadPropertiesWithDebeziumDefaults(Path propertiesFile) {
		final var props = loadProperties(propertiesFile);
		props.setProperty("name", "kbc_cdc");
		props.setProperty("topic.prefix", "testcdc");
		props.setProperty("transforms", "unwrap");
		props.setProperty("transforms.unwrap.type", "keboola.cdc.debezium.transforms.ExtractNewRecordStateSchemaChanges");
		props.setProperty("transforms.unwrap.drop.tombstones", "true");
		props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite");
		props.setProperty("transforms.unwrap.add.fields", "op:operation,source.ts_ms:event_timestamp");
		props.setProperty("transforms.unwrap.add.fields.prefix", "kbc__");
		return props;
	}

	private static Properties loadProperties(Path propertiesFile) {
		try {
			final var propsInputStream = new FileInputStream(propertiesFile.toString());
			return Configuration.load(propsInputStream).asProperties();
		} catch (IOException e) {
			throw new RuntimeException("Couldn't load properties file: " + propertiesFile);
		}
	}

	private void shutdown(ExecutorService executorService) {
		try {
			executorService.shutdown();
			while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
				log.trace("Waiting another 5 seconds for the embedded engine to shut down");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private boolean ended(ExecutorService executorService, SyncStats syncStats) {
		if (executorService.isShutdown()) {
			return true;
		}

		if (syncStats.isTaskStarted()
				&& this.maxWait != null
				&& syncStats.getLastRecord() != null
				&& ZonedDateTime.now().toEpochSecond() > syncStats.getLastRecord().plus(this.maxWait).toEpochSecond()) {
			log.info("Ended after max wait: {}. Last record: {}", this.maxWait, syncStats.getLastRecord());
			return true;
		}

		return false;
	}

}
