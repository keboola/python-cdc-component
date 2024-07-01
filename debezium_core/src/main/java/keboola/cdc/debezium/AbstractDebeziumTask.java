package keboola.cdc.debezium;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AbstractDebeziumTask {
	public static final String KBC_FIELDS_PREFIX = "kbc__";
	public static final String EVENT_TIMESTAMP_FIELD = "event_timestamp";
	public static final String KBC_EVENT_TIMESTAMP_FIELD = KBC_FIELDS_PREFIX + EVENT_TIMESTAMP_FIELD;

	public static int MAX_CHUNK_SIZE = 1000;
	public static int MAX_APPENDER_CACHE_SIZE = 20000;

	private final Properties debeziumProperties;
	private final Properties keboolaProperties;
	private final Duration maxDuration;
	private final Path resultFolder;
	private final DebeziumKBCWrapper.Mode mode;
	private final Duration maxWait;

	public AbstractDebeziumTask(Path debeziumPropertiesPath,
								Duration maxDuration,
								Duration maxWait,
								Path resultFolder,
								DebeziumKBCWrapper.Mode mode) {
		this(loadPropertiesWithDebeziumDefaults(debeziumPropertiesPath),
				new Properties(),
				maxDuration,
				maxWait, resultFolder,
				mode
		);
	}

	public AbstractDebeziumTask(Path debeziumPropertiesPath,
								Path keboolaPropertiesPath,
								Duration maxDuration,
								Duration maxWait,
								Path resultFolder,
								DebeziumKBCWrapper.Mode mode) {

		this(loadPropertiesWithDebeziumDefaults(debeziumPropertiesPath),
				loadProperties(keboolaPropertiesPath),
				maxDuration,
				maxWait,
				resultFolder,
				mode
		);
	}

	public AbstractDebeziumTask(Properties debeziumProperties,
								Properties keboolaProperties,
								Duration maxDuration,
								Duration maxWait,
								Path resultFolder,
								DebeziumKBCWrapper.Mode mode) {
		this.debeziumProperties = debeziumProperties;
		this.keboolaProperties = keboolaProperties;
		this.maxDuration = maxDuration;
		this.resultFolder = resultFolder;
		this.mode = mode;
		this.maxWait = maxWait == null ? Duration.ofSeconds(10) : maxWait;
		adjustMaxChunkSize(keboolaProperties);
		adjustMaxAppenderCacheSize(keboolaProperties);
		adjustTimezone(keboolaProperties);
		setSnapshotMode(debeziumProperties);
	}

	private static void adjustTimezone(Properties keboolaProperties) {
		var timezone = keboolaProperties.getProperty("keboola.timezone", "UTC");
		TimeZone.setDefault(TimeZone.getTimeZone(timezone));
	}

	private static void adjustMaxChunkSize(Properties keboolaProperties) {
		MAX_CHUNK_SIZE = Integer.parseInt(keboolaProperties.getProperty("keboola.converter.dedupe.max_chunk_size", "1000"));
	}

	private static void setSnapshotMode(Properties debeziumProperties) {
		SyncStats.setInitialSnapshotOnly(debeziumProperties.getProperty("snapshot.mode", "").equalsIgnoreCase("initial_only"));
	}

	private static void adjustMaxAppenderCacheSize(Properties keboolaProperties) {
		MAX_APPENDER_CACHE_SIZE = Integer.parseInt(keboolaProperties.getProperty("keboola.converter.dedupe.max_appender_cache_size", "20000"));
	}

	public void run() throws Exception {
		var executorService = Executors.newSingleThreadExecutor();
		var started = ZonedDateTime.now();

		// callback
		var completionCallback = new CompletionCallback(executorService);
		var connectorCallback = new ConnectorCallback();
		var changeConsumer = new DbChangeConsumer(this.resultFolder.toString(),
				new DuckDbWrapper(this.keboolaProperties), this.mode.converterProvider(this.debeziumProperties));

		// start
		try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
				.using(this.getClass().getClassLoader())
				.using(this.debeziumProperties)
				.notifying(changeConsumer)
				.using(connectorCallback)
				.using(completionCallback)
				.build()) {
			executorService.execute(engine);
			Await.until(() -> this.ended(executorService, started), Duration.ofMillis(500));
		} finally {
			changeConsumer.closeWriterStreams();
			changeConsumer.storeSchemaMap();
		}

		this.shutdown(executorService);

		if (!completionCallback.isSuccess()) {
			throw completionCallback.getException();
		}

		log.info("Ended after receiving records: {}", changeConsumer.getRecordsCount());

	}

	private static Properties loadPropertiesWithDebeziumDefaults(Path propertiesFile) {
		final var props = loadProperties(propertiesFile);
		props.setProperty("name", "kbc_cdc");
		props.setProperty("topic.prefix", "testcdc");
		props.setProperty("transforms", "unwrap");
		props.setProperty("transforms.unwrap.type", "keboola.cdc.debezium.transforms.ExtractNewRecordStateSchemaChanges");
		props.setProperty("transforms.unwrap.drop.tombstones", "true");
		props.setProperty("transforms.unwrap.delete.handling.mode", "rewrite");
		props.setProperty("transforms.unwrap.add.fields", "op:operation,source.ts_ms:" + EVENT_TIMESTAMP_FIELD);
		props.setProperty("transforms.unwrap.add.fields.prefix", KBC_FIELDS_PREFIX);
		props.setProperty("notification.enabled.channels", KeboolaNotification.KEBOOLA_NOTIFICATION_CHANNEL);
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

	private boolean ended(ExecutorService executorService, ZonedDateTime start) {
		if (executorService.isShutdown()) {
			return true;
		}
		if (this.maxDuration != null
				&& ZonedDateTime.now().toEpochSecond() > start.plus(this.maxDuration).toEpochSecond()) {
			log.info("Ended after max duration: {}", this.maxDuration);
			return true;
		}

		if (SyncStats.isSnapshotInProgress()) {
			return false;
		}

		if (SyncStats.taskStarted()
				&& !SyncStats.isProcessing()
				&& this.maxWait != null
				&& ZonedDateTime.now().toEpochSecond() > SyncStats.getLastRecord().plus(this.maxWait).toEpochSecond()) {
			log.info("Ended after max wait: {}. Last record: {}", this.maxWait, SyncStats.getLastRecord());
			return true;
		}

		return false;
	}

}
