package keboola.cdc.debezium;

import keboola.cdc.debezium.converter.AppendDbConverter;
import keboola.cdc.debezium.converter.DedupeDbConverter;
import keboola.cdc.debezium.converter.DedubeOnEmptyStateDbConverter;
import keboola.cdc.debezium.converter.JsonConverter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;

@Slf4j
public class DebeziumKBCWrapper implements Runnable {

	@Parameters(index = "0", description = "The debezium properties path")
	private String debeziumPropertiesPath;

	@Parameters(index = "1", description = "The result folder path")
	private String resultFolderPath;

	@Option(names = {"-pf", "--properties-file"}, description = "The keboola properties file path, if not specified, the default value is used")
	private String keboolaPropertiesPath;

	@Option(names = {"-md", "--max-duration"}, description = "The maximum duration (s) before engine stops")
	private int maxDuration;

	@Option(names = {"-mw", "--max-wait"}, description = "The maximum wait duration(s) for next event before engine stops")
	private int maxWait;

	@Option(names = {"-m", "--mode"}, description = "Mode in which values will be stored in DB", defaultValue = "APPEND")
	private Mode mode;

	@Override
	public void run() {
		log.info("Engine started");

		var debeziumTask = this.keboolaPropertiesPath == null
				? new AbstractDebeziumTask(Path.of(this.debeziumPropertiesPath),
				Duration.ofSeconds(this.maxDuration),
				Duration.ofSeconds(this.maxWait),
				Path.of(this.resultFolderPath),
				this.mode)
				: new AbstractDebeziumTask(Path.of(this.debeziumPropertiesPath),
				Path.of(this.keboolaPropertiesPath),
				Duration.ofSeconds(this.maxDuration),
				Duration.ofSeconds(this.maxWait),
				Path.of(this.resultFolderPath),
				this.mode);
		try {
			debeziumTask.run();
		} catch (Exception e) {
			log.error("{}", e.getMessage(), e);
			System.exit(1);
		}
		log.info("Engine terminated");
	}

	public static void main(String[] args) {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		new CommandLine(new DebeziumKBCWrapper()).execute(args);
	}

	@Getter
	@AllArgsConstructor
	public enum Mode {
		APPEND, DEDUPE;

		public JsonConverter.ConverterProvider converterProvider(Properties debeziumProperties) {
			if (this == APPEND)
				return AppendDbConverter::new;
			if (this == DEDUPE) {
				var isFileOffsetStorage = Objects.equals(debeziumProperties.getProperty("offset.storage"),
						"org.apache.kafka.connect.storage.FileOffsetBackingStore");
				if (isFileOffsetStorage) {
					var offsetStoragePath = debeziumProperties.getProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG);
					try {
						if (Files.size(Path.of(offsetStoragePath)) == 0) {
							return DedubeOnEmptyStateDbConverter::new;
						}
					} catch (IOException ignored) {
					}
				}
				return DedupeDbConverter::new;
			}
			// default converter
			return AppendDbConverter::new;
		}
	}
}
