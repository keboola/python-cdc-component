package keboola.cdc.debezium;

import keboola.cdc.debezium.converter.AppendDbConverter;
import keboola.cdc.debezium.converter.DedupeDbConverter;
import keboola.cdc.debezium.converter.JsonConverter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.time.Duration;

@Slf4j
public class DebeziumKBCWrapper implements Runnable {

	@Parameters(index = "0", description = "The debezium properties path")
	private String debeziumPropertiesPath;

	@Parameters(index = "1", description = "The result folder path")
	private String resultFolderPath;

	@Option(names = {"-pf", "--properties-file"}, description = "The keboola properties path, if not specified, the default value is used")
	private String keboolaPropertiesPath;

	@Option(names = {"-md", "--max-duration"}, description = "The maximum duration (s) before engine stops")
	private int maxDuration;

	@Option(names = {"-mw", "--max-wait"}, description = "The maximum wait duration(s) for next event before engine stops")
	private int maxWait;
	@Option(names = {"-m", "--mode"}, description = "The maximum wait duration(s) for next event before engine stops", defaultValue = "APPEND")
	private Mode mode;


	@Override
	public void run() {
		log.info("Engine started");

		var debeziumTask = this.keboolaPropertiesPath == null
				? new AbstractDebeziumTask(Path.of(this.debeziumPropertiesPath),
						Duration.ofSeconds(this.maxDuration),
						Duration.ofSeconds(this.maxWait),
						Path.of(this.resultFolderPath),
						this.mode.getConverterProvider())
				: new AbstractDebeziumTask(Path.of(this.debeziumPropertiesPath),
						Path.of(this.keboolaPropertiesPath),
						Duration.ofSeconds(this.maxDuration),
						Duration.ofSeconds(this.maxWait),
						Path.of(this.resultFolderPath),
						this.mode.getConverterProvider());
		try {
			debeziumTask.run();
		} catch (Exception e) {
			log.error("{}", e.getMessage(), e);
			System.exit(1);
		}
		log.info("Engine terminated");
	}


	public static void main(String[] args) {
		new CommandLine(new DebeziumKBCWrapper()).execute(args);
	}

	@Getter
	@AllArgsConstructor
	private enum Mode {
		APPEND(AppendDbConverter::new),
		DEDUPE(DedupeDbConverter::new);
		private final JsonConverter.ConverterProvider converterProvider;
	}
}
