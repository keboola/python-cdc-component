package keboola.cdc.debezium;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.time.Duration;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
@Slf4j
public class DebeziumKBCWrapper implements Runnable {

    @Parameters(index = "0", description = "The debezium properties path")
    private String debeziumPropertiesPath;

    @Parameters(index = "1", description = "The result folder path")
    private String resultFolderPath;

    @Option(names = {"-md", "--max-duration"}, description = "The maximum duration (s) before engine stops")
    private int maxDuration;

    @Option(names = {"-mw", "--max-wait"}, description = "The maximum wait duration(s) for next event before engine stops")
    private int maxWait;


    @Override
    public void run() {
        log.info("Engine started");
        var debeziumTask = new AbstractDebeziumTask(Path.of(this.debeziumPropertiesPath), log,
                Duration.ofSeconds(this.maxDuration),
                Duration.ofSeconds(this.maxWait),
                Path.of(this.resultFolderPath));
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
}
