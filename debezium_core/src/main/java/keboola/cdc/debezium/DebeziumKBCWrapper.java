package keboola.cdc.debezium;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;

import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class DebeziumKBCWrapper implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumKBCWrapper.class);

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
        LOG.info("Engine started");
        var debeziumtask = new AbstractDebeziumTask(Path.of(this.debeziumPropertiesPath), LOG,
                Duration.ofSeconds(this.maxDuration),
                Duration.ofSeconds(this.maxWait),
                Path.of(this.resultFolderPath));
        try {
            debeziumtask.run();
        } catch (Exception e) {
//            e.printStackTrace();
            LOG.error("{}", e.getMessage(), e);
            System.exit(1);
        }
        LOG.info("Engine terminated");
    }


    public static void main(String[] args) {
        new CommandLine(new DebeziumKBCWrapper()).execute(args);
    }
}
