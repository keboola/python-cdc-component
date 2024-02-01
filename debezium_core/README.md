# Debezium Engine to CSV

Generic Debezium Engine wrapper to collect the latest change events and store them in CSV file. To be used with Keboola
Connection components.

## Configuration

### Properties path

Path to `.properties` file containing Debezium properties related to the selected connector.

**NOTE:** The wrapper injects default properties for internal purposes. These should not be included:

```
(name", "kbc_cdc");
(topic.prefix", "testcdc");
(transforms", "unwrap");
(transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
(transforms.unwrap.drop.tombstones", "true");
(transforms.unwrap.delete.handling.mode", "rewrite");
(transforms.unwrap.add.fields", "table,source.ts_ms");
(transforms.unwrap.add.fields.prefix", "kbc__");
```

**Example content**

```properties
connector.class=io.debezium.connector.postgresql.PostgresConnector
offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
offset.storage.file.filename=/path/to/offsetfile/offsets.dat
offset.flush.interval.ms=0
database.hostname=localhost
database.port=5432
database.user=postgres
database.password=postgres
database.dbname=postgres
database.server.name=tutorial
schema.whitelist=inventory
plugin.name=pgoutput
signal.enabled.channels=file
signal.file=/testing_config/signal-file.jsonl
```


## Enforcing snapshot (blocking)

To enforce a [blocking snapshot](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-blocking-snapshots) 
(e.g. when new table is added to a configuration), make sure you define following properties:

```properties
signal.enabled.channels=file
signal.file=/testing_config/signal-file.jsonl
```

The `signal.file` value is a path to a signal file that contains events to trigger.
To force blocking snapshot for table `public.example_table` add following row:

```json
{"id":"d139b9b7-7777-4547-917d-111111111111", "type":"execute-snapshot", "data":{"type":"BLOCKING", "data-collections": ["public.example_table"]}}
```


## Usage

**Parameters**

```
Usage: <main class> [-md=<maxDuration>] [-mw=<maxWait>]
                    <debeziumPropertiesPath> <resultFolderPath>
      <debeziumPropertiesPath>
                           The debezium properties path
      <resultFolderPath>   The result folder path
      -md, --max-duration=<maxDuration>
                           The maximum duration (s) before engine stops
      -mw, --max-wait=<maxWait>
                           The maximum wait duration(s) for next event before
                             engine stops

```

**Example**:

```shell
java -jar /path_to_jar.jar /path/to/config/application.properties /result/folder -md=3600 -mw=10
```

## Output